"""单一职责：坏记录隔离与诊断样本回放。

设计要点：

* **失败关闭**：本模块只接收 ``decision == "block"`` 的记录；调用方在隔离
  成功之前不得写入正式结果。
* **双层持久化**：
  1. 只追加日志（JSONL + ``fsync``）——第一落点，保证即使 SQLite 隔离库
     写入失败，诊断样本仍然存在且可回放；
  2. SQLite 隔离库——结构化查询入口，唯一键 ``(key, raw_digest)`` 保证重跑
     幂等，且与正式结果库物理分离。
* **可回放**：样本同时保存原始记录与完整 schema 契约（规则版本、原始摘要、
  逐字段结果），``replay()`` 可用当前规则重新判定并校验摘要未漂移。

正式结果库绝不接收坏记录；坏记录的原始值只存在于隔离区。
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from . import config as _config
from . import schema_check

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS quarantined (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    raw_digest TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    schema_version TEXT,
    source_format TEXT,
    decision TEXT NOT NULL,
    stage TEXT NOT NULL,
    raw TEXT NOT NULL,
    report TEXT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE(key, raw_digest)
)
"""


class IsolationError(RuntimeError):
    """隔离样本无法持久化——调用方必须中止本次运行，不得落正式结果。"""


def connect(path=None) -> sqlite3.Connection:
    path = Path(path or _config.QUARANTINE_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute(_TABLE_SQL)
    con.commit()
    return con


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sample(raw: Any, report: dict[str, Any], key: str, stage: str) -> dict[str, Any]:
    now = _now()
    return {
        "key": str(key),
        "raw_digest": report.get("raw_digest") or schema_check.digest_of(raw),
        "rule_version": report.get("rule_version") or schema_check.RULE_VERSION,
        "schema_version": report.get("schema_version"),
        "source_format": report.get("format"),
        "decision": report.get("decision", "block"),
        "stage": stage,
        "raw": raw,
        "report": report,
        "received_at": now,
    }


def _append_journal(sample: dict[str, Any], path) -> str:
    path = Path(path or _config.QUARANTINE_JOURNAL_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(sample, ensure_ascii=False, sort_keys=True, default=str)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    return str(path)


def _insert(con: sqlite3.Connection, sample: dict[str, Any]) -> None:
    con.execute(
        "INSERT INTO quarantined(key,raw_digest,rule_version,schema_version,"
        "source_format,decision,stage,raw,report,received_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(key,raw_digest) DO UPDATE SET "
        "decision=excluded.decision, stage=excluded.stage, "
        "report=excluded.report, received_at=excluded.received_at",
        (
            sample["key"],
            sample["raw_digest"],
            sample["rule_version"],
            sample["schema_version"],
            sample["source_format"],
            sample["decision"],
            sample["stage"],
            json.dumps(sample["raw"], ensure_ascii=False, sort_keys=True, default=str),
            json.dumps(sample["report"], ensure_ascii=False, sort_keys=True, default=str),
            sample["received_at"],
        ),
    )


def isolate(raw: Any, report: dict[str, Any], key: str, *, stage: str = "schema_check",
            con: sqlite3.Connection | None = None, journal_path=None) -> dict[str, Any]:
    """保存一条坏记录及其诊断契约。

    返回回执：``durable`` 表示至少一条落点成功。``journal`` 落点失败会直接
    抛 :class:`IsolationError`（因为那意味着诊断样本可能永久丢失）；SQLite
    失败但日志成功时 ``durable`` 仍为 ``True``，并带 ``db_error`` 供运维重放
    ``recover_from_journal()``。
    """
    sample = _sample(raw, report, key, stage)

    if sample["decision"] != "block":
        raise ValueError("quarantine.isolate 只接受 decision=block 的记录")

    try:
        journal = _append_journal(sample, journal_path)  # 第一落点，失败即中止
    except Exception as exc:
        # 连只追加日志都写不了 → 诊断样本会永久丢失，必须中止整次运行。
        raise IsolationError(
            f"写隔离日志失败，诊断样本无法保留：{type(exc).__name__}: {exc}") from exc

    receipt: dict[str, Any] = {
        "key": sample["key"],
        "raw_digest": sample["raw_digest"],
        "journal": journal,
        "durable": True,
        "db_error": None,
    }

    own = con is None
    try:
        if con is None:
            con = connect()
        _insert(con, sample)
        con.commit()
        receipt["persistence"] = "journal+sqlite"
    except Exception as exc:  # SQLite 不可用时仍需保留日志样本
        try:
            if con is not None:
                con.rollback()
        except Exception:
            pass
        receipt["persistence"] = "journal"
        receipt["db_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if own and con is not None:
            con.close()

    return receipt


def recover_from_journal(journal_path=None, con: sqlite3.Connection | None = None) -> int:
    """从只追加日志重建 SQLite 隔离库（隔离库写入失败后的恢复路径）。

    返回本次写入的样本数；重跑幂等（按 key+raw_digest 覆盖）。
    """
    path = Path(journal_path or _config.QUARANTINE_JOURNAL_PATH)
    if not path.exists():
        return 0

    own = con is None
    con = con or connect()
    n = 0
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            sample = json.loads(line)
            _insert(con, sample)
            n += 1
        con.commit()
        return n
    finally:
        if own:
            con.close()


def samples(*, con: sqlite3.Connection | None = None, journal_path=None) -> list[dict[str, Any]]:
    """读取隔离样本（SQLite + 日志并集，按 key+raw_digest 去重）。

    SQLite 不可用时只返回日志样本，保证诊断始终可回放。
    """
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    path = Path(journal_path or _config.QUARANTINE_JOURNAL_PATH)
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            sample = json.loads(line)
            merged[(sample["key"], sample["raw_digest"])] = sample

    own = False
    try:
        if con is None:
            con = connect()
            own = True
        rows = con.execute(
            "SELECT key,raw_digest,rule_version,schema_version,source_format,"
            "decision,stage,raw,report,received_at FROM quarantined"
        )
        for row in rows:
            sample = {
                "key": row[0],
                "raw_digest": row[1],
                "rule_version": row[2],
                "schema_version": row[3],
                "source_format": row[4],
                "decision": row[5],
                "stage": row[6],
                "raw": json.loads(row[7]),
                "report": json.loads(row[8]),
                "received_at": row[9],
            }
            merged[(sample["key"], sample["raw_digest"])] = sample
    except sqlite3.Error:
        pass
    finally:
        if own and con is not None:
            con.close()

    return list(merged.values())


def replay(sample: dict[str, Any]) -> dict[str, Any]:
    """用当前规则重放一份诊断样本，返回可比较的回放结果。

    ``digest_ok`` 校验样本原始值没有被篡改；``still_blocked`` 表示当前规则
    仍然会拦截该记录。回放是只读的，不会写入正式结果。
    """
    recomputed = schema_check.digest_of(sample["raw"])
    _, report = schema_check.evaluate(sample["raw"])
    return {
        "key": sample.get("key"),
        "sample_digest": sample.get("raw_digest"),
        "recomputed_digest": recomputed,
        "digest_ok": recomputed == sample.get("raw_digest"),
        "original_rule_version": sample.get("rule_version"),
        "current_rule_version": schema_check.RULE_VERSION,
        "original_decision": sample.get("decision"),
        "current_decision": report["decision"],
        "current_report": report,
        "still_blocked": report["decision"] == "block",
    }


def replay_all(**kwargs) -> list[dict[str, Any]]:
    return [replay(s) for s in samples(**kwargs)]


def count(con: sqlite3.Connection) -> int:
    return con.execute("SELECT COUNT(*) FROM quarantined").fetchone()[0]


def process(record):
    """向后兼容的朴素阶段（历史流水线使用）。新流水线通过 ``isolate`` 隔离。"""
    rec = dict(record)
    rec.setdefault("_stage_quarantine", "naive")
    return rec
