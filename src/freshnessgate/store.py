"""单一职责：本地 sqlite3 持久化，提供幂等 upsert / 计数 / 全量读取。

正式结果表保持旧格式 ``records(key, data)`` 不变；质量门契约以追加列的方式
落库（``schema_version / rule_version / raw_digest / schema_report / decision /
source_format``），因此旧消费方无需改动即可继续读取，新消费方可审计每条正式
记录是由哪个规则版本、哪份原始样本摘要放行的。
"""
import json
import sqlite3

from . import config as _config

#: 追加的契约列（列名 -> SQLite 类型）。旧库通过 ALTER TABLE 平滑迁移。
CONTRACT_COLUMNS: dict[str, str] = {
    "schema_version": "TEXT",
    "rule_version": "TEXT",
    "raw_digest": "TEXT",
    "schema_report": "TEXT",
    "decision": "TEXT",
    "source_format": "TEXT",
}

_UPSERT_SQL = (
    "INSERT INTO records(key,data,schema_version,rule_version,raw_digest,"
    "schema_report,decision,source_format) VALUES(?,?,?,?,?,?,?,?) "
    "ON CONFLICT(key) DO UPDATE SET data=excluded.data, "
    "schema_version=excluded.schema_version, rule_version=excluded.rule_version, "
    "raw_digest=excluded.raw_digest, schema_report=excluded.schema_report, "
    "decision=excluded.decision, source_format=excluded.source_format"
)


def _migrate(con):
    """为旧库追加契约列；已存在则跳过（兼容旧格式路径）。"""
    existing = {row[1] for row in con.execute("PRAGMA table_info(records)")}
    for name, sql_type in CONTRACT_COLUMNS.items():
        if name not in existing:
            con.execute(f"ALTER TABLE records ADD COLUMN {name} {sql_type}")


def connect(path=None):
    con = sqlite3.connect(path or _config.DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    _migrate(con)
    return con


def _report_json(report):
    if report is None:
        return None
    return json.dumps(report, ensure_ascii=False, sort_keys=True)


def upsert(con, record):
    """幂等写入。``record`` 可以是旧的 ``Record(key, data)``，契约字段允许为空。"""
    con.execute(
        _UPSERT_SQL,
        (
            record.key,
            json.dumps(record.data, ensure_ascii=False),
            getattr(record, "schema_version", None),
            getattr(record, "rule_version", None),
            getattr(record, "raw_digest", None),
            _report_json(getattr(record, "schema_report", None)),
            getattr(record, "decision", None),
            getattr(record, "source_format", None),
        ),
    )


def commit(con):
    con.commit()


def rollback(con):
    con.rollback()


def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    """旧格式读取路径：返回 ``(key, data)``，形状与基线一致。"""
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]


def contract_of(con, key):
    """读取单条正式记录的完整契约。"""
    row = con.execute(
        "SELECT key,data,schema_version,rule_version,raw_digest,schema_report,"
        "decision,source_format FROM records WHERE key=?", (key,)
    ).fetchone()
    if row is None:
        return None
    return {
        "key": row[0],
        "data": json.loads(row[1]),
        "schema_version": row[2],
        "rule_version": row[3],
        "raw_digest": row[4],
        "schema_report": json.loads(row[5]) if row[5] else None,
        "decision": row[6],
        "source_format": row[7],
    }


def all_contracts(con):
    return [contract_of(con, key) for (key,) in con.execute("SELECT key FROM records")]
