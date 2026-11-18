"""薄编排：loader → schema_check(质量门) → 其余阶段 → 坏记录隔离 → keyer → store。

编排层只负责顺序与失败路由，业务判定全部在 ``schema_check`` / ``quarantine``：

1. 每条原始记录先过质量入口；``decision == "block"`` 的记录 **不会** 进入
   后续阶段，也不会写入正式结果库，而是交给隔离区保存“原始值 + 完整契约”。
2. 其余阶段（normalizer/freshness/reconcile/alert）抛异常时同样 fail-closed，
   按 ``internal_error`` 隔离，避免半成品写进正式结果。
3. 隔离失败（诊断样本无法持久化）会回滚并中止本次运行；重启后处理仍然只会
   得到同样的隔离结果，正式结果库中不会出现坏记录。
4. 每次运行开始都会尝试从只追加日志恢复隔离库（幂等），覆盖“隔离库写入失败
   后重启”的场景。
"""
import importlib

from . import loader as _loader
from . import quarantine as _quarantine
from . import schema_check
from . import store as _store
from .keyer import key_of
from .models import Record

#: 质量门之后仍按顺序执行的变换。quarantine 的隔离动作由编排层显式调用，
#: 因此不再列入链中（保持旧模块的 process() 仍可单独使用）。
CHAIN_NAMES = ['normalizer', 'freshness', 'reconcile', 'alert']

#: 写入正式结果前必须从记录里剥离的校验契约键（契约改走 store 的追加列）。
_CONTRACT_KEY = "_schema"


def _load_chain():
    chain = []
    for n in CHAIN_NAMES:
        mod = importlib.import_module("." + n, __package__)
        chain.append((n, mod.process))
    return chain


def _record_from(rec, report):
    """正式记录的键沿用基线行为：由处理后的业务数据派生（契约键不参与）。"""
    data = dict(rec)
    data.pop(_CONTRACT_KEY, None)
    return Record(
        key=key_of(data),
        data=data,
        schema_version=report.get("schema_version"),
        rule_version=report.get("rule_version"),
        raw_digest=report.get("raw_digest"),
        schema_report=report,
        decision=report.get("decision"),
        source_format=report.get("format"),
    )


def run(con=None, quarantine_con=None, journal_path=None, fixtures_path=None):
    """跑一遍采集流水线；返回统计。坏记录只进隔离区，不进正式结果。"""
    own = con is None
    con = con or _store.connect()
    own_q = quarantine_con is None
    qcon = quarantine_con
    if qcon is None:
        try:
            qcon = _quarantine.connect()
        except Exception:
            qcon = None  # 隔离库不可用时仍有只追加日志兜底

    stats = {
        "fed": 0,
        "rows": 0,
        "persisted": 0,
        "quarantined": 0,
        "repaired": 0,
        "blocked_keys": [],
        "receipts": [],
        "isolation_failures": [],
        "recovery": {"recovered": 0, "error": None},
    }

    try:
        # 重启场景：日志是诊断样本的第一落点，先把未入库样本补进隔离库。
        try:
            stats["recovery"]["recovered"] = _quarantine.recover_from_journal(
                journal_path, con=qcon)
        except Exception as exc:
            stats["recovery"]["error"] = f"{type(exc).__name__}: {exc}"

        chain = _load_chain()

        for raw in _loader.load(fixtures_path):
            stats["fed"] += 1
            key = key_of(raw)

            try:
                rec, report = schema_check.evaluate(raw)
            except Exception as exc:  # 质量门自身异常也必须 fail-closed
                report = schema_check.error_report(raw, exc)
                rec = raw

            if report["blocked"]:
                try:
                    receipt = _quarantine.isolate(
                        raw, report, key, stage="schema_check",
                        con=qcon, journal_path=journal_path)
                except Exception as exc:
                    # 诊断样本都保不住时中止整次运行，绝不写正式结果。
                    con.rollback()
                    raise _quarantine.IsolationError(
                        f"无法隔离坏记录 {key!r}：{type(exc).__name__}: {exc}") from exc
                if receipt.get("db_error"):
                    stats["isolation_failures"].append(receipt)
                stats["receipts"].append(receipt)
                stats["blocked_keys"].append(key)
                stats["quarantined"] += 1
                continue

            try:
                for name, fn in chain:
                    rec = fn(rec)
            except Exception as exc:
                blocked = schema_check.error_report(raw, exc, stage=name)
                try:
                    receipt = _quarantine.isolate(
                        raw, blocked, key, stage=name,
                        con=qcon, journal_path=journal_path)
                except Exception as iso_exc:
                    con.rollback()
                    raise _quarantine.IsolationError(
                        f"无法隔离 {name} 失败记录 {key!r}："
                        f"{type(iso_exc).__name__}: {iso_exc}") from iso_exc
                if receipt.get("db_error"):
                    stats["isolation_failures"].append(receipt)
                stats["receipts"].append(receipt)
                stats["blocked_keys"].append(key)
                stats["quarantined"] += 1
                continue

            _store.upsert(con, _record_from(rec, report))
            stats["persisted"] += 1
            if report["decision"] == "repair":
                stats["repaired"] += 1

        _store.commit(con)
        stats["rows"] = _store.count(con)
        return stats
    except Exception:
        # 任一失败路径都先回滚：正式结果库不能留下“半进步”状态。
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        if own:
            con.close()
        if own_q and qcon is not None:
            qcon.close()
