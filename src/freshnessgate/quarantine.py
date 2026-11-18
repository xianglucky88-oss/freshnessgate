"""单一职责：隔离判定与释放。

判定规则保持纯函数化（should_quarantine），状态持久化交给 store。
保留旧流水线入口 process(record)，新流程使用 hold / release_quarantine。
"""
from . import store
from .models import Record
from .source import TOMBSTONE


def should_quarantine(record):
    """返回隔离原因字符串；无需隔离返回 None。tombstone 不参与隔离判定。"""
    if record.get("op") == TOMBSTONE:
        return None
    if int(record.get("value", 0)) < 0:
        return "negative_value"
    if not str(record.get("name", "")).strip():
        return "empty_name"
    if str(record.get("status", "")).strip().lower() == "invalid":
        return "invalid_status"
    return None


def hold(con, key, record, reason, at):
    store.put_quarantine(con, key, record, reason, "held", at)


def release_quarantine(con, key, at):
    """尝试释放一条 held 记录。

    返回 (state, reason)：
      ("released", None)  —— 复核通过，已写入正式记录；
      ("held", reason)    —— 复核仍不通过，继续隔离（幂等，可日后再放）。
    """
    item = store.get_quarantine(con, key)
    if item is None:
        return ("absent", None)
    if item["state"] != "held":
        return (item["state"], item["reason"])
    reason = should_quarantine(item["data"])
    if reason is not None:
        store.put_quarantine(con, key, item["data"], reason, "held", at)
        return ("held", reason)
    store.put_quarantine(con, key, item["data"], item["reason"], "released", at)
    store.upsert(con, Record(key=key, data=item["data"]))
    store.release_ledger(con, key)
    return ("released", None)


def process(record):
    """旧流水线兼容入口：只打阶段标记，不改变数据流。"""
    rec = dict(record)
    rec.setdefault("_stage_quarantine", "naive")
    return rec
