"""单一职责：根据对账差异决定是否升级告警，并留下发送台账。

升级策略（幂等、不反复升级已解释异常）：
- 只有 reconcile.ESCALATING_REASONS（真正丢失 missing、落库侧意外 unexpected）
  才构成故障；
- not_fetched / quarantined / duplicate / late 均为已解释差异，永不告警；
- 同一差异指纹只在首次出现（finding 状态 new 且历史未 escalated）时发送一次；
  重跑时指纹稳定命中旧记录，不会被当作新故障再次升级。
保留旧流水线入口 process(record)。
"""
from . import reconcile, store


def should_escalate(reason, is_new, finding_row):
    if reason not in reconcile.ESCALATING_REASONS:
        return False
    if finding_row is not None and finding_row.get("escalated"):
        return False          # 历史已升级过，重跑不重复告警
    return bool(is_new)       # 仅新故障首次升级


def send(con, finding, at=None, run_id=None):
    """记录一条告警（离线环境不联网，落 alert_log 表即可被运维/测试消费）。"""
    import time
    at = at if at is not None else time.time()
    store.log_alert(con, finding["fingerprint"], finding["reason"],
                    finding.get("key"), at, run_id or finding.get("run_id", "recon"))
    return {"fingerprint": finding["fingerprint"], "reason": finding["reason"],
            "key": finding.get("key"), "detail": finding.get("detail", {})}


def process(record):
    """旧流水线兼容入口：只打阶段标记，不改变数据流。"""
    rec = dict(record)
    rec.setdefault("_stage_alert", "ok")
    return rec
