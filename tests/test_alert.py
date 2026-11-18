"""alert 模块测试：指纹去重、聚合/抑制/升级/确认/恢复、通知失败重试与双向追溯。

以 pytest 风格编写；在无 pytest 的离线环境中可用标准库 runner 执行同一份用例。
"""
import os
import sqlite3
import sys
import tempfile
from datetime import timedelta

import pytest

from src.freshnessgate import alert
from src.freshnessgate.alert import (
    ACTIVE, ACKED, AlertManager, EXHAUSTED, FAILED, FINDINGS_KEY,
    NotificationError, RECOVERED, SENT, SUPPRESSED, fingerprint_of, process,
)


# ---------------------------------------------------------------- 测试替身
class FakeClock:
    """可控时钟，替代真实 UTC 时间。"""

    def __init__(self, start):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t = self.t + timedelta(seconds=seconds)


class FakeNotifier:
    """前 fail_times 次抛 NotificationError（模拟离线发送失败），之后成功。"""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times
        self.calls = 0
        self.delivered = []

    def __call__(self, alert_row, note):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise NotificationError("offline: webhook unreachable")
        self.delivered.append((alert_row["fp"], note["kind"]))


def make_manager(notifier=None, clock=None, **kw):
    con = sqlite3.connect(":memory:")
    return AlertManager(con, notifier=notifier or FakeNotifier(),
                        clock=clock or FakeClock(alert.now_utc()), **kw)


def finding(src="alpha", rule="freshness", event_key="r-001", *,
            source="quarantine", source_ref="q-1", batch="b1",
            kind=alert.KIND_ANOMALY, detail=None):
    return {
        "src": src, "rule": rule, "event_key": event_key,
        "source": source, "source_ref": source_ref, "batch": batch,
        "kind": kind, "detail": detail or {"metric": "lag_seconds"},
    }


# ---------------------------------------------------------------- 流水线契约
def test_process_is_passthrough_on_normal_chain():
    rec = {"id": "r-001", "src": "alpha"}
    out = process(rec)
    assert out["_stage_alert"] == "ok"
    assert rec == {"id": "r-001", "src": "alpha"}  # 原记录不被修改
    assert out is not rec


def test_extract_findings_seam():
    f = finding()
    assert alert.extract_findings({}) == []
    assert alert.extract_findings({FINDINGS_KEY: f}) == [f]
    assert alert.extract_findings({FINDINGS_KEY: [f, f]}) == [f, f]


# ---------------------------------------------------------------- 指纹去重
def test_fingerprint_stable_and_distinguishes_events():
    fp1 = fingerprint_of("alpha", "freshness", "r-001")
    fp2 = fingerprint_of("alpha", "freshness", "r-001")
    fp3 = fingerprint_of("alpha", "freshness", "r-002")
    fp4 = fingerprint_of("beta", "freshness", "r-001")
    assert fp1 == fp2 and len(fp1) == 64
    assert fp1 != fp3 != fp4


def test_same_fault_single_alert_across_quarantine_reconcile_and_reruns():
    mgr = make_manager()
    r1 = mgr.ingest_finding(finding(source="quarantine", source_ref="q-1"))
    # reconcile 对同一故障（同 src/rule/event_key）也上报一次
    r2 = mgr.ingest_finding(finding(source="reconcile", source_ref="rc-9"))
    # 重跑：完全相同的来源/批次/引用 -> 幂等重复，不新增观察
    r3 = mgr.ingest_finding(finding(source="quarantine", source_ref="q-1"))
    r4 = mgr.ingest_finding(finding(source="reconcile", source_ref="rc-9"))

    assert r1["new"] is True
    assert r2["new"] is False and r2["replay"] is False
    assert r3["replay"] is True and r4["replay"] is True

    alerts = mgr.list_alerts()
    assert len(alerts) == 1                      # 始终只有一条持续演变的告警
    a = alerts[0]
    assert a["occurrence_count"] == 2           # 重跑不无限计数
    assert a["status"] == ACTIVE
    # 只发过一次 open 通知，重跑不制造新通知
    notes = mgr.list_notifications(alert_fp=a["fp"])
    assert len(notes) == 1 and notes[0]["kind"] == "open"
    # 两个来源的上报都挂在同一条告警下
    sources = {f["source"] for f in mgr.list_findings(alert_fp=a["fp"])}
    assert sources == {"quarantine", "reconcile"}


def test_invalid_finding_raises():
    mgr = make_manager()
    with pytest.raises(ValueError):
        mgr.ingest_finding({"src": "alpha", "rule": ""})


# ---------------------------------------------------------------- 通知失败重试
def test_notification_failure_retried_then_confirmed():
    clock = FakeClock(alert.now_utc())
    notifier = FakeNotifier(fail_times=1)
    mgr = make_manager(notifier=notifier, clock=clock)

    mgr.ingest_finding(finding())
    note = mgr.list_notifications()[0]
    assert note["status"] == FAILED              # 离线失败可观测
    assert note["fail_reason"] == "offline: webhook unreachable"
    assert note["attempt"] == 2                  # 下次将是第 2 次尝试

    clock.advance(29)                            # 退避未到不重试
    assert mgr.pump()["retried"] == 0
    clock.advance(2)                             # 30s 退避到期
    assert mgr.pump()["retried"] == 1
    note = mgr.list_notifications()[0]
    assert note["status"] == SENT and note["sent_at"] is not None
    assert len(notifier.delivered) == 1


def test_notification_exhausted_after_max_attempts():
    clock = FakeClock(alert.now_utc())
    mgr = make_manager(notifier=FakeNotifier(fail_times=99), clock=clock,
                       max_attempts=3, backoff_base=1)
    mgr.ingest_finding(finding())
    for _ in range(3):
        clock.advance(10)
        mgr.pump()
    note = mgr.list_notifications()[0]
    assert note["status"] == EXHAUSTED
    assert note["attempt"] == 3 and note["fail_reason"]
    assert mgr.stats()["failed_notifications"] == 0  # 已终止，不再计入待重试


# ---------------------------------------------------------------- 聚合窗口
def test_aggregation_window_collapses_repeats_then_reminds():
    clock = FakeClock(alert.now_utc())
    mgr = make_manager(clock=clock, aggregate_window=300)
    mgr.ingest_finding(finding(batch="b1"))
    # 窗口内新批次再次上报同一故障：只计数，不新增通知
    mgr.ingest_finding(finding(batch="b2", source_ref="q-2"))
    assert len(mgr.list_notifications()) == 1
    assert mgr.pump()["reminded"] == 0           # 窗口未过不补发

    clock.advance(301)
    out = mgr.pump()
    assert out["reminded"] == 1
    kinds = [n["kind"] for n in mgr.list_notifications()]
    assert kinds == ["open", "aggregate"]


# ---------------------------------------------------------------- 抑制
def test_rule_suppression_blocks_notifications_and_expires():
    clock = FakeClock(alert.now_utc())
    until = clock() + timedelta(seconds=60)
    mgr = make_manager(
        clock=clock,
        suppressions=[{"src": "alpha", "rule": "freshness", "until": until}],
    )
    mgr.ingest_finding(finding())
    a = mgr.list_alerts()[0]
    assert a["status"] == SUPPRESSED
    assert mgr.list_notifications() == []        # 抑制期内零通知

    clock.advance(30)
    assert mgr.pump() == {"retried": 0, "escalated": 0,
                          "reminded": 0, "unsuppressed": 0}
    clock.advance(31)                            # 抑制到期
    out = mgr.pump()
    assert out["unsuppressed"] == 1
    assert mgr.get_alert(a["fp"])["status"] == ACTIVE
    assert len(mgr.list_notifications()) == 1    # 解除后才允许通知


def test_manual_suppress_cancels_pending_and_holds_off_escalation():
    clock = FakeClock(alert.now_utc())
    mgr = make_manager(notifier=FakeNotifier(fail_times=99), clock=clock,
                       escalation_intervals=(10, 20))
    mgr.ingest_finding(finding())
    mgr.suppress(mgr.list_alerts()[0]["fp"], clock() + timedelta(seconds=100))
    # 待重试通知被取消
    assert {n["status"] for n in mgr.list_notifications()} == {"cancelled"}
    clock.advance(50)                            # 已超升级时限
    assert mgr.pump()["escalated"] == 0          # 抑制优先于升级
    assert mgr.list_alerts()[0]["severity"] == "warning"


# ---------------------------------------------------------------- 升级与确认
def test_escalation_then_acknowledge_stops_further_escalation():
    clock = FakeClock(alert.now_utc())
    mgr = make_manager(clock=clock, escalation_intervals=(10, 20))
    mgr.ingest_finding(finding())
    fp = mgr.list_alerts()[0]["fp"]

    clock.advance(10)
    assert mgr.pump()["escalated"] == 1
    assert mgr.get_alert(fp)["severity"] == "major"
    clock.advance(10)                            # age = 20
    assert mgr.pump()["escalated"] == 1
    assert mgr.get_alert(fp)["severity"] == "critical"
    assert mgr.get_alert(fp)["escalations"] == 2

    mgr.acknowledge(fp)
    assert mgr.get_alert(fp)["status"] == ACKED
    # 确认后迟到异常：只计数，不重开、不升级、不通知
    clock.advance(30)
    r = mgr.ingest_finding(finding(batch="b-late", source_ref="q-late"))
    assert r["status"] == ACKED and r["reopened"] is False
    mgr.pump()
    a = mgr.get_alert(fp)
    assert a["severity"] == "critical" and a["escalations"] == 2
    # open + 2 次升级通知，确认后无新增
    assert len(mgr.list_notifications(alert_fp=fp)) == 3


def test_acknowledge_recovered_alert_raises():
    mgr = make_manager()
    mgr.ingest_finding(finding())
    fp = mgr.list_alerts()[0]["fp"]
    mgr.mark_recovered(fp)
    with pytest.raises(ValueError):
        mgr.acknowledge(fp)


# ---------------------------------------------------------------- 恢复与重放
def test_recovery_cancels_pending_and_old_batch_replay_stays_closed():
    clock = FakeClock(alert.now_utc())
    mgr = make_manager(notifier=FakeNotifier(fail_times=99), clock=clock)
    mgr.ingest_finding(finding(batch="b1"))
    fp = mgr.list_alerts()[0]["fp"]
    assert mgr.list_notifications()[0]["status"] == FAILED

    recovered = mgr.close_batch("b2")            # b2 批次故障消失 -> 自动恢复
    assert recovered == [fp]
    a = mgr.get_alert(fp)
    assert a["status"] == RECOVERED and a["recovered_at"]
    assert {n["status"] for n in mgr.list_notifications()} == {"cancelled"}

    # 旧批次 b1 重放：留痕但不重开、不通知
    r = mgr.ingest_finding(finding(batch="b1"))
    assert r["replay"] is True
    assert mgr.get_alert(fp)["status"] == RECOVERED
    assert mgr.list_notifications(alert_fp=fp)  # 仍只有被取消的那条

    # 新批次 b3 再次出现同一故障：重开原告警，升级计数重置
    r2 = mgr.ingest_finding(finding(batch="b3", source_ref="q-3"))
    assert r2["reopened"] is True
    a2 = mgr.get_alert(fp)
    assert a2["status"] == ACTIVE and a2["escalations"] == 0
    assert [n["kind"] for n in mgr.list_notifications(alert_fp=fp)
            if n["status"] != "cancelled"] == ["reopen"]


def test_recovery_finding_from_reconcile():
    mgr = make_manager()
    mgr.ingest_finding(finding())
    fp = mgr.list_alerts()[0]["fp"]
    r = mgr.ingest_finding(finding(kind=alert.KIND_RECOVERY,
                                   source="reconcile", source_ref="rc-ok",
                                   batch="b2"))
    assert r["status"] == RECOVERED
    assert mgr.get_alert(fp)["status"] == RECOVERED


# ---------------------------------------------------------------- 双向追溯
def test_traceback_both_directions_and_persistence():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    try:
        con1 = sqlite3.connect(tmp.name)
        mgr1 = AlertManager(con1)
        f1 = finding(source="quarantine", source_ref="q-1", batch="b1")
        mgr1.ingest_finding(f1)
        mgr1.ingest_finding(finding(source="reconcile", source_ref="rc-9",
                                    batch="b1"))
        fp = fingerprint_of("alpha", "freshness", "r-001")
        # 从来源上报反查告警
        back = mgr1.find_alert_by_finding("reconcile", "rc-9", "b1")
        assert back and back["fp"] == fp
        con1.commit()
        con1.close()

        # 新连接重开同一库：状态持久，重跑仍不新增告警
        con2 = sqlite3.connect(tmp.name)
        mgr2 = AlertManager(con2)
        r = mgr2.ingest_finding(finding(source="quarantine",
                                        source_ref="q-1", batch="b1"))
        assert r["replay"] is True
        assert len(mgr2.list_alerts()) == 1

        trace = mgr2.traceback(fp)              # 沿来源回查完整演变链
        assert {f["source"] for f in trace["findings"]} == {
            "quarantine", "reconcile"}
        events = {row["event"] for row in trace["audit"]}
        assert "open" in events and "repeat" in events
        assert trace["notifications"] and trace["alert"]["fp"] == fp
        con2.close()
    finally:
        os.unlink(tmp.name)


def test_stats_observability():
    mgr = make_manager()
    mgr.ingest_finding(finding())
    mgr.ingest_finding(finding(src="beta", event_key="r-002",
                               source_ref="q-2"))
    stats = mgr.stats()
    assert stats["alerts"] == 2 and stats["active"] == 2
    assert stats["findings"] == 2 and stats["notifications"] == 2
