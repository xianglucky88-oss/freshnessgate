"""离线构造季节变化 / 突发空值 / 恢复补采等场景，验证 anomaly 判定、落库与告警去重。

覆盖：
* 冷启动无基线只积累不告警；
* 周末季节性低量不被误判、工作日真实突降能检出；
* 突发空值 / 数值离群 / 重复率 / 时间延迟的真实异常检出与可解释证据；
* 短暂故障补采恢复 → 告警解析、窗口回改 transient_fault；
* 宽限期后未恢复 → 告警确认真实异常；
* 不同分批结果一致、重启后状态延续、重复运行幂等且同一异常不重复告警。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from src.freshnessgate import anomaly, store
from src.freshnessgate.anomaly import (
    AnomalyDetector, NO_BASELINE, SEASONAL, TRANSIENT_FAULT, ANOMALY,
    compute_metrics, day_window, parse_ts,
)

SRC = "alpha"


# ---------------------------------------------------------------- 夹具与构造器
def obs_at(day):
    """每个窗口独立的观测时刻：事件日次日中午，使健康数据延迟稳定在 ~1.5 天。"""
    return datetime(2026, 11, day, 12, 0, tzinfo=timezone.utc) + timedelta(days=1)


def rec(i, day, *, value=10, name="n", status="active", ts=None, src=SRC):
    return {
        "id": f"{src}-{day}-{i}", "src": src, "name": name, "value": value,
        "status": status,
        "ts": ts or f"2026-11-{day:02d}T00:00:00Z",
    }


def healthy(day, n=10, **kw):
    return [rec(f"{day}-{i}", day, value=10 + (i % 3), **kw) for i in range(n)]


def mondays():
    """2026-11 的周一：2/9/16/23/30。"""
    out, d = [], datetime(2026, 11, 2, tzinfo=timezone.utc)
    while d.month == 11:
        out.append(d.day)
        d += timedelta(days=7)
    return out


def saturdays():
    out, d = [], datetime(2026, 11, 7, tzinfo=timezone.utc)
    while d.month == 11:
        out.append(d.day)
        d += timedelta(days=7)
    return out


@pytest.fixture
def det():
    return AnomalyDetector()  # 内存状态


@pytest.fixture
def sqldet(tmp_path):
    con = store.connect(tmp_path / "anomaly_test.db")
    yield AnomalyDetector(con)
    con.close()


# ---------------------------------------------------------------- 纯函数
def test_parse_ts_and_day_window():
    assert parse_ts("2026-11-02T00:00:00Z").day == 2
    assert parse_ts("not-a-date") is None
    assert parse_ts(None) is None
    assert day_window("2026-11-02T08:30:00Z") == "2026-11-02"


def test_compute_metrics_null_dup_outlier_delay():
    obs = obs_at(1)
    records = [
        rec(1, 1, value=10), rec(1, 1, value=10),          # 完全重复行
        rec(2, 1, value=11, name=""),                       # 空值
        rec(3, 1, value=12), rec(4, 1, value=999999),       # 离群
    ]
    m, keys = compute_metrics(records, observed_at=obs)
    assert m["total"] == 5 and m["unique"] == 4
    assert m["duplicate_rate"] == pytest.approx(0.2)
    assert m["null_ratio"] == pytest.approx(1 / 20)
    assert m["outlier_count"] == 1 and m["outlier_values"] == [999999.0]
    assert m["delay_median_s"] > 0 and m["delay_max_s"] >= m["delay_median_s"]
    assert len(keys) == 5
    # 重放：窗口台账命中后全部跳过 → 幂等
    m2, _ = compute_metrics(records, observed_at=obs, prior_seen=set(keys))
    assert m2["total"] == 0


# ---------------------------------------------------------------- 1. 冷启动
def test_cold_start_accumulates_baseline_without_alert(det):
    for day in mondays()[:2]:
        art = det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day),
                           observed_at=obs_at(day))
        assert art["verdict"] == NO_BASELINE
        assert art["new_alert_fingerprints"] == []
    base = det.get_baseline(SRC)
    assert len(base["windows"]) == 2
    assert all(w["included"] for w in base["windows"])
    assert det.list_alerts(SRC) == []


# ---------------------------------------------------------------- 2. 季节性：周末低量不误判
def test_weekend_seasonal_dip_is_not_anomaly(det):
    # 先积累两个周六的低量基线
    for day in saturdays()[:2]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=3),
                     observed_at=obs_at(day))
    # 第三个周六仍是低量 → 季节性，不告警
    third = saturdays()[2]
    art = det.evaluate(SRC, f"2026-11-{third:02d}", healthy(third, n=3),
                       observed_at=obs_at(third))
    assert art["checks"]["count"]["verdict"] == SEASONAL
    assert "季节性" in art["checks"]["count"]["evidence"]["reason"]
    assert det.list_alerts(SRC) == []


def test_first_weekend_without_seasonal_baseline_never_alerts(det):
    # 只有工作日历史时迎来首个周六 → 季节性冷启动 no_baseline，而不是异常
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    first_sat = saturdays()[0]
    art = det.evaluate(SRC, f"2026-11-{first_sat:02d}", healthy(first_sat, n=2),
                       observed_at=obs_at(first_sat))
    assert art["checks"]["count"]["verdict"] == NO_BASELINE
    assert "季节性冷启动" in art["checks"]["count"]["evidence"]["reason"]
    assert det.list_alerts(SRC) == []


# ---------------------------------------------------------------- 3. 真实突降能检出
def test_real_weekday_count_drop_detected(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    # 第四个周一（11-23）记录数从 10 突降到 1
    art = det.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                       observed_at=obs_at(23))
    assert art["verdict"] == ANOMALY
    ev = art["checks"]["count"]["evidence"]
    assert ev["direction"] == "突降" and ev["seasonal_median"] == 10
    # 历史完全平稳（MAD=0）：证据明确说明偏离恒定水平，稳健 z 以 None 留痕
    assert ev["robust_z"] is None
    assert "完全平稳" in ev["reason"]
    alerts = det.list_alerts(SRC)
    assert len(alerts) == 1 and alerts[0]["check"] == "count"
    assert alerts[0]["status"] == "open"
    # 异常窗口被排除在基线之外
    base = det.get_baseline(SRC)
    assert all(w["window"] != "2026-11-23" for w in base["windows"])


# ---------------------------------------------------------------- 4. 突发空值
def test_sudden_null_spike_detected_with_evidence(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    bad = [rec(f"23-{i}", 23, value=10, name="", status=None) for i in range(10)]
    art = det.evaluate(SRC, "2026-11-23", bad, observed_at=obs_at(23))
    assert art["checks"]["null_ratio"]["verdict"] == ANOMALY
    ev = art["checks"]["null_ratio"]["evidence"]
    assert ev["value"] == pytest.approx(0.5)          # name/status 两列全空
    assert ev["baseline_median"] == pytest.approx(0.0)
    assert "空值比例" in ev["reason"] and ev["direction"] == "升高"
    assert any(a["check"] == "null_ratio" for a in det.list_alerts(SRC))


# ---------------------------------------------------------------- 5. 离群 / 重复 / 延迟
def test_outlier_spike_detected(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    bad = healthy(23, n=9) + [rec("x", 23, value=999999)]
    art = det.evaluate(SRC, "2026-11-23", bad, observed_at=obs_at(23))
    assert art["checks"]["outlier"]["verdict"] == ANOMALY
    assert art["metrics"]["outlier_values"] == [999999.0]


def test_duplicate_rate_detected(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    payload = healthy(23, n=5)
    art = det.evaluate(SRC, "2026-11-23", payload + payload,
                       observed_at=obs_at(23))
    assert art["checks"]["duplicate"]["verdict"] == ANOMALY
    assert art["metrics"]["duplicate_rate"] == pytest.approx(0.5)


def test_delay_spike_detected(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    # 陈旧数据：事件时间比正常早 30 天，延迟远超基线
    stale = [rec(f"23-{i}", 23, value=10,
                 ts=(datetime(2026, 11, 23, tzinfo=timezone.utc)
                     - timedelta(days=30)).isoformat()) for i in range(10)]
    art = det.evaluate(SRC, "2026-11-23", stale, observed_at=obs_at(23))
    assert art["checks"]["delay"]["verdict"] == ANOMALY
    ev = art["checks"]["delay"]["evidence"]
    assert ev["unit"] == "s" and ev["direction"] == "升高"


# ---------------------------------------------------------------- 6. 短暂故障 + 补采恢复
def test_transient_fault_backfill_resolves_alert(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    # 故障窗口：只有 1 条 → 突降告警
    fault = det.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                         batch_id="b-1123", observed_at=obs_at(23))
    assert fault["verdict"] == ANOMALY
    fp = fault["alert_fingerprints"][0]
    # 补采：同窗口补齐 10 条（新批次），指标回到基线
    recovered = det.evaluate(SRC, "2026-11-23", healthy(23, n=10),
                             batch_id="b-1123-backfill",
                             observed_at=obs_at(23), backfill=True)
    assert recovered["verdict"] == TRANSIENT_FAULT
    assert recovered["checks"]["count"]["verdict"] == TRANSIENT_FAULT
    assert fp in recovered["resolved_alert_fingerprints"]
    alerts = {a["fingerprint"]: a for a in det.list_alerts(SRC)}
    assert alerts[fp]["status"] == "resolved"
    assert alerts[fp]["resolution"] == "backfill_recovered"
    # 始终只有这一条告警，没有因补采新增
    assert len(alerts) == 1
    # 短暂故障窗口同样不进基线
    base = det.get_baseline(SRC)
    assert all(w["window"] != "2026-11-23" for w in base["windows"])


def test_partial_backfill_keeps_overall_anomaly_and_resolves_recovered_check(det):
    """补采只修复了部分检查：整体仍是异常，但已恢复检查的告警被解析。"""
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    # 同时触发 count 突降 + 空值飙升
    bad = [rec("23-bad", 23, value=10, name="", status=None)]
    det.evaluate(SRC, "2026-11-23", bad, observed_at=obs_at(23))
    assert {a["check"] for a in det.list_alerts(SRC)} == {"count", "null_ratio"}

    # 补采：记录数恢复但空值问题依旧
    backfill = [rec(f"bf-{i}", 23, value=10, name="", status=None)
                for i in range(10)]
    art = det.evaluate(SRC, "2026-11-23", backfill,
                       observed_at=obs_at(23), backfill=True)
    assert art["verdict"] == ANOMALY
    assert art["checks"]["count"]["verdict"] == TRANSIENT_FAULT
    assert art["checks"]["null_ratio"]["verdict"] == ANOMALY
    statuses = {a["check"]: a["status"] for a in det.list_alerts(SRC)}
    assert statuses["count"] == "resolved"
    assert statuses["null_ratio"] == "open"


def test_anomaly_samples_excluded_from_next_baseline(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    det.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                 observed_at=obs_at(23))  # 异常窗口
    det.evaluate(SRC, "2026-11-30", healthy(30, n=10),
                 observed_at=obs_at(30))  # 恢复正常
    base = det.get_baseline(SRC)
    included = [w["window"] for w in base["windows"] if w["included"]]
    assert "2026-11-23" not in included
    # 同星期几基线仍由三个健康周一构成，未被 1 条污染
    ev = det.get_window(SRC, "2026-11-30")["checks"]["count"]["evidence"]
    assert ev["seasonal_median"] == 10 and ev["seasonal_n"] == 3


def test_sources_are_isolated(det):
    for day in mondays()[:3]:
        det.evaluate("alpha", f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    # beta 首次出现，alpha 的历史不能成为 beta 的基线
    art = det.evaluate("beta", "2026-11-23", healthy(23, n=2, src="beta"),
                       observed_at=obs_at(23))
    assert art["verdict"] == NO_BASELINE
    assert det.get_baseline("beta")["src"] == "beta"
    assert len(det.list_alerts("beta")) == 0


# ---------------------------------------------------------------- 7. 宽限期确认
def test_unrecovered_anomaly_confirmed_after_grace(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    det.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                 observed_at=obs_at(23))  # 异常
    # 下一个窗口仍无补采 → 宽限扫描确认
    det.evaluate(SRC, "2026-11-24", healthy(24, n=10),
                 observed_at=obs_at(24))
    alert = det.list_alerts(SRC)[0]
    assert alert["status"] == "confirmed"
    assert "确认为真实异常" in alert["confirmation"]


# ---------------------------------------------------------------- 8. 分批一致
def test_batching_does_not_change_verdict(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    full = healthy(23, n=10)
    one_shot = det.evaluate(SRC, "2026-11-23", full,
                            batch_id="b-full", observed_at=obs_at(23))

    other = AnomalyDetector()
    for day in mondays()[:3]:
        other.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                       observed_at=obs_at(day))
    first_part = other.evaluate(SRC, "2026-11-23", full[:4],
                                batch_id="b-split", observed_at=obs_at(23))
    second_part = other.evaluate(SRC, "2026-11-23", full[4:],
                                 batch_id="b-split", observed_at=obs_at(23))
    # 同一窗口分两批：计数合并为 10，与一次性摄入的判定逐项一致
    assert second_part["metrics"]["total"] == one_shot["metrics"]["total"] == 10
    assert second_part["verdict"] == one_shot["verdict"]
    assert second_part["checks"] == one_shot["checks"]
    # 第一批的中间判定与最终判定同源
    assert first_part["src"] == SRC


# ---------------------------------------------------------------- 9. 重复运行幂等 + 同异常不重复告警
def test_repeated_run_is_idempotent_and_no_duplicate_alert(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     batch_id=f"b-{day}", observed_at=obs_at(day))
    payload = healthy(23, n=1)
    first = det.evaluate(SRC, "2026-11-23", payload,
                         batch_id="b-1123", observed_at=obs_at(23))
    # 同载荷再跑两遍（模拟任务重试 / 定时重复调度 / 进程重启重放）
    rerun1 = det.evaluate(SRC, "2026-11-23", payload,
                          batch_id="b-1123", observed_at=obs_at(23))
    rerun2 = det.evaluate(SRC, "2026-11-23", payload,
                          batch_id="b-1123", observed_at=obs_at(23))
    assert rerun1["metrics"] == first["metrics"]
    assert rerun2["metrics"] == first["metrics"]
    alerts = det.list_alerts(SRC)
    assert len(alerts) == 1
    assert alerts[0]["fingerprint"] == first["alert_fingerprints"][0]
    # 重放走幂等回显，告警历史不追加，不产生新告警
    assert len(alerts[0]["history"]) == 1


# ---------------------------------------------------------------- 10. 重启：sqlite 持久化
def test_restart_persists_state_via_sqlite(tmp_path):
    db = tmp_path / "restart.db"
    con = store.connect(db)
    det = AnomalyDetector(con)
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    det.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                 batch_id="b-1123", observed_at=obs_at(23))
    con.close()

    # 模拟进程重启：新连接、新判定器，状态全部从 sqlite 恢复
    con2 = store.connect(db)
    det2 = AnomalyDetector(con2)
    assert det2.get_baseline(SRC) is not None
    assert det2.get_window(SRC, "2026-11-23")["verdict"] == ANOMALY
    assert len(det2.list_alerts(SRC)) == 1
    # 重启后同载荷重跑仍然幂等
    again = det2.evaluate(SRC, "2026-11-23", healthy(23, n=1),
                          batch_id="b-1123", observed_at=obs_at(23))
    assert again["metrics"]["total"] == 1
    assert len(det2.list_alerts(SRC)) == 1
    con2.close()


def test_artifacts_persisted_through_existing_model(sqldet):
    """窗口/基线/告警工件都经 store 的 records 表（稳定键 + upsert）落库。"""
    sqldet.evaluate(SRC, "2026-11-02", healthy(2, n=5), observed_at=obs_at(2))
    keys = {k for k, _ in store.all_records(sqldet.con)}
    assert "anomaly:window:alpha:2026-11-02" in keys
    assert "anomaly:baseline:alpha" in keys
    # 工件内容是可 json 还原的证据结构
    win = sqldet.get_window(SRC, "2026-11-02")
    assert set(win) >= {"src", "window", "metrics", "checks", "verdict"}
    json.dumps(win, ensure_ascii=False)


def test_fingerprint_stable_and_distinct():
    ev1 = {"direction": "突降"}
    fp1 = anomaly.fingerprint("alpha", "2026-11-23", "count", ev1)
    fp2 = anomaly.fingerprint("alpha", "2026-11-23", "count", ev1)
    fp3 = anomaly.fingerprint("alpha", "2026-11-24", "count", ev1)
    fp4 = anomaly.fingerprint("alpha", "2026-11-23", "null_ratio",
                              {"direction": "升高"})
    assert fp1 == fp2 and fp1 != fp3 and fp1 != fp4


def test_empty_payload_returns_prior_without_creating_zero_window(det):
    for day in mondays()[:3]:
        det.evaluate(SRC, f"2026-11-{day:02d}", healthy(day, n=10),
                     observed_at=obs_at(day))
    art = det.evaluate(SRC, "2026-11-23", healthy(23, n=10),
                       observed_at=obs_at(23))
    again = det.evaluate(SRC, "2026-11-23", [],
                         observed_at=obs_at(23))
    # 回显同一窗口工件（仅落库时间戳 updated_at 由存储层维护，按业务字段比较）
    for field in ("src", "window", "verdict", "metrics", "checks"):
        assert again[field] == art[field]
    assert len(det.list_alerts(SRC)) == 0


def test_process_shim_passthrough():
    r = {"id": "x", "src": "a"}
    out = anomaly.process(r)
    assert out["_stage_anomaly"] == "windowed-elsewhere"
    assert r is not out and r.get("_stage_anomaly") is None
