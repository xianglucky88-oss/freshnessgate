"""新鲜度阶段测试：跨时区发布、时钟漂移、迟到/补采、规则切换、幂等与恢复。

本环境可能未安装 pytest，文件底部自带 mini runner，可直接：
    python3 -I tests/test_freshness.py
在有 pytest 的环境中同样可被 pytest 收集。
"""
import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from src.freshnessgate import freshness as fr  # noqa: E402
from src.freshnessgate import pipeline, store  # noqa: E402

UTC = timezone.utc
FIXTURE_TS = ROOT / "fixtures" / "timeseries.json"


def _clock(dt):
    return lambda: dt


def _evaluator(tmpdir, sources=None, clock=None):
    db = Path(tmpdir) / "fresh.db"
    cfgs = sources if sources is not None else fr.load_sources(ROOT / "src" / "freshnessgate" / "sources.json")
    return fr.FreshnessEvaluator(cfgs, db_path=db, clock=clock or fr.now_utc)


def _records():
    return json.loads(FIXTURE_TS.read_text(encoding="utf-8"))


def _by_id(results):
    return {r["id"]: r["_stage_freshness"] for r in results}


# --------------------------------------------------------------------------- #
# 1. 每天发布与每分钟发布使用不同阈值
# --------------------------------------------------------------------------- #
def test_daily_and_minute_sources_have_distinct_thresholds():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp)
        daily = ev.sources["daily-sh"]
        minute = ev.sources["minute-us"]
        assert daily.threshold_seconds == 86400 + 5400          # 1d + 90m
        assert minute.threshold_seconds == 60 + 30              # 1m + 30s
        assert daily.threshold_seconds > minute.threshold_seconds * 1000


# --------------------------------------------------------------------------- #
# 2. 跨时区：事件时间按源时区归位到本地计划槽位
# --------------------------------------------------------------------------- #
def test_cross_timezone_slots_are_localized():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 2, 0, tzinfo=UTC)))
        st = ev.evaluate({"id": "sh-1", "src": "daily-sh",
                          "ts": "2026-11-10T09:00:00+08:00",
                          "received_at": "2026-11-10T01:02:00Z"})
        # 09:00+08:00 == 01:00Z；槽位起点与过期边界都应在 UTC 上可定位
        assert st["event_time_iso"] == "2026-11-10T01:00:00.000000Z"
        assert st["boundary_iso"] == "2026-11-11T02:30:00.000000Z"
        assert st["status"] == "fresh"
        assert st["timezone"] == "Asia/Shanghai"

        eu = ev.evaluate({"id": "eu-1", "src": "daily-eu",
                          "ts": "2026-11-10T08:00:00+01:00",
                          "received_at": "2026-11-10T07:03:00Z"})
        # 08:00+01:00 == 07:00Z；柏林 08:00 槽 + 2h 容忍 → 次日 09:00Z 边界
        assert eu["event_time_iso"] == "2026-11-10T07:00:00.000000Z"
        assert eu["boundary_iso"] == "2026-11-11T09:00:00.000000Z"


def test_naive_timestamp_uses_source_timezone():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 2, 0, tzinfo=UTC)))
        st = ev.evaluate({"id": "sh-naive", "src": "daily-sh",
                          "ts": "2026-11-10T09:00:00",
                          "received_at": "2026-11-10T01:00:00Z"})
        assert st["event_time_iso"] == "2026-11-10T01:00:00.000000Z"


# --------------------------------------------------------------------------- #
# 3. 时钟漂移：容忍带内的小幅超前；超出容忍拒绝推进水位
# --------------------------------------------------------------------------- #
def test_clock_skew_tolerance_band():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 0, 4, tzinfo=UTC)))
        small = ev.evaluate({"id": "drift-1", "src": "minute-us",
                             "ts": "2026-11-10T00:03:03Z",
                             "received_at": "2026-11-10T00:03:00Z"})
        assert small["status"] == "fresh"
        assert small["latency_seconds"] == 0.0          # 带内超前按 0 延迟
        assert any("时钟漂移容忍" in r for r in small["reasons"])
        assert small["advanced"] is True

        big = ev.evaluate({"id": "drift-2", "src": "minute-us",
                           "ts": "2026-11-10T00:04:30Z",
                           "received_at": "2026-11-10T00:04:00Z"})
        assert big["status"] == "clock_skew"
        assert big["advanced"] is False
        # 超出容忍的事件不得推进水位
        snap = ev.source_snapshot("minute-us")
        assert snap["watermark_event_time"] == "2026-11-10T00:03:03.000000Z"


# --------------------------------------------------------------------------- #
# 4. 补采与重复批次不得刷新最后更新时间
# --------------------------------------------------------------------------- #
def test_backfill_and_duplicate_do_not_refresh_watermark():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 0, 6, tzinfo=UTC)))
        for rec in _records()[:6]:       # 先建立 us-1..us-3 水位
            ev.evaluate(rec)
        wm_before = ev.source_snapshot("minute-us")["watermark_event_time"]
        assert wm_before == "2026-11-10T00:02:00.000000Z"

        backfill = ev.evaluate({"id": "backfill-1", "src": "minute-us",
                                "ts": "2026-11-09T23:00:00Z",
                                "received_at": "2026-11-10T00:05:00Z"})
        assert backfill["status"] == "backfill"
        assert backfill["advanced"] is False
        assert backfill["watermark_after"] == wm_before
        assert any("不得冒充" in r for r in backfill["reasons"])

        dup = ev.evaluate({"id": "dup-1", "src": "minute-us",
                           "ts": "2026-11-10T00:02:00Z",
                           "received_at": "2026-11-10T00:06:00Z"})
        assert dup["status"] == "duplicate"
        assert dup["advanced"] is False
        # 即使补采/重复的“接收时间”更晚，最后更新时间仍由 us-3 保持
        snap = ev.source_snapshot("minute-us")
        assert snap["watermark_event_time"] == wm_before
        assert snap["updater_id"] == "us-3"
        assert snap["last_received_at"] == "2026-11-10T00:02:09.000000Z"


# --------------------------------------------------------------------------- #
# 5. 迟到数据：延迟超阈值但事件最新 → late，仍推进水位
# --------------------------------------------------------------------------- #
def test_late_data_is_flagged_but_advances_watermark():
    with tempfile.TemporaryDirectory() as tmp:
        # now=00:01:20：未越过该事件过期边界 00:01:30，但 120s 接收延迟已超 90s 阈值
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 0, 1, 20, tzinfo=UTC)))
        late = ev.evaluate({"id": "late-1", "src": "minute-us",
                            "ts": "2026-11-10T00:00:00Z",
                            "received_at": "2026-11-10T00:02:00Z"})  # 延迟 120s > 90s 阈值
        assert late["late"] is True
        assert late["status"] == "fresh"
        assert late["latency_seconds"] == 120.0
        assert late["advanced"] is True
        assert ev.source_snapshot("minute-us")["updater_id"] == "late-1"


# --------------------------------------------------------------------------- #
# 6. 缺口：跨槽位缺失可计数，来源快照给出 missed_slots
# --------------------------------------------------------------------------- #
def test_gap_detection_between_events_and_snapshot():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 1, 0, tzinfo=UTC)))
        ev.evaluate({"id": "g1", "src": "minute-us",
                     "ts": "2026-11-10T00:00:00Z",
                     "received_at": "2026-11-10T00:00:05Z"})
        g3 = ev.evaluate({"id": "g3", "src": "minute-us",
                          "ts": "2026-11-10T00:03:00Z",
                          "received_at": "2026-11-10T00:03:05Z"})
        assert g3["missed_slots"] == 2          # 缺 00:01 与 00:02 两个槽
        assert g3["gap_seconds"] == 180.0

        # 日更来源缺两天
        ev.evaluate({"id": "d1", "src": "daily-sh",
                     "ts": "2026-11-08T09:00:00+08:00",
                     "received_at": "2026-11-08T01:05:00Z"})
        d2 = ev.evaluate({"id": "d2", "src": "daily-sh",
                          "ts": "2026-11-11T09:00:00+08:00",
                          "received_at": "2026-11-11T01:05:00Z"})
        assert d2["missed_slots"] == 2          # 缺 11-09、11-10


# --------------------------------------------------------------------------- #
# 7. 过期来源：诊断证据定位到具体时间边界
# --------------------------------------------------------------------------- #
def test_stale_snapshot_points_to_boundary():
    with tempfile.TemporaryDirectory() as tmp:
        # 00:05 时刻，分钟源最后事件停在 00:02：首个缺失槽 00:03，其边界 00:04:30 已过 → stale
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 0, 5, 0, tzinfo=UTC)))
        for rec in _records()[:6]:
            ev.evaluate(rec)
        snap = ev.source_snapshot("minute-us")
        assert snap["status"] == "stale"
        assert snap["first_missing_slot_iso"] == "2026-11-10T00:03:00.000000Z"
        assert snap["boundary_iso"] == "2026-11-10T00:04:30.000000Z"
        assert snap["overdue_seconds"] == 30.0
        assert snap["missed_slots"] == 2        # 00:03、00:04 槽无数据
        assert any("过期边界" in r for r in snap["reasons"])
        assert snap["current_slot_iso"] == "2026-11-10T00:05:00.000000Z"
        ev.close()

    with tempfile.TemporaryDirectory() as tmp:
        # 00:03:15：首个缺失槽 00:03 的边界 00:04:30 未到 → 仍 fresh（容忍窗内）
        ev2 = _evaluator(tmp, clock=_clock(datetime(2026, 11, 10, 0, 3, 15, tzinfo=UTC)))
        for rec in _records()[:6]:
            ev2.evaluate(rec)
        snap2 = ev2.source_snapshot("minute-us")
        assert snap2["status"] == "fresh"
        assert snap2["boundary_iso"] == "2026-11-10T00:04:30.000000Z"
        assert snap2["missed_slots"] == 0

        # 更老的历史事件对当前水位而言仍是补采，不改变来源状态
        old = ev2.evaluate({"id": "old-1", "src": "minute-us",
                            "ts": "2026-11-09T00:00:00Z",
                            "received_at": "2026-11-09T00:00:05Z"})
        assert old["status"] == "backfill"
        ev2.close()


# --------------------------------------------------------------------------- #
# 8. 规则切换（源配置变更）：水位不重置、判定稳定且留痕
# --------------------------------------------------------------------------- #
def test_rule_switch_keeps_watermark_and_flags_change():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "fresh.db"
        cfgs_v1 = fr.load_sources(ROOT / "src" / "freshnessgate" / "sources.json")
        ev1 = fr.FreshnessEvaluator(cfgs_v1, db_path=db,
                                    clock=_clock(datetime(2026, 11, 10, 0, 2, tzinfo=UTC)))
        r1 = ev1.evaluate({"id": "us-1", "src": "minute-us",
                           "ts": "2026-11-10T00:00:00Z",
                           "received_at": "2026-11-10T00:00:10Z"})
        assert r1["rule_version"] == "v1"
        ev1.close()

        # 切换为 v2：容忍从 30s 收紧到 10s（阈值 70s）
        v2 = {**cfgs_v1["minute-us"].__dict__}
        v2["tolerance_seconds"] = 10.0
        v2["rule_version"] = "v2"
        cfg_v2 = fr.SourceConfig(**v2)
        ev2 = fr.FreshnessEvaluator({**cfgs_v1, "minute-us": cfg_v2}, db_path=db,
                                    clock=_clock(datetime(2026, 11, 10, 0, 3, tzinfo=UTC)))
        # 水位仍然是 v1 写入的事件，没有因配置变更被重置
        assert ev2.source_snapshot("minute-us")["watermark_event_time"] == \
            "2026-11-10T00:00:00.000000Z"

        late = ev2.evaluate({"id": "us-late", "src": "minute-us",
                             "ts": "2026-11-10T00:01:00Z",
                             "received_at": "2026-11-10T00:02:20Z"})  # 延迟 80s
        assert late["rule_switched"] is True
        assert late["config_changed"] is True
        assert late["late"] is True                 # 80s > 新阈值 70s；旧阈值 90s 下不会迟到
        assert late["threshold_seconds"] == 70.0

        # 同一份输入在 v2 下重放，判定完全一致（稳定性）
        again = ev2.evaluate({"id": "us-late", "src": "minute-us",
                              "ts": "2026-11-10T00:01:00Z",
                              "received_at": "2026-11-10T00:02:20Z"})
        assert again["status"] == "duplicate"
        assert again["advanced"] is False
        ev2.close()


# --------------------------------------------------------------------------- #
# 9. 重复运行（幂等）：重放全部落为 backfill/duplicate，水位与更新者保持不变
# --------------------------------------------------------------------------- #
def test_repeated_runs_are_stable():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "fresh.db"
        clock = _clock(datetime(2026, 11, 10, 0, 6, tzinfo=UTC))

        def run_all():
            ev = fr.FreshnessEvaluator(
                fr.load_sources(ROOT / "src" / "freshnessgate" / "sources.json"),
                db_path=db, clock=clock)
            statuses = {r["id"]: ev.evaluate(dict(r))["status"] for r in _records()}
            snap = {s: ev.source_snapshot(s) for s in
                    ("minute-us", "daily-sh", "daily-eu")}
            ev.close()
            return statuses, snap

        first, snap1 = run_all()
        second, snap2 = run_all()
        # 首轮：新事件入水位；次轮：全部重放为补采/重复（正确语义，不刷新水位）
        assert first["backfill-1"] == "backfill"
        assert first["dup-1"] == "backfill"       # 与先到的 us-3 同事件时间
        assert first["drift-2"] == "clock_skew"
        assert second["us-3"] == "backfill"
        assert second["backfill-1"] == "backfill"
        assert second["dup-1"] == "backfill"
        assert second["drift-2"] == "clock_skew"     # 漂移判定与是否见过无关，稳定
        # 水位快照两轮完全一致（幂等的核心）
        for s in snap1:
            assert snap1[s]["watermark_event_time"] == snap2[s]["watermark_event_time"]
            assert snap1[s]["updater_id"] == snap2[s]["updater_id"]

        con = sqlite3.connect(db)
        rows = con.execute("SELECT src, event_time, updater_id FROM freshness_watermark").fetchall()
        con.close()
        wm = {r[0]: (r[1], r[2]) for r in rows}
        # us 水位停在 drift-1（us-3 之后唯一被容忍推进的事件）；补采/重复/越界漂移都没改写它
        assert wm["minute-us"] == ("2026-11-10T00:03:03.000000Z", "drift-1")


# --------------------------------------------------------------------------- #
# 10. 中断恢复：水位已落盘，新实例能识别重放为补采/重复
# --------------------------------------------------------------------------- #
def test_recovery_after_interruption():
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "fresh.db"
        ev = fr.FreshnessEvaluator(
            fr.load_sources(ROOT / "src" / "freshnessgate" / "sources.json"),
            db_path=db, clock=_clock(datetime(2026, 11, 10, 0, 3, tzinfo=UTC)))
        ev.evaluate({"id": "us-1", "src": "minute-us",
                     "ts": "2026-11-10T00:00:00Z",
                     "received_at": "2026-11-10T00:00:10Z"})
        ev.close()                                          # 模拟进程中断

        ev2 = fr.FreshnessEvaluator(
            fr.load_sources(ROOT / "src" / "freshnessgate" / "sources.json"),
            db_path=db, clock=_clock(datetime(2026, 11, 10, 0, 4, tzinfo=UTC)))
        replay = ev2.evaluate({"id": "us-1", "src": "minute-us",
                               "ts": "2026-11-10T00:00:00Z",
                               "received_at": "2026-11-10T00:00:10Z"})
        assert replay["status"] == "duplicate"
        assert replay["advanced"] is False
        ev2.close()


# --------------------------------------------------------------------------- #
# 11. 旧输入兼容：无 received_at、未配置来源、坏时间戳
# --------------------------------------------------------------------------- #
def test_legacy_inputs_without_received_at():
    with tempfile.TemporaryDirectory() as tmp:
        ev = _evaluator(tmp, clock=_clock(datetime(2026, 11, 1, 1, 0, tzinfo=UTC)))
        st = ev.evaluate({"id": "legacy-1", "src": "alpha", "ts": "2026-11-01T00:00:00Z"})
        assert st["status"] == "fresh"
        assert st["received_at_iso"] == st["event_time_iso"]
        assert any("旧输入兼容" in r for r in st["reasons"])
        assert st["latency_seconds"] == 0.0

        unknown = ev.evaluate({"id": "x", "src": "ghost", "ts": "2026-11-01T00:00:00Z"})
        assert unknown["status"] == "unconfigured"

        bad = ev.evaluate({"id": "bad", "src": "alpha", "ts": "not-a-timestamp"})
        assert bad["status"] == "invalid"
        assert ev.source_snapshot("alpha")["updater_id"] == "legacy-1"


# --------------------------------------------------------------------------- #
# 12. 流水线集成：默认源配置下全链路仍幂等，且状态挂在记录上
# --------------------------------------------------------------------------- #
def test_pipeline_integration_and_idempotency(monkeypatch=None):
    db = ROOT / "freshnessgate.db"
    for p in (db, Path(str(db) + "-wal"), Path(str(db) + "-shm")):
        if p.exists():
            p.unlink()
    a = pipeline.run()
    b = pipeline.run()
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3
    con = store.connect()
    try:
        rows = dict(store.all_records(con))
        assert rows["r-003"]["_stage_freshness"]["status"] == "duplicate"
        assert rows["r-001"]["_stage_freshness"]["source"] == "alpha"
        assert "boundary_iso" in rows["r-001"]["_stage_freshness"]
        wm = con.execute("SELECT updater_id FROM freshness_watermark WHERE src='alpha'").fetchone()
        assert wm[0] == "r-003"
    finally:
        con.close()


def _run_all_tests():
    fns = [(name, obj) for name, obj in sorted(globals().items())
           if name.startswith("test_") and callable(obj)]
    failures = 0
    for name, fn in fns:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(fns) - failures}/{len(fns)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_run_all_tests())
