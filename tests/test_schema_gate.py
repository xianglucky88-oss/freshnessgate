"""质量入口（schema_check）与隔离/落库契约的回归测试。

覆盖题目要求的关键场景：

* 必填 / 可空 / 类型 / 枚举 / 版本兼容 / 未知字段策略；
* 字段重命名（无损修复）、重名冲突（阻断）、部分批次（缺字段）；
* 旧格式路径（无 schema_version）与新格式共存；
* 半批旧格式 + 半批新格式、未知枚举、缺失必填的混合批次；
* 失败关闭：坏记录不进入正式结果库；
* 隔离库写入失败后重启：正式结果不含坏记录，诊断样本仍可回放。
"""
import json
import sqlite3

import pytest

from src.freshnessgate import config, pipeline, quarantine, schema_check, store


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------


def legacy(**over):
    rec = {
        "id": "L1",
        "src": "alpha",
        "name": "Legacy",
        "value": 10,
        "status": "active",
        "ts": "2026-11-01T00:00:00Z",
    }
    rec.update(over)
    return rec


def v2(**over):
    rec = {
        "id": "V1",
        "schema_version": "2",
        "src": "beta",
        "title": "Modern",
        "value": 10.5,
        "status": "archived",
        "observed_at": "2026-11-02T00:00:00Z",
    }
    rec.update(over)
    return rec


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """每个测试独立的正式库 / 隔离库 / 日志 / 夹具路径。"""
    paths = {
        "db": tmp_path / "main.db",
        "qdb": tmp_path / "q.db",
        "journal": tmp_path / "q.jsonl",
    }
    monkeypatch.setattr(config, "DB_PATH", paths["db"])
    monkeypatch.setattr(config, "QUARANTINE_DB_PATH", paths["qdb"])
    monkeypatch.setattr(config, "QUARANTINE_JOURNAL_PATH", paths["journal"])
    monkeypatch.delenv(schema_check.UNKNOWN_POLICY_ENV, raising=False)
    paths["tmp"] = tmp_path
    return paths


def write_batch(path, records):
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# 基础字段契约
# ---------------------------------------------------------------------------


def test_legacy_record_passes_untouched():
    out, report = schema_check.evaluate(legacy())
    assert report["decision"] == "pass"
    assert report["blocked"] is False
    assert report["schema_version"] == "1"
    assert report["format"] == "legacy"
    assert report["compatibility"]["mode"] == "legacy-missing-version"
    assert out["value"] == 10
    assert "_schema" in out


def test_missing_required_blocks():
    raw = legacy()
    del raw["value"]
    _, report = schema_check.evaluate(raw)
    assert report["decision"] == "block"
    assert report["route"] == "quarantine"
    assert report["fields"]["value"]["status"] == "missing"
    assert report["fields"]["value"]["policy"] == "missing_required"
    codes = {r["code"] for r in report["reasons"]}
    assert "missing_required" in codes


def test_null_field_rules():
    # v1 没有可空字段 → null 阻断
    _, r1 = schema_check.evaluate(legacy(value=None))
    assert r1["decision"] == "block"
    assert r1["fields"]["value"]["policy"] == "null_not_allowed"

    # v2.unit 显式声明可空 → 放行
    _, r2 = schema_check.evaluate(v2(unit=None))
    assert r2["decision"] == "pass"
    assert r2["fields"]["unit"]["status"] == "null"


def test_type_mismatch_blocks_and_legacy_string_int_repairs():
    _, blocked = schema_check.evaluate(legacy(value="not-a-number"))
    assert blocked["decision"] == "block"
    assert blocked["fields"]["value"]["policy"] == "type_mismatch"

    out, repaired = schema_check.evaluate(legacy(value="10"))
    assert repaired["decision"] == "repair"
    assert out["value"] == 10
    assert repaired["fields"]["value"]["status"] == "coerced"
    assert repaired["repairs"][0]["policy"] == "type_coerced"

    # v2 不允许字符串→数字的隐式修复（接口升级后应直接用 number）
    _, v2_blocked = schema_check.evaluate(v2(value="10"))
    assert v2_blocked["decision"] == "block"


def test_unknown_enum_blocks_and_known_extension_passes():
    _, bad = schema_check.evaluate(v2(status="mystery"))
    assert bad["decision"] == "block"
    assert bad["fields"]["status"]["policy"] == "unknown_enum"

    _, good = schema_check.evaluate(v2(status="discontinued"))
    assert good["decision"] == "pass"


def test_timestamp_format_checked_only_when_declared():
    _, bad = schema_check.evaluate(v2(observed_at="not-a-date"))
    assert bad["decision"] == "block"
    assert bad["fields"]["observed_at"]["policy"] == "invalid_format"


def test_nested_or_bool_types_are_not_silently_coerced():
    _, report = schema_check.evaluate(legacy(value=True))
    assert report["decision"] == "block"
    assert report["fields"]["value"]["actual_type"] == "bool"


# ---------------------------------------------------------------------------
# 版本兼容
# ---------------------------------------------------------------------------


def test_version_aliases_and_formats():
    for declared in ("2", 2, "2.0", "v2"):
        _, report = schema_check.evaluate(v2(schema_version=declared))
        assert report["decision"] == "pass", declared
        assert report["schema_version"] == "2"

    _, camel = schema_check.evaluate(
        {"schemaVersion": "2", **{k: v for k, v in v2().items() if k != "schema_version"}}
    )
    assert camel["decision"] == "pass"


def test_unsupported_version_blocks():
    _, report = schema_check.evaluate(v2(schema_version="9"))
    assert report["decision"] == "block"
    assert report["compatibility"]["supported"] is False
    assert report["compatibility"]["declared"] == "9"
    assert {r["code"] for r in report["reasons"]} >= {"unsupported_schema_version"}


def test_conflicting_version_declarations_block():
    raw = v2(schemaVersion="1")
    _, report = schema_check.evaluate(raw)
    assert report["decision"] == "block"
    assert {r["code"] for r in report["reasons"]} >= {"version_conflict"}


# ---------------------------------------------------------------------------
# 重命名 / 未知字段
# ---------------------------------------------------------------------------


def test_rename_repairs_only_registered_aliases():
    raw = v2()
    del raw["title"]
    del raw["observed_at"]
    raw["name"] = "From Legacy"
    raw["ts"] = "2026-11-02T00:00:00Z"

    out, report = schema_check.evaluate(raw)
    assert report["decision"] == "repair"
    assert out["title"] == "From Legacy"
    assert out["observed_at"] == "2026-11-02T00:00:00Z"
    assert "name" not in out and "ts" not in out
    assert {r["field"] for r in report["repairs"]} >= {"title", "observed_at"}
    assert report["fields"]["title"]["renamed_from"] == "name"


def test_rename_conflict_blocks():
    raw = v2()
    raw["name"] = "different"
    _, report = schema_check.evaluate(raw)
    assert report["decision"] == "block"
    assert report["fields"]["title"]["policy"] == "rename_conflict"


def test_duplicate_alias_with_equal_value_is_deduped():
    raw = v2()
    raw["name"] = raw["title"]
    out, report = schema_check.evaluate(raw)
    assert report["decision"] == "repair"
    assert "name" not in out
    assert report["fields"]["title"]["status"] == "deduped"


def test_unknown_fields_are_allowed_and_recorded_by_default():
    out, report = schema_check.evaluate(v2(extra_note="new"))
    assert report["decision"] == "pass"
    assert report["unknown_fields"] == ["extra_note"]
    assert out["extra_note"] == "new"
    assert report["fields"]["extra_note"]["policy"] == "unknown_field"


def test_unknown_field_policy_is_configurable(monkeypatch):
    monkeypatch.setenv(schema_check.UNKNOWN_POLICY_ENV, "strip")
    out, report = schema_check.evaluate(v2(extra_note="new"))
    assert report["decision"] == "repair"
    assert "extra_note" not in out
    assert report["fields"]["extra_note"]["action"] == "repair"

    monkeypatch.setenv(schema_check.UNKNOWN_POLICY_ENV, "block")
    _, blocked = schema_check.evaluate(v2(extra_note="new"))
    assert blocked["decision"] == "block"


def test_partial_batch_validates_each_record_independently(tmp_path):
    half = write_batch(tmp_path / "half.json", [legacy(id="a"), v2(id="b"), legacy(id="c", status="bogus")])
    records = json.loads(half.read_text(encoding="utf-8"))
    decisions = [schema_check.evaluate(r)[1]["decision"] for r in records]
    assert decisions == ["pass", "pass", "block"]


# ---------------------------------------------------------------------------
# 策略可审计
# ---------------------------------------------------------------------------


def test_policy_table_documents_every_decision():
    snapshot = schema_check.describe_policy()
    assert snapshot["rule_version"] == schema_check.RULE_VERSION
    assert snapshot["supported_versions"] == ["1", "2"]
    for code in (
        "missing_required",
        "type_mismatch",
        "type_coerced",
        "unknown_enum",
        "rename",
        "rename_conflict",
        "unknown_field",
        "unsupported_schema_version",
    ):
        assert snapshot["policies"][code]["rationale"]
        assert snapshot["policies"][code]["action"] in ("pass", "repair", "block", "allow")

    added = snapshot["policies"]["unknown_field"]
    assert added["action"] == "pass"
    assert added["effect"] == "allow"
    assert set(added["configurable"]) == {"allow", "strip", "block"}


def test_report_contains_rule_version_digest_and_per_field_results():
    raw = legacy()
    _, report = schema_check.evaluate(raw)
    assert report["rule_version"] == schema_check.RULE_VERSION
    assert report["raw_digest"] == schema_check.digest_of(raw)
    assert report["raw_digest"].startswith("sha256:")
    for name in ("id", "src", "name", "value", "status", "ts"):
        assert report["fields"][name]["status"] == "ok"
        assert report["fields"][name]["expected"]


# ---------------------------------------------------------------------------
# 落库契约与旧格式兼容
# ---------------------------------------------------------------------------


def test_store_contract_columns_and_legacy_read_path(env):
    from src.freshnessgate.models import Record

    con = store.connect()
    try:
        out, report = schema_check.evaluate(legacy())
        store.upsert(con, Record(key="L1", data=out,
                                 schema_version=report["schema_version"],
                                 rule_version=report["rule_version"],
                                 raw_digest=report["raw_digest"],
                                 schema_report=report, decision=report["decision"],
                                 source_format=report["format"]))
        con.commit()
        assert store.count(con) == 1
        assert store.all_records(con)[0][1]["id"] == "L1"  # 旧形状不变

        contract = store.contract_of(con, "L1")
        assert contract["rule_version"] == schema_check.RULE_VERSION
        assert contract["raw_digest"] == report["raw_digest"]
        assert contract["schema_report"]["fields"]["value"]["status"] == "ok"
        assert contract["schema_version"] == "1"
        assert contract["decision"] == "pass"
    finally:
        con.close()


def test_store_migrates_old_format_database(env):
    """旧版本留下的 records(key,data) 库可以直接升级，不丢数据。"""
    con = sqlite3.connect(env["db"])
    con.execute("CREATE TABLE records(key TEXT PRIMARY KEY, data TEXT NOT NULL)")
    con.execute("INSERT INTO records VALUES('old-1','{\"id\":\"old-1\"}')")
    con.commit()
    con.close()

    con = store.connect()
    try:
        cols = {row[1] for row in con.execute("PRAGMA table_info(records)")}
        assert set(store.CONTRACT_COLUMNS) <= cols
        assert store.all_records(con) == [("old-1", {"id": "old-1"})]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 混合批次 + 隔离
# ---------------------------------------------------------------------------


def mixed_records():
    bad_missing = v2(id="bad-missing")
    del bad_missing["value"]          # 缺失必填
    bad_enum = v2(id="bad-enum", status="mystery")
    bad_type = v2(id="bad-type", value="fifty")
    return [
        legacy(id="legacy-ok"),
        legacy(id="legacy-repair", value="20"),
        v2(id="v2-ok", unit="kg", extra_note="x"),
        v2(id="v2-renamed"),
        bad_enum,
        bad_missing,
        bad_type,
    ]


def _drop(rec, field):
    rec = dict(rec)
    rec.pop(field, None)
    return rec


def test_mixed_batch_routes_bad_records_to_quarantine(env):
    records = mixed_records()
    records[3] = _drop(records[3], "title")
    records[3] = _drop(records[3], "observed_at")
    records[3]["name"] = "Legacy Name"
    records[3]["ts"] = "2026-11-04T00:00:00Z"

    batch = write_batch(env["tmp"] / "mixed.json", records)
    stats = pipeline.run(fixtures_path=batch)

    assert stats["fed"] == 7
    assert stats["quarantined"] == 3
    assert stats["persisted"] == 4
    assert stats["repaired"] == 2
    assert set(stats["blocked_keys"]) == {"bad-enum", "bad-missing", "bad-type"}

    con = store.connect()
    try:
        persisted = {k for k, _ in store.all_records(con)}
        assert persisted == {"legacy-ok", "legacy-repair", "v2-ok", "v2-renamed"}
        # 坏记录完全没有进入正式结果
        assert not persisted & {"bad-enum", "bad-missing", "bad-type"}
        # 每条正式记录都带完整契约
        for key in persisted:
            contract = store.contract_of(con, key)
            assert contract["rule_version"] == schema_check.RULE_VERSION
            assert contract["raw_digest"].startswith("sha256:")
            assert contract["schema_report"]["fields"]
    finally:
        con.close()


def test_mixed_batch_quarantine_samples_are_replayable(env):
    batch = write_batch(env["tmp"] / "mixed.json", mixed_records())
    pipeline.run(fixtures_path=batch)

    samples = quarantine.samples()
    assert {s["key"] for s in samples} == {"bad-enum", "bad-missing", "bad-type"}
    for sample in samples:
        assert sample["report"]["rule_version"] == schema_check.RULE_VERSION
        assert sample["report"]["raw_digest"] == sample["raw_digest"]
        assert sample["report"]["fields"]
        assert sample["raw"]["id"] == sample["key"]

    replays = quarantine.replay_all()
    assert len(replays) == 3
    assert all(r["digest_ok"] for r in replays)
    assert all(r["still_blocked"] for r in replays)


def test_rerun_is_idempotent_for_both_stores(env):
    batch = write_batch(env["tmp"] / "mixed.json", mixed_records())
    first = pipeline.run(fixtures_path=batch)
    second = pipeline.run(fixtures_path=batch)
    assert second["rows"] == first["rows"] == 4
    assert second["quarantined"] == 3

    qcon = quarantine.connect()
    try:
        assert quarantine.count(qcon) == 3
    finally:
        qcon.close()


# ---------------------------------------------------------------------------
# 隔离失败 → 重启
# ---------------------------------------------------------------------------


def test_isolation_db_failure_keeps_diagnostics_and_blocks_persistence(env):
    batch = write_batch(env["tmp"] / "mixed.json", mixed_records())

    main = store.connect()
    broken = sqlite3.connect(env["qdb"])
    broken.execute(
        "CREATE TABLE quarantined(id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "key TEXT NOT NULL, raw_digest TEXT NOT NULL, rule_version TEXT NOT NULL,"
        "schema_version TEXT, source_format TEXT, decision TEXT NOT NULL,"
        "stage TEXT NOT NULL, raw TEXT NOT NULL, report TEXT NOT NULL,"
        "received_at TEXT NOT NULL, UNIQUE(key,raw_digest))"
    )
    broken.close()  # 关闭后写入会抛 ProgrammingError，模拟隔离库不可用

    try:
        stats = pipeline.run(con=main, quarantine_con=broken, fixtures_path=batch)
    finally:
        main.close()

    # 正式结果只有合法记录，坏记录一条也没进去
    con = store.connect()
    try:
        assert {k for k, _ in store.all_records(con)} == {
            "legacy-ok", "legacy-repair", "v2-ok", "v2-renamed"
        }
    finally:
        con.close()

    # SQLite 隔离失败，但只追加日志保住了诊断样本
    assert stats["quarantined"] == 3
    assert len(stats["isolation_failures"]) == 3
    assert all(r["persistence"] == "journal" for r in stats["receipts"])
    assert env["journal"].exists()

    journal_samples = quarantine.samples(journal_path=env["journal"])
    assert {s["key"] for s in journal_samples} == {"bad-enum", "bad-missing", "bad-type"}


def test_restart_recovers_quarantine_without_leaking_bad_records(env):
    batch = write_batch(env["tmp"] / "mixed.json", mixed_records())

    # 第一次运行：隔离库写入失败，只留下日志。
    main = store.connect()
    broken = sqlite3.connect(env["qdb"])
    broken.execute(
        "CREATE TABLE quarantined(id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "key TEXT NOT NULL, raw_digest TEXT NOT NULL, rule_version TEXT NOT NULL,"
        "schema_version TEXT, source_format TEXT, decision TEXT NOT NULL,"
        "stage TEXT NOT NULL, raw TEXT NOT NULL, report TEXT NOT NULL,"
        "received_at TEXT NOT NULL, UNIQUE(key,raw_digest))"
    )
    broken.close()
    pipeline.run(con=main, quarantine_con=broken, fixtures_path=batch)
    main.close()

    # 重启：隔离库恢复可用，日志里的样本被补写进隔离库。
    stats = pipeline.run(fixtures_path=batch)
    assert stats["recovery"]["recovered"] == 3
    assert stats["blocked_keys"] == ["bad-enum", "bad-missing", "bad-type"]

    qcon = quarantine.connect()
    try:
        rows = list(qcon.execute("SELECT key, raw, report FROM quarantined ORDER BY key"))
    finally:
        qcon.close()
    assert [r[0] for r in rows] == ["bad-enum", "bad-missing", "bad-type"]
    for key, raw_json, report_json in rows:
        raw = json.loads(raw_json)
        report = json.loads(report_json)
        assert raw["id"] == key
        assert report["decision"] == "block"
        assert report["raw_digest"] == schema_check.digest_of(raw)
        replay = quarantine.replay({"key": key, "raw": raw, "report": report,
                                    "raw_digest": report["raw_digest"],
                                    "rule_version": report["rule_version"],
                                    "decision": report["decision"]})
        assert replay["digest_ok"] and replay["still_blocked"]

    # 正式结果始终不含坏记录
    con = store.connect()
    try:
        assert {k for k, _ in store.all_records(con)} == {
            "legacy-ok", "legacy-repair", "v2-ok", "v2-renamed"
        }
        assert store.count(con) == 4
    finally:
        con.close()


def test_unwritable_journal_aborts_before_persisting(env):
    """诊断样本都无法保存时必须中止，且不得留下部分正式结果。"""
    batch = write_batch(env["tmp"] / "mixed.json", mixed_records())

    main = store.connect()
    bad_journal = env["tmp"] / "blocked" / "q.jsonl"
    bad_journal.parent.mkdir(parents=True)
    bad_journal.mkdir()  # 让隔离日志的打开操作必然失败（IsADirectoryError）

    with pytest.raises(quarantine.IsolationError):
        pipeline.run(con=main, journal_path=bad_journal, fixtures_path=batch)
    main.close()

    con = store.connect()
    try:
        # 运行被中止 → 事务回滚，正式库没有本次写入
        assert store.count(con) == 0
    finally:
        con.close()


def test_non_object_record_is_quarantined_with_digest(env):
    batch = write_batch(env["tmp"] / "bad.json", [legacy(id="ok"), ["not", "an", "object"]])
    stats = pipeline.run(fixtures_path=batch)
    assert stats["persisted"] == 1
    assert stats["quarantined"] == 1
    sample = quarantine.samples()[0]
    assert sample["report"]["fields"]["<record>"]["policy"] == "record_not_object"
    assert quarantine.replay(sample)["digest_ok"] is True


def test_downstream_stage_failure_is_isolated_fail_closed(env, monkeypatch):
    """质量门之后的阶段异常也必须隔离，不能把半成品写进正式结果。"""
    from src.freshnessgate import pipeline as pipeline_mod

    original = pipeline_mod._load_chain

    def broken_chain():
        def boom(record):
            raise RuntimeError("stage exploded")

        return [("freshness", boom)]

    monkeypatch.setattr(pipeline_mod, "_load_chain", broken_chain)

    batch = write_batch(env["tmp"] / "one.json", [legacy(id="boom")])
    stats = pipeline.run(fixtures_path=batch)
    assert stats["persisted"] == 0
    assert stats["quarantined"] == 1
    assert stats["receipts"][0]["key"] == "boom"

    sample = quarantine.samples()[0]
    assert sample["stage"] == "freshness"
    assert sample["report"]["fields"]["<record>"]["policy"] == "internal_error"

    con = store.connect()
    try:
        assert store.count(con) == 0
    finally:
        con.close()
    monkeypatch.setattr(pipeline_mod, "_load_chain", original)


def test_duplicate_ids_with_same_content_collapse(env):
    batch = write_batch(env["tmp"] / "dup.json", [legacy(id="dup"), legacy(id="dup")])
    stats = pipeline.run(fixtures_path=batch)
    assert stats["fed"] == 2
    assert stats["persisted"] == 2
    assert stats["rows"] == 1


def test_cli_supports_run_policy_and_replay(env, tmp_path, capsys):
    from src.freshnessgate import cli

    batch = write_batch(tmp_path / "mixed.json", mixed_records())
    assert cli.main(["run", "--fixtures", str(batch)]) == 0
    run_out = json.loads(capsys.readouterr().out)
    assert run_out["quarantined"] == 3

    assert cli.main(["policy"]) == 0
    policy_out = json.loads(capsys.readouterr().out)
    assert policy_out["rule_version"] == schema_check.RULE_VERSION

    assert cli.main(["replay"]) == 0
    replay_out = json.loads(capsys.readouterr().out)
    assert len(replay_out) == 3
    assert all(r["digest_ok"] and r["still_blocked"] for r in replay_out)


# ---------------------------------------------------------------------------
# 旧格式回归
# ---------------------------------------------------------------------------


def test_default_fixture_still_runs_with_legacy_contract(env):
    stats = pipeline.run()
    assert stats["fed"] == 4
    assert stats["rows"] == 3          # r-003 去重
    assert stats["quarantined"] == 0

    con = store.connect()
    try:
        for key, data in store.all_records(con):
            assert "_schema" not in data       # 契约不混进业务数据
            contract = store.contract_of(con, key)
            assert contract["source_format"] == "legacy"
            assert contract["schema_version"] == "1"
    finally:
        con.close()
