"""单一职责：源端声明 vs 落库结果的对账，输出可定位、可告警、幂等的差异报告。

同一对账范围涵盖四类账：
  1. 源批次/分页边界（source_pages：fetched/failed/pending）
  2. 正式记录（records）
  3. 隔离记录（quarantine：held / released）
  4. 迟到状态（页 visible_at 与批次 grace_deadline）

差异五分类（reason 常量即 alert 消费的原因码）：
  not_fetched  未采到：页未翻到（pending/重试失败），尚不能判定丢失
  quarantined  被隔离：采到了但在隔离区，未入正式表（释放后转 resolved）
  duplicate    重复：同一 key 的额外 occurrence，落库侧计数（源端只算一次）
  late         延迟：迟到批次/页越过宽限仍未采到，已解释，不升级
  missing      真正丢失：声明页全部 fetched 且不在任何已知状态里

归属约定（见 README/测试）：
  删除标记 tombstone 计【源端】：源声明的 expected 含该 id（状态 deleted），
      落库侧正式行移除、单独入 tombstones 账；
  跨批次重复计【落库侧】：源端 declared 按 id 只算一次，额外 occurrence 入台账。

幂等：差异用确定性 fingerprint（reason+key+关键定位字段的 sha256）持久化，
重跑时已解释/已存在的差异只刷新 last_seen，不会被当作新故障再次升级。
"""
import hashlib
import json
import time
import uuid

from . import alert, store

# ---- 差异原因码（供 alert 使用，勿随意改名） ----
NOT_FETCHED = "not_fetched"
QUARANTINED = "quarantined"
DUPLICATE = "duplicate"
LATE = "late"
MISSING = "missing"
UNEXPECTED = "unexpected"          # 落库侧多出未声明 id：已解释的信息性差异
RESOLVED = "resolved"              # 历史差异本轮已消失（仅用于状态归档）

# 只有真正丢失（以及落库侧意外数据）构成需要升级的故障
ESCALATING_REASONS = frozenset({MISSING, UNEXPECTED})
# 已解释的差异：重跑不升级
EXPLAINED_REASONS = frozenset({NOT_FETCHED, QUARANTINED, DUPLICATE, LATE})


def fingerprint(reason, key, detail):
    """差异指纹：只依赖稳定身份与定位字段，不含 run_id/时间，保证重跑不变。"""
    payload = json.dumps(
        {"reason": reason, "key": key,
         "detail": {k: detail[k] for k in sorted(detail)
                    if k not in ("observed_at", "run_id")}},
        sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _page_index(page_states):
    fetched, failed, pending = set(), set(), set()
    for p in page_states:
        {
            "fetched": fetched,
            "failed": failed,
            "pending": pending,
        }.get(p["state"], pending).add(p["page_id"])
    return fetched, failed, pending


def _classify_key(key, *, tombstoned, held, fetched_pages, failed_pages,
                  pending_pages, late_pages, page_of_key, batch_grace,
                  ledger_keys):
    """对源端声明的单个 key 做五分类，返回 (reason, detail) 或 None（账平）。

    注意"页 fetched"不等于"页上每条记录都落账"：页边界已越过但台账缺该 key
    时才是真正丢失，不能因为页状态是 fetched 就当作账平。
    """
    pages = page_of_key.get(key, ())
    in_fetched = any(p in fetched_pages for p in pages)
    in_failed = any(p in failed_pages for p in pages)
    in_pending = any(p in pending_pages for p in pages)
    in_late = any(p in late_pages for p in pages)
    location = {"pages": sorted(pages),
                "batches": sorted({p.split("/", 1)[0] for p in pages})}

    if key in tombstoned:
        return None  # 源端删除：账在 tombstones，已解释，不产生差异

    # 先看落账事实（跨多页时，任一页已把该 key 落账即视为采到）
    if key in ledger_keys:
        if key in held:
            return QUARANTINED, {**location, "state": "held",
                                 "quarantine_reason": held[key]["reason"]}
        # released 或正式入库 => 账平（released 行同时在正式表）
        return None

    # 台账没有该 key：页边界全部越过 => 真正丢失
    if in_fetched and not (in_pending or in_failed or in_late):
        return MISSING, {**location, "state": "declared_but_absent_after_fetch"}
    # 以下均为"承载该 key 的页尚未全部越过"
    if in_late:
        return LATE, {**location, "state": "late",
                      "grace_deadline": min(batch_grace[b] for b in location["batches"]
                                            if b in batch_grace)}
    if in_failed:
        return NOT_FETCHED, {**location, "state": "page_retry_exhausted"}
    if in_pending:
        return NOT_FETCHED, {**location, "state": "page_not_turned"}
    # 无任何页状态记录（理论上 run 开头已同步 pending），保守按未采到处理
    return NOT_FETCHED, {**location, "state": "page_state_unknown"}


def run(con, manifest, now=None):
    """执行一次对账，返回 report dict。con 由调用方持有（支持注入临时库/时钟）。"""
    now = now if now is not None else time.time()
    # 时间戳 + 进程内序号 + 随机后缀：进程重启后计数器归零也不会撞 run_id
    run_id = f"recon-{int(now * 1000)}-{_next_ctr()}-{uuid.uuid4().hex[:8]}"

    # 1. 同步源批次/分页边界状态（未到可见时刻 => pending）
    for page in manifest.pages():
        existing = con.execute(
            "SELECT state FROM source_pages WHERE page_id=?", (page.page_id,)).fetchone()
        if existing is None:
            store.upsert_page_state(con, page.page_id, page.batch_id, page.page_no,
                                    page.visible_at, "pending", now)
    page_states = store.all_page_states(con)
    fetched_pages, failed_pages, pending_pages = _page_index(page_states)

    # 迟到判定：批次越过 grace_deadline，且其页仍未 fetched
    batch_grace = {b.batch_id: b.grace_deadline for b in manifest.batches}
    late_pages = set()
    for page in manifest.pages():
        grace = batch_grace.get(page.batch_id, float("inf"))
        if page.page_id not in fetched_pages and now > grace:
            late_pages.add(page.page_id)

    # 2. 取齐四类账
    formal = store.record_keys(con)
    quarantined = {q["key"]: q for q in store.all_quarantine(con)}
    held = {k: v for k, v in quarantined.items() if v["state"] == "held"}
    released = {k: v for k, v in quarantined.items() if v["state"] == "released"}
    tombstoned = store.tombstone_keys(con)
    outcomes = store.ledger_outcomes(con)
    ledger_keys = set(outcomes)

    declared = manifest.declared_ids()
    tombstone_decl = manifest.tombstone_ids()
    expected_live = declared - tombstone_decl

    # key -> 承载它的页（跨批次重复会出现在多页）
    page_of_key: dict[str, tuple[str, ...]] = {}
    for page in manifest.pages():
        for rec in page.records:
            page_of_key.setdefault(str(rec["id"]), []).append(page.page_id)
    page_of_key = {k: tuple(v) for k, v in page_of_key.items()}

    # 3. 逐 key 分类
    findings = []
    reason_counts = {NOT_FETCHED: 0, QUARANTINED: 0, DUPLICATE: 0,
                     LATE: 0, MISSING: 0, UNEXPECTED: 0}

    for key in sorted(expected_live):
        classified = _classify_key(
            key, tombstoned=tombstoned, held=held,
            fetched_pages=fetched_pages, failed_pages=failed_pages,
            pending_pages=pending_pages, late_pages=late_pages,
            page_of_key=page_of_key, batch_grace=batch_grace,
            ledger_keys=ledger_keys)
        if classified is None:
            continue
        reason, detail = classified
        fp = fingerprint(reason, key, detail)
        findings.append({"fingerprint": fp, "reason": reason, "key": key, "detail": detail})
        reason_counts[reason] += 1

    # 4. 跨批次/同批重复：落库侧台账同一 key 有 >=2 个"正式" occurrence
    #    （landed/duplicate/released 才占正式账；quarantined/tombstoned 不算）。
    #    源端对每个 id 只声明一次，额外 occurrence 计落库侧差异。
    formal_outcomes = {"landed", "duplicate", "released"}
    duplicate_keys = sorted(
        k for k, occ in outcomes.items()
        if sum(1 for o in occ if o in formal_outcomes) >= 2 and k in expected_live)
    for key in duplicate_keys:
        occ = outcomes[key]
        detail = {"occurrences": len(occ), "outcomes": occ,
                  "pages": sorted(page_of_key.get(key, ()))}
        fp = fingerprint(DUPLICATE, key, detail)
        if not any(f["fingerprint"] == fp for f in findings):
            findings.append({"fingerprint": fp, "reason": DUPLICATE,
                             "key": key, "detail": detail})
            reason_counts[DUPLICATE] += 1

    # 5. 落库侧意外数据：正式表里有源端从未声明的 key
    unexpected = sorted(formal - declared)
    for key in unexpected:
        detail = {"state": "formal_record_without_source_declaration"}
        fp = fingerprint(UNEXPECTED, key, detail)
        findings.append({"fingerprint": fp, "reason": UNEXPECTED,
                         "key": key, "detail": detail})
        reason_counts[UNEXPECTED] += 1

    # 6. 持久化指纹并驱动 alert（已解释差异不重复升级）
    store.start_recon_run(con, run_id, now, {"run_id": run_id, "status": "running"})
    current_fps = {f["fingerprint"] for f in findings}
    alerts_sent = []
    for f in findings:
        status = store.upsert_finding(con, f["fingerprint"], f["reason"], f["key"],
                                      f["detail"], "open", run_id, now)
        if alert.should_escalate(f["reason"], status == "new",
                                 store.finding(con, f["fingerprint"])):
            f["run_id"] = run_id
            alert.send(con, f, at=now, run_id=run_id)
            store.mark_finding_escalated(con, f["fingerprint"])
            alerts_sent.append(f["fingerprint"])

    # 历史差异本轮已消失 => 归档 resolved，不再占用 open 差异
    for old in store.all_findings(con):
        if old["fingerprint"] not in current_fps and old["status"] == "open":
            store.mark_finding_status(con, old["fingerprint"], RESOLVED)

    # 7. 把账算明白：源端计数、落库计数、恒等式
    ledger_rows = store.all_ledger(con)
    src = {
        "declared_total": len(declared),
        "expected_live": len(expected_live),
        "tombstones_declared": len(tombstone_decl),
        "batches": len(manifest.batches),
    }
    landed = {
        "formal_rows": len(formal),
        "quarantined_held": len(held),
        "quarantined_released": len(released),
        "tombstoned": len(tombstoned),
        "ledger_occurrences": len(ledger_rows),
        "duplicate_occurrences": sum(
            max(sum(1 for o in occ if o in formal_outcomes) - 1, 0)
            for occ in outcomes.values()),
    }
    # 恒等式：源端声明的活 id == 范围内正式入库 + 隔离中 + 未采到 + 延迟 + 真丢。
    # formal 中未声明的 key 走 unexpected 差异；已声明删除（tombstone）的 key
    # 计源端删除账——两者都不混入本恒等式。
    formal_in_scope = (formal & declared) - tombstone_decl
    held_keys = set(held)
    accounted = (len(formal_in_scope) + len(held_keys)
                 + reason_counts[NOT_FETCHED] + reason_counts[LATE]
                 + reason_counts[MISSING])
    balanced = accounted == len(expected_live)

    report = {
        "run_id": run_id,
        "at": now,
        "source": src,
        "landed": landed,
        "differences": reason_counts,
        "balanced": balanced,
        "identity": f"{len(expected_live)} expected_live == {accounted} accounted",
        "findings": findings,
        "alerts_sent": alerts_sent,
        "page_states": [{"page_id": p["page_id"], "batch_id": p["batch_id"],
                         "state": p["state"], "visible_at": p["visible_at"]}
                        for p in page_states],
    }
    run_fingerprint = hashlib.sha256(
        json.dumps({"findings": sorted(current_fps),
                    "counts": reason_counts, "balanced": balanced},
                   sort_keys=True).encode("utf-8")).hexdigest()[:16]
    report["run_fingerprint"] = run_fingerprint
    store.finish_recon_run(con, run_id, now, run_fingerprint, report)
    store.commit(con)
    return report


def _next_ctr():
    """进程内单调序号，保证同毫秒多次对账 run_id 不撞。"""
    global _CTR
    _CTR += 1
    return _CTR


_CTR = 0


def process(record):
    """旧流水线兼容入口：保留原对账阶段标记，不改变记录数据流。"""
    rec = dict(record)
    rec.setdefault("_stage_reconcile", "naive")
    return rec
