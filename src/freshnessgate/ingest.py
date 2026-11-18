"""单一职责：从分页源摄取记录并幂等落账。

每个 record occurrence 在 ingest_ledger 中有确定性主键
（batch_id/page_id/seq），因此"进程退出再恢复"时重放同一页不会重复计数：
- 普通记录     -> records 正式表，outcome=landed（同 key 再来 => duplicate，正式行不变）
- 隔离记录     -> quarantine 表，outcome=quarantined；释放后改为 released
- 删除标记     -> tombstones 表 + 移除正式行，outcome=tombstoned（删除计源端账）
分页状态（fetched/failed/pending）写 source_pages，供对账定位分页边界。
"""
from . import quarantine, store
from .keyer import key_of
from .models import Record
from .normalizer import process as normalize
from .source import PaginatedSource, PageSpec, TransientError, TOMBSTONE

MAX_RETRIES = 5


class FetchExhausted(RuntimeError):
    """分页重试次数耗尽仍未取到（分页边界未越过，对账应归 not_fetched 而非 missing）。"""


def _occurrence_id(page: PageSpec, seq: int) -> str:
    return f"{page.batch_id}|{page.page_id}|{seq:06d}"


def _record_id(record):
    return str(record["id"])


def fetch_with_retry(source: PaginatedSource, page: PageSpec, max_retries=MAX_RETRIES):
    """带退避语义的取页重试（离线无 sleep，仅计数）。耗尽抛 FetchExhausted。"""
    last_err = None
    for _ in range(max_retries):
        try:
            return source.fetch_page(page)
        except TransientError as exc:
            last_err = exc
    raise FetchExhausted(f"page {page.page_id} failed after {max_retries} retries: {last_err}")


def _ingest_one(con, page: PageSpec, seq: int, raw: dict, at: float) -> str | None:
    """摄取单条 occurrence，返回 ledger outcome；已存在则返回 None（重放不计数）。"""
    occ = _occurrence_id(page, seq)
    if store.ledger_has(con, occ):
        return None

    rid = _record_id(raw)
    key = key_of({"id": rid})  # 正式/隔离/墓碑共用稳定主键，便于跨表对账

    if raw.get("op") == TOMBSTONE:
        store.put_tombstone(con, key, page.batch_id, page.page_id, raw, at)
        store.delete_record(con, key)
        outcome = "tombstoned"
    else:
        rec = normalize(raw)
        reason = quarantine.should_quarantine(rec)
        if reason is not None:
            quarantine.hold(con, key, rec, reason, at)
            # 即使 key 此前已正式入库，本次 occurrence 本身进隔离，不算 duplicate
            outcome = "quarantined"
        else:
            existed = key in store.record_keys(con)
            store.upsert(con, Record(key=key, data=rec))
            outcome = "duplicate" if existed else "landed"

    store.add_ledger(con, occ, key, page.batch_id, page.page_id, seq, outcome, rec if
                     raw.get("op") != TOMBSTONE else raw, at)
    return outcome


def ingest_page(con, source: PaginatedSource, page: PageSpec, at: float):
    """取一页并逐条落账。返回 (state, counts)；失败也记录边界状态，不抛走异常。"""
    counts = {"landed": 0, "duplicate": 0, "quarantined": 0, "tombstoned": 0,
              "released": 0, "failed": 0}
    try:
        records = fetch_with_retry(source, page)
    except FetchExhausted:
        store.upsert_page_state(con, page.page_id, page.batch_id, page.page_no,
                                page.visible_at, "failed", at)
        counts["failed"] = 1
        return "failed", counts

    for seq, raw in enumerate(records):
        outcome = _ingest_one(con, page, seq, raw, at)
        if outcome is not None:
            counts[outcome] = counts.get(outcome, 0) + 1

    store.upsert_page_state(con, page.page_id, page.batch_id, page.page_no,
                            page.visible_at, "fetched", at)
    return "fetched", counts


def ingest_visible(con, source: PaginatedSource, at: float):
    """摄取当前时刻所有可见页（含此前 failed、重试后成功的页）。汇总计数。"""
    totals = {"landed": 0, "duplicate": 0, "quarantined": 0, "tombstoned": 0,
              "released": 0, "failed": 0, "pages_fetched": 0, "pages_failed": 0}
    for page in source.visible_pages():
        state, counts = ingest_page(con, source, page, at)
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        totals["pages_fetched" if state == "fetched" else "pages_failed"] += 1
    store.commit(con)
    return totals
