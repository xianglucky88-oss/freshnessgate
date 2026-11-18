"""单一职责：模拟带批次边界与分页的采集源（离线，基于 manifest）。

源只负责"声明"与"取页"两件事：
- 声明：每个批次有哪些 id、每页何时可见（迟到批次的页 visible_at 更晚）、
  宽限截止（grace_deadline）、哪些 id 是删除标记（tombstone，计源端账）。
- 取页：按页回放记录；可注入瞬时故障（fail_times）验证分页重试。
源不做任何对账判断，也不接触 sqlite。
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

TOMBSTONE = "__tombstone__"


@dataclass
class PageSpec:
    page_id: str
    batch_id: str
    page_no: int
    records: list[dict[str, Any]]
    visible_at: float = 0.0
    fail_times: int = 0          # 前 N 次取页抛 TransientError，之后成功（分页重试）


@dataclass
class BatchSpec:
    batch_id: str
    pages: list[PageSpec]
    grace_deadline: float = float("inf")   # 超过该时刻仍未取到 => late（已解释）


@dataclass
class Manifest:
    batches: list[BatchSpec] = field(default_factory=list)

    def pages(self) -> list[PageSpec]:
        return [p for b in self.batches for p in sorted(b.pages, key=lambda p: p.page_no)]

    def batch(self, batch_id: str) -> Optional[BatchSpec]:
        return next((b for b in self.batches if b.batch_id == batch_id), None)

    def declared_ids(self) -> set[str]:
        """源端声明的全部 id（含 tombstone），跨批次去重——源端每个 id 只算一次。"""
        ids = set()
        for page in self.pages():
            for rec in page.records:
                ids.add(str(rec["id"]))
        return ids

    def tombstone_ids(self) -> set[str]:
        out = set()
        for page in self.pages():
            for rec in page.records:
                if rec.get("op") == TOMBSTONE:
                    out.add(str(rec["id"]))
        return out


class TransientError(RuntimeError):
    """可重试的取页故障（分页边界上的瞬时失败）。"""


class PaginatedSource:
    """带故障注入与时钟判断的分页源。now_fn 决定页是否已可见。"""

    def __init__(self, manifest: Manifest, now_fn: Callable[[], float] = None):
        self.manifest = manifest
        self.now_fn = now_fn or (lambda: 0.0)
        self._attempts: dict[str, int] = {}

    def visible_pages(self) -> list[PageSpec]:
        now = self.now_fn()
        return [p for p in self.manifest.pages() if p.visible_at <= now]

    def is_visible(self, page: PageSpec) -> bool:
        return page.visible_at <= self.now_fn()

    def fetch_page(self, page: PageSpec) -> list[dict[str, Any]]:
        """取一页。fail_times 用尽前持续抛 TransientError，调用方负责重试。"""
        attempts = self._attempts.get(page.page_id, 0) + 1
        self._attempts[page.page_id] = attempts
        if not self.is_visible(page):
            raise TransientError(f"page {page.page_id} not visible yet")
        if attempts <= page.fail_times:
            raise TransientError(f"transient failure on {page.page_id} "
                                 f"(attempt {attempts}/{page.fail_times})")
        return [dict(r) for r in page.records]

    def attempts(self, page_id: str) -> int:
        return self._attempts.get(page_id, 0)
