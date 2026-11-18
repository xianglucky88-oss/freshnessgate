"""单一职责：派生稳定主键。

接受任意 JSON 值（坏记录可能是列表/标量），保证质量门在隔离前也能取到键；
对映射仍优先使用 ``id``，与基线行为一致。
"""
import hashlib
import json
from collections.abc import Mapping


def key_of(record):
    if isinstance(record, Mapping):
        rid = record.get("id")
        if rid:
            return str(rid)
    return hashlib.sha256(
        json.dumps(record, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()[:16]
