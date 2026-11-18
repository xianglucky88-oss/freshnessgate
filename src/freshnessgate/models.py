from dataclasses import dataclass, field
from typing import Any


@dataclass
class Record:
    """落库记录。

    ``key`` / ``data`` 保持旧格式兼容（旧消费方仍然只读这两列）。
    其余字段是质量门契约：规则版本、原始样本摘要、逐字段校验结果与决策。
    """

    key: str
    data: dict[str, Any] = field(default_factory=dict)
    schema_version: str | None = None
    rule_version: str | None = None
    raw_digest: str | None = None
    schema_report: dict[str, Any] | None = None
    decision: str | None = None
    source_format: str | None = None
