"""配置路径。测试可通过环境变量或 monkeypatch 模块属性重定向到临时目录。"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"

#: 正式结果库（旧格式兼容）
DB_PATH = Path(os.environ.get("FRESHNESSGATE_DB", ROOT / "freshnessgate.db"))

#: 隔离区：坏记录与其诊断契约独立存放，避免污染正式结果库。
QUARANTINE_DB_PATH = Path(
    os.environ.get("FRESHNESSGATE_QUARANTINE_DB", ROOT / "var" / "quarantine.db")
)

#: 只追加的隔离日志。即使隔离区 SQLite 写入失败，诊断样本仍可回放/重建。
QUARANTINE_JOURNAL_PATH = Path(
    os.environ.get(
        "FRESHNESSGATE_QUARANTINE_JOURNAL", ROOT / "var" / "quarantine_journal.jsonl"
    )
)
