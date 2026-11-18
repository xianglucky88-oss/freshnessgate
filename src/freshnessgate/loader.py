"""单一职责：从本地 fixtures 读取原始记录（离线摄取入口，不联网）。

默认读取 ``fixtures/input.json``；可通过参数或环境变量
``FRESHNESSGATE_FIXTURES`` 指向其它批次（例如混合新旧格式的回归夹具），
便于在不改代码的情况下回放不同的采集样本。
"""
import json
import os

from .config import FIXTURES


def load(path=None):
    target = path or os.environ.get("FRESHNESSGATE_FIXTURES") or FIXTURES
    with open(target, encoding="utf-8") as f:
        return json.load(f)
