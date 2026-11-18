"""标准库 test runner（环境无 pytest 时的本地验证工具，非项目交付物）。"""
import sys, types, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# 最小 pytest 垫片
pytest = types.ModuleType("pytest")
class _Raises:
    def __init__(self, exc): self.exc = exc
    def __enter__(self): return self
    def __exit__(self, et, ev, tb):
        return et is not None and issubclass(et, self.exc)
def raises(exc): return _Raises(exc)
pytest.raises = raises
sys.modules["pytest"] = pytest

import importlib.util
spec = importlib.util.spec_from_file_location(
    "test_alert", ROOT / "tests" / "test_alert.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

failures = 0
count = 0
for name in sorted(dir(mod)):
    if name.startswith("test_"):
        count += 1
        try:
            getattr(mod, name)()
            print(f"PASS {name}")
        except Exception:
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
print(f"\n{count - failures}/{count} passed")
sys.exit(1 if failures else 0)
