import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from src.freshnessgate import pipeline, store

def test_pipeline_runs_and_idempotent():
    a = pipeline.run()
    b = pipeline.run()   # 重跑不增行（幂等 upsert）
    assert a["fed"] == 4
    assert b["rows"] == a["rows"] == 3   # r-003 重复被去重

def test_store_roundtrip():
    con = store.connect()
    try:
        assert store.count(con) >= 0
    finally:
        con.close()


if __name__ == "__main__":
    import sys
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{2 - failures}/2 passed")
    sys.exit(1 if failures else 0)
