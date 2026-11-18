"""薄编排：loader → normalizer→freshness→quarantine→reconcile→alert → keyer → store。不含业务判断。"""
import importlib
from . import store as _store
from . import loader as _loader
from . import freshness as _freshness
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'freshness', 'quarantine', 'reconcile', 'alert']

def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        # freshness 复用主存储连接写水位表，避免同库多连接写互锁；仅为注入，不含业务判断。
        evaluator = _freshness.FreshnessEvaluator(
            _freshness.load_sources(), con=con)
        chain = []
        for n in CHAIN_NAMES:
            if n == 'freshness':
                chain.append(evaluator.process)
            else:
                mod = importlib.import_module("." + n, __package__)
                chain.append(mod.process)
        fed = 0
        for raw in _loader.load():
            rec = raw
            for fn in chain:
                rec = fn(rec)
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()
