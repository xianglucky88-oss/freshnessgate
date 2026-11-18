"""单一职责：本地 sqlite3 持久化。

提供正式记录（records）、隔离（quarantine）、摄取台账（ingest_ledger）、
删除标记（tombstones）、源批次/分页（source_pages）、对账运行与差异
（recon_runs / recon_findings）的幂等读写。所有写操作均为确定性 upsert，
进程退出后重放同一输入不会产生重复行——对账与摄取因此可安全重跑。
"""
import json
import sqlite3
from .config import DB_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS records(
    key TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine(
    key TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'held',
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_ledger(
    occurrence_id TEXT PRIMARY KEY,
    key TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    page_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    data TEXT NOT NULL,
    ingested_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tombstones(
    key TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    page_id TEXT NOT NULL,
    data TEXT NOT NULL,
    deleted_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS source_pages(
    page_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    page_no INTEGER NOT NULL,
    visible_at REAL NOT NULL,
    state TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_runs(
    run_id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    finished_at REAL,
    fingerprint TEXT,
    report TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recon_findings(
    fingerprint TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    key TEXT,
    detail TEXT NOT NULL,
    status TEXT NOT NULL,
    first_seen_run TEXT NOT NULL,
    last_seen_run TEXT NOT NULL,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    escalated INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS alert_log(
    fingerprint TEXT NOT NULL,
    reason TEXT NOT NULL,
    key TEXT,
    sent_at REAL NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (fingerprint, run_id)
);
"""

def connect(db_path=None):
    con = sqlite3.connect(str(db_path or DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(SCHEMA)
    return con

# ---------- 正式记录 ----------
def upsert(con, record):
    con.execute("INSERT INTO records(key,data) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                (record.key, json.dumps(record.data, ensure_ascii=False)))

def delete_record(con, key):
    return con.execute("DELETE FROM records WHERE key=?", (key,)).rowcount

def commit(con):
    con.commit()

def count(con):
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]

def record_keys(con):
    return {r[0] for r in con.execute("SELECT key FROM records")}

# ---------- 隔离 ----------
def put_quarantine(con, key, data, reason, state, at):
    con.execute(
        "INSERT INTO quarantine(key,data,reason,state,updated_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET data=excluded.data, reason=excluded.reason, "
        "state=excluded.state, updated_at=excluded.updated_at",
        (key, json.dumps(data, ensure_ascii=False), reason, state, at))

def get_quarantine(con, key):
    row = con.execute("SELECT * FROM quarantine WHERE key=?", (key,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["data"] = json.loads(d["data"])
    return d

def all_quarantine(con):
    out = []
    for row in con.execute("SELECT * FROM quarantine ORDER BY key"):
        d = dict(row)
        d["data"] = json.loads(d["data"])
        out.append(d)
    return out

# ---------- 摄取台账（每次 occurrence 一行，天然支持跨批次重复计数） ----------
def ledger_has(con, occurrence_id):
    return con.execute(
        "SELECT 1 FROM ingest_ledger WHERE occurrence_id=?", (occurrence_id,)).fetchone() is not None

def add_ledger(con, occurrence_id, key, batch_id, page_id, seq, outcome, data, at):
    con.execute(
        "INSERT OR IGNORE INTO ingest_ledger"
        "(occurrence_id,key,batch_id,page_id,seq,outcome,data,ingested_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (occurrence_id, key, batch_id, page_id, seq, outcome,
         json.dumps(data, ensure_ascii=False), at))

def all_ledger(con):
    rows = con.execute("SELECT * FROM ingest_ledger ORDER BY ingested_at,seq").fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["data"] = json.loads(d["data"])
        out.append(d)
    return out

def ledger_outcomes(con):
    """{key: [outcome, ...]}，按摄取顺序。"""
    out = {}
    for row in con.execute("SELECT key,outcome FROM ingest_ledger ORDER BY ingested_at,seq"):
        out.setdefault(row[0], []).append(row[1])
    return out

def release_ledger(con, key):
    """隔离释放后，把该 key 最近一次 quarantined 台账改为 released（幂等）。"""
    con.execute(
        "UPDATE ingest_ledger SET outcome='released' "
        "WHERE rowid=(SELECT rowid FROM ingest_ledger WHERE key=? "
        "AND outcome='quarantined' ORDER BY ingested_at DESC, seq DESC LIMIT 1)",
        (key,))

# ---------- 删除标记（源端声明的删除事实，计源端账） ----------
def put_tombstone(con, key, batch_id, page_id, data, at):
    con.execute(
        "INSERT INTO tombstones(key,batch_id,page_id,data,deleted_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET batch_id=excluded.batch_id, page_id=excluded.page_id, "
        "data=excluded.data, deleted_at=excluded.deleted_at",
        (key, batch_id, page_id, json.dumps(data, ensure_ascii=False), at))

def all_tombstones(con):
    rows = con.execute("SELECT * FROM tombstones ORDER BY key").fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["data"] = json.loads(d["data"])
        out.append(d)
    return out

def tombstone_keys(con):
    return {r[0] for r in con.execute("SELECT key FROM tombstones")}

# ---------- 源批次 / 分页边界状态 ----------
def upsert_page_state(con, page_id, batch_id, page_no, visible_at, state, at):
    con.execute(
        "INSERT INTO source_pages(page_id,batch_id,page_no,visible_at,state,updated_at) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(page_id) DO UPDATE SET state=excluded.state, "
        "updated_at=excluded.updated_at, visible_at=excluded.visible_at",
        (page_id, batch_id, page_no, visible_at, state, at))

def all_page_states(con):
    return [dict(r) for r in
            con.execute("SELECT * FROM source_pages ORDER BY batch_id,page_no")]

# ---------- 对账运行与差异指纹 ----------
def start_recon_run(con, run_id, at, blank_report):
    con.execute(
        "INSERT OR IGNORE INTO recon_runs(run_id,started_at,report) VALUES(?,?,?)",
        (run_id, at, json.dumps(blank_report, ensure_ascii=False)))

def finish_recon_run(con, run_id, at, fingerprint, report):
    con.execute(
        "UPDATE recon_runs SET finished_at=?, fingerprint=?, report=? WHERE run_id=?",
        (at, fingerprint, json.dumps(report, ensure_ascii=False), run_id))

def get_recon_run(con, run_id):
    row = con.execute("SELECT * FROM recon_runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["report"] = json.loads(d["report"])
    return d

def latest_recon_run(con):
    row = con.execute(
        "SELECT * FROM recon_runs WHERE finished_at IS NOT NULL "
        "ORDER BY started_at DESC, run_id DESC LIMIT 1").fetchone()
    if row is None:
        return None
    d = dict(row)
    d["report"] = json.loads(d["report"])
    return d

def finding(con, fingerprint):
    row = con.execute("SELECT * FROM recon_findings WHERE fingerprint=?",
                      (fingerprint,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["detail"] = json.loads(d["detail"])
    return d

def upsert_finding(con, fp, reason, key, detail, status, run_id, at, escalated=None):
    existing = finding(con, fp)
    detail_s = json.dumps(detail, ensure_ascii=False, sort_keys=True)
    if existing is None:
        con.execute(
            "INSERT INTO recon_findings(fingerprint,reason,key,detail,status,"
            "first_seen_run,last_seen_run,first_seen_at,last_seen_at,escalated) "
            "VALUES(?,?,?,?,?,?,?,?,?,0)",
            (fp, reason, key, detail_s, status, run_id, run_id, at, at))
        return "new"
    con.execute(
        "UPDATE recon_findings SET reason=?, key=?, detail=?, status=?, "
        "last_seen_run=?, last_seen_at=? WHERE fingerprint=?",
        (reason, key, detail_s, status, run_id, at, fp))
    return "seen"

def mark_finding_escalated(con, fingerprint):
    con.execute("UPDATE recon_findings SET escalated=1 WHERE fingerprint=?",
                (fingerprint,))

def mark_finding_status(con, fingerprint, status):
    con.execute("UPDATE recon_findings SET status=? WHERE fingerprint=?",
                (status, fingerprint))

def all_findings(con):
    rows = con.execute("SELECT * FROM recon_findings ORDER BY first_seen_at,fingerprint").fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["detail"] = json.loads(d["detail"])
        out.append(d)
    return out

# ---------- 告警发送台账（同指纹同 run 只发一次） ----------
def log_alert(con, fingerprint, reason, key, at, run_id):
    con.execute(
        "INSERT OR IGNORE INTO alert_log(fingerprint,reason,key,sent_at,run_id) "
        "VALUES(?,?,?,?,?)", (fingerprint, reason, key, at, run_id))

def alert_count(con):
    return con.execute("SELECT COUNT(*) FROM alert_log").fetchone()[0]

def alert_fingerprints(con):
    return {r[0] for r in con.execute("SELECT fingerprint FROM alert_log")}
