"""
result_store.py — SQLite-backed result store.
All test modules write here; the report generator reads from here.
"""
import sqlite3
import time
import json
import os


DB_PATH = os.path.join(os.path.dirname(__file__), "..", "results", "results.db")


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    """Create tables if they don't exist."""
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                module      TEXT NOT NULL,
                phase       TEXT,
                db_label    TEXT DEFAULT 'tidb',
                ts          REAL NOT NULL,
                query_type  TEXT,
                latency_ms  REAL,
                success     INTEGER DEFAULT 1,
                retries     INTEGER DEFAULT 0,
                error       TEXT
            );
            CREATE TABLE IF NOT EXISTS module_meta (
                module      TEXT PRIMARY KEY,
                status      TEXT,
                start_ts    REAL,
                end_ts      REAL,
                notes       TEXT
            );
            CREATE TABLE IF NOT EXISTS compat_checks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                check_name  TEXT,
                status      TEXT,
                note        TEXT
            );
            CREATE TABLE IF NOT EXISTS import_stats (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ts              REAL,
                rows_imported   INTEGER,
                gb_imported     REAL,
                duration_sec    REAL,
                throughput_gbpm REAL
            );
        """)


def log_result(module: str, latency_ms: float, success: bool,
               phase: str = None, db_label: str = "tidb",
               query_type: str = None, retries: int = 0, error: str = None):
    with _conn() as c:
        c.execute(
            "INSERT INTO results (module,phase,db_label,ts,query_type,latency_ms,success,retries,error) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (module, phase, db_label, time.time(), query_type,
             latency_ms, int(success), retries, error)
        )


def log_results_batch(rows: list):
    """Bulk insert for performance. rows = list of dicts matching log_result kwargs."""
    with _conn() as c:
        c.executemany(
            "INSERT INTO results (module,phase,db_label,ts,query_type,latency_ms,success,retries,error) "
            "VALUES (:module,:phase,:db_label,:ts,:query_type,:latency_ms,:success,:retries,:error)",
            rows
        )


def set_module_status(module: str, status: str, notes: str = None):
    with _conn() as c:
        now = time.time()
        c.execute("""
            INSERT INTO module_meta (module, status, start_ts, end_ts, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(module) DO UPDATE SET
                status=excluded.status,
                end_ts=excluded.end_ts,
                notes=excluded.notes
        """, (module, status, now, now, notes))


def start_module(module: str):
    with _conn() as c:
        c.execute("""
            INSERT INTO module_meta (module, status, start_ts)
            VALUES (?, 'running', ?)
            ON CONFLICT(module) DO UPDATE SET status='running', start_ts=excluded.start_ts
        """, (module, time.time()))


def end_module(module: str, status: str = "passed", notes: str = None):
    with _conn() as c:
        c.execute("""
            UPDATE module_meta SET status=?, end_ts=?, notes=? WHERE module=?
        """, (status, time.time(), notes, module))


def log_compat_check(check_name: str, status: str, note: str = ""):
    with _conn() as c:
        c.execute(
            "INSERT INTO compat_checks (check_name, status, note) VALUES (?,?,?)",
            (check_name, status, note)
        )


def log_import_stat(rows_imported: int, gb_imported: float,
                    duration_sec: float, throughput_gbpm: float):
    with _conn() as c:
        c.execute(
            "INSERT INTO import_stats (ts,rows_imported,gb_imported,duration_sec,throughput_gbpm) "
            "VALUES (?,?,?,?,?)",
            (time.time(), rows_imported, gb_imported, duration_sec, throughput_gbpm)
        )


def get_latency_stats(module: str, phase: str = None, db_label: str = "tidb") -> dict:
    """Return p50/p95/p99/max/avg/tps stats for a module+phase."""
    with _conn() as c:
        where = "module=? AND db_label=? AND success=1"
        params = [module, db_label]
        if phase:
            where += " AND phase=?"
            params.append(phase)
        rows = c.execute(
            f"SELECT latency_ms, ts FROM results WHERE {where} ORDER BY latency_ms",
            params
        ).fetchall()

    if not rows:
        return {}

    latencies = [r["latency_ms"] for r in rows]
    n = len(latencies)
    duration = rows[-1]["ts"] - rows[0]["ts"] if n > 1 else 1

    def pct(p):
        idx = max(0, int(n * p / 100) - 1)
        return round(latencies[idx], 2)

    return {
        "count": n,
        "avg_ms": round(sum(latencies) / n, 2),
        "p50_ms": pct(50),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
        "max_ms": round(latencies[-1], 2),
        "tps": round(n / duration, 1) if duration > 0 else 0,
    }


def get_time_series(module: str, bucket_sec: int = 10,
                    db_label: str = "tidb", phase: str = None) -> list:
    """Return [{bucket_ts, tps, avg_ms, p99_ms}] bucketed by time."""
    with _conn() as c:
        where = "module=? AND db_label=? AND success=1"
        params = [module, db_label]
        if phase:
            where += " AND phase=?"
            params.append(phase)
        rows = c.execute(
            f"SELECT ts, latency_ms FROM results WHERE {where} ORDER BY ts",
            params
        ).fetchall()

    if not rows:
        return []

    from collections import defaultdict
    buckets = defaultdict(list)
    t0 = rows[0]["ts"]
    for r in rows:
        b = int((r["ts"] - t0) / bucket_sec)
        buckets[b].append(r["latency_ms"])

    result = []
    for b in sorted(buckets):
        lats = sorted(buckets[b])
        n = len(lats)
        result.append({
            "elapsed_sec": b * bucket_sec,
            "tps": round(n / bucket_sec, 1),
            "avg_ms": round(sum(lats) / n, 2),
            "p99_ms": round(lats[int(n * 0.99)], 2) if n > 1 else lats[0],
        })
    return result
