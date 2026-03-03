#!/usr/bin/env python3
"""
Module 3b — Write Contention & Hot Region
Phase A: High-concurrency UPSERT with sequential (monotonic) keys → induces hotspot.
Phase B: Same workload after ALTER to AUTO_RANDOM → hotspot mitigated.
Compares p99 latency and KV backoff metrics between phases.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, get_latency_stats
from lib.db_utils import get_connection, execute_timed
from load.load_runner import LoadRunner
from load.workload_definitions import schema_b_hotspot_workload, schema_b_autorand_workload, build_weighted_pool

MODULE = "03b_write_contention"
CONCURRENCY = 64
PHASE_SEC   = 180   # 3 minutes per phase


def run(cfg: dict):
    init_db()
    start_module(MODULE)
    counts = _get_counts(cfg)

    print(f"\n{'='*60}")
    print(f"  Module 3b: Write Contention & Hot Region")
    print(f"  Concurrency: {CONCURRENCY} | Phase duration: {PHASE_SEC}s each")
    print(f"{'='*60}")

    runner = LoadRunner(tidb_cfg=cfg["tidb"], counts=counts, module=MODULE)
    conn = get_connection(cfg["tidb"])
    cur = conn.cursor()

    # ── Phase A: Sequential key UPSERT (hot region) ──────────────────────────
    print("\n  Phase A — Sequential UPSERT (hot region induced)...")
    _ensure_hotspot_table(cur, conn, auto_random=False)
    pool_a = build_weighted_pool(schema_b_hotspot_workload(counts))
    runner.run(pool_a, concurrency=CONCURRENCY, duration_sec=PHASE_SEC,
               phase="sequential")
    stats_a = get_latency_stats(MODULE, phase="sequential")

    # ── Collect KV diagnostics between phases ─────────────────────────────────
    kv_before = _collect_kv_diagnostics(cur)

    # ── Phase B: AUTO_RANDOM key UPSERT (hotspot mitigated) ──────────────────
    print("\n  Phase B — AUTO_RANDOM UPSERT (hotspot mitigated)...")
    _ensure_hotspot_table(cur, conn, auto_random=True)  # recreates with AUTO_RANDOM
    pool_b = build_weighted_pool(schema_b_autorand_workload(counts))
    runner.run(pool_b, concurrency=CONCURRENCY, duration_sec=PHASE_SEC,
               phase="autorand")
    stats_b = get_latency_stats(MODULE, phase="autorand")

    # ── UPSERT behaviour comparison ───────────────────────────────────────────
    upsert_results = _test_upsert_behaviour(cur)

    conn.close()

    improvement = 0
    if stats_a.get("p99_ms") and stats_b.get("p99_ms"):
        improvement = (stats_a["p99_ms"] - stats_b["p99_ms"]) / stats_a["p99_ms"] * 100

    summary = {
        "sequential": stats_a,
        "autorand": stats_b,
        "p99_improvement_pct": round(improvement, 1),
        "kv_diagnostics": kv_before,
        "upsert_comparison": upsert_results,
    }
    end_module(MODULE, "passed",
               f"AUTO_RANDOM reduced p99 by {improvement:.1f}%")
    print(f"\n  p99 improvement: {improvement:.1f}% "
          f"({stats_a.get('p99_ms',0):.1f}ms → {stats_b.get('p99_ms',0):.1f}ms)")
    return summary


def _ensure_hotspot_table(cur, conn, auto_random: bool):
    """Drop and recreate events table with sequential or AUTO_RANDOM PK."""
    cur.execute("DROP TABLE IF EXISTS events_contention_test")
    if auto_random:
        cur.execute("""
            CREATE TABLE events_contention_test (
                id         BIGINT AUTO_RANDOM PRIMARY KEY,
                source     VARCHAR(100),
                event_type VARCHAR(100),
                user_id    BIGINT,
                ts         DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
            )
        """)
    else:
        cur.execute("""
            CREATE TABLE events_contention_test (
                id         BIGINT AUTO_INCREMENT PRIMARY KEY,
                source     VARCHAR(100),
                event_type VARCHAR(100),
                user_id    BIGINT,
                ts         DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
            )
        """)
    conn.commit()


def _collect_kv_diagnostics(cur) -> dict:
    """Pull available contention metrics from INFORMATION_SCHEMA."""
    results = {}
    try:
        cur.execute("""
            SELECT COUNT(*) as lock_count
            FROM information_schema.DATA_LOCK_WAITS
        """)
        row = cur.fetchone()
        results["lock_wait_count"] = row[0] if row else 0
    except Exception as e:
        results["lock_wait_count"] = f"unavailable: {e}"

    try:
        cur.execute("""
            SELECT DEADLOCK_ID, TRY_LOCK_TRX_SQL
            FROM information_schema.DEADLOCKS LIMIT 5
        """)
        results["recent_deadlocks"] = len(cur.fetchall())
    except Exception:
        results["recent_deadlocks"] = 0
    return results


def _test_upsert_behaviour(cur) -> list:
    """Compare INSERT...ON DUPLICATE KEY UPDATE vs REPLACE INTO."""
    results = []
    cur.execute("DROP TABLE IF EXISTS upsert_test")
    cur.execute("""
        CREATE TABLE upsert_test (
            id    INT PRIMARY KEY,
            val   INT DEFAULT 0,
            extra VARCHAR(50) DEFAULT 'original'
        )
    """)
    cur.execute("INSERT INTO upsert_test VALUES (1, 100, 'original')")

    # Test INSERT ... ON DUPLICATE KEY UPDATE
    t0 = time.perf_counter()
    cur.execute("INSERT INTO upsert_test (id, val) VALUES (1, 200) "
                "ON DUPLICATE KEY UPDATE val = val + 1")
    lat1 = (time.perf_counter() - t0) * 1000
    cur.execute("SELECT val, extra FROM upsert_test WHERE id = 1")
    row = cur.fetchone()
    results.append({
        "method": "INSERT...ON DUPLICATE KEY UPDATE",
        "latency_ms": round(lat1, 2),
        "val_after": row[0] if row else None,
        "extra_preserved": row[1] == "original" if row else None,
        "note": "Updates only specified columns; preserves unmentioned columns",
    })

    # Test REPLACE INTO
    cur.execute("INSERT INTO upsert_test VALUES (1, 100, 'original')"
                " ON DUPLICATE KEY UPDATE val=100, extra='original'")  # reset
    t0 = time.perf_counter()
    cur.execute("REPLACE INTO upsert_test (id, val) VALUES (1, 300)")
    lat2 = (time.perf_counter() - t0) * 1000
    cur.execute("SELECT val, extra FROM upsert_test WHERE id = 1")
    row = cur.fetchone()
    results.append({
        "method": "REPLACE INTO",
        "latency_ms": round(lat2, 2),
        "val_after": row[0] if row else None,
        "extra_preserved": row[1] == "original" if row else None,
        "note": "DELETE + INSERT — resets all columns not specified (extra becomes NULL)",
    })

    cur.execute("DROP TABLE IF EXISTS upsert_test")
    return results


def _get_counts(cfg):
    import json
    manifest = os.path.join("results", "data_manifest.json")
    if os.path.exists(manifest):
        with open(manifest) as f:
            return json.load(f).get("counts", {})
    from setup.generate_data import SCALE_CONFIG
    return SCALE_CONFIG.get(cfg["test"].get("data_scale", "medium"), {})


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
