#!/usr/bin/env python3
"""
Module 9 — TiDB-Specific Feature Showcase

Runs eight focused sub-tests that highlight TiDB Cloud capabilities:

  1. Transaction Mode Comparison       — Optimistic vs Pessimistic concurrency control.
  2. Isolation Level Comparison        — READ COMMITTED vs REPEATABLE READ under contention.
  3. TiFlash Read-After-Write          — Measures replication lag from TiKV to TiFlash
                                         and confirms that reads from TiFlash are consistent
                                         with recently committed writes.
  4. Range Scan Size Impact            — Shows how scan selectivity (LIMIT 100 → 100 000)
                                         affects latency and throughput.
  5. Stale Reads                       — Compares fresh reads vs stale reads using
                                         SET @@tidb_read_staleness.
  6. Resource Group Isolation          — Demonstrates resource group isolation between
                                         two tenant workloads (requires TiDB >= 7.1).
  7. Clustered vs Non-Clustered Index  — Compares point-get latency on clustered PK
                                         vs non-clustered PK.
  8. Batch DML                         — Shows TiDB handling large batch operations
                                         with tidb_dml_batch_size.
"""
import sys, os, time, random, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.db_utils import get_connection, execute_timed
from lib.result_store import init_db, start_module, end_module, log_results_batch, get_latency_stats

MODULE = "09_tidb_features"

FLUSH_EVERY = 200


# ─── Generic timed worker ─────────────────────────────────────────────────────

def _run_timed_workload(tidb_cfg: dict, queries: list, concurrency: int,
                        duration_sec: int, phase: str, session_setup: list = None):
    """
    Run a mixed query pool for `duration_sec` seconds at `concurrency` threads.

    queries : list of (sql_template, params_fn) where params_fn() -> tuple
    session_setup : list of SQL strings executed once per connection after opening
    """
    end_ts = time.time() + duration_sec
    result_buf = []
    buf_lock = threading.Lock()
    flush_lock = threading.Lock()

    def flush(force=False):
        with buf_lock:
            if not result_buf:
                return
            if not force and len(result_buf) < FLUSH_EVERY:
                return
            rows = result_buf.copy()
            result_buf.clear()
        with flush_lock:
            log_results_batch(rows)

    def worker():
        conn = get_connection(tidb_cfg)
        cur = conn.cursor()
        if session_setup:
            for stmt in session_setup:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
        local = []
        while time.time() < end_ts:
            sql, params_fn = random.choice(queries)
            try:
                params = params_fn()
            except Exception:
                params = ()
            res = execute_timed(cur, sql, params)
            local.append({
                "module": MODULE,
                "phase": phase,
                "db_label": "tidb",
                "ts": time.time(),
                "query_type": phase,
                "latency_ms": res["latency_ms"],
                "success": int(res["success"]),
                "retries": 0,
                "error": res.get("error"),
            })
            if len(local) >= FLUSH_EVERY:
                with buf_lock:
                    result_buf.extend(local)
                local.clear()
                flush()
        with buf_lock:
            result_buf.extend(local)
        flush(force=True)
        conn.close()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    flush(force=True)


# ─── Helper: build a contention-heavy write+read workload ─────────────────────

def _contention_queries(acct_count: int):
    """Mixed read/write pool that exercises lock contention."""
    return [
        (
            "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s",
            lambda: (round(random.uniform(0.01, 50), 2),
                     random.randint(1, acct_count),
                     round(random.uniform(0.01, 50), 2)),
        ),
        (
            "SELECT id, balance FROM accounts WHERE id = %s",
            lambda: (random.randint(1, acct_count),),
        ),
        (
            "INSERT INTO transactions (account_id, type, amount, status, reference_id) "
            "VALUES (%s, 'transfer', %s, 'completed', %s)",
            lambda: (random.randint(1, acct_count),
                     round(random.uniform(0.01, 500), 2),
                     ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))),
        ),
    ]


# ─── Sub-test 1: Transaction Mode Comparison ──────────────────────────────────

def _run_txn_mode(cfg: dict, counts: dict, duration: int, concurrency: int):
    acct_count = max(1, counts.get("accounts", 750_000))
    queries = _contention_queries(acct_count)

    for mode, phase, setup in [
        ("optimistic",  "txn_optimistic",  ["SET @@tidb_txn_mode = 'optimistic'"]),
        ("pessimistic", "txn_pessimistic", ["SET @@tidb_txn_mode = 'pessimistic'"]),
    ]:
        print(f"\n  [Txn Mode] Running {mode} mode ({concurrency} threads, {duration}s) ...")
        _run_timed_workload(cfg["tidb"], queries, concurrency, duration, phase,
                            session_setup=setup)
        s = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        print(f"    {mode}: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms  "
              f"count={s.get('count', 0)}")


# ─── Sub-test 2: Isolation Level Comparison ───────────────────────────────────

def _run_isolation(cfg: dict, counts: dict, duration: int, concurrency: int):
    acct_count = max(1, counts.get("accounts", 750_000))
    queries = _contention_queries(acct_count)

    for iso, phase, setup in [
        ("READ-COMMITTED",  "iso_read_committed",
         ["SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"]),
        ("REPEATABLE-READ", "iso_repeatable_read",
         ["SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"]),
    ]:
        print(f"\n  [Isolation] Running {iso} ({concurrency} threads, {duration}s) ...")
        _run_timed_workload(cfg["tidb"], queries, concurrency, duration, phase,
                            session_setup=setup)
        s = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        print(f"    {iso}: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms  "
              f"count={s.get('count', 0)}")


# ─── Sub-test 3: TiFlash Read-After-Write ─────────────────────────────────────

def _run_tiflash_raw(cfg: dict, counts: dict):
    """
    Writes a batch of rows and immediately reads them back from TiFlash.
    Records:
      - tiflash_raw_write  : individual write latencies
      - tiflash_raw_read   : read latencies via TiFlash hint
    The gap between write commit and TiFlash visibility is the replication lag.
    """
    tidb_cfg = cfg["tidb"]
    # Check if TiFlash is available by testing the hint
    conn = get_connection(tidb_cfg)
    cur = conn.cursor()

    tiflash_available = False
    try:
        cur.execute(
            "SELECT /*+ READ_FROM_STORAGE(tiflash[events]) */ COUNT(*) FROM events LIMIT 1"
        )
        cur.fetchone()
        tiflash_available = True
    except Exception as e:
        print(f"\n  [TiFlash RAW] TiFlash unavailable (no replica or hint unsupported): {e}")
        print("  Skipping TiFlash read-after-write sub-test.")
        conn.close()
        return

    print(f"\n  [TiFlash RAW] TiFlash confirmed available. Running read-after-write test ...")

    batch_size = 500
    write_buf = []
    read_buf = []

    # Phase A: write batch
    write_start = time.time()
    inserted_ids = []
    for i in range(batch_size):
        t0 = time.time()
        try:
            cur.execute(
                "INSERT INTO events (source, event_type, user_id, session_id, properties) "
                "VALUES ('tiflash_raw_test', 'raw_write', %s, 1, '{\"probe\":1}')",
                (random.randint(1, max(1, counts.get("users", 10_000))),)
            )
            conn.commit()
            last_id = cur.lastrowid
            inserted_ids.append(last_id)
            lat = (time.time() - t0) * 1000
            write_buf.append({
                "module": MODULE, "phase": "tiflash_raw_write", "db_label": "tidb",
                "ts": time.time(), "query_type": "tiflash_raw_write",
                "latency_ms": lat, "success": 1, "retries": 0, "error": None,
            })
        except Exception as ex:
            write_buf.append({
                "module": MODULE, "phase": "tiflash_raw_write", "db_label": "tidb",
                "ts": time.time(), "query_type": "tiflash_raw_write",
                "latency_ms": (time.time() - t0) * 1000, "success": 0, "retries": 0,
                "error": str(ex),
            })

    if write_buf:
        log_results_batch(write_buf)

    write_duration = time.time() - write_start
    print(f"    Wrote {batch_size} rows in {write_duration:.2f}s")

    if not inserted_ids:
        print("    No rows inserted; skipping TiFlash read phase.")
        conn.close()
        return

    # Phase B: read from TiFlash with replication-lag measurement
    # Poll until all inserted rows are visible or timeout.
    check_id = inserted_ids[-1]
    lag_start = time.time()
    lag_timeout = 30  # seconds max wait
    lag_sec = None

    while time.time() - lag_start < lag_timeout:
        try:
            cur.execute(
                "SELECT /*+ READ_FROM_STORAGE(tiflash[events]) */ COUNT(*) "
                "FROM events WHERE id = %s AND source = 'tiflash_raw_test'",
                (check_id,)
            )
            row = cur.fetchone()
            if row and int(row[0]) > 0:
                lag_sec = time.time() - lag_start
                break
        except Exception:
            pass
        time.sleep(0.05)

    if lag_sec is not None:
        print(f"    TiFlash replication lag: {lag_sec*1000:.1f}ms for last inserted row")
    else:
        print(f"    TiFlash replication did not complete within {lag_timeout}s timeout")
        lag_sec = lag_timeout

    # Run 30s of TiFlash reads for latency characterization
    print("    Running TiFlash read phase (30s) ...")
    read_end = time.time() + 30
    acct_count = max(1, counts.get("accounts", 750_000))
    while time.time() < read_end:
        t0 = time.time()
        try:
            cur.execute(
                "SELECT /*+ READ_FROM_STORAGE(tiflash[transactions]) */ "
                "COUNT(*), AVG(amount) FROM transactions "
                "WHERE account_id = %s",
                (random.randint(1, acct_count),)
            )
            cur.fetchone()
            lat = (time.time() - t0) * 1000
            read_buf.append({
                "module": MODULE, "phase": "tiflash_raw_read", "db_label": "tidb",
                "ts": time.time(), "query_type": "tiflash_raw_read",
                "latency_ms": lat, "success": 1, "retries": 0, "error": None,
            })
        except Exception as ex:
            read_buf.append({
                "module": MODULE, "phase": "tiflash_raw_read", "db_label": "tidb",
                "ts": time.time(), "query_type": "tiflash_raw_read",
                "latency_ms": (time.time() - t0) * 1000, "success": 0, "retries": 0,
                "error": str(ex),
            })

    if read_buf:
        log_results_batch(read_buf)

    conn.close()
    s = get_latency_stats(MODULE, phase="tiflash_raw_read", db_label="tidb")
    print(f"    TiFlash reads: TPS={s.get('tps', 0):.1f}  "
          f"p99={s.get('p99_ms', 0):.1f}ms  "
          f"lag={lag_sec*1000:.0f}ms")
    return lag_sec


# ─── Sub-test 4: Range Scan Size Impact ───────────────────────────────────────

RANGE_STEPS = [100, 1_000, 10_000, 100_000]


def _run_range_scans(cfg: dict, counts: dict, duration_per_step: int, concurrency: int):
    acct_count = max(1, counts.get("accounts", 750_000))

    for limit in RANGE_STEPS:
        phase = f"range_{limit}"
        queries = [
            (
                "SELECT id, amount, status, created_at "
                "FROM transactions "
                "WHERE account_id >= %s "
                "ORDER BY created_at DESC "
                f"LIMIT {limit}",
                lambda: (random.randint(1, max(1, acct_count - limit)),),
            ),
        ]
        print(f"\n  [Range Scan] LIMIT {limit:,} ({concurrency} threads, {duration_per_step}s) ...")
        _run_timed_workload(cfg["tidb"], queries, concurrency, duration_per_step, phase)
        s = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        print(f"    LIMIT {limit:,}: TPS={s.get('tps', 0):.1f}  "
              f"p50={s.get('p50_ms', 0):.1f}ms  p99={s.get('p99_ms', 0):.1f}ms")


# ─── Sub-test 5: Stale Reads ─────────────────────────────────────────────────

def _run_stale_reads(cfg: dict, counts: dict, duration: int, concurrency: int):
    """Compare fresh reads vs stale reads using SET @@tidb_read_staleness."""
    acct_count = max(1, counts.get("accounts", 750_000))

    read_queries = [
        (
            "SELECT id, balance, status FROM accounts WHERE id = %s",
            lambda: (random.randint(1, acct_count),),
        ),
        (
            "SELECT COUNT(*) FROM transactions WHERE account_id = %s",
            lambda: (random.randint(1, acct_count),),
        ),
    ]

    # Phase A: fresh reads (default)
    print(f"\n  [Stale Reads] Fresh reads ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(cfg["tidb"], read_queries, concurrency, duration,
                        "stale_fresh")
    s = get_latency_stats(MODULE, phase="stale_fresh", db_label="tidb")
    print(f"    Fresh: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms")

    # Phase B: stale reads with 5-second staleness
    print(f"\n  [Stale Reads] Stale reads -5s ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(cfg["tidb"], read_queries, concurrency, duration,
                        "stale_5s",
                        session_setup=["SET @@tidb_read_staleness = -5"])
    s = get_latency_stats(MODULE, phase="stale_5s", db_label="tidb")
    print(f"    Stale -5s: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms")


# ─── Sub-test 6: Resource Groups ─────────────────────────────────────────────

def _run_resource_groups(cfg: dict, counts: dict, duration: int, concurrency: int):
    """Demonstrate resource group isolation between two tenant workloads."""
    tidb_cfg = cfg["tidb"]
    acct_count = max(1, counts.get("accounts", 750_000))
    conn = get_connection(tidb_cfg)
    cur = conn.cursor()

    # Try to create resource groups (requires TiDB >= 7.1)
    rg_available = False
    try:
        cur.execute("CREATE RESOURCE GROUP IF NOT EXISTS pov_high BURSTABLE RU_PER_SEC = 10000")
        cur.execute("CREATE RESOURCE GROUP IF NOT EXISTS pov_low BURSTABLE RU_PER_SEC = 1000")
        rg_available = True
        print("\n  [Resource Groups] Created pov_high (10000 RU/s) and pov_low (1000 RU/s)")
    except Exception as e:
        print(f"\n  [Resource Groups] Resource groups unavailable: {e}")
        print("  Skipping resource group sub-test.")
        conn.close()
        return
    conn.close()

    queries = _contention_queries(acct_count)

    # Phase A: high resource group
    print(f"  [Resource Groups] High-priority tenant ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(tidb_cfg, queries, concurrency, duration,
                        "rg_high",
                        session_setup=["SET RESOURCE GROUP pov_high"])
    s = get_latency_stats(MODULE, phase="rg_high", db_label="tidb")
    print(f"    High RG: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms")

    # Phase B: low resource group
    print(f"  [Resource Groups] Low-priority tenant ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(tidb_cfg, queries, concurrency, duration,
                        "rg_low",
                        session_setup=["SET RESOURCE GROUP pov_low"])
    s = get_latency_stats(MODULE, phase="rg_low", db_label="tidb")
    print(f"    Low RG: TPS={s.get('tps', 0):.1f}  p99={s.get('p99_ms', 0):.1f}ms")

    # Cleanup
    try:
        conn2 = get_connection(tidb_cfg)
        cur2 = conn2.cursor()
        cur2.execute("DROP RESOURCE GROUP IF EXISTS pov_high")
        cur2.execute("DROP RESOURCE GROUP IF EXISTS pov_low")
        conn2.close()
    except Exception:
        pass


# ─── Sub-test 7: Clustered vs Non-Clustered Index ───────────────────────────

def _run_clustered_vs_nonclustered(cfg: dict, counts: dict, duration: int, concurrency: int):
    """Compare point-get latency on clustered PK (transaction_items) vs non-clustered PK (users)."""
    tidb_cfg = cfg["tidb"]
    user_count = max(1, counts.get("users", 10_000))
    txn_items_count = max(1, counts.get("transaction_items", 500_000))

    # Non-clustered: users table (BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED)
    nc_queries = [
        (
            "SELECT id, email, name, status FROM users WHERE id = %s",
            lambda: (random.randint(1, user_count),),
        ),
    ]

    print(f"\n  [Index Type] Non-clustered PK lookups on users ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(tidb_cfg, nc_queries, concurrency, duration,
                        "idx_nonclustered")
    s = get_latency_stats(MODULE, phase="idx_nonclustered", db_label="tidb")
    print(f"    Non-clustered: TPS={s.get('tps', 0):.1f}  p50={s.get('p50_ms', 0):.1f}ms  p99={s.get('p99_ms', 0):.1f}ms")

    # Clustered: transaction_items (AUTO_RANDOM PRIMARY KEY = clustered by default)
    cl_queries = [
        (
            "SELECT id, transaction_id, amount, quantity FROM transaction_items WHERE id = %s",
            lambda: (random.randint(1, txn_items_count),),
        ),
    ]

    print(f"  [Index Type] Clustered PK lookups on transaction_items ({concurrency} threads, {duration}s) ...")
    _run_timed_workload(tidb_cfg, cl_queries, concurrency, duration,
                        "idx_clustered")
    s = get_latency_stats(MODULE, phase="idx_clustered", db_label="tidb")
    print(f"    Clustered: TPS={s.get('tps', 0):.1f}  p50={s.get('p50_ms', 0):.1f}ms  p99={s.get('p99_ms', 0):.1f}ms")


# ─── Sub-test 8: Batch DML (Large Transaction Splitting) ────────────────────

def _run_batch_dml(cfg: dict, counts: dict):
    """Show TiDB handling large batch operations with tidb_dml_batch_size."""
    tidb_cfg = cfg["tidb"]
    acct_count = max(1, counts.get("accounts", 750_000))
    batch_rows = min(50_000, acct_count)

    conn = get_connection(tidb_cfg)
    cur = conn.cursor()

    results_buf = []

    # Phase A: single large UPDATE without batch splitting
    print(f"\n  [Batch DML] Single large UPDATE ({batch_rows:,} rows) without batch splitting ...")
    try:
        cur.execute("SET @@tidb_dml_batch_size = 0")  # disable batching
    except Exception:
        pass
    t0 = time.time()
    try:
        cur.execute(
            "UPDATE accounts SET balance = balance + 0.01 WHERE id <= %s",
            (batch_rows,)
        )
        conn.commit()
        lat = (time.time() - t0) * 1000
        results_buf.append({
            "module": MODULE, "phase": "batch_dml_single", "db_label": "tidb",
            "ts": time.time(), "query_type": "batch_dml_single",
            "latency_ms": lat, "success": 1, "retries": 0, "error": None,
        })
        print(f"    Single txn: {lat:.0f}ms for {batch_rows:,} rows")
    except Exception as ex:
        lat = (time.time() - t0) * 1000
        results_buf.append({
            "module": MODULE, "phase": "batch_dml_single", "db_label": "tidb",
            "ts": time.time(), "query_type": "batch_dml_single",
            "latency_ms": lat, "success": 0, "retries": 0, "error": str(ex),
        })
        print(f"    Single txn failed ({lat:.0f}ms): {ex}")

    # Phase B: same UPDATE with batch splitting enabled
    print(f"  [Batch DML] Same UPDATE with tidb_dml_batch_size = 1000 ...")
    try:
        cur.execute("SET @@tidb_dml_batch_size = 1000")
    except Exception:
        pass
    t0 = time.time()
    try:
        cur.execute(
            "UPDATE accounts SET balance = balance + 0.01 WHERE id <= %s",
            (batch_rows,)
        )
        conn.commit()
        lat = (time.time() - t0) * 1000
        results_buf.append({
            "module": MODULE, "phase": "batch_dml_split", "db_label": "tidb",
            "ts": time.time(), "query_type": "batch_dml_split",
            "latency_ms": lat, "success": 1, "retries": 0, "error": None,
        })
        print(f"    Batch split: {lat:.0f}ms for {batch_rows:,} rows")
    except Exception as ex:
        lat = (time.time() - t0) * 1000
        results_buf.append({
            "module": MODULE, "phase": "batch_dml_split", "db_label": "tidb",
            "ts": time.time(), "query_type": "batch_dml_split",
            "latency_ms": lat, "success": 0, "retries": 0, "error": str(ex),
        })
        print(f"    Batch split failed ({lat:.0f}ms): {ex}")

    if results_buf:
        log_results_batch(results_buf)

    conn.close()


# ─── Main entrypoint ──────────────────────────────────────────────────────────

def run(cfg: dict):
    init_db()
    start_module(MODULE)

    counts = _get_counts(cfg)
    test_cfg = cfg.get("test") or {}

    # Duration tuning (keep short for validation mode)
    contention_duration = max(20, int(test_cfg.get("tidb_features_duration_seconds", 45)))
    concurrency = max(4, int(test_cfg.get("tidb_features_concurrency",
                                           (test_cfg.get("concurrency_levels") or [16])[0])))
    range_duration = max(15, int(test_cfg.get("tidb_features_range_duration_seconds", 30)))

    print(f"\n{'='*60}")
    print(f"  Module 9: TiDB-Specific Feature Showcase (8 sub-tests)")
    print(f"  Contention duration: {contention_duration}s | Concurrency: {concurrency}")
    print(f"  Range scan duration: {range_duration}s/step")
    print(f"{'='*60}")

    notes = []
    any_success = False

    # 1. Transaction mode comparison
    print("\n--- 1/8: Transaction Mode Comparison ---")
    _run_txn_mode(cfg, counts, contention_duration, concurrency)
    for phase in ("txn_optimistic", "txn_pessimistic"):
        if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
            any_success = True

    # 2. Isolation level comparison
    print("\n--- 2/8: Isolation Level Comparison ---")
    _run_isolation(cfg, counts, contention_duration, concurrency)
    for phase in ("iso_read_committed", "iso_repeatable_read"):
        if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
            any_success = True

    # 3. TiFlash read-after-write
    print("\n--- 3/8: TiFlash Read-After-Write ---")
    try:
        lag = _run_tiflash_raw(cfg, counts)
        if get_latency_stats(MODULE, phase="tiflash_raw_read", db_label="tidb").get("count", 0) > 0:
            any_success = True
            if lag is not None:
                notes.append(f"TiFlash replication lag={lag*1000:.0f}ms")
    except Exception as ex:
        print(f"  TiFlash RAW sub-test error: {ex}")
        notes.append(f"TiFlash RAW skipped: {ex}")

    # 4. Range scan size impact
    print("\n--- 4/8: Range Scan Size Impact ---")
    _run_range_scans(cfg, counts, range_duration, max(2, concurrency // 4))
    for limit in RANGE_STEPS:
        if get_latency_stats(MODULE, phase=f"range_{limit}", db_label="tidb").get("count", 0) > 0:
            any_success = True

    # 5. Stale reads
    print("\n--- 5/8: Stale Reads (Fresh vs Staleness Window) ---")
    try:
        _run_stale_reads(cfg, counts, contention_duration, concurrency)
        for phase in ("stale_fresh", "stale_5s"):
            if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
                any_success = True
    except Exception as ex:
        print(f"  Stale reads sub-test error: {ex}")
        notes.append(f"Stale reads skipped: {ex}")

    # 6. Resource group isolation
    print("\n--- 6/8: Resource Group Isolation ---")
    try:
        _run_resource_groups(cfg, counts, contention_duration, concurrency)
        for phase in ("rg_high", "rg_low"):
            if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
                any_success = True
    except Exception as ex:
        print(f"  Resource groups sub-test error: {ex}")
        notes.append(f"Resource groups skipped: {ex}")

    # 7. Clustered vs non-clustered index
    print("\n--- 7/8: Clustered vs Non-Clustered Index ---")
    try:
        _run_clustered_vs_nonclustered(cfg, counts, contention_duration, concurrency)
        for phase in ("idx_nonclustered", "idx_clustered"):
            if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
                any_success = True
    except Exception as ex:
        print(f"  Clustered vs non-clustered sub-test error: {ex}")
        notes.append(f"Clustered vs non-clustered skipped: {ex}")

    # 8. Batch DML (large transaction splitting)
    print("\n--- 8/8: Batch DML (Large Transaction Splitting) ---")
    try:
        _run_batch_dml(cfg, counts)
        for phase in ("batch_dml_single", "batch_dml_split"):
            if get_latency_stats(MODULE, phase=phase, db_label="tidb").get("count", 0) > 0:
                any_success = True
    except Exception as ex:
        print(f"  Batch DML sub-test error: {ex}")
        notes.append(f"Batch DML skipped: {ex}")

    note_str = " | ".join(notes) if notes else None
    if any_success:
        end_module(MODULE, "passed", note_str)
    else:
        end_module(MODULE, "failed",
                   "No successful feature-showcase queries recorded. Check DB settings.")
    return {}


def _get_counts(cfg):
    import json
    manifest = os.path.join("results", "data_manifest.json")
    if os.path.exists(manifest):
        with open(manifest) as f:
            return json.load(f).get("counts", {})
    scale = (cfg.get("test") or {}).get("data_scale", "small")
    from setup.generate_data import SCALE_CONFIG
    return SCALE_CONFIG.get(scale, SCALE_CONFIG["small"])


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
