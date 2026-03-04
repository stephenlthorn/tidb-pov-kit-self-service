#!/usr/bin/env python3
"""
Module 4 — HTAP Concurrent Workload
Runs OLTP writes (TiKV) and analytical queries (TiFlash) simultaneously.
Demonstrates that analytics do not degrade transactional latency (isolation
via the TiFlash columnar replica).

Phases:
  baseline  — OLTP-only for PHASE_SEC seconds (no analytics)
  htap      — OLTP + analytics concurrently for PHASE_SEC seconds
Compares p99 OLTP latency between the two phases.
"""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, get_latency_stats, log_result
from lib.db_utils import get_connection, execute_timed
from load.load_runner import LoadRunner
from load.workload_definitions import apply_workload_profile, schema_a_workload, analytical_workload, build_weighted_pool

MODULE      = "04_htap_concurrent"
OLTP_CONC   = 32
ANAL_CONC   = 4       # analytical query threads — kept low, they're heavy
PHASE_SEC   = 120     # 2 minutes per phase


def run(cfg: dict):
    init_db()
    start_module(MODULE)
    counts = _get_counts(cfg)

    print(f"\n{'='*60}")
    print(f"  Module 4: HTAP Concurrent Workload")
    print(f"  OLTP concurrency: {OLTP_CONC} | Analytical threads: {ANAL_CONC}")
    print(f"  Phase duration: {PHASE_SEC}s each")
    print(f"{'='*60}")

    conn = get_connection(cfg["tidb"])
    cur  = conn.cursor()

    # Ensure TiFlash replica exists (best-effort — may need a few minutes to replicate)
    _ensure_tiflash_replicas(cur)
    conn.close()

    oltp_pool = build_weighted_pool(
        apply_workload_profile(
            schema_a_workload(counts),
            mix=cfg.get("test", {}).get("workload_mix", "mixed"),
            read_multiplier=cfg.get("test", {}).get("read_weight_multiplier", 1.0),
            write_multiplier=cfg.get("test", {}).get("write_weight_multiplier", 1.0),
        )
    )
    anal_pool = build_weighted_pool(analytical_workload(counts))
    runner    = LoadRunner(tidb_cfg=cfg["tidb"], counts=counts, module=MODULE)

    # ── Phase 1: OLTP-only baseline ──────────────────────────────────────────
    print("\n  Phase 1 — OLTP-only baseline (no analytics)...")
    runner.run(oltp_pool, concurrency=OLTP_CONC,
               duration_sec=PHASE_SEC, phase="oltp_only")
    stats_baseline = get_latency_stats(MODULE, phase="oltp_only")
    _print_stats("OLTP-only", stats_baseline)

    # ── Phase 2: OLTP + Analytics concurrently ───────────────────────────────
    print("\n  Phase 2 — HTAP: OLTP + Analytics concurrently...")
    stop_event = threading.Event()
    anal_thread = threading.Thread(
        target=_run_analytics_continuously,
        args=(cfg["tidb"], anal_pool, ANAL_CONC, stop_event, counts),
        daemon=True,
    )
    anal_thread.start()

    runner.run(oltp_pool, concurrency=OLTP_CONC,
               duration_sec=PHASE_SEC, phase="htap")
    stop_event.set()
    anal_thread.join(timeout=10)

    stats_htap = get_latency_stats(MODULE, phase="htap")
    _print_stats("HTAP (OLTP+Analytics)", stats_htap)

    # ── Analytical query standalone timing ───────────────────────────────────
    anal_stats  = get_latency_stats(MODULE, phase="analytics")
    htap_conn   = get_connection(cfg["tidb"])
    htap_cur    = htap_conn.cursor()
    tiflash_ok  = _check_tiflash_replication(htap_cur)
    htap_conn.close()

    degradation = 0.0
    if stats_baseline.get("p99_ms") and stats_htap.get("p99_ms"):
        degradation = (
            (stats_htap["p99_ms"] - stats_baseline["p99_ms"])
            / stats_baseline["p99_ms"] * 100
        )

    summary = {
        "oltp_only":    stats_baseline,
        "htap":         stats_htap,
        "analytics":    anal_stats,
        "p99_degradation_pct": round(degradation, 1),
        "tiflash_replicated": tiflash_ok,
    }
    status = "passed" if abs(degradation) < 30 else "warning"
    end_module(MODULE, status,
               f"OLTP p99 degradation under analytics load: {degradation:.1f}%")
    print(f"\n  OLTP p99 degradation with analytics: {degradation:+.1f}%")
    print(f"  TiFlash replicas active: {tiflash_ok}")
    return summary


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ensure_tiflash_replicas(cur):
    """Best-effort: set TiFlash replica for the main tables."""
    tables = ["transactions", "transaction_items", "events", "metrics"]
    for t in tables:
        try:
            cur.execute(f"ALTER TABLE `{t}` SET TIFLASH REPLICA 1")
            print(f"    TiFlash replica requested for {t}")
        except Exception as e:
            # Table may not exist or TiFlash not available in this cluster tier
            print(f"    TiFlash replica for {t}: {e}")


def _check_tiflash_replication(cur) -> bool:
    """Return True if at least one TiFlash replica is fully replicated."""
    try:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.TIFLASH_REPLICA
            WHERE PROGRESS = 1
        """)
        row = cur.fetchone()
        return (row[0] if row else 0) > 0
    except Exception:
        return False


def _run_analytics_continuously(tidb_cfg, pool, concurrency, stop_event, counts):
    """
    Worker function for the analytics thread pool.
    Runs analytical queries in a tight loop until stop_event is set.
    Results are logged to SQLite under phase='analytics'.
    """
    import concurrent.futures
    from load.workload_definitions import sample_query

    def anal_worker(_):
        conn = get_connection(tidb_cfg)
        cur  = conn.cursor()
        # Hint: route through TiFlash
        try:
            cur.execute("SET SESSION tidb_isolation_read_engines='tiflash,tikv'")
        except Exception:
            pass
        while not stop_event.is_set():
            sql, params_fn, qtype = sample_query(pool)
            params = params_fn(counts) if params_fn else ()
            res = execute_timed(cur, sql, params)
            log_result(
                module="04_htap_concurrent",
                latency_ms=res["latency_ms"],
                success=res["success"],
                phase="analytics",
                query_type=qtype,
            )
        conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(anal_worker, i) for i in range(concurrency)]
        for f in concurrent.futures.as_completed(futures):
            pass


def _print_stats(label, s):
    if not s:
        print(f"    {label}: no data")
        return
    print(f"    {label}: p50={s.get('p50_ms',0):.1f}ms  "
          f"p99={s.get('p99_ms',0):.1f}ms  TPS={s.get('tps',0):.0f}")


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
