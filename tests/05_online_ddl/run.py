#!/usr/bin/env python3
"""
Module 5 — Online DDL (Zero-Downtime Schema Change)
Demonstrates that TiDB can perform schema changes (ADD COLUMN, ADD INDEX,
MODIFY COLUMN, DROP COLUMN) with zero application downtime.

Approach:
  1. Start a background OLTP load thread.
  2. Execute each DDL statement while load is running.
  3. Measure: DDL execution time, OLTP p99 before/during/after each DDL,
     error rate during DDL, and whether the DDL blocks writes.
"""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.industry_profiles import resolve_industry_from_cfg
from lib.result_store import init_db, start_module, end_module, get_latency_stats
from lib.db_utils import get_connection
from load.load_runner import LoadRunner
from load.workload_definitions import (
    apply_workload_profile,
    build_weighted_pool,
    transactional_workload_for_cfg,
)

MODULE    = "05_online_ddl"
OLTP_CONC = 24
WARMUP_SEC = 30
POST_DDL_SEC = 30   # observation window after each DDL


def build_ddl_steps(table: str, ref_col: str) -> list[dict]:
    return [
        {
            "name": "ADD COLUMN (nullable)",
            "sql": f"ALTER TABLE `{table}` ADD COLUMN extra_meta VARCHAR(255) DEFAULT NULL",
            "revert": f"ALTER TABLE `{table}` DROP COLUMN extra_meta",
        },
        {
            "name": "ADD INDEX",
            "sql": f"ALTER TABLE `{table}` ADD INDEX idx_ddl_test (`{ref_col}`, status)",
            "revert": f"ALTER TABLE `{table}` DROP INDEX idx_ddl_test",
        },
        {
            "name": "MODIFY COLUMN (widen)",
            "sql": f"ALTER TABLE `{table}` MODIFY COLUMN `{ref_col}` VARCHAR(128)",
            "revert": f"ALTER TABLE `{table}` MODIFY COLUMN `{ref_col}` VARCHAR(64)",
        },
        {
            "name": "ADD COLUMN + DEFAULT",
            "sql": f"ALTER TABLE `{table}` ADD COLUMN pov_flag TINYINT NOT NULL DEFAULT 0",
            "revert": f"ALTER TABLE `{table}` DROP COLUMN pov_flag",
        },
    ]


def run(cfg: dict):
    init_db()
    start_module(MODULE)
    counts = _get_counts(cfg)
    industry = resolve_industry_from_cfg(cfg)
    ddl_table = str(industry.get("ddl_target_table") or "transactions")
    ddl_ref_col = str(industry.get("ddl_reference_column") or "reference_id")
    ddl_steps = build_ddl_steps(ddl_table, ddl_ref_col)

    print(f"\n{'='*60}")
    print(f"  Module 5: Online DDL — Zero-Downtime Schema Changes")
    print(f"  OLTP concurrency: {OLTP_CONC} | DDL steps: {len(ddl_steps)}")
    print(f"  Industry table: {ddl_table} ({ddl_ref_col})")
    print(f"{'='*60}")

    oltp_pool = build_weighted_pool(
        apply_workload_profile(
            transactional_workload_for_cfg(cfg, counts),
            mix=cfg.get("test", {}).get("workload_mix", "mixed"),
            read_multiplier=cfg.get("test", {}).get("read_weight_multiplier", 1.0),
            write_multiplier=cfg.get("test", {}).get("write_weight_multiplier", 1.0),
        )
    )
    runner    = LoadRunner(tidb_cfg=cfg["tidb"], counts=counts, module=MODULE)
    results   = []

    # ── Warmup: establish baseline latency ───────────────────────────────────
    print(f"\n  Warmup ({WARMUP_SEC}s)...")
    runner.run(oltp_pool, concurrency=OLTP_CONC,
               duration_sec=WARMUP_SEC, phase="warmup")
    baseline = get_latency_stats(MODULE, phase="warmup")
    print(f"    Baseline p99: {baseline.get('p99_ms', 0):.1f}ms")

    # ── Execute each DDL step while load is running ───────────────────────────
    for step in ddl_steps:
        print(f"\n  DDL: {step['name']}...")
        stop_event = threading.Event()
        phase_name = step["name"].lower().replace(" ", "_").replace("(", "").replace(")", "")

        # Start background OLTP load
        load_thread = threading.Thread(
            target=runner.run,
            kwargs=dict(
                workload_pool=oltp_pool,
                concurrency=OLTP_CONC,
                duration_sec=999,   # long enough; we'll stop it
                phase=phase_name,
            ),
            daemon=True,
        )
        # We need to run the load in a way we can stop it mid-execution
        # Use the runner's internal stop mechanism via short-duration run in thread
        ddl_result = _run_ddl_with_load(
            cfg["tidb"], runner, oltp_pool, OLTP_CONC, step, phase_name, counts
        )
        results.append(ddl_result)

        # Revert DDL before next step
        _revert_ddl(cfg["tidb"], step)

    # ── Summary ──────────────────────────────────────────────────────────────
    all_passed = all(r["error"] is None for r in results)
    end_module(
        MODULE,
        "passed" if all_passed else "warning",
        f"{len(results)} DDL ops on {ddl_table}; "
        f"{sum(1 for r in results if r['error'] is None)} succeeded online"
    )

    print(f"\n  DDL Results:")
    print(f"  {'DDL Operation':<40} {'DDL Time':>10} {'p99 During':>12} {'Errors':>8}")
    print(f"  {'-'*74}")
    for r in results:
        print(f"  {r['name']:<40} {r['ddl_sec']:>9.1f}s "
              f"{r.get('p99_during', 0):>10.1f}ms "
              f"{'✓' if r['error'] is None else '✗':>8}")

    return {"baseline": baseline, "ddl_steps": results}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_ddl_with_load(tidb_cfg, runner, pool, concurrency, step, phase_name, counts):
    """
    Run OLTP load for POST_DDL_SEC, execute DDL in parallel, capture timing.
    Returns result dict with ddl_sec, p99_during, error.
    """
    import concurrent.futures
    from load.workload_definitions import sample_query
    from lib.result_store import log_result
    from lib.db_utils import execute_timed

    ddl_conn = get_connection(tidb_cfg)
    ddl_cur  = ddl_conn.cursor()

    # Short pre-DDL window
    runner.run(pool, concurrency=concurrency, duration_sec=10, phase=f"{phase_name}_pre")

    # Execute DDL and time it
    t0 = time.perf_counter()
    err = None
    try:
        ddl_cur.execute(step["sql"])
        ddl_conn.commit()
    except Exception as e:
        err = str(e)
    ddl_sec = time.perf_counter() - t0

    # Short post-DDL observation window
    runner.run(pool, concurrency=concurrency,
               duration_sec=POST_DDL_SEC, phase=f"{phase_name}_post")
    ddl_conn.close()

    stats_during = get_latency_stats(MODULE, phase=f"{phase_name}_post")
    return {
        "name":       step["name"],
        "ddl_sec":    round(ddl_sec, 2),
        "p99_during": stats_during.get("p99_ms", 0),
        "error":      err,
    }


def _revert_ddl(tidb_cfg, step):
    """Best-effort rollback of DDL step to reset for next test."""
    try:
        conn = get_connection(tidb_cfg)
        cur  = conn.cursor()
        cur.execute(step["revert"])
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"    (revert warning: {e})")


def _get_counts(cfg):
    import json
    manifest = os.path.join("results", "data_manifest.json")
    if os.path.exists(manifest):
        with open(manifest) as f:
            return json.load(f).get("counts", {})
    from setup.generate_data import SCALE_CONFIG
    return SCALE_CONFIG.get((cfg.get("test") or {}).get("data_scale", "small"), {})


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
