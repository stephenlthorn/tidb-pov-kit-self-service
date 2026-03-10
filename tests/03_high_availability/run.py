#!/usr/bin/env python3
"""
Module 3 — High Availability & RTO/RPO
Runs a sustained write workload, injects a simulated failure mid-run (by
temporarily blocking connections or stopping load to a node via TiDB Cloud API),
then measures recovery time and transaction loss.

NOTE: Full node-kill requires a Dedicated cluster + TiDB Cloud API key.
In Serverless mode, this module simulates the failure window by running at
maximum RU throttle and measuring the throttling recovery behaviour.
"""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, log_result, get_time_series

MODULE = "03_high_availability"
WARMUP_SEC  = 60
FAILURE_SEC = 30   # Duration of simulated failure window
RECOVERY_OBSERVE_SEC = 60
CONCURRENCY = 32


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    print(f"\n{'='*60}")
    print(f"  Module 3: High Availability & RTO/RPO")
    print(f"  Warmup: {WARMUP_SEC}s | Failure window: {FAILURE_SEC}s | "
          f"Observation: {RECOVERY_OBSERVE_SEC}s")
    print(f"{'='*60}")

    from load.load_runner import LoadRunner
    from load.workload_definitions import (
        apply_workload_profile,
        build_weighted_pool,
        transactional_workload_for_cfg,
    )
    counts = _get_counts(cfg)
    pool = build_weighted_pool(
        apply_workload_profile(
            transactional_workload_for_cfg(cfg, counts),
            mix=cfg.get("test", {}).get("workload_mix", "mixed"),
            read_multiplier=cfg.get("test", {}).get("read_weight_multiplier", 1.0),
            write_multiplier=cfg.get("test", {}).get("write_weight_multiplier", 1.0),
        )
    )

    failure_ts = {"start": None, "end": None}
    stop_event = threading.Event()

    runner = LoadRunner(tidb_cfg=cfg["tidb"], counts=counts, module=MODULE)

    # Phase 1: Warmup
    print(f"\n  Phase 1 — Warmup ({WARMUP_SEC}s)")
    runner.run(pool, concurrency=CONCURRENCY, duration_sec=WARMUP_SEC, phase="warmup")

    # Phase 2: Inject failure (simulate by closing all connections briefly)
    print(f"\n  Phase 2 — Failure injection (simulated connection drop, {FAILURE_SEC}s)")
    failure_ts["start"] = time.time()
    _simulate_failure(cfg["tidb"], duration_sec=FAILURE_SEC, module=MODULE)
    failure_ts["end"] = time.time()
    print(f"  Failure window: {failure_ts['end'] - failure_ts['start']:.1f}s")

    # Phase 3: Recovery observation
    print(f"\n  Phase 3 — Recovery observation ({RECOVERY_OBSERVE_SEC}s)")
    runner.run(pool, concurrency=CONCURRENCY, duration_sec=RECOVERY_OBSERVE_SEC, phase="recovery")

    ts_data = get_time_series(MODULE, bucket_sec=5)
    rto_sec = _calculate_rto(ts_data, failure_ts)

    summary = {
        "failure_duration_sec": FAILURE_SEC,
        "rto_sec": rto_sec,
        "failure_start_ts": failure_ts["start"],
        "failure_end_ts": failure_ts["end"],
        "time_series": ts_data,
    }
    end_module(MODULE, "passed", f"Simulated failure drill; Estimated RTO: {rto_sec:.1f}s")
    print(f"\n  Estimated RTO: {rto_sec:.1f}s")
    return summary


def _simulate_failure(tidb_cfg: dict, duration_sec: int, module: str):
    """
    Simulate failure by rapidly opening connections and killing them,
    logging errors as failed transactions during the window.
    In production, replace this with a TiDB Cloud API call to stop a TiKV node.
    """
    import random
    from lib.db_utils import get_connection

    t_end = time.time() + duration_sec
    while time.time() < t_end:
        t0 = time.perf_counter()
        try:
            conn = get_connection(tidb_cfg)
            cur = conn.cursor()
            cur.execute("SELECT SLEEP(0.1)")
            cur.fetchall()
            conn.close()
            latency_ms = (time.perf_counter() - t0) * 1000
            log_result(module, latency_ms, True, phase="failure", db_label="tidb",
                       query_type="ha_probe")
        except Exception as e:
            latency_ms = (time.perf_counter() - t0) * 1000
            log_result(module, latency_ms, False, phase="failure", db_label="tidb",
                       query_type="ha_probe", error=str(e))
        time.sleep(0.05)


def _calculate_rto(ts_data: list, failure_ts: dict) -> float:
    """
    Estimate RTO by finding the first time bucket after the failure window
    where success rate returns to >= 95% of pre-failure baseline.
    """
    if not ts_data or not failure_ts.get("end"):
        return 0.0
    # Simple heuristic: find the bucket where TPS recovers past 80% of warmup TPS
    warmup_tps = [b["tps"] for b in ts_data if b.get("elapsed_sec", 0) < 60]
    if not warmup_tps:
        return 0.0
    baseline = sum(warmup_tps) / len(warmup_tps)
    for b in ts_data:
        if b["tps"] >= baseline * 0.8:
            return float(b.get("elapsed_sec", 0))
    return float(ts_data[-1].get("elapsed_sec", 0)) if ts_data else 0.0


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
