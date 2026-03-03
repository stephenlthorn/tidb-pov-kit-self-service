#!/usr/bin/env python3
"""
Module 1 — Baseline Performance
Runs OLTP workload at multiple concurrency levels against TiDB (and optionally
a comparison DB), capturing TPS and latency percentiles.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, get_latency_stats
from load.load_runner import LoadRunner
from load.workload_definitions import schema_a_workload, build_weighted_pool

MODULE = "01_baseline_perf"


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    counts = _get_counts(cfg)
    concurrency_levels = cfg["test"].get("concurrency_levels", [16, 64, 256])
    duration = cfg["test"].get("duration_seconds", 300)
    customer_queries = cfg.get("customer_queries", [])
    customer_ratio = cfg.get("customer_query_ratio", 0.3)

    comparison_cfg = cfg.get("comparison_db") or {}
    has_comparison = bool(comparison_cfg.get("host"))

    runner = LoadRunner(
        tidb_cfg=cfg["tidb"],
        counts=counts,
        module=MODULE,
        comparison_cfg=comparison_cfg if has_comparison else None,
        comparison_label=comparison_cfg.get("label", "comparison"),
    )

    workload = schema_a_workload(counts)
    pool = build_weighted_pool(workload)

    print(f"\n{'='*60}")
    print(f"  Module 1: Baseline Performance")
    print(f"  Concurrency levels: {concurrency_levels}")
    print(f"  Duration per level: {duration}s")
    if has_comparison:
        print(f"  Comparison DB: {comparison_cfg.get('label', 'comparison')}")
    print(f"{'='*60}")

    summary = {}
    for c in concurrency_levels:
        phase = f"c{c}"
        runner.run(pool, concurrency=c, duration_sec=duration, phase=phase,
                   customer_queries=customer_queries, customer_ratio=customer_ratio)
        summary[phase] = {
            "concurrency": c,
            "tidb": get_latency_stats(MODULE, phase=phase, db_label="tidb"),
        }
        if has_comparison:
            summary[phase]["comparison"] = get_latency_stats(
                MODULE, phase=phase, db_label=comparison_cfg.get("label", "comparison"))

    _print_summary(summary)
    end_module(MODULE, "passed")
    return summary


def _print_summary(summary):
    print(f"\n  {'Concurrency':<14} {'TPS':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'Label'}")
    print(f"  {'-'*60}")
    for phase, data in summary.items():
        for label, stats in [("TiDB", data.get("tidb", {})),
                              ("Comparison", data.get("comparison", {}))]:
            if not stats:
                continue
            print(f"  {data['concurrency']:<14} {stats.get('tps',0):>8.1f} "
                  f"{stats.get('p50_ms',0):>8.1f} {stats.get('p95_ms',0):>8.1f} "
                  f"{stats.get('p99_ms',0):>8.1f} {label}")


def _get_counts(cfg):
    import json, os
    manifest = os.path.join("results", "data_manifest.json")
    if os.path.exists(manifest):
        with open(manifest) as f:
            return json.load(f).get("counts", {})
    scale = cfg["test"].get("data_scale", "medium")
    from setup.generate_data import SCALE_CONFIG
    return SCALE_CONFIG.get(scale, SCALE_CONFIG["medium"])


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
