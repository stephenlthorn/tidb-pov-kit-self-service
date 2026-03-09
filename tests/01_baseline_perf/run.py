#!/usr/bin/env python3
"""
Module 1 — Baseline Performance
Runs OLTP workload at multiple concurrency levels against TiDB (and optionally
a comparison DB), capturing TPS and latency percentiles.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.comparison_targets import comparison_can_run, comparison_reason, normalize_comparison_cfg, target_label
from lib.result_store import init_db, start_module, end_module, get_latency_stats
from load.load_runner import LoadRunner
from load.workload_definitions import (
    apply_workload_profile,
    build_weighted_pool,
    transactional_workload_for_cfg,
)

MODULE = "01_baseline_perf"


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    counts = _get_counts(cfg)
    test_cfg = cfg.get("test") or {}
    concurrency_levels = test_cfg.get("concurrency_levels", [16, 64, 256])
    duration = test_cfg.get("duration_seconds", 300)
    pre_warm_enabled = bool(test_cfg.get("pre_warm_enabled", True))
    pre_warm_duration = max(30, int(test_cfg.get("pre_warm_duration_seconds", 120)))
    pre_warm_concurrency = max(1, int(test_cfg.get("pre_warm_concurrency", (concurrency_levels or [16])[0])))
    warm_enabled = bool(test_cfg.get("warm_phase_enabled", True))
    warm_duration = max(30, int(test_cfg.get("warm_phase_duration_seconds", max(300, duration))))
    warm_concurrency = max(1, int(test_cfg.get("warm_phase_concurrency", max(concurrency_levels or [16]))))
    customer_queries = cfg.get("customer_queries", [])
    customer_ratio = cfg.get("customer_query_ratio", 0.3)

    comparison_cfg = normalize_comparison_cfg(cfg.get("comparison_db") or {})
    has_comparison = comparison_can_run(comparison_cfg)
    comparison_label = comparison_cfg.get("label") or target_label(comparison_cfg.get("target", "aurora_mysql"))

    runner = LoadRunner(
        tidb_cfg=cfg["tidb"],
        counts=counts,
        module=MODULE,
        comparison_cfg=comparison_cfg if has_comparison else None,
        comparison_label=comparison_label,
    )

    workload = apply_workload_profile(
        transactional_workload_for_cfg(cfg, counts),
        mix=cfg.get("test", {}).get("workload_mix", "mixed"),
        read_multiplier=cfg.get("test", {}).get("read_weight_multiplier", 1.0),
        write_multiplier=cfg.get("test", {}).get("write_weight_multiplier", 1.0),
    )
    pool = build_weighted_pool(workload)

    print(f"\n{'='*60}")
    print(f"  Module 1: Baseline Performance")
    print(f"  Concurrency levels: {concurrency_levels}")
    print(f"  Duration per level: {duration}s")
    print(
        "  Workload profile: "
        f"{test_cfg.get('workload_mix', 'mixed')} "
        f"(read x{test_cfg.get('read_weight_multiplier', 1.0)}, "
        f"write x{test_cfg.get('write_weight_multiplier', 1.0)})"
    )
    if has_comparison:
        print(f"  Comparison DB: {comparison_label}")
    elif comparison_cfg.get("enabled"):
        print(f"  Comparison DB disabled for run: {comparison_reason(comparison_cfg)}")
    if pre_warm_enabled:
        print(f"  Pre-warm phase: enabled ({pre_warm_concurrency} threads, {pre_warm_duration}s)")
    else:
        print("  Pre-warm phase: disabled")
    if warm_enabled:
        print(f"  Warm workload phase: enabled ({warm_concurrency} threads, {warm_duration}s)")
    else:
        print("  Warm workload phase: disabled")
    print(f"{'='*60}")

    summary = {}
    any_success = False
    if pre_warm_enabled:
        print(f"\n  Pre-warming dataset: concurrency={pre_warm_concurrency}, duration={pre_warm_duration}s")
        runner.run(
            pool,
            concurrency=pre_warm_concurrency,
            duration_sec=pre_warm_duration,
            phase="pre_warm",
            customer_queries=customer_queries,
            customer_ratio=customer_ratio,
        )
    for c in concurrency_levels:
        phase = f"c{c}"
        runner.run(pool, concurrency=c, duration_sec=duration, phase=phase,
                   customer_queries=customer_queries, customer_ratio=customer_ratio)
        tidb_stats = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        if tidb_stats.get("count", 0) > 0:
            any_success = True
        summary[phase] = {
            "concurrency": c,
            "tidb": tidb_stats,
        }
        if has_comparison:
            summary[phase]["comparison"] = get_latency_stats(
                MODULE, phase=phase, db_label=comparison_label)

    if warm_enabled:
        phase = "warm_steady"
        print(f"\n  Warm steady-state run: concurrency={warm_concurrency}, duration={warm_duration}s")
        runner.run(
            pool,
            concurrency=warm_concurrency,
            duration_sec=warm_duration,
            phase=phase,
            customer_queries=customer_queries,
            customer_ratio=customer_ratio,
        )
        tidb_stats = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        if tidb_stats.get("count", 0) > 0:
            any_success = True
        summary[phase] = {
            "concurrency": warm_concurrency,
            "tidb": tidb_stats,
        }
        if has_comparison:
            summary[phase]["comparison"] = get_latency_stats(
                MODULE, phase=phase, db_label=comparison_label
            )

    _print_summary(summary)
    if any_success:
        end_module(MODULE, "passed")
    else:
        end_module(
            MODULE,
            "failed",
            "No successful baseline queries were recorded. Check database/schema and connection settings.",
        )
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
    scale = (cfg.get("test") or {}).get("data_scale", "small")
    from setup.generate_data import SCALE_CONFIG
    return SCALE_CONFIG.get(scale, SCALE_CONFIG["small"])


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
