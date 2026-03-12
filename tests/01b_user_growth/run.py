#!/usr/bin/env python3
"""
Module 1b — User Growth Ramp
Simulates user-base growth by progressively expanding the active user pool and
measuring how TPS and p99 latency scale.

Active-user steps by data scale:
  small  : 1 → 100 → 1,000
  medium : 1 → 1,000 → 10,000
  large  : 1 → 10,000 → 50,000

Each step issues the same transactional workload but restricts random ID
selection to the current active-user window so the query set mirrors real
user-growth behaviour rather than always saturating the full table.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, get_latency_stats
from load.load_runner import LoadRunner
from load.workload_definitions import (
    apply_workload_profile,
    build_weighted_pool,
    transactional_workload_for_cfg,
)

MODULE = "01b_user_growth"

# Active-user steps per scale tier (users table row IDs 1..N will be used)
USER_GROWTH_STEPS = {
    "small":  [1, 100, 1_000],
    "medium": [1, 1_000, 10_000],
    "large":  [1, 10_000, 50_000],
}


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    full_counts = _get_counts(cfg)
    scale = str((cfg.get("test") or {}).get("data_scale", "small")).strip().lower()
    if scale not in USER_GROWTH_STEPS:
        scale = "small"

    steps = USER_GROWTH_STEPS[scale]
    test_cfg = cfg.get("test") or {}
    duration = max(30, int(test_cfg.get("user_growth_duration_seconds",
                                         test_cfg.get("duration_seconds", 120))))
    concurrency = max(4, int(test_cfg.get("user_growth_concurrency",
                                           test_cfg.get("concurrency_levels", [16])[0])))

    print(f"\n{'='*60}")
    print(f"  Module 1b: User Growth Ramp")
    print(f"  Scale: {scale.upper()}")
    print(f"  Active-user steps: {steps}")
    print(f"  Concurrency: {concurrency} | Duration per step: {duration}s")
    print(f"{'='*60}")

    summary = {}
    any_success = False

    for active_users in steps:
        phase = f"ug_{active_users}"

        # Cap active users to actual table size
        capped_users = min(active_users, full_counts.get("users", active_users))
        # Scale accounts proportionally
        user_ratio = capped_users / max(1, full_counts.get("users", capped_users))
        capped_accounts = max(1, int(full_counts.get("accounts", capped_users) * user_ratio))
        capped_transactions = max(1, int(full_counts.get("transactions", capped_users * 10) * user_ratio))

        step_counts = dict(full_counts)
        step_counts["users"] = capped_users
        step_counts["accounts"] = capped_accounts
        step_counts["transactions"] = capped_transactions

        runner = LoadRunner(
            tidb_cfg=cfg["tidb"],
            counts=step_counts,
            module=MODULE,
        )

        workload = apply_workload_profile(
            transactional_workload_for_cfg(cfg, step_counts),
            mix=test_cfg.get("workload_mix", "mixed"),
        )
        pool = build_weighted_pool(workload)

        print(f"\n  Step: {active_users:,} active users "
              f"(accounts: {capped_accounts:,}, transactions: {capped_transactions:,})")
        runner.run(pool, concurrency=concurrency, duration_sec=duration, phase=phase)

        tidb_stats = get_latency_stats(MODULE, phase=phase, db_label="tidb")
        if tidb_stats.get("count", 0) > 0:
            any_success = True
        summary[phase] = {
            "active_users": capped_users,
            "tidb": tidb_stats,
        }

    _print_summary(summary)
    if any_success:
        end_module(MODULE, "passed",
                   f"Scale={scale}; steps={steps}; concurrency={concurrency}; duration={duration}s/step")
    else:
        end_module(MODULE, "failed",
                   "No successful user-growth queries recorded. Check database/schema settings.")
    return summary


def _print_summary(summary):
    print(f"\n  {'Active Users':<16} {'TPS':>8} {'p50':>8} {'p95':>8} {'p99':>8}")
    print(f"  {'-'*55}")
    for phase, data in summary.items():
        s = data.get("tidb", {})
        print(f"  {data['active_users']:<16,} {s.get('tps', 0):>8.1f} "
              f"{s.get('p50_ms', 0):>8.1f} {s.get('p95_ms', 0):>8.1f} "
              f"{s.get('p99_ms', 0):>8.1f}")


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
