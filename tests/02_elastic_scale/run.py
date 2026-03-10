#!/usr/bin/env python3
"""
Module 2 — Elastic Scale + Headroom Test
Ramps load from baseline → 4× peak, sustains, then ramps back down.
Annotates scale events on the time-series chart.
"""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, get_time_series
from load.load_runner import RampRunner
from load.workload_definitions import (
    apply_workload_profile,
    build_weighted_pool,
    transactional_workload_for_cfg,
)

MODULE = "02_elastic_scale"


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    counts = _get_counts(cfg)
    test_cfg = cfg.get("test") or {}
    base_concurrency = test_cfg.get("concurrency_levels", [16, 64, 256])[0]
    peak_concurrency = base_concurrency * 4
    ramp_sec = int(test_cfg.get("ramp_duration_seconds", 1200) or 1200)
    sustain_sec = int(test_cfg.get("ramp_sustain_seconds", 300) or 300)
    ramp_down_sustain_sec = int(test_cfg.get("ramp_down_sustain_seconds", 120) or 120)

    print(f"\n{'='*60}")
    print(f"  Module 2: Elastic Scale + Headroom Test")
    print(f"  Base concurrency: {base_concurrency} → Peak: {peak_concurrency}")
    print(
        "  Ramp duration: "
        f"{ramp_sec}s | Sustain: {sustain_sec}s | Ramp-down sustain: {ramp_down_sustain_sec}s"
    )
    print(f"{'='*60}")

    workload = apply_workload_profile(
        transactional_workload_for_cfg(cfg, counts),
        mix=cfg.get("test", {}).get("workload_mix", "mixed"),
        read_multiplier=cfg.get("test", {}).get("read_weight_multiplier", 1.0),
        write_multiplier=cfg.get("test", {}).get("write_weight_multiplier", 1.0),
    )
    pool = build_weighted_pool(workload)

    runner = RampRunner(tidb_cfg=cfg["tidb"], counts=counts, module=MODULE)

    # Phase A+B: ramp up then sustain
    print("\n  Phase A — Ramp up...")
    annotations = runner.run_ramp(
        workload_pool=pool,
        start_concurrency=base_concurrency,
        end_concurrency=peak_concurrency,
        ramp_sec=ramp_sec,
        sustain_sec=sustain_sec,
        phase="ramp_up",
    )

    # Phase C: ramp back down
    print("\n  Phase D — Ramp down...")
    runner.run_ramp(
        workload_pool=pool,
        start_concurrency=peak_concurrency,
        end_concurrency=base_concurrency,
        ramp_sec=ramp_sec // 2,
        sustain_sec=ramp_down_sustain_sec,
        phase="ramp_down",
    )

    ts_data = get_time_series(MODULE, bucket_sec=30)
    bucket_count = len(ts_data)
    if bucket_count <= 0:
        note = (
            "No elastic-scale time buckets captured. Increase "
            "test.ramp_duration_seconds and/or test.ramp_sustain_seconds for chart evidence."
        )
        end_module(MODULE, "warning", note)
        print(f"\n  Complete — {bucket_count} time buckets captured.")
        print(f"  Warning: {note}")
    else:
        end_module(
            MODULE,
            "passed",
            f"Ramped from {base_concurrency} to {peak_concurrency} threads over {ramp_sec}s",
        )
        print(f"\n  Complete — {bucket_count} time buckets captured.")
    return {"time_series": ts_data, "annotations": annotations}


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
