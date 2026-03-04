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
from load.workload_definitions import apply_workload_profile, schema_a_workload, build_weighted_pool

MODULE = "02_elastic_scale"


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    counts = _get_counts(cfg)
    base_concurrency = cfg["test"].get("concurrency_levels", [16, 64, 256])[0]
    peak_concurrency = base_concurrency * 4
    ramp_sec = cfg["test"].get("ramp_duration_seconds", 1200)
    sustain_sec = 300

    print(f"\n{'='*60}")
    print(f"  Module 2: Elastic Scale + Headroom Test")
    print(f"  Base concurrency: {base_concurrency} → Peak: {peak_concurrency}")
    print(f"  Ramp duration: {ramp_sec}s | Sustain: {sustain_sec}s")
    print(f"{'='*60}")

    workload = apply_workload_profile(
        schema_a_workload(counts),
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
        sustain_sec=120,
        phase="ramp_down",
    )

    ts_data = get_time_series(MODULE, bucket_sec=30)
    end_module(MODULE, "passed",
               f"Ramped from {base_concurrency} to {peak_concurrency} threads over {ramp_sec}s")
    print(f"\n  Complete — {len(ts_data)} time buckets captured.")
    return {"time_series": ts_data, "annotations": annotations}


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
