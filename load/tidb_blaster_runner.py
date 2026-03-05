#!/usr/bin/env python3
"""Background runner entrypoint for Workload Generator actions from the web UI."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from load.tidb_blaster import (  # noqa: E402
    MODES,
    create_run_dir,
    latest_run_dir,
    list_recent_runs,
    normalize_blaster_config,
    regenerate_report,
    run_blaster,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Workload Generator actions")
    p.add_argument("--config", default="config.yaml", help="Path to PoV config YAML")
    p.add_argument("--action", choices=["validate", "dry_run", "run", "report"], default="run")
    p.add_argument("--mode", choices=list(MODES), default=None)
    p.add_argument("--tag", default="ui")
    p.add_argument("--run", dest="run_dir", default="", help="Run directory for report action")
    return p.parse_args()


def load_config(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (ROOT / config_path).resolve()

    cfg = load_config(config_path)
    blaster_raw = ((cfg.get("workload_lab") or {}).get("blaster") or {})
    resolved = normalize_blaster_config(blaster_raw, cfg.get("tidb") or {}, mode=args.mode, tag=args.tag)

    if args.action == "report":
        if args.run_dir:
            run_dir = Path(args.run_dir)
            if not run_dir.is_absolute():
                run_dir = (ROOT / run_dir).resolve()
        else:
            run_dir = latest_run_dir()
            if run_dir is None:
                print("[workload-generator] no prior runs found; cannot build report")
                return 2

        print(f"[workload-generator] rebuilding report for run: {run_dir}")
        summary = regenerate_report(run_dir)
        print(json.dumps(summary, indent=2))
        return 0

    run_dir = create_run_dir(resolved["mode"], resolved["tag"])
    print(f"[workload-generator] action={args.action} mode={resolved['mode']} tag={resolved['tag']}")
    print(f"[workload-generator] run_dir={run_dir}")

    if args.action == "validate":
        summary = run_blaster(resolved, run_dir, execute=False)
        print("[workload-generator] validation + dry-run command plan complete")
        print(json.dumps(summary.get("validation", {}), indent=2))
        return 0 if summary.get("validation_ok") else 1

    if args.action == "dry_run":
        summary = run_blaster(resolved, run_dir, execute=False)
        print("[workload-generator] dry-run complete (commands captured; no load executed)")
        print(json.dumps(summary, indent=2))
        return 0

    print("[workload-generator] executing loadgen plan")
    summary = run_blaster(resolved, run_dir, execute=True)
    print("[workload-generator] run complete")
    print(json.dumps(summary, indent=2))
    return 0 if summary.get("status") == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
