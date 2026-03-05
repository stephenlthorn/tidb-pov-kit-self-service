#!/usr/bin/env python3
"""
collect_metrics.py — Aggregates all results from results.db and
data_manifest.json into a single JSON payload used by generate_report.py.

Run standalone:
    python report/collect_metrics.py > results/metrics_summary.json
"""
import sys, os, json, time, argparse, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.result_store import (
    _conn, get_latency_stats, get_time_series,
    DB_PATH
)

MODULES = [
    "00_customer_queries",
    "01_baseline_perf",
    "02_elastic_scale",
    "03_high_availability",
    "03b_write_contention",
    "04_htap_concurrent",
    "05_online_ddl",
    "06_mysql_compat",
    "07_data_import",
    "08_vector_search",
]


def collect() -> dict:
    """Return a fully aggregated metrics dict."""
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "modules":      {},
        "summary":      {},
        "data_manifest": _load_manifest(),
        "comparison_enabled": False,
        "comparison_label": "Comparison DB",
    }

    with _conn() as c:
        # Module meta (status, duration)
        meta_rows = c.execute(
            "SELECT module, status, start_ts, end_ts, notes FROM module_meta"
        ).fetchall()
        module_meta = {r["module"]: dict(r) for r in meta_rows}

        # Compat checks
        compat_rows = c.execute(
            "SELECT check_name, status, note FROM compat_checks"
        ).fetchall()

        # Import stats
        import_rows = c.execute(
            "SELECT ts, rows_imported, gb_imported, duration_sec, throughput_gbpm "
            "FROM import_stats ORDER BY ts"
        ).fetchall()

        # Detect if comparison DB results exist and keep the dominant label.
        comp_rows = c.execute(
            "SELECT db_label, COUNT(*) AS n "
            "FROM results "
            "WHERE db_label IS NOT NULL AND db_label <> 'tidb' "
            "GROUP BY db_label ORDER BY n DESC"
        ).fetchall()
        if comp_rows:
            payload["comparison_enabled"] = True
            payload["comparison_label"] = comp_rows[0]["db_label"] or "Comparison DB"

    for mod in MODULES:
        meta   = module_meta.get(mod, {})
        status = meta.get("status", "not_run")
        dur    = 0
        if meta.get("start_ts") and meta.get("end_ts"):
            dur = max(0, meta["end_ts"] - meta["start_ts"])

        entry = {
            "status":       status,
            "duration_sec": round(dur, 1),
            "notes":        meta.get("notes"),
            "tidb":         {},
        }

        # Per-phase latency stats for TiDB
        phases = _get_phases_for_module(mod)
        for phase in phases:
            stats = get_latency_stats(mod, phase=phase, db_label="tidb")
            if stats:
                entry["tidb"][phase] = stats

        # Overall (all phases combined) if no explicit phases
        if not entry["tidb"]:
            stats = get_latency_stats(mod, db_label="tidb")
            if stats:
                entry["tidb"]["overall"] = stats

        # Comparison DB stats
        if payload["comparison_enabled"]:
            entry["comparison"] = {}
            comp_label = payload["comparison_label"]
            for phase in phases:
                stats = get_latency_stats(mod, phase=phase, db_label=comp_label)
                if stats:
                    entry["comparison"][phase] = stats

        # Time-series data (for chart rendering)
        entry["time_series"] = {}
        for phase in (phases or [None]):
            ts_data = get_time_series(mod, bucket_sec=10, phase=phase)
            if ts_data:
                entry["time_series"][phase or "overall"] = ts_data

        payload["modules"][mod] = entry

    # Compat check summary
    compat_list = [dict(r) for r in compat_rows]
    passed = sum(1 for r in compat_list if r["status"] == "pass")
    payload["compat_checks"] = {
        "total":   len(compat_list),
        "passed":  passed,
        "failed":  len(compat_list) - passed,
        "pct":     round(passed / len(compat_list) * 100, 1) if compat_list else 0,
        "details": compat_list,
    }

    # Import stats summary
    imp_list = [dict(r) for r in import_rows]
    payload["import_stats"] = imp_list

    # High-level summary card values
    payload["summary"] = _build_summary(payload)

    return payload


def _get_phases_for_module(mod: str) -> list:
    """Return ordered phase names per module, preferring actual observed phases."""
    phase_map = {
        "01_baseline_perf": ["c8", "c16", "c32", "c64", "warm_steady"],
        "02_elastic_scale": ["ramp_up", "sustain", "ramp_down"],
        "03_high_availability": ["warmup", "during_failure", "recovery"],
        "03b_write_contention": ["sequential", "autorand"],
        "04_htap_concurrent": ["oltp_only", "htap", "analytics"],
        "05_online_ddl": [],  # phases are dynamic DDL step names
        "06_mysql_compat": [],
        "07_data_import": [],
        "08_vector_search": ["ann_conc1", "ann_conc4", "ann_conc8", "ann_conc16", "hybrid"],
    }
    default_phases = phase_map.get(mod, [])
    observed = _get_observed_phases(mod)
    if not observed:
        return default_phases
    return _order_phases(mod, observed, default_phases)


def _get_observed_phases(mod: str) -> list:
    with _conn() as c:
        rows = c.execute(
            "SELECT DISTINCT phase FROM results WHERE module=? AND phase IS NOT NULL AND TRIM(phase) <> ''",
            (mod,),
        ).fetchall()
    return [str(r["phase"]) for r in rows if r["phase"] is not None]


def _order_phases(mod: str, observed: list, defaults: list) -> list:
    seen = set()
    ordered = []
    for phase in observed:
        if phase not in seen:
            ordered.append(phase)
            seen.add(phase)

    if mod == "01_baseline_perf":
        def baseline_key(phase: str):
            m = re.fullmatch(r"c(\d+)", phase)
            if m:
                return (0, int(m.group(1)))
            if phase == "warm_steady":
                return (1, 0)
            return (2, phase)

        return sorted(ordered, key=baseline_key)

    default_rank = {name: idx for idx, name in enumerate(defaults)}
    return sorted(ordered, key=lambda p: (0, default_rank[p]) if p in default_rank else (1, p))


def _build_summary(payload: dict) -> dict:
    """Derive top-level KPI cards for the executive summary page."""
    modules      = payload["modules"]
    total_mods   = len([m for m in MODULES if modules.get(m, {}).get("status") != "not_run"])
    passed_mods  = len([m for m in MODULES if modules.get(m, {}).get("status") == "passed"])

    # Best p99 from baseline OLTP
    baseline = modules.get("01_baseline_perf", {}).get("tidb", {})
    best_p99 = min(
        (v.get("p99_ms", 9999) for v in baseline.values() if isinstance(v, dict)),
        default=None
    )
    best_tps = max(
        (v.get("tps", 0) for v in baseline.values() if isinstance(v, dict)),
        default=None
    )

    # HA RTO
    ha = modules.get("03_high_availability", {})
    rto_sec = None
    if ha.get("notes") and "RTO" in str(ha.get("notes", "")):
        import re
        m = re.search(r"RTO[=: ]+([0-9.]+)", str(ha.get("notes", "")))
        if m:
            rto_sec = float(m.group(1))

    # Hotspot improvement
    wc = modules.get("03b_write_contention", {})
    hotspot_improvement = None
    if wc.get("notes"):
        import re
        m = re.search(r"([0-9.]+)%", str(wc.get("notes", "")))
        if m:
            hotspot_improvement = float(m.group(1))

    # MySQL compat
    compat = payload.get("compat_checks", {})

    return {
        "modules_run":          total_mods,
        "modules_passed":       passed_mods,
        "best_p99_ms":          best_p99,
        "best_tps":             best_tps,
        "rto_sec":              rto_sec,
        "hotspot_improvement_pct": hotspot_improvement,
        "mysql_compat_pct":     compat.get("pct"),
        "comparison_enabled":   payload["comparison_enabled"],
    }


def _load_manifest() -> dict:
    manifest_path = os.path.join(
        os.path.dirname(__file__), "..", "results", "data_manifest.json"
    )
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            return json.load(f)
    return {}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate PoV metrics into JSON.")
    parser.add_argument("--quiet", action="store_true", help="Write metrics file without printing full JSON payload")
    args = parser.parse_args()

    metrics = collect()
    out_path = os.path.join(
        os.path.dirname(__file__), "..", "results", "metrics_summary.json"
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Metrics written to {out_path}")

    if not args.quiet:
        print(json.dumps(metrics, indent=2), file=sys.stdout)
