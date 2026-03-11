#!/usr/bin/env python3
"""
collect_metrics.py — Aggregates all results from results.db and
data_manifest.json into a single JSON payload used by generate_report.py.

Run standalone:
    python report/collect_metrics.py > results/metrics_summary.json
"""
import sys, os, json, time, argparse, re
from pathlib import Path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import yaml
from lib.result_store import (
    _conn, get_latency_stats, get_time_series,
    DB_PATH, init_db
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
        "source_unsupported_inventory": _load_source_unsupported_inventory(),
        "run_context": _load_run_context(),
        "workload_generator": _load_workload_generator_summary(),
        "comparison_enabled": False,
        "comparison_label": "Comparison DB",
    }

    init_db()
    with _conn() as c:
        # Module meta (status, duration)
        meta_rows = c.execute(
            "SELECT module, status, start_ts, end_ts, notes FROM module_meta"
        ).fetchall()
        module_meta = {r["module"]: dict(r) for r in meta_rows}

        # Compat checks
        compat_rows = c.execute(
            "SELECT check_name, category, status, note FROM compat_checks"
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
            stats = _phase_latency_stats(mod, phase=phase, db_label="tidb")
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
                stats = _phase_latency_stats(mod, phase=phase, db_label=comp_label)
                if stats:
                    entry["comparison"][phase] = stats

        # Time-series data (for chart rendering)
        entry["time_series"] = {}
        for phase in (phases or [None]):
            ts_data = _phase_time_series(mod, phase=phase, bucket_sec=10)
            if ts_data:
                entry["time_series"][phase or "overall"] = ts_data

        payload["modules"][mod] = entry

    # Compat check summary
    compat_list = [dict(r) for r in compat_rows]
    passed = sum(1 for r in compat_list if str(r.get("status", "")).lower() == "pass")
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
        "01_baseline_perf": ["c8", "c16", "c32", "c64", "warm_steady", "point_get"],
        "02_elastic_scale": ["ramp_up", "sustain", "ramp_down"],
        "03_high_availability": ["warmup", "failure", "recovery"],
        "03b_write_contention": ["sequential", "autorand"],
        "04_htap_concurrent": ["oltp_only", "htap", "analytics", "analytics_tiflash", "analytics_tikv"],
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
    out = []
    seen = set()
    for row in rows:
        raw = row["phase"]
        if raw is None:
            continue
        canonical = _canonical_phase(mod, str(raw))
        if canonical in seen:
            continue
        out.append(canonical)
        seen.add(canonical)
    return out


def _canonical_phase(mod: str, phase: str) -> str:
    p = str(phase or "")
    if mod == "03_high_availability" and p == "during_failure":
        return "failure"
    return p


def _phase_candidates(mod: str, phase: str | None) -> list:
    if phase is None:
        return [None]
    p = _canonical_phase(mod, str(phase))
    if mod == "03_high_availability" and p == "failure":
        return ["failure", "during_failure"]
    return [p]


def _phase_latency_stats(mod: str, phase: str | None, db_label: str) -> dict:
    for candidate in _phase_candidates(mod, phase):
        stats = get_latency_stats(mod, phase=candidate, db_label=db_label)
        if stats and stats.get("count", 0) > 0:
            return stats
    return {}


def _phase_time_series(mod: str, phase: str | None, bucket_sec: int) -> list:
    for candidate in _phase_candidates(mod, phase):
        ts_data = get_time_series(mod, bucket_sec=bucket_sec, phase=candidate)
        if ts_data:
            return ts_data
    return []


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
                return (0, int(m.group(1)), "")
            if phase == "warm_steady":
                return (1, 0, "")
            if phase == "point_get":
                return (2, 0, "")
            return (3, 0, phase)

        return sorted(ordered, key=baseline_key)

    default_rank = {name: idx for idx, name in enumerate(defaults)}
    return sorted(ordered, key=lambda p: (0, default_rank[p]) if p in default_rank else (1, p))


def _build_summary(payload: dict) -> dict:
    """Derive top-level KPI cards for the executive summary page."""
    modules      = payload["modules"]
    run_context = payload.get("run_context", {}) or {}
    run_mode = str(run_context.get("run_mode") or "validation")
    total_mods   = len([m for m in MODULES if modules.get(m, {}).get("status") != "not_run"])
    passed_mods  = len([m for m in MODULES if modules.get(m, {}).get("status") == "passed"])

    wg = payload.get("workload_generator") or {}
    workload_qps = _maybe_float(wg.get("achieved_qps"))
    workload_tps = _maybe_float(wg.get("achieved_tps"))
    workload_p95 = _maybe_float(wg.get("p95_ms"))
    workload_p99 = _maybe_float(wg.get("p99_ms"))
    workload_error_rate = _maybe_float(wg.get("error_rate"))

    # Best p99 from baseline OLTP
    baseline_mod = modules.get("01_baseline_perf", {}) if isinstance(modules.get("01_baseline_perf"), dict) else {}
    baseline = baseline_mod.get("tidb", {}) if isinstance(baseline_mod.get("tidb"), dict) else {}
    baseline_ts = baseline_mod.get("time_series", {}) if isinstance(baseline_mod.get("time_series"), dict) else {}
    best_p99 = min(
        (v.get("p99_ms", 9999) for v in baseline.values() if isinstance(v, dict)),
        default=None
    )
    best_tps = max(
        (v.get("tps", 0) for v in baseline.values() if isinstance(v, dict)),
        default=None
    )
    warm_steady = baseline.get("warm_steady", {}) if isinstance(baseline, dict) else {}
    warm_count = int(warm_steady.get("count", 0)) if isinstance(warm_steady, dict) else 0
    warm_p50 = warm_steady.get("p50_ms") if warm_count > 0 else None
    warm_p95 = warm_steady.get("p95_ms") if warm_count > 0 else None
    warm_p99 = warm_steady.get("p99_ms") if warm_count > 0 else None
    warm_tps = warm_steady.get("tps") if warm_count > 0 else None
    point_get = baseline.get("point_get", {}) if isinstance(baseline, dict) else {}
    point_get_count = int(point_get.get("count", 0)) if isinstance(point_get, dict) else 0
    point_get_p50 = point_get.get("p50_ms") if point_get_count > 0 else None
    point_get_p95 = point_get.get("p95_ms") if point_get_count > 0 else None
    point_get_p99 = point_get.get("p99_ms") if point_get_count > 0 else None
    point_get_tps = point_get.get("tps") if point_get_count > 0 else None

    # QPS rollups used by dashboard + executive summary cards.
    qps_samples = []
    if isinstance(baseline_ts, dict):
        for points in baseline_ts.values():
            if not isinstance(points, list):
                continue
            for row in points:
                if not isinstance(row, dict):
                    continue
                qps = _maybe_float(row.get("tps"))
                if qps is not None and qps > 0:
                    qps_samples.append(qps)
    if not qps_samples and isinstance(baseline, dict):
        for stats in baseline.values():
            if not isinstance(stats, dict):
                continue
            qps = _maybe_float(stats.get("tps"))
            if qps is not None and qps > 0:
                qps_samples.append(qps)

    if run_mode == "performance":
        if warm_p95 is None:
            warm_p95 = workload_p95
        if warm_p99 is None:
            warm_p99 = workload_p99
        if warm_tps is None:
            warm_tps = workload_tps

    if best_p99 is None:
        best_p99 = workload_p99
    if best_tps is None:
        best_tps = workload_tps

    max_qps = max(qps_samples) if qps_samples else None
    avg_qps = (sum(qps_samples) / len(qps_samples)) if qps_samples else None
    if max_qps is None:
        max_qps = workload_qps if workload_qps is not None else best_tps
    if avg_qps is None:
        avg_qps = workload_qps if workload_qps is not None else best_tps

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
    source_inv = payload.get("source_unsupported_inventory", {}) or {}

    return {
        "modules_run":          total_mods,
        "modules_passed":       passed_mods,
        "run_mode":             run_context.get("run_mode"),
        "schema_mode":          run_context.get("schema_mode"),
        "industry":             run_context.get("industry"),
        "best_observed_p99_ms": best_p99,
        "best_p99_ms":          best_p99,
        "best_tps":             best_tps,
        "warm_p50_ms":          warm_p50,
        "warm_p95_ms":          warm_p95,
        "warm_p99_ms":          warm_p99,
        "warm_tps":             warm_tps,
        "point_get_p50_ms":     point_get_p50,
        "point_get_p95_ms":     point_get_p95,
        "point_get_p99_ms":     point_get_p99,
        "point_get_tps":        point_get_tps,
        "max_qps":              max_qps,
        "avg_qps":              avg_qps,
        "rto_sec":              rto_sec,
        "hotspot_improvement_pct": hotspot_improvement,
        "mysql_compat_pct":     compat.get("pct"),
        "source_unsupported_findings": source_inv.get("failing_features"),
        "source_inventory_target": source_inv.get("target_label"),
        "comparison_enabled":   payload["comparison_enabled"],
        "workload_mode":        str(wg.get("mode") or ""),
        "workload_status":      str(wg.get("status") or ""),
        "workload_qps":         workload_qps,
        "workload_tps":         workload_tps,
        "workload_p95_ms":      workload_p95,
        "workload_p99_ms":      workload_p99,
        "workload_error_rate":  workload_error_rate,
        "workload_run_dir":     str(wg.get("run_dir") or ""),
    }


def _load_manifest() -> dict:
    manifest_path = os.path.join(
        os.path.dirname(__file__), "..", "results", "data_manifest.json"
    )
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            return json.load(f)
    return {}


def _load_source_unsupported_inventory() -> dict:
    path = os.path.join(
        os.path.dirname(__file__), "..", "results", "compat_source_unsupported_summary.json"
    )
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def _load_run_context() -> dict:
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "results", "config.resolved.yaml"),
        os.path.join(os.path.dirname(__file__), "..", "config.yaml"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path) as f:
                cfg = yaml.safe_load(f) or {}
            test_cfg = cfg.get("test") or {}
            run_mode = str(test_cfg.get("run_mode", "validation")).strip().lower() or "validation"
            if run_mode not in {"validation", "performance"}:
                run_mode = "validation"
            schema_mode = str(test_cfg.get("schema_mode", "tidb_optimized")).strip().lower() or "tidb_optimized"
            if schema_mode not in {"tidb_optimized", "mysql_compatible"}:
                schema_mode = "tidb_optimized"
            industry_cfg = cfg.get("industry") or {}
            industry = str(industry_cfg.get("selected", "general_auto")).strip().lower() or "general_auto"
            return {
                "run_mode": run_mode,
                "schema_mode": schema_mode,
                "industry": industry,
                "source_config": os.path.basename(path),
            }
        except Exception:
            continue
    return {
        "run_mode": "validation",
        "schema_mode": "tidb_optimized",
        "industry": "general_auto",
        "source_config": "",
    }


def _load_workload_generator_summary() -> dict:
    root = Path(__file__).resolve().parents[1]
    results_dir = root / "results"
    runs_dir = root / "runs"

    # Preferred: explicit copy made by run_all.sh after a performance run.
    explicit = results_dir / "workload_generator_summary.json"
    if explicit.exists():
        try:
            return json.loads(explicit.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Fallback: run pointer maintained by load/tidb_blaster.py.
    last_run_file = results_dir / "blaster_last_run.txt"
    if last_run_file.exists():
        try:
            raw = last_run_file.read_text(encoding="utf-8").strip()
            if raw:
                run_dir = Path(raw)
                if not run_dir.is_absolute():
                    run_dir = (root / run_dir).resolve()
                summary_path = run_dir / "summary.json"
                if summary_path.exists():
                    return json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Last fallback: newest run folder with summary.json.
    if runs_dir.exists():
        for candidate in sorted([p for p in runs_dir.iterdir() if p.is_dir()], reverse=True):
            summary_path = candidate / "summary.json"
            if not summary_path.exists():
                continue
            try:
                return json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                continue

    return {}


def _maybe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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
