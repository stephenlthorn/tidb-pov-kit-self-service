#!/usr/bin/env python3
"""Dark-themed web UI for TiDB Cloud PoV kit configuration and workflow actions."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
except ModuleNotFoundError:
    print(
        "Missing dependency: flask. Install dependencies first "
        "(bash setup/01_install_deps.sh or pip install flask).",
        file=sys.stderr,
    )
    raise SystemExit(3)

try:
    import yaml
except ModuleNotFoundError:
    print(
        "Missing dependency: pyyaml. Install dependencies first "
        "(bash setup/01_install_deps.sh or pip install pyyaml).",
        file=sys.stderr,
    )
    raise SystemExit(3)

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
RUN_SCRIPT = ROOT / "run_all.sh"
BASELINE_SCRIPT = ROOT / "tests" / "01_baseline_perf" / "run.py"
IMPORT_SCRIPT = ROOT / "tests" / "07_data_import" / "run.py"

REPORT_PDF = RESULTS_DIR / "tidb_pov_report.pdf"
METRICS_JSON = RESULTS_DIR / "metrics_summary.json"
RUN_LOG = RESULTS_DIR / "web_ui_run.log"
RUN_PID = RESULTS_DIR / "web_ui_run.pid"

sys.path.insert(0, str(ROOT))
from setup.pre_poc_intake import (  # type: ignore  # noqa: E402
    SCENARIOS,
    SECURITY_ITEMS,
    TIER_LABELS,
    TIERS,
    build_tier_modules,
    tier_test_profile,
)
from lib.comparison_targets import (  # type: ignore  # noqa: E402
    TARGET_DEFINITIONS,
    comparison_can_run,
    comparison_reason,
    normalize_comparison_cfg,
    target_label,
)


DEFAULT_CFG = {
    "tidb": {
        "host": "",
        "port": 4000,
        "user": "",
        "password": "",
        "database": "test",
        "ssl": True,
    },
    "comparison_db": {
        "enabled": False,
        "target": "aurora_mysql",
        "host": "",
        "port": 3306,
        "user": "",
        "password": "",
        "database": "",
        "schema": "public",
        "label": "Aurora MySQL",
        "ssl": False,
        "ssl_mode": "require",
        "sqlserver_driver": "ODBC Driver 18 for SQL Server",
        "sqlserver_encrypt": True,
        "sqlserver_trust_server_certificate": False,
        "connect_timeout_sec": 30,
        "statement_timeout_sec": 60,
        "retry_count": 1,
        "retry_backoff_ms": 500,
        "max_pool_size": 8,
        "session_init_sql": "",
        "read_only_mode": True,
        "parity_sample_size": 250,
        "capture_explain_plans": True,
        "include_tables": "",
        "exclude_tables": "",
        "tls_ca_path": "",
        "tls_cert_path": "",
        "tls_key_path": "",
        "mysql_sql_mode": "",
        "mysql_time_zone": "UTC",
        "mysql_tx_isolation": "READ COMMITTED",
        "pg_application_name": "tidb_pov_comparison",
        "pg_search_path": "public",
        "pg_lock_timeout_ms": 5000,
        "sqlserver_command_timeout_sec": 60,
        "sqlserver_application_intent": "ReadWrite",
        "sqlserver_mars": False,
        "sqlserver_packet_size": 4096,
    },
    "tier": {
        "selected": "serverless",
    },
    "test": {
        "data_scale": "small",
        "duration_seconds": 120,
        "concurrency_levels": [8, 16, 32],
        "ramp_duration_seconds": 300,
        "import_rows": 1000000,
        "workload_mix": "mixed",
        "read_weight_multiplier": 1.0,
        "write_weight_multiplier": 1.0,
        "import_batch_size": 5000,
        "import_methods": ["batched_insert", "load_data_infile", "import_into"],
    },
    "customer_queries": [],
    "customer_query_ratio": 0.30,
    "modules": {
        "customer_queries": True,
        "baseline_perf": True,
        "elastic_scale": True,
        "high_availability": False,
        "write_contention": True,
        "htap": False,
        "online_ddl": True,
        "mysql_compat": True,
        "data_import": True,
        "vector_search": False,
    },
    "pre_poc": {
        "scenario_template": "oltp_migration",
    },
    "report": {
        "company_name": "Your Company",
        "include_tco_model": True,
        "current_db_monthly_cost_usd": 0,
        "output_dir": "results",
    },
    "tco": {
        "data_size_gb": 1000,
        "annual_growth_pct": 40,
        "aurora_shards_year0": 4,
        "engineers_managing_shards": 2,
        "engineer_annual_cost": 180000,
        "sharding_eng_fraction": 0.25,
    },
}

MODULE_ORDER = [
    "customer_queries",
    "baseline_perf",
    "elastic_scale",
    "high_availability",
    "write_contention",
    "htap",
    "online_ddl",
    "mysql_compat",
    "data_import",
    "vector_search",
]

MODULE_LABELS = {
    "customer_queries": "M0 - Customer Query Validation",
    "baseline_perf": "M1 - Baseline OLTP Performance",
    "elastic_scale": "M2 - Elastic Auto-Scaling",
    "high_availability": "M3 - High Availability",
    "write_contention": "M3b - Write Contention",
    "htap": "M4 - HTAP Concurrent",
    "online_ddl": "M5 - Online DDL",
    "mysql_compat": "M6 - MySQL Compatibility",
    "data_import": "M7 - Data Import",
    "vector_search": "M8 - Vector Search",
}

MODULE_INSIGHTS = {
    "customer_queries": {
        "focus": "Customer SQL validity",
        "runs": "Executes EXPLAIN FORMAT='brief' for each query in customer_queries and records pass/fail with planner output.",
        "value": "Validates migration SQL before load/perf testing starts.",
    },
    "baseline_perf": {
        "focus": "OLTP baseline",
        "runs": "Runs schema-A OLTP workload at configured concurrency levels; captures TPS plus p50/p95/p99 latency. Optional side-by-side target comparison is included when supported.",
        "value": "Establishes migration baseline and throughput/latency envelope.",
    },
    "elastic_scale": {
        "focus": "Scale headroom",
        "runs": "Ramps load from baseline to 4x, sustains, then ramps down while collecting time-series TPS and latency.",
        "value": "Shows autoscaling behavior under demand spikes.",
    },
    "high_availability": {
        "focus": "Recovery behavior",
        "runs": "Performs warmup, simulated failure window, and recovery observation; estimates recovery timing from workload telemetry.",
        "value": "Demonstrates resilience and recovery expectations.",
    },
    "write_contention": {
        "focus": "Hotspot mitigation",
        "runs": "Compares sequential key UPSERT vs AUTO_RANDOM under high concurrency and reports p99 delta plus contention diagnostics.",
        "value": "Shows write hotspot risk and mitigation approach.",
    },
    "htap": {
        "focus": "HTAP isolation",
        "runs": "Runs OLTP workload alone, then OLTP + analytical queries routed to TiFlash; compares OLTP degradation and checks TiFlash readiness.",
        "value": "Proves analytics coexist with transactional workload.",
    },
    "online_ddl": {
        "focus": "Zero-downtime schema change",
        "runs": "Runs DDL operations (add column/index, modify column) with concurrent OLTP load and tracks DDL duration + p99 impact.",
        "value": "Demonstrates online schema evolution without app downtime.",
    },
    "mysql_compat": {
        "focus": "Migration compatibility",
        "runs": "Executes broad MySQL feature checks across DDL/DML/functions/JSON/window functions/transactions/EXPLAIN and logs detailed results.",
        "value": "Quantifies MySQL feature compatibility for migration planning.",
    },
    "data_import": {
        "focus": "Migration ingest speed",
        "runs": "Generates CSV and benchmarks batched INSERT, LOAD DATA LOCAL INFILE, and IMPORT INTO (if available) with rows/s and GB/min metrics.",
        "value": "Provides ingestion strategy and throughput evidence.",
    },
    "vector_search": {
        "focus": "AI/vector capability",
        "runs": "Creates VECTOR table/index, inserts embeddings, runs ANN and hybrid vector+SQL queries across concurrencies, and captures latency/QPS.",
        "value": "Validates vector workload readiness for AI use cases.",
    },
}

MODULE_SUITE_KEYS = {
    "tier_recommended": [],
    "all": list(MODULE_ORDER),
    "oltp_migration": [
        "customer_queries",
        "baseline_perf",
        "elastic_scale",
        "write_contention",
        "online_ddl",
        "mysql_compat",
        "data_import",
    ],
    "htap_analytics": [
        "customer_queries",
        "baseline_perf",
        "elastic_scale",
        "htap",
        "online_ddl",
        "mysql_compat",
        "data_import",
    ],
    "ai_vector": [
        "customer_queries",
        "baseline_perf",
        "elastic_scale",
        "htap",
        "online_ddl",
        "mysql_compat",
        "data_import",
        "vector_search",
    ],
    "smoke": [
        "baseline_perf",
        "mysql_compat",
    ],
    "none": [],
}

MODULE_SUITES = {
    "tier_recommended": {
        "label": "Tier Recommended",
        "description": "Use tier/scenario-aware defaults from the decision logic.",
    },
    "all": {
        "label": "All Modules (M0-M8)",
        "description": "Enable every module for full coverage.",
    },
    "oltp_migration": {
        "label": "OLTP + Migration Suite",
        "description": "Focus on transactional migration readiness and import validation.",
    },
    "htap_analytics": {
        "label": "HTAP Analytics Suite",
        "description": "Includes OLTP baseline plus concurrent HTAP analytics checks.",
    },
    "ai_vector": {
        "label": "AI / Vector Suite",
        "description": "Adds vector search testing to HTAP-focused coverage.",
    },
    "smoke": {
        "label": "Smoke Validation",
        "description": "Fast minimal validation for environment shakeout.",
    },
    "none": {
        "label": "Disable All",
        "description": "Turn all modules off for manual re-selection.",
    },
}

UI_TIERS = ["serverless", "essential", "premium", "dedicated"]
UI_TIER_LABELS = {
    "serverless": "Starter",
    "essential": "Essential",
    "premium": "Premium",
    "dedicated": "Dedicated",
    "byoc": "BYOC",
}

TIER_CHIP_CLASSES = {
    "serverless": "chip-tier-starter",
    "essential": "chip-tier-essential",
    "premium": "chip-tier-premium",
    "dedicated": "chip-tier-dedicated",
    "byoc": "chip-tier-byoc",
}

SCENARIO_CHIP_CLASSES = {
    "oltp_migration": "chip-scenario-oltp",
    "htap_analytics": "chip-scenario-htap",
    "ai_vector": "chip-scenario-ai",
}

MODULE_REPORT_LABELS = {
    "00_customer_queries": "M0 - Customer Query Validation",
    "01_baseline_perf": "M1 - Baseline OLTP Performance",
    "02_elastic_scale": "M2 - Elastic Auto-Scaling",
    "03_high_availability": "M3 - High Availability",
    "03b_write_contention": "M3b - Write Contention",
    "04_htap_concurrent": "M4 - HTAP Concurrent",
    "05_online_ddl": "M5 - Online DDL",
    "06_mysql_compat": "M6 - MySQL Compatibility",
    "07_data_import": "M7 - Data Import",
    "08_vector_search": "M8 - Vector Search",
}

STATUS_LABELS = {
    "passed": "Passed",
    "failed": "Failed",
    "skipped": "Skipped",
    "not_run": "Not Run",
}

STATUS_CLASSES = {
    "passed": "pill-pass",
    "failed": "pill-fail",
    "skipped": "pill-skip",
    "not_run": "pill-na",
}

WORKLOAD_TARGETS = {
    "baseline_perf": {
        "label": "Baseline OLTP (M1)",
        "script": BASELINE_SCRIPT,
    },
    "data_import": {
        "label": "Data Import Benchmark (M7)",
        "script": IMPORT_SCRIPT,
    },
}

IMPORT_METHOD_LABELS = {
    "batched_insert": "Batched INSERT",
    "load_data_infile": "LOAD DATA LOCAL INFILE",
    "import_into": "IMPORT INTO",
}

QUICKSTART_WORKLOAD_PRESETS = {
    "fast_validation": {
        "label": "Fast Validation",
        "description": "Fast connectivity and smoke validation with lighter runtime.",
        "overrides": {
            "data_scale": "small",
            "duration_seconds": 60,
            "concurrency_levels": [4, 8, 16],
            "ramp_duration_seconds": 120,
            "import_rows": 250000,
            "workload_mix": "mixed",
            "read_weight_multiplier": 1.0,
            "write_weight_multiplier": 1.0,
            "import_batch_size": 2500,
        },
    },
    "balanced_poc": {
        "label": "Balanced PoC",
        "description": "Recommended default profile for most PoV runs.",
        "overrides": {},
    },
    "stress_run": {
        "label": "Stress Run",
        "description": "Higher load profile for deeper performance characterization.",
        "overrides": {
            "data_scale": "medium",
            "duration_seconds": 240,
            "concurrency_levels": [16, 32, 64],
            "ramp_duration_seconds": 480,
            "import_rows": 3000000,
            "workload_mix": "mixed",
            "read_weight_multiplier": 1.2,
            "write_weight_multiplier": 1.1,
            "import_batch_size": 7500,
        },
    },
}

QUICKSTART_PRESET_SUITES = {
    "fast_validation": "smoke",
    "balanced_poc": "oltp_migration",
    "stress_run": "all",
}

QUICKSTART_PRESET_MODULES = {
    preset: list(MODULE_SUITE_KEYS.get(suite, MODULE_SUITE_KEYS["oltp_migration"]))
    for preset, suite in QUICKSTART_PRESET_SUITES.items()
}

QUICKSTART_TEST_CATEGORIES = {
    "customer_queries": "Validation",
    "baseline_perf": "Performance",
    "elastic_scale": "Performance",
    "high_availability": "Resilience",
    "write_contention": "Performance",
    "htap": "HTAP / Analytics",
    "online_ddl": "Schema Change",
    "mysql_compat": "Compatibility",
    "data_import": "Migration",
    "vector_search": "AI / Vector",
}

QUICKSTART_SECURITY_MODES = {
    "keep_existing": "Keep existing screener state",
    "all_pass": "Mark all controls PASS (for controlled test envs)",
    "all_na": "Mark all controls N/A (forces review-required hold)",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Launch PoC web UI")
    p.add_argument("--config", default="config.yaml", help="Path to config file")
    p.add_argument("--host", default="127.0.0.1", help="Bind host")
    p.add_argument("--port", default=8787, type=int, help="Bind port")
    p.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    return p.parse_args()


def deep_merge(dst: Dict, src: Dict) -> Dict:
    for k, v in src.items():
        if isinstance(v, dict):
            dst.setdefault(k, {})
            if isinstance(dst[k], dict):
                deep_merge(dst[k], v)
            else:
                dst[k] = v
        else:
            dst.setdefault(k, v)
    return dst


def normalize_cfg(cfg: Dict) -> Dict:
    cfg = deep_merge(cfg or {}, DEFAULT_CFG)
    cfg["comparison_db"] = normalize_comparison_cfg(cfg.get("comparison_db"))

    scenario = cfg.get("pre_poc", {}).get("scenario_template", "oltp_migration")
    if scenario not in SCENARIOS:
        cfg["pre_poc"]["scenario_template"] = "oltp_migration"

    tier = cfg.get("tier", {}).get("selected", "serverless")
    if tier not in TIERS:
        cfg["tier"]["selected"] = "serverless"

    for key in MODULE_ORDER:
        cfg["modules"][key] = bool(cfg.get("modules", {}).get(key, DEFAULT_CFG["modules"][key]))

    cl = cfg.get("test", {}).get("concurrency_levels", [8, 16, 32])
    if not isinstance(cl, list):
        cfg["test"]["concurrency_levels"] = [8, 16, 32]

    mix = str(cfg.get("test", {}).get("workload_mix", "mixed")).lower()
    if mix not in {"mixed", "read_heavy", "write_heavy"}:
        cfg["test"]["workload_mix"] = "mixed"

    try:
        cfg["test"]["read_weight_multiplier"] = float(cfg.get("test", {}).get("read_weight_multiplier", 1.0))
    except (TypeError, ValueError):
        cfg["test"]["read_weight_multiplier"] = 1.0

    try:
        cfg["test"]["write_weight_multiplier"] = float(cfg.get("test", {}).get("write_weight_multiplier", 1.0))
    except (TypeError, ValueError):
        cfg["test"]["write_weight_multiplier"] = 1.0

    try:
        batch = int(cfg.get("test", {}).get("import_batch_size", 5000))
    except (TypeError, ValueError):
        batch = 5000
    cfg["test"]["import_batch_size"] = max(100, batch)

    methods = cfg.get("test", {}).get("import_methods")
    if not isinstance(methods, list):
        cfg["test"]["import_methods"] = list(IMPORT_METHOD_LABELS.keys())
    else:
        normalized = [str(m).strip().lower() for m in methods if str(m).strip().lower() in IMPORT_METHOD_LABELS]
        cfg["test"]["import_methods"] = normalized or list(IMPORT_METHOD_LABELS.keys())

    return cfg


def load_cfg(config_path: Path) -> Dict:
    if not config_path.exists():
        return normalize_cfg({})
    with config_path.open("r", encoding="utf-8") as f:
        return normalize_cfg(yaml.safe_load(f) or {})


def save_cfg(config_path: Path, cfg: Dict) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def to_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


def to_int(raw: str | None, default: int) -> int:
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def to_float(raw: str | None, default: float) -> float:
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def parse_concurrency(raw: str, default: List[int]) -> List[int]:
    if not raw.strip():
        return default
    vals = []
    for part in re.split(r"[\s,]+", raw.strip()):
        if not part:
            continue
        if part.isdigit() and int(part) > 0:
            vals.append(int(part))
    return vals or default


def parse_import_methods_from_form(form, prefix: str) -> List[str]:
    selected = []
    for method in IMPORT_METHOD_LABELS:
        if form.get(f"{prefix}{method}") == "on":
            selected.append(method)
    return selected or list(IMPORT_METHOD_LABELS.keys())


def apply_comparison_advanced_from_form(comp: Dict, form, prefix: str = "") -> None:
    def g(name: str, default: str = "") -> str:
        return form.get(f"{prefix}{name}", default)

    comp["connect_timeout_sec"] = max(1, to_int(g("comparison_connect_timeout_sec"), int(comp.get("connect_timeout_sec", 30))))
    comp["statement_timeout_sec"] = max(
        1, to_int(g("comparison_statement_timeout_sec"), int(comp.get("statement_timeout_sec", 60)))
    )
    comp["retry_count"] = max(0, to_int(g("comparison_retry_count"), int(comp.get("retry_count", 1))))
    comp["retry_backoff_ms"] = max(
        0, to_int(g("comparison_retry_backoff_ms"), int(comp.get("retry_backoff_ms", 500)))
    )
    comp["max_pool_size"] = max(1, to_int(g("comparison_max_pool_size"), int(comp.get("max_pool_size", 8))))
    comp["session_init_sql"] = g("comparison_session_init_sql", str(comp.get("session_init_sql", ""))).strip()
    comp["read_only_mode"] = to_bool(
        form.get(f"{prefix}comparison_read_only_mode"), bool(comp.get("read_only_mode", True))
    )
    comp["parity_sample_size"] = max(
        1, to_int(g("comparison_parity_sample_size"), int(comp.get("parity_sample_size", 250)))
    )
    comp["capture_explain_plans"] = to_bool(
        form.get(f"{prefix}comparison_capture_explain_plans"), bool(comp.get("capture_explain_plans", True))
    )
    comp["include_tables"] = g("comparison_include_tables", str(comp.get("include_tables", ""))).strip()
    comp["exclude_tables"] = g("comparison_exclude_tables", str(comp.get("exclude_tables", ""))).strip()
    comp["tls_ca_path"] = g("comparison_tls_ca_path", str(comp.get("tls_ca_path", ""))).strip()
    comp["tls_cert_path"] = g("comparison_tls_cert_path", str(comp.get("tls_cert_path", ""))).strip()
    comp["tls_key_path"] = g("comparison_tls_key_path", str(comp.get("tls_key_path", ""))).strip()

    comp["mysql_sql_mode"] = g("comparison_mysql_sql_mode", str(comp.get("mysql_sql_mode", ""))).strip()
    comp["mysql_time_zone"] = g("comparison_mysql_time_zone", str(comp.get("mysql_time_zone", "UTC"))).strip() or "UTC"
    comp["mysql_tx_isolation"] = (
        g("comparison_mysql_tx_isolation", str(comp.get("mysql_tx_isolation", "READ COMMITTED"))).strip()
        or "READ COMMITTED"
    )

    comp["pg_application_name"] = (
        g("comparison_pg_application_name", str(comp.get("pg_application_name", "tidb_pov_comparison"))).strip()
        or "tidb_pov_comparison"
    )
    comp["pg_search_path"] = g("comparison_pg_search_path", str(comp.get("pg_search_path", "public"))).strip() or "public"
    comp["pg_lock_timeout_ms"] = max(
        0, to_int(g("comparison_pg_lock_timeout_ms"), int(comp.get("pg_lock_timeout_ms", 5000)))
    )

    comp["sqlserver_command_timeout_sec"] = max(
        1,
        to_int(
            g("comparison_sqlserver_command_timeout_sec"),
            int(comp.get("sqlserver_command_timeout_sec", 60)),
        ),
    )
    comp["sqlserver_application_intent"] = (
        g("comparison_sqlserver_application_intent", str(comp.get("sqlserver_application_intent", "ReadWrite"))).strip()
        or "ReadWrite"
    )
    if comp["sqlserver_application_intent"] not in {"ReadWrite", "ReadOnly"}:
        comp["sqlserver_application_intent"] = "ReadWrite"
    comp["sqlserver_mars"] = to_bool(
        form.get(f"{prefix}comparison_sqlserver_mars"), bool(comp.get("sqlserver_mars", False))
    )
    comp["sqlserver_packet_size"] = max(512, to_int(g("comparison_sqlserver_packet_size"), int(comp.get("sqlserver_packet_size", 4096))))


def modules_from_suite(
    suite_id: str,
    *,
    tier: str,
    scenario: str,
    run_ha_sim: bool,
    enable_optional_advanced: bool,
    existing: Dict,
) -> Dict:
    suite = str(suite_id or "tier_recommended").strip().lower()
    if suite == "tier_recommended":
        return build_tier_modules(
            tier=tier,
            scenario=scenario,
            run_ha_sim=run_ha_sim,
            enable_optional_advanced=enable_optional_advanced,
            existing=existing,
        )

    if suite not in MODULE_SUITE_KEYS:
        suite = "tier_recommended"
        return build_tier_modules(
            tier=tier,
            scenario=scenario,
            run_ha_sim=run_ha_sim,
            enable_optional_advanced=enable_optional_advanced,
            existing=existing,
        )

    enabled = set(MODULE_SUITE_KEYS[suite])
    return {k: (k in enabled) for k in MODULE_ORDER}


def infer_scenario_from_modules(modules: Dict) -> str:
    if modules.get("vector_search"):
        return "ai_vector"
    if modules.get("htap"):
        return "htap_analytics"
    return "oltp_migration"


def load_counts_for_preview(cfg: Dict) -> Dict:
    manifest = RESULTS_DIR / "data_manifest.json"
    if manifest.exists():
        try:
            raw = json.loads(manifest.read_text(encoding="utf-8"))
            counts = raw.get("counts")
            if isinstance(counts, dict) and counts:
                return counts
        except Exception:
            pass

    try:
        from setup.generate_data import SCALE_CONFIG  # type: ignore

        scale = cfg.get("test", {}).get("data_scale", "small")
        counts = SCALE_CONFIG.get(scale, SCALE_CONFIG.get("small", {}))
        if isinstance(counts, dict) and counts:
            return counts
    except Exception:
        pass

    return {"users": 100_000, "accounts": 150_000, "transactions": 5_000_000}


def query_activity(limit: int = 25) -> List[Dict]:
    db_path = RESULTS_DIR / "results.db"
    if not db_path.exists():
        return []

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                query_type,
                COUNT(*) AS count_total,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) AS count_success,
                ROUND(AVG(latency_ms), 2) AS avg_ms,
                ROUND(MAX(latency_ms), 2) AS max_ms
            FROM results
            WHERE query_type IS NOT NULL
              AND module IN ('01_baseline_perf', '02_elastic_scale', '03_high_availability', '04_htap_concurrent', '05_online_ddl')
            GROUP BY query_type
            ORDER BY count_total DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        conn.close()
    except Exception:
        return []

    out = []
    for row in rows:
        total = parse_int(row["count_total"], 0)
        success = parse_int(row["count_success"], 0)
        out.append(
            {
                "query_type": row["query_type"],
                "count_total": total,
                "success_pct": pct(success, total or 1),
                "avg_ms": parse_float(row["avg_ms"], 0.0),
                "max_ms": parse_float(row["max_ms"], 0.0),
            }
        )
    return out


def build_workload_insights(cfg: Dict) -> Dict:
    try:
        from load.workload_definitions import apply_workload_profile, classify_query_kind, schema_a_workload
    except Exception:
        return {"ready": False, "queries": [], "activity": [], "customer_queries": []}

    test_cfg = cfg.get("test", {})
    mix = str(test_cfg.get("workload_mix", "mixed")).lower()
    read_mult = parse_float(test_cfg.get("read_weight_multiplier", 1.0), 1.0)
    write_mult = parse_float(test_cfg.get("write_weight_multiplier", 1.0), 1.0)

    counts = load_counts_for_preview(cfg)
    base_workload = schema_a_workload(counts)
    tuned = apply_workload_profile(
        base_workload,
        mix=mix,
        read_multiplier=max(0.1, read_mult),
        write_multiplier=max(0.1, write_mult),
    )

    total_weight = sum(parse_float(item.get("weight"), 0.0) for item in tuned) or 1.0
    query_rows = []
    for item in tuned:
        qtype = str(item.get("query_type", "unknown"))
        weight = parse_float(item.get("weight"), 0.0)
        kind = classify_query_kind(qtype)
        query_rows.append(
            {
                "query_type": qtype,
                "kind": kind,
                "kind_class": "kind-read" if kind == "read" else ("kind-write" if kind == "write" else "kind-other"),
                "weight": round(weight, 2),
                "share_pct": round((weight / total_weight) * 100.0, 1),
                "sql": str(item.get("sql", "")),
            }
        )

    query_rows.sort(key=lambda r: r["weight"], reverse=True)
    customer_queries = [q for q in cfg.get("customer_queries", []) if isinstance(q, str) and q.strip()]
    import_methods = cfg.get("test", {}).get("import_methods", list(IMPORT_METHOD_LABELS.keys()))
    if not isinstance(import_methods, list):
        import_methods = list(IMPORT_METHOD_LABELS.keys())

    levels = cfg.get("test", {}).get("concurrency_levels", [8])
    if not isinstance(levels, list) or not levels:
        levels = [8]

    return {
        "ready": True,
        "mix": mix,
        "read_multiplier": round(read_mult, 2),
        "write_multiplier": round(write_mult, 2),
        "queries": query_rows,
        "activity": query_activity(limit=25),
        "customer_queries": customer_queries,
        "target": str(cfg.get("workload_lab", {}).get("target", "baseline_perf")),
        "concurrency": parse_int(cfg.get("workload_lab", {}).get("concurrency"), parse_int(levels[0], 8)),
        "duration_seconds": parse_int(cfg.get("workload_lab", {}).get("duration_seconds"), cfg.get("test", {}).get("duration_seconds", 120)),
        "customer_ratio": round(parse_float(cfg.get("customer_query_ratio", 0.3), 0.3), 2),
        "import_rows": parse_int(cfg.get("test", {}).get("import_rows"), 1_000_000),
        "import_batch_size": parse_int(cfg.get("test", {}).get("import_batch_size"), 5000),
        "import_methods": [m for m in import_methods if m in IMPORT_METHOD_LABELS],
    }


def ui_tier_label(tier: str) -> str:
    return UI_TIER_LABELS.get(tier, tier.replace("_", " ").title())


def visible_tiers_for_ui(selected_tier: str) -> List[str]:
    visible = list(UI_TIERS)
    if selected_tier in TIERS and selected_tier not in visible:
        visible.append(selected_tier)
    return visible


def parse_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def mean_series(points: List[Dict], key: str) -> float:
    vals = [parse_float(p.get(key), 0.0) for p in points if isinstance(p, dict)]
    vals = [v for v in vals if v > 0]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def pct(value: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return round((value / total) * 100.0, 1)


def parse_level_num(label: str) -> int:
    m = re.search(r"\d+", label or "")
    if not m:
        return 10**6
    return int(m.group(0))


def build_svg_points(values: List[float], width: int = 460, height: int = 150) -> Tuple[str, float, float, float]:
    if len(values) < 2:
        return "", 0.0, 0.0, 0.0
    min_v = min(values)
    max_v = max(values)
    span = max(max_v - min_v, 1.0)
    points = []
    for idx, val in enumerate(values):
        x = (idx / (len(values) - 1)) * width
        y = height - (((val - min_v) / span) * height)
        points.append(f"{x:.2f},{y:.2f}")
    return " ".join(points), min_v, max_v, values[-1]


def load_metrics_summary() -> Dict | None:
    if not METRICS_JSON.exists():
        return None
    try:
        return json.loads(METRICS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_report_dashboard() -> Dict:
    out = {
        "ready": False,
        "generated_at": "",
        "summary_cards": [],
        "status_segments": [],
        "module_rows": [],
        "baseline_tps": [],
        "baseline_p99": [],
        "contention_tps": [],
        "import_bars": [],
        "compat_pct": 0.0,
        "compat_total": 0,
        "compat_passed": 0,
        "compat_failed": 0,
        "compat_failed_checks": [],
        "line_charts": [],
    }

    metrics = load_metrics_summary()
    if not metrics:
        return out

    out["ready"] = True
    out["generated_at"] = str(metrics.get("generated_at") or "")

    summary = metrics.get("summary") or {}
    modules = metrics.get("modules") or {}
    compat = metrics.get("compat_checks") or {}
    import_stats = metrics.get("import_stats") or []

    status_counts = {"passed": 0, "failed": 0, "skipped": 0, "not_run": 0}
    module_rows = []
    for key, data in modules.items():
        if not isinstance(data, dict):
            continue
        status = str(data.get("status") or "not_run").lower()
        if status not in status_counts:
            status = "not_run"
        status_counts[status] += 1
        module_rows.append(
            {
                "name": MODULE_REPORT_LABELS.get(key, key),
                "status": status,
                "status_label": STATUS_LABELS.get(status, status),
                "status_class": STATUS_CLASSES.get(status, "pill-na"),
            }
        )
    out["module_rows"] = sorted(module_rows, key=lambda r: r["name"])

    status_total = sum(status_counts.values()) or 1
    out["status_segments"] = [
        {
            "label": STATUS_LABELS["passed"],
            "count": status_counts["passed"],
            "pct": pct(status_counts["passed"], status_total),
            "class": "seg-pass",
        },
        {
            "label": STATUS_LABELS["failed"],
            "count": status_counts["failed"],
            "pct": pct(status_counts["failed"], status_total),
            "class": "seg-fail",
        },
        {
            "label": STATUS_LABELS["skipped"],
            "count": status_counts["skipped"],
            "pct": pct(status_counts["skipped"], status_total),
            "class": "seg-skip",
        },
        {
            "label": STATUS_LABELS["not_run"],
            "count": status_counts["not_run"],
            "pct": pct(status_counts["not_run"], status_total),
            "class": "seg-na",
        },
    ]

    best_tps = parse_float(summary.get("best_tps"), 0.0)
    best_p99 = parse_float(summary.get("best_p99_ms"), 0.0)
    mysql_compat_pct = parse_float(summary.get("mysql_compat_pct"), parse_float(compat.get("pct"), 0.0))
    modules_passed = parse_int(summary.get("modules_passed"), status_counts["passed"])
    modules_run = parse_int(summary.get("modules_run"), status_total)
    import_throughputs = [parse_float(item.get("throughput_gbpm"), 0.0) for item in import_stats if isinstance(item, dict)]
    best_import = max(import_throughputs) if import_throughputs else 0.0

    out["summary_cards"] = [
        {"label": "Modules Passed", "value": f"{modules_passed}/{modules_run}", "sub": "Execution coverage"},
        {"label": "Best TPS", "value": f"{best_tps:,.1f}", "sub": "Higher is better"},
        {"label": "Best P99 (ms)", "value": f"{best_p99:,.2f}", "sub": "Lower is better"},
        {"label": "MySQL Compatibility", "value": f"{mysql_compat_pct:.1f}%", "sub": "Syntax/behavior checks"},
        {"label": "Best Import GB/min", "value": f"{best_import:.4f}", "sub": "Data import throughput"},
    ]

    baseline = modules.get("01_baseline_perf") if isinstance(modules.get("01_baseline_perf"), dict) else {}
    baseline_tidb = baseline.get("tidb") if isinstance(baseline.get("tidb"), dict) else {}
    baseline_ts = baseline.get("time_series") if isinstance(baseline.get("time_series"), dict) else {}
    baseline_rows = []
    for level in sorted(baseline_tidb.keys(), key=parse_level_num):
        vals = baseline_tidb.get(level) or {}
        ts_points = baseline_ts.get(level) if isinstance(baseline_ts.get(level), list) else []
        tps_val = parse_float(vals.get("tps"), 0.0)
        if tps_val <= 0:
            tps_val = mean_series(ts_points, "tps")
        p99_val = parse_float(vals.get("p99_ms"), 0.0)
        baseline_rows.append(
            {
                "label": level.upper(),
                "tps": round(tps_val, 2),
                "p99_ms": round(p99_val, 2),
                "avg_ms": round(parse_float(vals.get("avg_ms"), 0.0), 2),
            }
        )

    max_tps = max((r["tps"] for r in baseline_rows), default=0.0)
    max_p99 = max((r["p99_ms"] for r in baseline_rows), default=0.0)
    for row in baseline_rows:
        row["tps_pct"] = pct(row["tps"], max_tps or 1.0)
        row["p99_pct"] = pct(row["p99_ms"], max_p99 or 1.0)
    out["baseline_tps"] = baseline_rows
    out["baseline_p99"] = baseline_rows

    contention = modules.get("03b_write_contention") if isinstance(modules.get("03b_write_contention"), dict) else {}
    contention_tidb = contention.get("tidb") if isinstance(contention.get("tidb"), dict) else {}
    contention_ts = contention.get("time_series") if isinstance(contention.get("time_series"), dict) else {}
    contention_rows = []
    for mode in sorted(contention_tidb.keys()):
        vals = contention_tidb.get(mode) or {}
        ts_points = contention_ts.get(mode) if isinstance(contention_ts.get(mode), list) else []
        tps_val = parse_float(vals.get("tps"), 0.0)
        if tps_val <= 0:
            tps_val = mean_series(ts_points, "tps")
        contention_rows.append(
            {
                "label": mode.replace("_", " ").title(),
                "tps": round(tps_val, 2),
                "p99_ms": round(parse_float(vals.get("p99_ms"), 0.0), 2),
            }
        )
    max_contention = max((r["tps"] for r in contention_rows), default=0.0)
    for row in contention_rows:
        row["tps_pct"] = pct(row["tps"], max_contention or 1.0)
    out["contention_tps"] = contention_rows

    import_rows = []
    for idx, item in enumerate(import_stats, start=1):
        if not isinstance(item, dict):
            continue
        throughput = parse_float(item.get("throughput_gbpm"), 0.0)
        import_rows.append(
            {
                "label": f"Run {idx}",
                "throughput": round(throughput, 5),
                "duration_sec": round(parse_float(item.get("duration_sec"), 0.0), 2),
                "rows_imported": parse_int(item.get("rows_imported"), 0),
            }
        )
    max_import = max((r["throughput"] for r in import_rows), default=0.0)
    for row in import_rows:
        row["throughput_pct"] = pct(row["throughput"], max_import or 1.0)
    out["import_bars"] = import_rows

    out["compat_total"] = parse_int(compat.get("total"), 0)
    out["compat_passed"] = parse_int(compat.get("passed"), 0)
    out["compat_failed"] = parse_int(compat.get("failed"), 0)
    out["compat_pct"] = parse_float(compat.get("pct"), 0.0)
    out["compat_failed_checks"] = [
        d for d in (compat.get("details") or []) if isinstance(d, dict) and d.get("status") == "fail"
    ]

    line_charts = []
    for module_key, data in modules.items():
        if not isinstance(data, dict):
            continue
        ts_map = data.get("time_series")
        if not isinstance(ts_map, dict):
            continue

        module_label = MODULE_REPORT_LABELS.get(module_key, module_key)
        for series_name, points in ts_map.items():
            if not isinstance(points, list) or len(points) < 2:
                continue
            for metric, metric_label, color_class in (
                ("tps", "TPS", "line-tps"),
                ("p99_ms", "P99 Latency (ms)", "line-p99"),
            ):
                values = [parse_float(p.get(metric), 0.0) for p in points if isinstance(p, dict)]
                if len(values) < 2:
                    continue
                svg_points, min_v, max_v, last_v = build_svg_points(values)
                if not svg_points:
                    continue
                line_charts.append(
                    {
                        "title": f"{module_label} - {series_name.upper()} - {metric_label}",
                        "points": svg_points,
                        "min": round(min_v, 2),
                        "max": round(max_v, 2),
                        "last": round(last_v, 2),
                        "class": color_class,
                        "samples": len(values),
                    }
                )
    out["line_charts"] = line_charts
    return out


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def run_status() -> Dict:
    status = {
        "running": False,
        "pid": None,
        "last_log": None,
        "last_lines": "",
    }

    if RUN_LOG.exists():
        status["last_log"] = dt.datetime.fromtimestamp(RUN_LOG.stat().st_mtime).isoformat()
        try:
            with RUN_LOG.open("r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            status["last_lines"] = "".join(lines[-120:])
        except Exception:
            status["last_lines"] = ""

    if RUN_PID.exists():
        try:
            pid = int(RUN_PID.read_text().strip())
            if pid_alive(pid):
                status["running"] = True
                status["pid"] = pid
            else:
                RUN_PID.unlink(missing_ok=True)
        except Exception:
            RUN_PID.unlink(missing_ok=True)

    return status


def start_background(command: List[str], label: str) -> Tuple[bool, str]:
    st = run_status()
    if st["running"]:
        return False, f"A run is already in progress (pid {st['pid']})."

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with RUN_LOG.open("a", encoding="utf-8") as log:
        stamp = dt.datetime.now().isoformat(timespec="seconds")
        log.write(f"\n\n=== {label} started {stamp} ===\n")
        log.flush()
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    RUN_PID.write_text(str(proc.pid), encoding="utf-8")
    return True, f"Started {label} (pid {proc.pid})."


def clear_local_results() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for child in RESULTS_DIR.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def drop_configured_database(cfg: Dict) -> Tuple[bool, str]:
    tidb = cfg.get("tidb") or {}
    host = tidb.get("host")
    user = tidb.get("user")
    password = tidb.get("password")
    database = tidb.get("database")
    port = int(tidb.get("port", 4000))
    ssl = bool(tidb.get("ssl", True))

    if not all([host, user, password, database]):
        return False, "Missing one or more required TiDB fields (host/user/password/database)."

    try:
        import mysql.connector

        ssl_args = {"ssl_disabled": False} if ssl else {"ssl_disabled": True}
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            connection_timeout=30,
            **ssl_args,
        )
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{database}`")
        conn.close()
        return True, f"Dropped database `{database}`."
    except Exception as e:
        return False, str(e)


def security_from_cfg(cfg: Dict) -> Dict:
    sec = cfg.get("pre_poc", {}).get("security")
    if isinstance(sec, dict):
        return sec

    items = []
    for item in SECURITY_ITEMS:
        items.append(
            {
                "id": item["id"],
                "prompt": item["prompt"],
                "status": "not_assessed",
                "blocking": item["blocking"],
                "owner": item["owner"],
            }
        )

    return {
        "items": items,
        "blocking_failures": [],
        "non_blocking_failures": [],
        "recommendation": "review_required",
        "proceed": True,
    }


def build_security_result(form) -> Dict:
    items = []
    for item in SECURITY_ITEMS:
        key = f"sec_{item['id']}"
        status = (form.get(key) or "not_assessed").lower()
        if status not in {"pass", "fail", "na", "not_assessed"}:
            status = "not_assessed"

        items.append(
            {
                "id": item["id"],
                "prompt": item["prompt"],
                "status": status,
                "blocking": item["blocking"],
                "owner": item["owner"],
            }
        )

    blocking_failures = [r["id"] for r in items if r["status"] == "fail" and r["blocking"]]
    non_blocking_failures = [r["id"] for r in items if r["status"] == "fail" and not r["blocking"]]
    has_review_required = any(r["status"] in {"na", "not_assessed"} for r in items)

    if blocking_failures:
        recommendation = "hold"
        proceed = False
    elif has_review_required:
        recommendation = "review_required"
        proceed = False
    elif non_blocking_failures:
        recommendation = "proceed_with_risks"
        proceed = True
    else:
        recommendation = "proceed"
        proceed = True

    return {
        "items": items,
        "blocking_failures": blocking_failures,
        "non_blocking_failures": non_blocking_failures,
        "recommendation": recommendation,
        "proceed": proceed,
    }


def build_security_profile(profile: str) -> Dict | None:
    profile = str(profile or "").strip().lower()
    if profile not in {"all_pass", "all_na"}:
        return None

    status = "pass" if profile == "all_pass" else "na"
    items = [
        {
            "id": item["id"],
            "prompt": item["prompt"],
            "status": status,
            "blocking": item["blocking"],
            "owner": item["owner"],
        }
        for item in SECURITY_ITEMS
    ]

    if profile == "all_pass":
        return {
            "items": items,
            "blocking_failures": [],
            "non_blocking_failures": [],
            "recommendation": "proceed",
            "proceed": True,
        }

    return {
        "items": items,
        "blocking_failures": [],
        "non_blocking_failures": [],
        "recommendation": "review_required",
        "proceed": False,
    }


def create_app(config_path: Path) -> Flask:
    app = Flask(__name__, template_folder=str(TEMPLATES_DIR))
    app.secret_key = "tidb-pov-local-ui"

    @app.get("/")
    def index():
        cfg = load_cfg(config_path)
        cfg["comparison_db"] = normalize_comparison_cfg(cfg.get("comparison_db"))
        sec = security_from_cfg(cfg)
        st = run_status()
        report_ready = REPORT_PDF.exists()
        report_dashboard = build_report_dashboard()
        workload_insights = build_workload_insights(cfg)

        selected_tier = str(cfg.get("tier", {}).get("selected", "serverless"))
        selected_scenario = str(cfg.get("pre_poc", {}).get("scenario_template", "oltp_migration"))
        tiers_for_ui = visible_tiers_for_ui(selected_tier)

        return render_template(
            "poc_web_ui.html",
            cfg=cfg,
            report_ready=report_ready,
            report_dashboard=report_dashboard,
            workload_insights=workload_insights,
            report_path=str(REPORT_PDF),
            run_status=st,
            tiers=tiers_for_ui,
            tier_labels=UI_TIER_LABELS,
            scenarios=SCENARIOS,
            security_items=SECURITY_ITEMS,
            security_result=sec,
            module_order=MODULE_ORDER,
            module_labels=MODULE_LABELS,
            config_path=str(config_path),
            selected_tier_label=ui_tier_label(selected_tier),
            selected_scenario_label=SCENARIOS.get(selected_scenario, selected_scenario),
            tier_chip_class=TIER_CHIP_CLASSES.get(selected_tier, "chip-tier-starter"),
            scenario_chip_class=SCENARIO_CHIP_CLASSES.get(selected_scenario, "chip-scenario-oltp"),
            report_chip_class="chip-report-ready" if report_ready else "chip-report-missing",
            status_classes=STATUS_CLASSES,
            workload_targets=WORKLOAD_TARGETS,
            import_method_labels=IMPORT_METHOD_LABELS,
            quickstart_workload_presets=QUICKSTART_WORKLOAD_PRESETS,
            quickstart_preset_suites=QUICKSTART_PRESET_SUITES,
            quickstart_preset_modules=QUICKSTART_PRESET_MODULES,
            quickstart_test_categories=QUICKSTART_TEST_CATEGORIES,
            quickstart_security_modes=QUICKSTART_SECURITY_MODES,
            module_insights=MODULE_INSIGHTS,
            module_suites=MODULE_SUITES,
            enabled_module_count=sum(1 for key in MODULE_ORDER if cfg.get("modules", {}).get(key)),
            comparison_targets=TARGET_DEFINITIONS,
            comparison_target_label=target_label(cfg["comparison_db"]["target"]),
            comparison_runner_supported=comparison_can_run(cfg["comparison_db"]),
            comparison_runner_reason=comparison_reason(cfg["comparison_db"]),
        )

    @app.get("/ui-skeleton")
    def ui_skeleton_route():
        return render_template("ui_skeleton.html")

    @app.post("/save-config")
    def save_config_route():
        cfg = load_cfg(config_path)

        tidb = cfg.setdefault("tidb", {})
        tidb["host"] = request.form.get("tidb_host", "").strip()
        tidb["port"] = to_int(request.form.get("tidb_port"), tidb.get("port", 4000))
        tidb["user"] = request.form.get("tidb_user", "").strip()
        tidb["password"] = request.form.get("tidb_password", "")
        tidb["database"] = request.form.get("tidb_database", "test").strip() or "test"
        tidb["ssl"] = to_bool(request.form.get("tidb_ssl"), True)

        comp = cfg.setdefault("comparison_db", {})
        comp["enabled"] = to_bool(request.form.get("comparison_enabled"), False)
        comp["target"] = request.form.get("comparison_target", "aurora_mysql").strip().lower() or "aurora_mysql"
        comp["host"] = request.form.get("comparison_host", "").strip()
        default_port = int(TARGET_DEFINITIONS.get(comp["target"], TARGET_DEFINITIONS["aurora_mysql"])["default_port"])
        comp["port"] = to_int(request.form.get("comparison_port"), default_port)
        comp["user"] = request.form.get("comparison_user", "").strip()
        comp["password"] = request.form.get("comparison_password", "")
        comp["database"] = request.form.get("comparison_database", "").strip()
        comp["schema"] = request.form.get("comparison_schema", "public").strip() or "public"
        comp["label"] = request.form.get("comparison_label", "").strip()
        comp["ssl"] = to_bool(request.form.get("comparison_ssl"), False)
        comp["ssl_mode"] = request.form.get("comparison_ssl_mode", "require").strip().lower() or "require"
        comp["sqlserver_driver"] = (
            request.form.get("comparison_sqlserver_driver", "ODBC Driver 18 for SQL Server").strip()
            or "ODBC Driver 18 for SQL Server"
        )
        comp["sqlserver_encrypt"] = to_bool(request.form.get("comparison_sqlserver_encrypt"), True)
        comp["sqlserver_trust_server_certificate"] = to_bool(
            request.form.get("comparison_sqlserver_trust_server_certificate"), False
        )
        apply_comparison_advanced_from_form(comp, request.form, "")
        cfg["comparison_db"] = normalize_comparison_cfg(comp)

        chosen_tier = request.form.get("tier_selected", "serverless")
        cfg.setdefault("tier", {})["selected"] = chosen_tier if chosen_tier in TIERS else "serverless"

        test = cfg.setdefault("test", {})
        test["data_scale"] = request.form.get("test_data_scale", "small")
        test["duration_seconds"] = to_int(request.form.get("test_duration_seconds"), 120)
        test["concurrency_levels"] = parse_concurrency(request.form.get("test_concurrency_levels", "8,16,32"), [8, 16, 32])
        test["ramp_duration_seconds"] = to_int(request.form.get("test_ramp_duration_seconds"), 300)
        test["import_rows"] = to_int(request.form.get("test_import_rows"), 1000000)
        test["workload_mix"] = request.form.get("test_workload_mix", "mixed")
        test["read_weight_multiplier"] = to_float(request.form.get("test_read_weight_multiplier"), 1.0)
        test["write_weight_multiplier"] = to_float(request.form.get("test_write_weight_multiplier"), 1.0)
        test["import_batch_size"] = to_int(request.form.get("test_import_batch_size"), 5000)
        test["import_methods"] = parse_import_methods_from_form(request.form, "test_import_method_")

        raw_queries = request.form.get("customer_queries", "")
        q_lines = [ln.strip() for ln in raw_queries.splitlines() if ln.strip()]
        cfg["customer_queries"] = q_lines
        cfg["customer_query_ratio"] = to_float(request.form.get("customer_query_ratio"), 0.30)

        mods = cfg.setdefault("modules", {})
        for key in MODULE_ORDER:
            mods[key] = (request.form.get(f"mod_{key}") == "on")

        pre = cfg.setdefault("pre_poc", {})
        pre["scenario_template"] = request.form.get("scenario_template", "oltp_migration")

        report = cfg.setdefault("report", {})
        report["company_name"] = request.form.get("report_company_name", "Your Company")
        report["include_tco_model"] = to_bool(request.form.get("report_include_tco_model"), True)
        report["current_db_monthly_cost_usd"] = to_float(request.form.get("report_current_db_monthly_cost_usd"), 0.0)
        report["output_dir"] = request.form.get("report_output_dir", "results") or "results"

        tco = cfg.setdefault("tco", {})
        tco["data_size_gb"] = to_int(request.form.get("tco_data_size_gb"), 1000)
        tco["annual_growth_pct"] = to_int(request.form.get("tco_annual_growth_pct"), 40)
        tco["aurora_shards_year0"] = to_int(request.form.get("tco_aurora_shards_year0"), 4)
        tco["engineers_managing_shards"] = to_int(request.form.get("tco_engineers_managing_shards"), 2)
        tco["engineer_annual_cost"] = to_int(request.form.get("tco_engineer_annual_cost"), 180000)
        tco["sharding_eng_fraction"] = to_float(request.form.get("tco_sharding_eng_fraction"), 0.25)

        save_cfg(config_path, cfg)
        action = str(request.form.get("save_action", "save")).strip().lower()
        allow_blocked = to_bool(request.form.get("manual_allow_blocked"), False)

        if action == "save_and_run":
            if not all([tidb.get("host"), tidb.get("user"), tidb.get("password")]):
                flash("Configuration saved, but run skipped: TiDB host/user/password are required.", "warning")
                return redirect(url_for("index") + "#manual-config")

            cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--no-wizard"]
            if allow_blocked:
                cmd.append("--allow-blocked")
            ok, msg = start_background(cmd, "manual-save-run")
            flash(msg, "success" if ok else "error")
            return redirect(url_for("index") + "#dashboards")

        flash("Configuration saved.", "success")
        return redirect(url_for("index") + "#manual-config")

    @app.post("/quickstart-deploy")
    def quickstart_deploy_route():
        cfg = load_cfg(config_path)

        selected_tier = request.form.get("wiz_tier", "serverless").strip().lower()
        if selected_tier not in TIERS:
            selected_tier = "serverless"

        apply_profile = to_bool(request.form.get("wiz_apply_profile"), False)
        workload_preset = request.form.get("wiz_workload_preset", "balanced_poc").strip().lower()
        if workload_preset not in QUICKSTART_WORKLOAD_PRESETS:
            workload_preset = "balanced_poc"

        cfg.setdefault("modules", {})
        cfg.setdefault("test", {})
        cfg.setdefault("tier", {})
        cfg.setdefault("pre_poc", {})
        preset_suite = QUICKSTART_PRESET_SUITES.get(workload_preset, "oltp_migration")

        selected_modules = {key: (request.form.get(f"wiz_mod_{key}") == "on") for key in MODULE_ORDER}
        tests_menu_present = request.form.get("wiz_tests_menu_present") == "1"

        if tests_menu_present:
            cfg["modules"] = selected_modules
            module_source_note = "Quickstart tests selected manually from wizard menu."
        else:
            cfg["modules"] = modules_from_suite(
                preset_suite,
                tier=selected_tier,
                scenario="oltp_migration",
                run_ha_sim=False,
                enable_optional_advanced=False,
                existing=cfg.get("modules", {}),
            )
            module_source_note = f"Quickstart tests derived from preset suite: {preset_suite}."

        if apply_profile:
            cfg["test"].update(tier_test_profile(selected_tier))

        preset_overrides = QUICKSTART_WORKLOAD_PRESETS[workload_preset]["overrides"]
        if preset_overrides:
            cfg["test"].update(preset_overrides)

        enabled_module_count = sum(1 for key in MODULE_ORDER if cfg["modules"].get(key))
        scenario = infer_scenario_from_modules(cfg["modules"])

        cfg["tier"].update(
            {
                "selected": selected_tier,
                "recommended": selected_tier,
                "decision_tree_version": "quickstart-wizard",
                "decision_notes": [
                    f"Quickstart wizard profile: {workload_preset}",
                    f"Quickstart preset suite: {preset_suite}",
                    f"Quickstart selected tests: {enabled_module_count}/{len(MODULE_ORDER)}",
                    module_source_note,
                ],
            }
        )
        cfg["pre_poc"]["scenario_template"] = scenario

        tidb = cfg.setdefault("tidb", {})
        tidb["host"] = request.form.get("wiz_tidb_host", "").strip()
        tidb["port"] = to_int(request.form.get("wiz_tidb_port"), tidb.get("port", 4000))
        tidb["user"] = request.form.get("wiz_tidb_user", "").strip()
        new_tidb_password = request.form.get("wiz_tidb_password", "")
        if new_tidb_password:
            tidb["password"] = new_tidb_password
        tidb["database"] = request.form.get("wiz_tidb_database", "test").strip() or "test"
        tidb["ssl"] = to_bool(request.form.get("wiz_tidb_ssl"), True)

        comp = cfg.setdefault("comparison_db", {})
        comp["enabled"] = to_bool(request.form.get("wiz_comparison_enabled"), False)
        comp["target"] = request.form.get("wiz_comparison_target", "aurora_mysql").strip().lower() or "aurora_mysql"
        comp["host"] = request.form.get("wiz_comparison_host", "").strip()
        default_port = int(TARGET_DEFINITIONS.get(comp["target"], TARGET_DEFINITIONS["aurora_mysql"])["default_port"])
        comp["port"] = to_int(request.form.get("wiz_comparison_port"), default_port)
        comp["user"] = request.form.get("wiz_comparison_user", "").strip()
        new_comp_password = request.form.get("wiz_comparison_password", "")
        if new_comp_password:
            comp["password"] = new_comp_password
        comp["database"] = request.form.get("wiz_comparison_database", "").strip()
        comp["schema"] = request.form.get("wiz_comparison_schema", "public").strip() or "public"
        comp["label"] = request.form.get("wiz_comparison_label", "").strip()
        comp["ssl"] = to_bool(request.form.get("wiz_comparison_ssl"), False)
        comp["ssl_mode"] = request.form.get("wiz_comparison_ssl_mode", "require").strip().lower() or "require"
        comp["sqlserver_driver"] = (
            request.form.get("wiz_comparison_sqlserver_driver", "ODBC Driver 18 for SQL Server").strip()
            or "ODBC Driver 18 for SQL Server"
        )
        comp["sqlserver_encrypt"] = to_bool(request.form.get("wiz_comparison_sqlserver_encrypt"), True)
        comp["sqlserver_trust_server_certificate"] = to_bool(
            request.form.get("wiz_comparison_sqlserver_trust_server_certificate"), False
        )
        apply_comparison_advanced_from_form(comp, request.form, "wiz_")
        cfg["comparison_db"] = normalize_comparison_cfg(comp)

        report = cfg.setdefault("report", {})
        company_name = request.form.get("wiz_company_name", "").strip()
        if company_name:
            report["company_name"] = company_name

        save_cfg(config_path, cfg)

        wiz_action = str(request.form.get("wiz_action", "save")).strip().lower()
        run_now = wiz_action == "run"
        allow_blocked = to_bool(request.form.get("wiz_allow_blocked"), False)

        if run_now and not all([tidb.get("host"), tidb.get("user"), tidb.get("password")]):
            flash("Quickstart saved, but run skipped: TiDB host/user/password are required.", "warning")
            return redirect(url_for("index") + "#quickstart")

        if run_now:
            cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--no-wizard"]
            if allow_blocked:
                cmd.append("--allow-blocked")
            ok, msg = start_background(cmd, "quickstart-run")
            flash(msg, "success" if ok else "error")
            return redirect(url_for("index") + "#dashboards")

        flash("Quickstart configuration saved.", "success")
        return redirect(url_for("index") + "#quickstart")

    @app.post("/apply-module-suite")
    def apply_module_suite_route():
        cfg = load_cfg(config_path)

        selected_tier = str(cfg.get("tier", {}).get("selected", "serverless"))
        if selected_tier not in TIERS:
            selected_tier = "serverless"

        scenario = str(cfg.get("pre_poc", {}).get("scenario_template", "oltp_migration"))
        if scenario not in SCENARIOS:
            scenario = "oltp_migration"

        selected_suites = [str(v).strip().lower() for v in request.form.getlist("module_suite_pick") if str(v).strip()]
        if not selected_suites:
            legacy_suite = str(request.form.get("module_suite", "")).strip().lower()
            if legacy_suite:
                selected_suites = [legacy_suite]
        selected_suites = [s for s in selected_suites if s in MODULE_SUITES]
        if not selected_suites:
            selected_suites = ["oltp_migration"]

        cfg.setdefault("modules", {})
        enabled_modules = set()
        for suite_id in selected_suites:
            if suite_id == "tier_recommended":
                tier_modules = modules_from_suite(
                    suite_id,
                    tier=selected_tier,
                    scenario=scenario,
                    run_ha_sim=False,
                    enable_optional_advanced=False,
                    existing=cfg.get("modules", {}),
                )
                for key, is_enabled in tier_modules.items():
                    if is_enabled:
                        enabled_modules.add(key)
            else:
                enabled_modules.update(MODULE_SUITE_KEYS.get(suite_id, []))

        cfg["modules"] = {key: (key in enabled_modules) for key in MODULE_ORDER}
        cfg.setdefault("pre_poc", {})
        cfg["pre_poc"]["scenario_template"] = infer_scenario_from_modules(cfg["modules"])

        if to_bool(request.form.get("suite_apply_profile"), False):
            cfg.setdefault("test", {})
            cfg["test"].update(tier_test_profile(selected_tier))

        save_cfg(config_path, cfg)
        suite_labels = [MODULE_SUITES.get(suite_id, {}).get("label", suite_id) for suite_id in selected_suites]
        enabled_count = sum(1 for key in MODULE_ORDER if cfg["modules"].get(key))
        flash(
            f"Applied module suite helper: {', '.join(suite_labels)} ({enabled_count}/{len(MODULE_ORDER)} modules enabled).",
            "success",
        )
        return redirect(url_for("index") + "#manual-config")

    @app.post("/save-module-selection")
    def save_module_selection_route():
        cfg = load_cfg(config_path)
        mods = cfg.setdefault("modules", {})
        for key in MODULE_ORDER:
            mods[key] = (request.form.get(f"plan_mod_{key}") == "on")
        save_cfg(config_path, cfg)
        enabled = sum(1 for key in MODULE_ORDER if mods.get(key))
        flash(f"Saved module selection ({enabled}/{len(MODULE_ORDER)} enabled).", "success")
        return redirect(url_for("index") + "#manual-config")

    @app.post("/apply-tier")
    def apply_tier_route():
        cfg = load_cfg(config_path)

        selected_tier = request.form.get("apply_tier", "serverless")
        if selected_tier not in TIERS:
            flash("Invalid tier selection.", "error")
            return redirect(url_for("index") + "#manual-config")

        scenario = cfg.get("pre_poc", {}).get("scenario_template", "oltp_migration")
        if scenario not in SCENARIOS:
            scenario = "oltp_migration"

        run_ha_sim = to_bool(request.form.get("apply_run_ha_sim"), False)
        enable_optional_advanced = to_bool(request.form.get("apply_enable_optional_advanced"), False)
        apply_profile = to_bool(request.form.get("apply_profile"), True)

        cfg.setdefault("modules", {})
        cfg.setdefault("test", {})
        cfg.setdefault("tier", {})

        cfg["modules"] = build_tier_modules(
            tier=selected_tier,
            scenario=scenario,
            run_ha_sim=run_ha_sim,
            enable_optional_advanced=enable_optional_advanced,
            existing=cfg.get("modules", {}),
        )

        if apply_profile:
            cfg["test"].update(tier_test_profile(selected_tier))

        cfg["tier"].update(
            {
                "selected": selected_tier,
                "recommended": selected_tier,
                "decision_tree_version": "web-ui-manual",
                "decision_notes": ["Selected from web UI tier panel."],
            }
        )

        save_cfg(config_path, cfg)
        flash(f"Applied tier profile: {ui_tier_label(selected_tier)}.", "success")
        return redirect(url_for("index") + "#manual-config")

    @app.post("/security")
    def security_route():
        cfg = load_cfg(config_path)
        result = build_security_result(request.form)

        cfg.setdefault("pre_poc", {})
        cfg["pre_poc"]["security"] = result
        cfg["pre_poc"]["go_no_go"] = "proceed" if result["proceed"] else "hold"

        save_cfg(config_path, cfg)
        flash(
            f"Security screener saved. Recommendation: {result['recommendation']}",
            "warning" if not result["proceed"] else "success",
        )
        return redirect(url_for("index") + "#security")

    @app.post("/run-defaults")
    def run_defaults_route():
        cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--no-wizard"]
        ok, msg = start_background(cmd, "run-defaults")
        flash(msg, "success" if ok else "error")
        return redirect(url_for("index") + "#dashboards")

    @app.post("/build-report")
    def build_report_route():
        cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--report-only"]
        ok, msg = start_background(cmd, "report-only")
        flash(msg, "success" if ok else "error")
        return redirect(url_for("index") + "#dashboards")

    @app.post("/run-workload")
    def run_workload_route():
        cfg = load_cfg(config_path)
        action = str(request.form.get("wl_action", "run")).lower()
        target = str(request.form.get("wl_target", "baseline_perf"))

        if target not in WORKLOAD_TARGETS:
            flash("Invalid workload target selected.", "error")
            return redirect(url_for("index") + "#workload-lab")

        test = cfg.setdefault("test", {})
        wl = cfg.setdefault("workload_lab", {})

        mix = str(request.form.get("wl_mix", "mixed")).lower()
        if mix not in {"mixed", "read_heavy", "write_heavy"}:
            mix = "mixed"

        concurrency = max(1, to_int(request.form.get("wl_concurrency"), 16))
        duration_sec = max(10, to_int(request.form.get("wl_duration_seconds"), 120))
        customer_ratio = to_float(request.form.get("wl_customer_ratio"), cfg.get("customer_query_ratio", 0.30))
        read_mult = max(0.1, to_float(request.form.get("wl_read_multiplier"), 1.0))
        write_mult = max(0.1, to_float(request.form.get("wl_write_multiplier"), 1.0))
        import_rows = max(10_000, to_int(request.form.get("wl_import_rows"), test.get("import_rows", 1_000_000)))
        import_batch_size = max(100, to_int(request.form.get("wl_import_batch_size"), test.get("import_batch_size", 5000)))
        import_methods = parse_import_methods_from_form(request.form, "wl_method_")

        test["workload_mix"] = mix
        test["read_weight_multiplier"] = read_mult
        test["write_weight_multiplier"] = write_mult
        test["import_rows"] = import_rows
        test["import_batch_size"] = import_batch_size
        test["import_methods"] = import_methods
        cfg["customer_query_ratio"] = customer_ratio

        if target == "baseline_perf":
            test["duration_seconds"] = duration_sec
            test["concurrency_levels"] = [concurrency]

        wl["target"] = target
        wl["concurrency"] = concurrency
        wl["duration_seconds"] = duration_sec

        save_cfg(config_path, cfg)

        if action == "save":
            flash("Workload tuning saved.", "success")
            return redirect(url_for("index") + "#workload-lab")

        script = WORKLOAD_TARGETS[target]["script"]
        if not Path(script).exists():
            flash(f"Workload runner not found: {script}", "error")
            return redirect(url_for("index") + "#workload-lab")

        cmd = [sys.executable, str(script), str(config_path)]
        ok, msg = start_background(cmd, f"workload-{target}")
        flash(msg, "success" if ok else "error")
        return redirect(url_for("index") + "#workload-lab")

    @app.post("/clear-data")
    def clear_data_route():
        token = request.form.get("clear_token", "")
        if token != "CLEAR":
            flash("Clear cancelled: token did not match CLEAR.", "error")
            return redirect(url_for("index") + "#data-reset")

        cfg = load_cfg(config_path)
        clear_local_results()
        msg = "Cleared local results artifacts."

        if to_bool(request.form.get("drop_db"), False):
            drop_token = request.form.get("drop_token", "")
            if drop_token != "DROP":
                flash("Local results cleared, but DB drop skipped (DROP token not provided).", "warning")
                return redirect(url_for("index") + "#data-reset")

            success, db_msg = drop_configured_database(cfg)
            if success:
                msg += f" {db_msg}"
                flash(msg, "success")
            else:
                flash(f"Local results cleared, but DB drop failed: {db_msg}", "error")
            return redirect(url_for("index") + "#data-reset")

        flash(msg, "success")
        return redirect(url_for("index") + "#data-reset")

    @app.get("/report")
    def report_route():
        if not REPORT_PDF.exists():
            flash("Report PDF not available yet.", "error")
            return redirect(url_for("index") + "#dashboards")
        return send_file(REPORT_PDF, as_attachment=False)

    @app.get("/run-status")
    def run_status_route():
        st = run_status()
        st["report_ready"] = REPORT_PDF.exists()
        return jsonify(st)

    return app


def main() -> int:
    args = parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (ROOT / config_path).resolve()

    app = create_app(config_path)

    print(f"\nPoC Web UI starting at http://{args.host}:{args.port}")
    print(f"Config file: {config_path}")
    print("Press Ctrl+C to stop.\n")

    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
