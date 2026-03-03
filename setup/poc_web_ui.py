#!/usr/bin/env python3
"""Dark-themed web UI for TiDB Cloud PoV kit configuration and workflow actions."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

try:
    from flask import Flask, flash, redirect, render_template, request, send_file, url_for
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

REPORT_PDF = RESULTS_DIR / "tidb_pov_report.pdf"
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


DEFAULT_CFG = {
    "tidb": {
        "host": "",
        "port": 4000,
        "user": "",
        "password": "",
        "database": "pov_test",
        "ssl": True,
    },
    "comparison_db": {
        "enabled": False,
        "host": "",
        "port": 3306,
        "user": "",
        "password": "",
        "database": "",
        "label": "Aurora MySQL",
        "ssl": False,
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

    if blocking_failures:
        recommendation = "hold"
        proceed = False
    elif non_blocking_failures:
        recommendation = "proceed_with_risks"
        proceed = True
    elif any(r["status"] == "not_assessed" for r in items):
        recommendation = "review_required"
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


def create_app(config_path: Path) -> Flask:
    app = Flask(__name__, template_folder=str(TEMPLATES_DIR))
    app.secret_key = "tidb-pov-local-ui"

    @app.get("/")
    def index():
        cfg = load_cfg(config_path)
        sec = security_from_cfg(cfg)
        st = run_status()
        report_ready = REPORT_PDF.exists()

        return render_template(
            "poc_web_ui.html",
            cfg=cfg,
            report_ready=report_ready,
            report_path=str(REPORT_PDF),
            run_status=st,
            tiers=TIERS,
            tier_labels=TIER_LABELS,
            scenarios=SCENARIOS,
            security_items=SECURITY_ITEMS,
            security_result=sec,
            module_order=MODULE_ORDER,
            module_labels=MODULE_LABELS,
            config_path=str(config_path),
        )

    @app.post("/save-config")
    def save_config_route():
        cfg = load_cfg(config_path)

        tidb = cfg.setdefault("tidb", {})
        tidb["host"] = request.form.get("tidb_host", "").strip()
        tidb["port"] = to_int(request.form.get("tidb_port"), tidb.get("port", 4000))
        tidb["user"] = request.form.get("tidb_user", "").strip()
        tidb["password"] = request.form.get("tidb_password", "")
        tidb["database"] = request.form.get("tidb_database", "pov_test").strip() or "pov_test"
        tidb["ssl"] = to_bool(request.form.get("tidb_ssl"), True)

        comp = cfg.setdefault("comparison_db", {})
        comp["enabled"] = to_bool(request.form.get("comparison_enabled"), False)
        comp["host"] = request.form.get("comparison_host", "").strip()
        comp["port"] = to_int(request.form.get("comparison_port"), 3306)
        comp["user"] = request.form.get("comparison_user", "").strip()
        comp["password"] = request.form.get("comparison_password", "")
        comp["database"] = request.form.get("comparison_database", "").strip()
        comp["label"] = request.form.get("comparison_label", "Aurora MySQL").strip() or "Aurora MySQL"
        comp["ssl"] = to_bool(request.form.get("comparison_ssl"), False)

        cfg.setdefault("tier", {})["selected"] = request.form.get("tier_selected", "serverless")

        test = cfg.setdefault("test", {})
        test["data_scale"] = request.form.get("test_data_scale", "small")
        test["duration_seconds"] = to_int(request.form.get("test_duration_seconds"), 120)
        test["concurrency_levels"] = parse_concurrency(request.form.get("test_concurrency_levels", "8,16,32"), [8, 16, 32])
        test["ramp_duration_seconds"] = to_int(request.form.get("test_ramp_duration_seconds"), 300)
        test["import_rows"] = to_int(request.form.get("test_import_rows"), 1000000)
        test["workload_mix"] = request.form.get("test_workload_mix", "mixed")

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
        flash("Configuration saved.", "success")
        return redirect(url_for("index"))

    @app.post("/apply-tier")
    def apply_tier_route():
        cfg = load_cfg(config_path)

        selected_tier = request.form.get("apply_tier", "serverless")
        if selected_tier not in TIERS:
            flash("Invalid tier selection.", "error")
            return redirect(url_for("index"))

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
        flash(f"Applied tier profile: {selected_tier}.", "success")
        return redirect(url_for("index"))

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
        return redirect(url_for("index"))

    @app.post("/run-defaults")
    def run_defaults_route():
        cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--no-wizard"]
        ok, msg = start_background(cmd, "run-defaults")
        flash(msg, "success" if ok else "error")
        return redirect(url_for("index"))

    @app.post("/build-report")
    def build_report_route():
        cmd = [str(RUN_SCRIPT), str(config_path), "--no-menu", "--report-only"]
        ok, msg = start_background(cmd, "report-only")
        flash(msg, "success" if ok else "error")
        return redirect(url_for("index"))

    @app.post("/clear-data")
    def clear_data_route():
        token = request.form.get("clear_token", "")
        if token != "CLEAR":
            flash("Clear cancelled: token did not match CLEAR.", "error")
            return redirect(url_for("index"))

        cfg = load_cfg(config_path)
        clear_local_results()
        msg = "Cleared local results artifacts."

        if to_bool(request.form.get("drop_db"), False):
            drop_token = request.form.get("drop_token", "")
            if drop_token != "DROP":
                flash("Local results cleared, but DB drop skipped (DROP token not provided).", "warning")
                return redirect(url_for("index"))

            success, db_msg = drop_configured_database(cfg)
            if success:
                msg += f" {db_msg}"
                flash(msg, "success")
            else:
                flash(f"Local results cleared, but DB drop failed: {db_msg}", "error")
            return redirect(url_for("index"))

        flash(msg, "success")
        return redirect(url_for("index"))

    @app.get("/report")
    def report_route():
        if not REPORT_PDF.exists():
            flash("Report PDF not available yet.", "error")
            return redirect(url_for("index"))
        return send_file(REPORT_PDF, as_attachment=False)

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
