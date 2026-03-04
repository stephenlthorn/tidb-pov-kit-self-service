"""
comparison_targets.py — Comparison database target definitions + helpers.

The current benchmark workload generator and SQL templates are MySQL dialect.
Targets in non-MySQL families can be configured in the UI/config, but are
marked as not yet executable by the side-by-side benchmark runner.
"""

from __future__ import annotations

from typing import Dict

TARGET_DEFINITIONS: Dict[str, Dict] = {
    "aurora_mysql": {
        "label": "Aurora MySQL",
        "family": "mysql",
        "default_port": 3306,
        "runner_supported": True,
    },
    "mysql": {
        "label": "MySQL",
        "family": "mysql",
        "default_port": 3306,
        "runner_supported": True,
    },
    "rds_mysql": {
        "label": "RDS MySQL",
        "family": "mysql",
        "default_port": 3306,
        "runner_supported": True,
    },
    "singlestore": {
        "label": "SingleStore",
        "family": "mysql",
        "default_port": 3306,
        "runner_supported": True,
    },
    "postgres": {
        "label": "PostgreSQL",
        "family": "postgres",
        "default_port": 5432,
        "runner_supported": False,
    },
    "rds_postgres": {
        "label": "RDS PostgreSQL",
        "family": "postgres",
        "default_port": 5432,
        "runner_supported": False,
    },
    "aurora_postgres": {
        "label": "Aurora PostgreSQL",
        "family": "postgres",
        "default_port": 5432,
        "runner_supported": False,
    },
    "microsoft_sql_server": {
        "label": "Microsoft SQL Server",
        "family": "mssql",
        "default_port": 1433,
        "runner_supported": False,
    },
}

DEFAULT_TARGET = "aurora_mysql"


def target_definition(target: str) -> Dict:
    if target in TARGET_DEFINITIONS:
        return TARGET_DEFINITIONS[target]
    return TARGET_DEFINITIONS[DEFAULT_TARGET]


def target_label(target: str) -> str:
    return target_definition(target)["label"]


def is_runner_supported(target: str) -> bool:
    return bool(target_definition(target).get("runner_supported"))


def normalize_comparison_cfg(raw_cfg: Dict | None) -> Dict:
    cfg = dict(raw_cfg or {})
    target = str(cfg.get("target", DEFAULT_TARGET)).strip().lower()
    if target not in TARGET_DEFINITIONS:
        target = DEFAULT_TARGET
    meta = target_definition(target)
    family = str(meta.get("family", "mysql"))
    default_schema = "dbo" if family == "mssql" else "public"

    try:
        port = int(cfg.get("port", meta["default_port"]))
    except (TypeError, ValueError):
        port = int(meta["default_port"])

    return {
        "enabled": bool(cfg.get("enabled", False)),
        "target": target,
        "label": str(cfg.get("label") or meta["label"]),
        "host": str(cfg.get("host") or "").strip(),
        "port": port,
        "database": str(cfg.get("database") or "").strip(),
        "schema": str(cfg.get("schema") or default_schema).strip() or default_schema,
        "user": str(cfg.get("user") or "").strip(),
        "password": str(cfg.get("password") or ""),
        "ssl": bool(cfg.get("ssl", False)),
        "ssl_mode": str(cfg.get("ssl_mode") or "require").strip().lower(),
        "sqlserver_driver": str(cfg.get("sqlserver_driver") or "ODBC Driver 18 for SQL Server"),
        "sqlserver_encrypt": bool(cfg.get("sqlserver_encrypt", True)),
        "sqlserver_trust_server_certificate": bool(cfg.get("sqlserver_trust_server_certificate", False)),
    }


def comparison_can_run(cfg: Dict) -> bool:
    if not cfg.get("enabled"):
        return False
    if not cfg.get("host"):
        return False
    return is_runner_supported(str(cfg.get("target", DEFAULT_TARGET)))


def comparison_reason(cfg: Dict) -> str:
    if not cfg.get("enabled"):
        return "Comparison target is disabled."
    if not cfg.get("host"):
        return "Comparison target host is missing."
    target = str(cfg.get("target", DEFAULT_TARGET))
    if not is_runner_supported(target):
        return (
            f"Configured target {target_label(target)} is not yet supported by the automated "
            "MySQL-dialect side-by-side runner."
        )
    return "Comparison target ready."
