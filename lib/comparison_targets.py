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


def _to_int(value, default: int, minimum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = int(default)
    if minimum is not None:
        out = max(minimum, out)
    return out


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
        "connect_timeout_sec": _to_int(cfg.get("connect_timeout_sec", 30), 30, 1),
        "statement_timeout_sec": _to_int(cfg.get("statement_timeout_sec", 60), 60, 1),
        "retry_count": _to_int(cfg.get("retry_count", 1), 1, 0),
        "retry_backoff_ms": _to_int(cfg.get("retry_backoff_ms", 500), 500, 0),
        "max_pool_size": _to_int(cfg.get("max_pool_size", 8), 8, 1),
        "session_init_sql": str(cfg.get("session_init_sql") or "").strip(),
        "read_only_mode": bool(cfg.get("read_only_mode", True)),
        "parity_sample_size": _to_int(cfg.get("parity_sample_size", 250), 250, 1),
        "capture_explain_plans": bool(cfg.get("capture_explain_plans", True)),
        "include_tables": str(cfg.get("include_tables") or "").strip(),
        "exclude_tables": str(cfg.get("exclude_tables") or "").strip(),
        "tls_ca_path": str(cfg.get("tls_ca_path") or "").strip(),
        "tls_cert_path": str(cfg.get("tls_cert_path") or "").strip(),
        "tls_key_path": str(cfg.get("tls_key_path") or "").strip(),
        "mysql_sql_mode": str(cfg.get("mysql_sql_mode") or "").strip(),
        "mysql_time_zone": str(cfg.get("mysql_time_zone") or "UTC").strip() or "UTC",
        "mysql_tx_isolation": str(cfg.get("mysql_tx_isolation") or "READ COMMITTED").strip() or "READ COMMITTED",
        "pg_application_name": str(cfg.get("pg_application_name") or "tidb_pov_comparison").strip()
        or "tidb_pov_comparison",
        "pg_search_path": str(cfg.get("pg_search_path") or "public").strip() or "public",
        "pg_lock_timeout_ms": _to_int(cfg.get("pg_lock_timeout_ms", 5000), 5000, 0),
        "sqlserver_command_timeout_sec": _to_int(cfg.get("sqlserver_command_timeout_sec", 60), 60, 1),
        "sqlserver_application_intent": str(cfg.get("sqlserver_application_intent") or "ReadWrite").strip()
        or "ReadWrite",
        "sqlserver_mars": bool(cfg.get("sqlserver_mars", False)),
        "sqlserver_packet_size": _to_int(cfg.get("sqlserver_packet_size", 4096), 4096, 512),
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
            f"Configured target {target_label(target)} is captured for planning, but automated "
            "side-by-side execution is not available in the current runner."
        )
    return "Comparison target ready."
