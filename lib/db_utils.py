"""
db_utils.py - Shared database connection and query utilities.

Dispatches to mysql-connector or psycopg (PostgreSQL) based on the comparison
target family configured in cfg. TiDB and the mysql/aurora_mysql/rds_mysql/
singlestore comparison targets use mysql-connector. The postgres family
(postgres / rds_postgres / aurora_postgres) uses psycopg 3.

The TiDB-only path remains identical to its prior behavior; postgres support
is additive and gated by cfg["target"].
"""
from __future__ import annotations
import os
import time
import mysql.connector
from mysql.connector import Error as MySQLError

try:
    import psycopg
    _HAS_PSYCOPG = True
except ImportError:
    psycopg = None
    _HAS_PSYCOPG = False


_POSTGRES_TARGETS = {"postgres", "rds_postgres", "aurora_postgres"}


def family_of(cfg: dict) -> str:
    """Return 'postgres' or 'mysql' based on cfg['target']. Defaults to mysql."""
    target = str(cfg.get("target") or "").strip().lower()
    if target in _POSTGRES_TARGETS:
        return "postgres"
    return "mysql"


def get_connection(cfg: dict, autocommit: bool = True, session_vars: dict = None):
    """Return a driver-native connection appropriate for the cfg's family.

    For mysql-family targets (including TiDB) this is mysql-connector and
    session_vars / cfg["session_variables"] / cfg["txn_mode"] are applied
    via SET @@var statements after connect.

    For postgres-family targets this is psycopg 3; session_vars is currently
    ignored on that path because the upstream TiDB-style SET @@ syntax does
    not translate.
    """
    if family_of(cfg) == "postgres":
        return _get_pg_connection(cfg, autocommit)
    return _get_mysql_connection(cfg, autocommit, session_vars)


def _get_mysql_connection(cfg: dict, autocommit: bool, session_vars: dict = None):
    # MySQL 8.0+ servers using caching_sha2_password require encrypted transport
    # for password auth. Honor explicit cfg["ssl"]=True with verification, but
    # for cfg["ssl"]=False on a non-loopback host fall back to disabled SSL
    # (assumes a pre-shared trust path like VPC peering). For loopback hosts
    # use opportunistic SSL with cert verification off, which lets local Docker
    # MySQL containers connect without TLS configuration.
    host = cfg["host"]
    is_loopback = host in {"127.0.0.1", "localhost", "::1"} or host.startswith("127.")
    if cfg.get("ssl"):
        ssl_args = {"ssl_disabled": False, "ssl_verify_cert": False, "ssl_verify_identity": False}
    elif is_loopback:
        ssl_args = {"ssl_disabled": False, "ssl_verify_cert": False, "ssl_verify_identity": False}
    else:
        ssl_args = {"ssl_disabled": True}

    conn_kwargs = dict(
        host=host,
        port=cfg.get("port", 4000),
        user=cfg["user"],
        password=cfg["password"],
        connection_timeout=30,
        **ssl_args,
    )
    if cfg.get("database"):
        conn_kwargs["database"] = cfg["database"]
    conn = mysql.connector.connect(**conn_kwargs)
    conn.autocommit = autocommit

    # Apply TiDB session variables from config (preserved from upstream).
    all_vars = {}
    if cfg.get("session_variables"):
        all_vars.update(cfg["session_variables"])
    if cfg.get("txn_mode") and cfg["txn_mode"] != "autocommit":
        all_vars["tidb_txn_mode"] = cfg["txn_mode"]
    if session_vars:
        all_vars.update(session_vars)

    if all_vars:
        cur = conn.cursor()
        for var, val in all_vars.items():
            try:
                cur.execute(f"SET @@{var} = %s", (val,))
            except Exception:
                pass
        cur.close()

    return conn


def _get_pg_connection(cfg: dict, autocommit: bool):
    if not _HAS_PSYCOPG:
        raise RuntimeError(
            "psycopg is required for postgres targets but is not installed. "
            "Install with: pip install 'psycopg[binary]>=3.2'"
        )
    ssl_mode = cfg.get("ssl_mode") or ("require" if cfg.get("ssl") else "disable")
    conninfo = {
        "host": cfg["host"],
        "port": cfg.get("port", 5432),
        "user": cfg["user"],
        "password": cfg["password"],
        "sslmode": ssl_mode,
        "connect_timeout": 30,
        "application_name": cfg.get("pg_application_name") or "tidb_pov_comparison",
    }
    if cfg.get("database"):
        conninfo["dbname"] = cfg["database"]
    conn = psycopg.connect(**conninfo)
    conn.autocommit = autocommit
    # Set search_path if a non-default schema was requested.
    schema = (cfg.get("pg_search_path") or cfg.get("schema") or "").strip()
    if schema and schema != "public":
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {_safe_pg_ident_or_default(schema, 'public')}, public")
    return conn


def _safe_pg_ident_or_default(name: str, fallback: str) -> str:
    """Reject identifiers that aren't a simple SQL name; postgres has no
    parameterized identifier substitution and we don't want SQL injection."""
    import re
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$", name or ""):
        return name
    return fallback


def execute_timed(cursor, sql: str, params=None) -> dict:
    """Execute a SQL statement and return latency + success metadata.

    Works across mysql-connector and psycopg cursors. For non-result statements
    (INSERT/UPDATE/DDL) we skip the fetchall drain rather than letting the
    driver raise on an empty result set.
    """
    t0 = time.perf_counter()
    try:
        cursor.execute(sql, params or ())
        try:
            if getattr(cursor, "description", None) is not None:
                cursor.fetchall()
        except Exception:
            # Some drivers raise on fetch when there's nothing to drain; the
            # underlying execute already succeeded so we treat this as a no-op.
            pass
        latency_ms = (time.perf_counter() - t0) * 1000
        return {"latency_ms": latency_ms, "success": True, "error": None, "retries": 0}
    except (MySQLError, Exception) as e:
        latency_ms = (time.perf_counter() - t0) * 1000
        return {"latency_ms": latency_ms, "success": False, "error": str(e), "retries": 0}


def ping(cfg: dict) -> tuple[bool, str]:
    """Test connectivity. Returns (ok, message)."""
    try:
        conn = get_connection(cfg)
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        conn.close()
        return True, version
    except Exception as e:
        return False, str(e)


def capture_cluster_info(tidb_cfg: dict) -> dict:
    """Query TiDB for cluster metadata and save to results/cluster_info.json."""
    info = {}
    try:
        conn = get_connection(tidb_cfg)
        cur = conn.cursor()

        # TiDB version
        try:
            cur.execute("SELECT tidb_version()")
            row = cur.fetchone()
            if row:
                info["tidb_version"] = str(row[0])[:200]
        except Exception:
            pass

        # Store count
        try:
            cur.execute("SELECT TYPE, COUNT(*) AS cnt FROM INFORMATION_SCHEMA.CLUSTER_INFO GROUP BY TYPE")
            rows = cur.fetchall()
            for r in rows:
                info[f"node_count_{r[0].lower()}"] = int(r[1])
        except Exception:
            pass

        # Region count
        try:
            cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS")
            row = cur.fetchone()
            if row:
                info["region_count"] = int(row[0])
        except Exception:
            pass

        # TiFlash replica count
        try:
            cur.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TIFLASH_REPLICA "
                "WHERE AVAILABLE = 1"
            )
            row = cur.fetchone()
            if row:
                info["tiflash_available_replicas"] = int(row[0])
        except Exception:
            pass

        conn.close()
    except Exception:
        pass

    # Save to file
    if info:
        import json as _json
        path = os.path.join(os.path.dirname(__file__), "..", "results", "cluster_info.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            _json.dump(info, f, indent=2)

    return info


def create_database_if_missing(cfg: dict):
    """Create the target database if it doesn't exist."""
    db = cfg.get("database", "pov_test")
    if family_of(cfg) == "postgres":
        admin_cfg = dict(cfg)
        admin_cfg["database"] = cfg.get("admin_database") or "postgres"
        conn = get_connection(admin_cfg, autocommit=True)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
        if not cur.fetchone():
            safe = _safe_pg_ident_or_default(db, "")
            if not safe:
                raise ValueError(f"Unsafe postgres database identifier: {db!r}")
            cur.execute(f'CREATE DATABASE "{safe}"')
        conn.close()
        return
    root_cfg = dict(cfg)
    root_cfg.pop("database", None)
    conn = get_connection(root_cfg)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    conn.close()
