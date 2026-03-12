"""
db_utils.py — Shared database connection and query utilities.
Used by all test modules and the load runner.
"""
import os
import time
import mysql.connector
from mysql.connector import Error


def get_connection(cfg: dict, autocommit: bool = True, session_vars: dict = None):
    """Return a mysql-connector connection from a config dict block."""
    ssl_args = {"ssl_disabled": False} if cfg.get("ssl") else {"ssl_disabled": True}
    conn_kwargs = dict(
        host=cfg["host"],
        port=cfg.get("port", 4000),
        user=cfg["user"],
        password=cfg["password"],
        connection_timeout=30,
        **ssl_args,
    )
    # Allow server-level connections (no default schema) for bootstrap actions
    # such as CREATE DATABASE IF NOT EXISTS.
    if cfg.get("database"):
        conn_kwargs["database"] = cfg["database"]

    conn = mysql.connector.connect(**conn_kwargs)
    conn.autocommit = autocommit

    # Apply TiDB session variables from config
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


def execute_timed(cursor, sql: str, params=None) -> dict:
    """Execute a SQL statement and return latency + success metadata."""
    t0 = time.perf_counter()
    try:
        cursor.execute(sql, params or ())
        cursor.fetchall()   # drain result set
        latency_ms = (time.perf_counter() - t0) * 1000
        return {"latency_ms": latency_ms, "success": True, "error": None, "retries": 0}
    except Error as e:
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
    root_cfg = dict(cfg)
    root_cfg.pop("database", None)
    conn = get_connection(root_cfg)
    cur = conn.cursor()
    db = cfg.get("database", "pov_test")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    conn.close()
