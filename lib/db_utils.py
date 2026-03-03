"""
db_utils.py — Shared database connection and query utilities.
Used by all test modules and the load runner.
"""
import time
import mysql.connector
from mysql.connector import Error


def get_connection(cfg: dict, autocommit: bool = True):
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


def create_database_if_missing(cfg: dict):
    """Create the target database if it doesn't exist."""
    root_cfg = dict(cfg)
    root_cfg.pop("database", None)
    conn = get_connection(root_cfg)
    cur = conn.cursor()
    db = cfg.get("database", "pov_test")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    conn.close()
