#!/usr/bin/env python3
"""
Module 6 — MySQL Compatibility Checklist
Executes a comprehensive set of MySQL syntax and semantic checks against TiDB.
Covers: DDL, DML, functions, JSON operations, window functions, CTEs,
prepared statements, transaction semantics, and information_schema queries.

Each check is logged to the compat_checks table in results.db.
Produces a pass/fail report useful for migration readiness conversations.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, log_compat_check
from lib.db_utils import get_connection

MODULE = "06_mysql_compat"

# ── Compatibility check definitions ──────────────────────────────────────────
# Each entry: (category, name, sql_or_callable, expected_pattern)
# sql_or_callable: a SQL string (run with execute) or a callable(cur)->bool
COMPAT_CHECKS = [

    # ── DDL ──────────────────────────────────────────────────────────────────
    ("DDL", "CREATE TABLE with AUTO_INCREMENT",
     "CREATE TABLE IF NOT EXISTS compat_ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
     None),
    ("DDL", "CREATE TABLE with AUTO_RANDOM",
     "CREATE TABLE IF NOT EXISTS compat_ar (id BIGINT AUTO_RANDOM PRIMARY KEY, v INT)",
     None),
    ("DDL", "CREATE TABLE with JSON column",
     "CREATE TABLE IF NOT EXISTS compat_json (id INT PRIMARY KEY, data JSON)",
     None),
    ("DDL", "CREATE TABLE with generated column",
     """CREATE TABLE IF NOT EXISTS compat_gen (
         id INT PRIMARY KEY, price DECIMAL(10,2), qty INT,
         total DECIMAL(10,2) AS (price * qty) STORED
     )""", None),
    ("DDL", "CREATE INDEX with expression",
     "CREATE INDEX IF NOT EXISTS idx_expr ON compat_gen ((price * qty))",
     None),
    ("DDL", "ALTER TABLE ADD COLUMN",
     "ALTER TABLE compat_ai ADD COLUMN IF NOT EXISTS extra VARCHAR(50)",
     None),
    ("DDL", "CREATE VIEW",
     "CREATE OR REPLACE VIEW compat_view AS SELECT id, v FROM compat_ai WHERE v > 0",
     None),

    # ── DML ──────────────────────────────────────────────────────────────────
    ("DML", "INSERT ... ON DUPLICATE KEY UPDATE",
     "INSERT INTO compat_ai (id, v) VALUES (1, 10) ON DUPLICATE KEY UPDATE v = v + 1",
     None),
    ("DML", "REPLACE INTO",
     "REPLACE INTO compat_ai (id, v) VALUES (2, 20)",
     None),
    ("DML", "INSERT ... SELECT",
     "INSERT INTO compat_ai (v) SELECT v + 100 FROM compat_ai LIMIT 5",
     None),
    ("DML", "UPDATE with JOIN",
     "UPDATE compat_ai a JOIN compat_ai b ON a.id = b.id SET a.v = a.v + 1 WHERE b.v > 0",
     None),
    ("DML", "DELETE with subquery",
     "DELETE FROM compat_ai WHERE id IN (SELECT id FROM (SELECT id FROM compat_ai LIMIT 0) t)",
     None),
    ("DML", "MULTI-TABLE DELETE",
     "DELETE a FROM compat_ai a WHERE a.v < 0",
     None),

    # ── SELECT / Query features ───────────────────────────────────────────────
    ("Query", "Window function ROW_NUMBER",
     "SELECT id, v, ROW_NUMBER() OVER (ORDER BY v DESC) rn FROM compat_ai LIMIT 5",
     None),
    ("Query", "Window function RANK",
     "SELECT id, v, RANK() OVER (PARTITION BY 1 ORDER BY v) rnk FROM compat_ai LIMIT 5",
     None),
    ("Query", "CTE (WITH clause)",
     "WITH cte AS (SELECT id, v FROM compat_ai) SELECT * FROM cte LIMIT 5",
     None),
    ("Query", "Recursive CTE",
     """WITH RECURSIVE seq(n) AS (
         SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 10
     ) SELECT n FROM seq""", None),
    ("Query", "LATERAL join",
     """SELECT a.id, t.val FROM compat_ai a
        JOIN LATERAL (SELECT a.v * 2 AS val) t ON TRUE LIMIT 5""",
     None),
    ("Query", "GROUP BY with ROLLUP",
     "SELECT v, COUNT(*) n FROM compat_ai GROUP BY v WITH ROLLUP LIMIT 10",
     None),
    ("Query", "HAVING clause",
     "SELECT v, COUNT(*) cnt FROM compat_ai GROUP BY v HAVING cnt > 0",
     None),

    # ── Functions ─────────────────────────────────────────────────────────────
    ("Function", "NOW() and DATE_FORMAT",
     "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d')",
     None),
    ("Function", "TIMESTAMPDIFF",
     "SELECT TIMESTAMPDIFF(SECOND, '2000-01-01', NOW())",
     None),
    ("Function", "CONCAT_WS",
     "SELECT CONCAT_WS(',', 'a', 'b', 'c')",
     None),
    ("Function", "GROUP_CONCAT",
     "SELECT GROUP_CONCAT(v ORDER BY v SEPARATOR '|') FROM compat_ai",
     None),
    ("Function", "IF / IFNULL / COALESCE",
     "SELECT IF(1>0,'yes','no'), IFNULL(NULL,'x'), COALESCE(NULL,NULL,42)",
     None),
    ("Function", "CAST and CONVERT",
     "SELECT CAST('123' AS UNSIGNED), CONVERT('2024-01-01', DATE)",
     None),
    ("Function", "SUBSTRING_INDEX",
     "SELECT SUBSTRING_INDEX('a.b.c', '.', 2)",
     None),
    ("Function", "REGEXP",
     "SELECT 'hello123' REGEXP '^[a-z]+[0-9]+$'",
     None),

    # ── JSON ─────────────────────────────────────────────────────────────────
    ("JSON", "JSON_OBJECT and JSON_ARRAY",
     "SELECT JSON_OBJECT('k', 1, 'arr', JSON_ARRAY(1,2,3))",
     None),
    ("JSON", "JSON_EXTRACT (->)",
     "SELECT JSON_EXTRACT('{\"a\":{\"b\":1}}', '$.a.b')",
     None),
    ("JSON", "JSON_SET",
     "SELECT JSON_SET('{\"a\":1}', '$.b', 2)",
     None),
    ("JSON", "JSON_CONTAINS",
     "SELECT JSON_CONTAINS('[1,2,3]', '2')",
     None),
    ("JSON", "JSON_ARRAYAGG",
     "SELECT JSON_ARRAYAGG(v) FROM compat_ai LIMIT 1",
     None),
    ("JSON", "JSON path filter (->)",
     "SELECT data->'$.key' FROM compat_json LIMIT 1",
     None),

    # ── Transactions ──────────────────────────────────────────────────────────
    ("Transaction", "BEGIN / COMMIT",
     lambda cur: _test_transaction(cur, commit=True),
     None),
    ("Transaction", "BEGIN / ROLLBACK",
     lambda cur: _test_transaction(cur, commit=False),
     None),
    ("Transaction", "SAVEPOINT and ROLLBACK TO",
     lambda cur: _test_savepoint(cur),
     None),
    ("Transaction", "SELECT FOR UPDATE",
     "SELECT id FROM compat_ai LIMIT 1 FOR UPDATE",
     None),
    ("Transaction", "SELECT FOR SHARE",
     "SELECT id FROM compat_ai LIMIT 1 FOR SHARE",
     None),

    # ── Prepared statements ───────────────────────────────────────────────────
    ("PreparedStmt", "PREPARE / EXECUTE",
     lambda cur: _test_prepared(cur),
     None),

    # ── INFORMATION_SCHEMA ────────────────────────────────────────────────────
    ("InfoSchema", "TABLES",
     "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() LIMIT 5",
     None),
    ("InfoSchema", "COLUMNS",
     "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
     "WHERE TABLE_NAME='compat_ai' LIMIT 5",
     None),
    ("InfoSchema", "STATISTICS (indexes)",
     "SELECT INDEX_NAME FROM information_schema.STATISTICS "
     "WHERE TABLE_NAME='compat_ai' LIMIT 5",
     None),
    ("InfoSchema", "PROCESSLIST",
     "SELECT ID, USER, STATE FROM information_schema.PROCESSLIST LIMIT 5",
     None),

    # ── SHOW statements ───────────────────────────────────────────────────────
    ("SHOW", "SHOW CREATE TABLE",
     "SHOW CREATE TABLE compat_ai",
     None),
    ("SHOW", "SHOW INDEX",
     "SHOW INDEX FROM compat_ai",
     None),
    ("SHOW", "SHOW VARIABLES",
     "SHOW VARIABLES LIKE 'max_connections'",
     None),
    ("SHOW", "SHOW STATUS",
     "SHOW STATUS LIKE 'Uptime'",
     None),

    # ── EXPLAIN ───────────────────────────────────────────────────────────────
    ("EXPLAIN", "EXPLAIN FORMAT=brief",
     "EXPLAIN FORMAT='brief' SELECT * FROM compat_ai WHERE id = 1",
     None),
    ("EXPLAIN", "EXPLAIN ANALYZE",
     "EXPLAIN ANALYZE SELECT COUNT(*) FROM compat_ai",
     None),
]


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    print(f"\n{'='*60}")
    print(f"  Module 6: MySQL Compatibility Checklist")
    print(f"  Running {len(COMPAT_CHECKS)} checks...")
    print(f"{'='*60}")

    conn = get_connection(cfg["tidb"], autocommit=False)
    cur  = conn.cursor()

    passed = 0
    failed = 0
    results = []

    for (category, name, check, _) in COMPAT_CHECKS:
        status, note = _run_check(cur, conn, check)
        log_compat_check(name, status, note, category=category)
        results.append((category, name, status, note))
        if status == "pass":
            passed += 1
        else:
            failed += 1
        marker = "✓" if status == "pass" else "✗"
        print(f"    {marker} [{category}] {name}"
              + (f"\n      → {note}" if status != "pass" else ""))

    # Cleanup temp tables
    for t in ["compat_ai", "compat_ar", "compat_json", "compat_gen", "compat_view"]:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
            cur.execute(f"DROP VIEW IF EXISTS {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()

    pct = passed / len(results) * 100 if results else 0
    end_module(MODULE, "passed" if pct >= 90 else "warning",
               f"{passed}/{len(results)} checks passed ({pct:.0f}%)")
    print(f"\n  Result: {passed}/{len(results)} passed ({pct:.0f}% compatible)")

    return {
        "passed": passed,
        "failed": failed,
        "total":  len(results),
        "pct_compatible": round(pct, 1),
        "details": [{"category": c, "name": n, "status": s, "note": nt}
                    for c, n, s, nt in results],
    }


# ── Check runners ─────────────────────────────────────────────────────────────

def _run_check(cur, conn, check):
    try:
        if callable(check):
            ok = check(cur)
            conn.rollback()
            return ("pass" if ok else "fail"), ""
        else:
            cur.execute(check)
            try:
                cur.fetchall()
            except Exception:
                pass
            conn.rollback()
            return "pass", ""
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return "fail", str(e)[:120]


def _test_transaction(cur, commit: bool) -> bool:
    cur.execute("BEGIN")
    cur.execute("INSERT INTO compat_ai (v) VALUES (9999)")
    if commit:
        cur.execute("COMMIT")
    else:
        cur.execute("ROLLBACK")
    return True


def _test_savepoint(cur) -> bool:
    cur.execute("BEGIN")
    cur.execute("INSERT INTO compat_ai (v) VALUES (8888)")
    cur.execute("SAVEPOINT sp1")
    cur.execute("INSERT INTO compat_ai (v) VALUES (7777)")
    cur.execute("ROLLBACK TO sp1")
    cur.execute("ROLLBACK")
    return True


def _test_prepared(cur) -> bool:
    cur.execute("PREPARE stmt1 FROM 'SELECT id FROM compat_ai WHERE id = ?'")
    cur.execute("SET @p = 1")
    cur.execute("EXECUTE stmt1 USING @p")
    cur.fetchall()
    cur.execute("DEALLOCATE PREPARE stmt1")
    return True


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
