#!/usr/bin/env python3
"""
Module 6 — SQL Compatibility + Unsupported Feature Inventory

Part A: runs TiDB-side MySQL syntax/semantic compatibility checks.
Part B: when `comparison_db` is enabled, inspects the source engine metadata
        (MySQL/PostgreSQL/SQL Server) for objects/features that do not map
        directly to TiDB and logs required remediation items.

Each check is logged to the compat_checks table in results.db.
"""

from __future__ import annotations
import json
import os
import sys
from typing import Any, Dict, List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.comparison_targets import normalize_comparison_cfg, target_definition, target_label
from lib.db_utils import get_connection
from lib.result_store import end_module, init_db, log_compat_check, start_module

MODULE = "06_mysql_compat"
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "results")
SOURCE_SUMMARY_PATH = os.path.join(RESULTS_DIR, "compat_source_unsupported_summary.json")

MYSQL_SYSTEM_SCHEMAS = "'mysql','sys','information_schema','performance_schema'"
PG_SYSTEM_SCHEMAS = "'pg_catalog','information_schema'"

# Each entry: (category, name, sql_or_callable, expected_pattern)
# sql_or_callable: SQL string (run with execute) or callable(cur)->bool.
COMPAT_CHECKS = [
    # DDL
    (
        "DDL",
        "CREATE TABLE with AUTO_INCREMENT",
        "CREATE TABLE IF NOT EXISTS compat_ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)",
        None,
    ),
    (
        "DDL",
        "CREATE TABLE with AUTO_RANDOM",
        "CREATE TABLE IF NOT EXISTS compat_ar (id BIGINT AUTO_RANDOM PRIMARY KEY, v INT)",
        None,
    ),
    ("DDL", "CREATE TABLE with JSON column", "CREATE TABLE IF NOT EXISTS compat_json (id INT PRIMARY KEY, data JSON)", None),
    (
        "DDL",
        "CREATE TABLE with generated column",
        """CREATE TABLE IF NOT EXISTS compat_gen (
         id INT PRIMARY KEY, price DECIMAL(10,2), qty INT,
         total DECIMAL(10,2) AS (price * qty) STORED
     )""",
        None,
    ),
    ("DDL", "CREATE INDEX with expression", "CREATE INDEX IF NOT EXISTS idx_expr ON compat_gen ((price * qty))", None),
    ("DDL", "ALTER TABLE ADD COLUMN", "ALTER TABLE compat_ai ADD COLUMN IF NOT EXISTS extra VARCHAR(50)", None),
    ("DDL", "CREATE VIEW", "CREATE OR REPLACE VIEW compat_view AS SELECT id, v FROM compat_ai WHERE v > 0", None),

    # DML
    (
        "DML",
        "INSERT ... ON DUPLICATE KEY UPDATE",
        "INSERT INTO compat_ai (id, v) VALUES (1, 10) ON DUPLICATE KEY UPDATE v = v + 1",
        None,
    ),
    ("DML", "REPLACE INTO", "REPLACE INTO compat_ai (id, v) VALUES (2, 20)", None),
    ("DML", "INSERT ... SELECT", "INSERT INTO compat_ai (v) SELECT v + 100 FROM compat_ai LIMIT 5", None),
    (
        "DML",
        "UPDATE with JOIN",
        "UPDATE compat_ai a JOIN compat_ai b ON a.id = b.id SET a.v = a.v + 1 WHERE b.v > 0",
        None,
    ),
    (
        "DML",
        "DELETE with subquery",
        "DELETE FROM compat_ai WHERE id IN (SELECT id FROM (SELECT id FROM compat_ai LIMIT 0) t)",
        None,
    ),
    ("DML", "MULTI-TABLE DELETE", "DELETE a FROM compat_ai a WHERE a.v < 0", None),

    # Query features
    ("Query", "Window function ROW_NUMBER", "SELECT id, v, ROW_NUMBER() OVER (ORDER BY v DESC) rn FROM compat_ai LIMIT 5", None),
    ("Query", "Window function RANK", "SELECT id, v, RANK() OVER (PARTITION BY 1 ORDER BY v) rnk FROM compat_ai LIMIT 5", None),
    ("Query", "CTE (WITH clause)", "WITH cte AS (SELECT id, v FROM compat_ai) SELECT * FROM cte LIMIT 5", None),
    (
        "Query",
        "Recursive CTE",
        """WITH RECURSIVE seq(n) AS (
         SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 10
     ) SELECT n FROM seq""",
        None,
    ),
    (
        "Query",
        "LATERAL join",
        """SELECT a.id, t.val FROM compat_ai a
        JOIN LATERAL (SELECT a.v * 2 AS val) t ON TRUE LIMIT 5""",
        None,
    ),
    ("Query", "GROUP BY with ROLLUP", "SELECT v, COUNT(*) n FROM compat_ai GROUP BY v WITH ROLLUP LIMIT 10", None),
    ("Query", "HAVING clause", "SELECT v, COUNT(*) cnt FROM compat_ai GROUP BY v HAVING cnt > 0", None),

    # Functions
    ("Function", "NOW() and DATE_FORMAT", "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d')", None),
    ("Function", "TIMESTAMPDIFF", "SELECT TIMESTAMPDIFF(SECOND, '2000-01-01', NOW())", None),
    ("Function", "CONCAT_WS", "SELECT CONCAT_WS(',', 'a', 'b', 'c')", None),
    ("Function", "GROUP_CONCAT", "SELECT GROUP_CONCAT(v ORDER BY v SEPARATOR '|') FROM compat_ai", None),
    ("Function", "IF / IFNULL / COALESCE", "SELECT IF(1>0,'yes','no'), IFNULL(NULL,'x'), COALESCE(NULL,NULL,42)", None),
    ("Function", "CAST and CONVERT", "SELECT CAST('123' AS UNSIGNED), CONVERT('2024-01-01', DATE)", None),
    ("Function", "SUBSTRING_INDEX", "SELECT SUBSTRING_INDEX('a.b.c', '.', 2)", None),
    ("Function", "REGEXP", "SELECT 'hello123' REGEXP '^[a-z]+[0-9]+$'", None),

    # JSON
    ("JSON", "JSON_OBJECT and JSON_ARRAY", "SELECT JSON_OBJECT('k', 1, 'arr', JSON_ARRAY(1,2,3))", None),
    ("JSON", "JSON_EXTRACT (->)", "SELECT JSON_EXTRACT('{\"a\":{\"b\":1}}', '$.a.b')", None),
    ("JSON", "JSON_SET", "SELECT JSON_SET('{\"a\":1}', '$.b', 2)", None),
    ("JSON", "JSON_CONTAINS", "SELECT JSON_CONTAINS('[1,2,3]', '2')", None),
    ("JSON", "JSON_ARRAYAGG", "SELECT JSON_ARRAYAGG(v) FROM compat_ai LIMIT 1", None),
    ("JSON", "JSON path filter (->)", "SELECT data->'$.key' FROM compat_json LIMIT 1", None),

    # Transactions
    ("Transaction", "BEGIN / COMMIT", lambda cur: _test_transaction(cur, commit=True), None),
    ("Transaction", "BEGIN / ROLLBACK", lambda cur: _test_transaction(cur, commit=False), None),
    ("Transaction", "SAVEPOINT and ROLLBACK TO", lambda cur: _test_savepoint(cur), None),
    ("Transaction", "SELECT FOR UPDATE", "SELECT id FROM compat_ai LIMIT 1 FOR UPDATE", None),
    ("Transaction", "SELECT FOR SHARE", "SELECT id FROM compat_ai LIMIT 1 FOR SHARE", None),

    # Prepared statements
    ("PreparedStmt", "PREPARE / EXECUTE", lambda cur: _test_prepared(cur), None),

    # INFORMATION_SCHEMA
    (
        "InfoSchema",
        "TABLES",
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() LIMIT 5",
        None,
    ),
    (
        "InfoSchema",
        "COLUMNS",
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME='compat_ai' LIMIT 5",
        None,
    ),
    (
        "InfoSchema",
        "STATISTICS (indexes)",
        "SELECT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_NAME='compat_ai' LIMIT 5",
        None,
    ),
    ("InfoSchema", "PROCESSLIST", "SELECT ID, USER, STATE FROM information_schema.PROCESSLIST LIMIT 5", None),

    # SHOW statements
    ("SHOW", "SHOW CREATE TABLE", "SHOW CREATE TABLE compat_ai", None),
    ("SHOW", "SHOW INDEX", "SHOW INDEX FROM compat_ai", None),
    ("SHOW", "SHOW VARIABLES", "SHOW VARIABLES LIKE 'max_connections'", None),
    ("SHOW", "SHOW STATUS", "SHOW STATUS LIKE 'Uptime'", None),

    # EXPLAIN
    ("EXPLAIN", "EXPLAIN FORMAT=brief", "EXPLAIN FORMAT='brief' SELECT * FROM compat_ai WHERE id = 1", None),
    ("EXPLAIN", "EXPLAIN ANALYZE", "EXPLAIN ANALYZE SELECT COUNT(*) FROM compat_ai", None),
]

SOURCE_UNSUPPORTED_CHECKS: Dict[str, List[Dict[str, str]]] = {
    "mysql": [
        {
            "feature": "Stored Procedures",
            "sql": f"SELECT COUNT(*) FROM information_schema.routines WHERE routine_type='PROCEDURE' AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Move procedure logic into application services.",
        },
        {
            "feature": "Stored Functions",
            "sql": f"SELECT COUNT(*) FROM information_schema.routines WHERE routine_type='FUNCTION' AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Implement function logic in client code or SQL expressions TiDB supports.",
        },
        {
            "feature": "Triggers",
            "sql": f"SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Apply trigger behavior in app transactions or CDC pipelines.",
        },
        {
            "feature": "Events",
            "sql": f"SELECT COUNT(*) FROM information_schema.events WHERE event_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Move scheduled tasks to external schedulers (cron, Airflow, EventBridge).",
        },
        {
            "feature": "UDF Plugins",
            "sql": "SELECT COUNT(*) FROM mysql.func",
            "resolution": "Replace UDF plugin logic with application code or built-in functions.",
        },
        {
            "feature": "FULLTEXT Indexes",
            "sql": f"SELECT COUNT(*) FROM information_schema.statistics WHERE index_type='FULLTEXT' AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Use dedicated search service or app-side indexing for full-text search.",
        },
        {
            "feature": "SPATIAL Columns",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.columns WHERE data_type IN "
                "('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection') "
                f"AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Handle GIS decoding in the application layer.",
        },
        {
            "feature": "SPATIAL Indexes",
            "sql": f"SELECT COUNT(*) FROM information_schema.statistics WHERE index_type='SPATIAL' AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Use app-side spatial libraries or separate geospatial engine.",
        },
        {
            "feature": "Unsupported Charsets",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.columns WHERE character_set_name IS NOT NULL "
                "AND character_set_name <> '' "
                "AND character_set_name NOT IN ('ascii','latin1','binary','utf8','utf8mb4','gbk') "
                f"AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Convert columns to TiDB-supported character sets before migration.",
        },
        {
            "feature": "XML Function Calls in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "(UPPER(COALESCE(routine_definition,'')) LIKE '%EXTRACTVALUE%' "
                "OR UPPER(COALESCE(routine_definition,'')) LIKE '%UPDATEXML%' "
                "OR UPPER(COALESCE(routine_definition,'')) LIKE '%XML%') "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Parse XML in application code or ETL, not in-database functions.",
        },
        {
            "feature": "X-Protocol Plugin",
            "sql": "SELECT COUNT(*) FROM information_schema.plugins WHERE plugin_name='mysqlx'",
            "resolution": "Use standard MySQL protocol connectors.",
        },
        {
            "feature": "Column-level Privileges",
            "sql": f"SELECT COUNT(*) FROM information_schema.column_privileges WHERE table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Refactor to table-level privileges and app authorization.",
        },
        {
            "feature": "CTAS in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "UPPER(COALESCE(routine_definition,'')) LIKE '%CREATE TABLE%AS SELECT%' "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Replace CTAS routines with migration scripts or INSERT...SELECT flow.",
        },
        {
            "feature": "HANDLER Statements in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "UPPER(COALESCE(routine_definition,'')) LIKE '%HANDLER%' "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Replace HANDLER usage with regular SQL access paths.",
        },
        {
            "feature": "Tablespaces",
            "sql": "SELECT COUNT(*) FROM information_schema.tablespaces",
            "resolution": "Let TiDB manage storage placement automatically.",
        },
        {
            "feature": "Descending Indexes",
            "sql": f"SELECT COUNT(*) FROM information_schema.statistics WHERE collation='D' AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Re-evaluate index patterns and sort behavior in TiDB query plans.",
        },
        {
            "feature": "SKIP LOCKED Usage in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "UPPER(COALESCE(routine_definition,'')) LIKE '%SKIP LOCKED%' "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Use retry-safe queue consumers and idempotent worker logic.",
        },
        {
            "feature": "LATERAL Derived Tables in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "UPPER(COALESCE(routine_definition,'')) LIKE '%LATERAL%' "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Rewrite as explicit joins/CTEs or split into multiple app queries.",
        },
        {
            "feature": "JOIN ON Subquery in Routines",
            "sql": (
                "SELECT COUNT(*) FROM information_schema.routines WHERE "
                "UPPER(COALESCE(routine_definition,'')) LIKE '%JOIN%SELECT%' "
                f"AND routine_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Refactor nested subquery joins for TiDB optimizer-friendly plans.",
        },
        {
            "feature": "Partitioned Tables",
            "sql": f"SELECT COUNT(*) FROM information_schema.partitions WHERE partition_method IS NOT NULL AND table_schema NOT IN ({MYSQL_SYSTEM_SCHEMAS})",
            "resolution": "Validate partition strategy compatibility and merge unsupported patterns.",
        },
    ],
    "postgres": [
        {
            "feature": "User Functions and Procedures",
            "sql": f"SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname NOT IN ({PG_SYSTEM_SCHEMAS})",
            "resolution": "Move procedural logic to application services/jobs.",
        },
        {
            "feature": "Triggers",
            "sql": (
                "SELECT COUNT(*) FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE NOT t.tgisinternal "
                f"AND n.nspname NOT IN ({PG_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Rebuild trigger behavior in app transaction boundaries.",
        },
        {
            "feature": "Materialized Views",
            "sql": f"SELECT COUNT(*) FROM pg_matviews WHERE schemaname NOT IN ({PG_SYSTEM_SCHEMAS})",
            "resolution": "Use TiFlash replicas + scheduled refresh pipelines.",
        },
        {
            "feature": "Installed Extensions",
            "sql": "SELECT COUNT(*) FROM pg_extension WHERE extname NOT IN ('plpgsql')",
            "resolution": "Replace extension-specific behavior with app-side logic or ETL.",
        },
        {
            "feature": "Non-BTree Indexes (GIN/GiST/BRIN/HASH)",
            "sql": (
                "SELECT COUNT(*) FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema') "
                "AND indexdef ~* 'USING\\s+(gin|gist|brin|hash)'"
            ),
            "resolution": "Redesign index strategy with TiDB-supported secondary indexes.",
        },
        {
            "feature": "Array Columns",
            "sql": f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema NOT IN ({PG_SYSTEM_SCHEMAS}) AND data_type='ARRAY'",
            "resolution": "Normalize arrays into child tables or JSON structures.",
        },
        {
            "feature": "JSONB Columns",
            "sql": f"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema NOT IN ({PG_SYSTEM_SCHEMAS}) AND udt_name='jsonb'",
            "resolution": "Map JSONB workloads to TiDB JSON and validate operator usage.",
        },
        {
            "feature": "Table Inheritance",
            "sql": "SELECT COUNT(*) FROM pg_inherits",
            "resolution": "Flatten inheritance hierarchy into explicit tables/views.",
        },
        {
            "feature": "Row-Level Security Tables",
            "sql": (
                "SELECT COUNT(*) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.relrowsecurity "
                f"AND n.nspname NOT IN ({PG_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Implement row filtering in application authorization layer.",
        },
        {
            "feature": "Foreign Tables / FDW",
            "sql": "SELECT COUNT(*) FROM pg_foreign_table",
            "resolution": "Replace FDW with ETL/ELT data movement into TiDB.",
        },
        {
            "feature": "Domain Types",
            "sql": (
                "SELECT COUNT(*) FROM pg_type t "
                "JOIN pg_namespace n ON n.oid=t.typnamespace "
                "WHERE t.typtype='d' "
                f"AND n.nspname NOT IN ({PG_SYSTEM_SCHEMAS})"
            ),
            "resolution": "Inline domain constraints at table/column level.",
        },
        {
            "feature": "Exclusion Constraints",
            "sql": "SELECT COUNT(*) FROM pg_constraint WHERE contype='x'",
            "resolution": "Enforce exclusion constraints in app logic or pre-write validation tables.",
        },
        {
            "feature": "Rules",
            "sql": f"SELECT COUNT(*) FROM pg_rules WHERE schemaname NOT IN ({PG_SYSTEM_SCHEMAS})",
            "resolution": "Replace rules with views/app routing logic.",
        },
        {
            "feature": "LISTEN/NOTIFY Usage in Functions",
            "sql": (
                "SELECT COUNT(*) FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname NOT IN ('pg_catalog','information_schema') "
                "AND POSITION('pg_notify' IN LOWER(pg_get_functiondef(p.oid))) > 0"
            ),
            "resolution": "Move pub/sub signaling to external messaging systems.",
        },
    ],
    "mssql": [
        {
            "feature": "Stored Procedures",
            "sql": "SELECT COUNT(*) FROM sys.procedures WHERE is_ms_shipped=0",
            "resolution": "Move procedure logic into application services.",
        },
        {
            "feature": "User Functions",
            "sql": "SELECT COUNT(*) FROM sys.objects WHERE type IN ('FN','IF','TF','FS','FT') AND is_ms_shipped=0",
            "resolution": "Port function logic to application code or compatible SQL.",
        },
        {
            "feature": "Triggers",
            "sql": "SELECT COUNT(*) FROM sys.triggers WHERE is_ms_shipped=0",
            "resolution": "Re-implement trigger side effects in application transactions.",
        },
        {
            "feature": "SQL Agent Jobs",
            "sql": "SELECT COUNT(*) FROM msdb.dbo.sysjobs",
            "resolution": "Use external orchestrators for scheduled workloads.",
        },
        {
            "feature": "CLR Assemblies",
            "sql": "SELECT COUNT(*) FROM sys.assemblies WHERE is_user_defined=1",
            "resolution": "Replace CLR code with application services.",
        },
        {
            "feature": "FULLTEXT Indexes",
            "sql": "SELECT COUNT(*) FROM sys.fulltext_indexes",
            "resolution": "Use external full-text search platform.",
        },
        {
            "feature": "Columnstore Indexes",
            "sql": "SELECT COUNT(*) FROM sys.indexes WHERE type_desc LIKE '%COLUMNSTORE%'",
            "resolution": "Use TiFlash replicas for columnar analytics acceleration.",
        },
        {
            "feature": "XML Columns",
            "sql": "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE t.name='xml'",
            "resolution": "Parse XML in application/ETL layers.",
        },
        {
            "feature": "Spatial Columns",
            "sql": "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE t.name IN ('geometry','geography')",
            "resolution": "Use application-side GIS processing or dedicated spatial services.",
        },
        {
            "feature": "Synonyms",
            "sql": "SELECT COUNT(*) FROM sys.synonyms",
            "resolution": "Replace synonyms with explicit schema-qualified references.",
        },
        {
            "feature": "Temporal Tables",
            "sql": "SELECT COUNT(*) FROM sys.tables WHERE temporal_type=2",
            "resolution": "Implement temporal history using app-managed audit tables.",
        },
        {
            "feature": "Partition Schemes",
            "sql": "SELECT COUNT(*) FROM sys.partition_schemes",
            "resolution": "Map partition strategy to TiDB partitioning capabilities.",
        },
        {
            "feature": "Service Broker Queues",
            "sql": "SELECT COUNT(*) FROM sys.service_queues",
            "resolution": "Move queue workflows to Kafka/SQS/PubSub systems.",
        },
        {
            "feature": "FILESTREAM Columns",
            "sql": "SELECT COUNT(*) FROM sys.columns WHERE is_filestream=1",
            "resolution": "Store large binary objects in object storage and keep pointers in TiDB.",
        },
        {
            "feature": "XML Indexes",
            "sql": "SELECT COUNT(*) FROM sys.xml_indexes",
            "resolution": "Remove XML index dependency; move to app-level XML processing.",
        },
    ],
}


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    print(f"\n{'=' * 60}")
    print("  Module 6: SQL Compatibility + Unsupported Feature Inventory")
    print(f"  Running {len(COMPAT_CHECKS)} TiDB compatibility checks...")
    print(f"{'=' * 60}")

    conn = get_connection(cfg["tidb"], autocommit=False)
    cur = conn.cursor()

    passed = 0
    failed = 0
    results: List[Tuple[str, str, str, str]] = []

    for category, name, check, _ in COMPAT_CHECKS:
        status, note = _run_check(cur, conn, check)
        log_compat_check(name, status, note, category=category)
        results.append((category, name, status, note))
        if status == "pass":
            passed += 1
        else:
            failed += 1
        marker = "✓" if status == "pass" else "✗"
        print(f"    {marker} [{category}] {name}" + (f"\n      → {note}" if status != "pass" else ""))

    _cleanup_temp_objects(cur, conn)

    source_rows, source_summary = _run_source_unsupported_inventory(cfg)
    for row in source_rows:
        category = str(row.get("category") or "SOURCE_UNKNOWN")
        name = str(row.get("name") or "Unnamed source check")
        status = str(row.get("status") or "fail").lower()
        note = str(row.get("note") or "")
        results.append((category, name, status, note))
        if status == "pass":
            passed += 1
        else:
            failed += 1

    _write_source_summary(source_summary)

    total = len(results)
    pct = passed / total * 100 if total else 0

    notes = f"{passed}/{total} checks passed ({pct:.0f}%)"
    if source_summary.get("status") == "executed":
        notes += (
            f"; source inventory {source_summary.get('target_label')} "
            f"findings={source_summary.get('failing_features', 0)}"
        )
    elif source_summary.get("status") == "failed":
        notes += "; source inventory failed"

    end_module(MODULE, "passed" if pct >= 90 else "warning", notes)

    print(f"\n  Result: {passed}/{total} passed ({pct:.0f}% compatible)")
    if source_summary.get("status") == "executed":
        print(
            f"  Source inventory: {source_summary.get('target_label')} "
            f"({source_summary.get('family')}) -> "
            f"{source_summary.get('failing_features', 0)} features to review"
        )
    elif source_summary.get("status") == "failed":
        print(f"  Source inventory failed: {source_summary.get('reason')}")

    return {
        "passed": passed,
        "failed": failed,
        "total": total,
        "pct_compatible": round(pct, 1),
        "details": [
            {"category": c, "name": n, "status": s, "note": nt} for c, n, s, nt in results
        ],
        "source_inventory": source_summary,
    }


def _cleanup_temp_objects(cur, conn):
    for obj in ["compat_ai", "compat_ar", "compat_json", "compat_gen", "compat_view"]:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {obj}")
        except Exception:
            pass
        try:
            cur.execute(f"DROP VIEW IF EXISTS {obj}")
        except Exception:
            pass
    try:
        conn.commit()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


def _run_source_unsupported_inventory(cfg: dict) -> tuple[list[dict], dict]:
    comparison_cfg = normalize_comparison_cfg(cfg.get("comparison_db") or {})
    if not comparison_cfg.get("enabled"):
        return [], {"status": "skipped", "reason": "comparison_db.enabled=false"}
    if not comparison_cfg.get("host"):
        return [], {"status": "skipped", "reason": "comparison_db.host missing"}

    target = str(comparison_cfg.get("target") or "").strip().lower()
    meta = target_definition(target)
    family = str(meta.get("family") or "mysql")
    checks = SOURCE_UNSUPPORTED_CHECKS.get(family)
    source_label = target_label(target)

    if not checks:
        return [], {
            "status": "skipped",
            "reason": f"No unsupported-feature scanner for family '{family}'",
            "target": target,
            "target_label": source_label,
            "family": family,
        }

    print(f"\n  Source inventory: scanning {source_label} ({family}) for TiDB-unsupported features...")

    try:
        source_conn = _open_source_connection(comparison_cfg, family)
    except Exception as exc:
        note = str(exc).strip()[:180]
        row = {
            "category": f"SOURCE_{family.upper()}",
            "name": f"{source_label}: connectivity",
            "status": "fail",
            "note": note,
            "feature": "connectivity",
            "count": None,
        }
        log_compat_check(row["name"], row["status"], row["note"], category=row["category"])
        return [row], {
            "status": "failed",
            "reason": note,
            "target": target,
            "target_label": source_label,
            "family": family,
            "checks_total": 1,
            "failing_features": 1,
        }

    rows: list[dict] = []
    cur = source_conn.cursor()
    for check in checks:
        feature = str(check["feature"])
        sql = str(check["sql"])
        resolution = str(check["resolution"])
        status, count, note = _run_source_count_check(cur, sql, resolution)
        name = f"{source_label}: {feature}"
        category = f"SOURCE_{family.upper()}"
        log_compat_check(name, status, note, category=category)
        rows.append(
            {
                "category": category,
                "name": name,
                "status": status,
                "note": note,
                "feature": feature,
                "count": count,
                "resolution": resolution,
            }
        )
        marker = "✓" if status == "pass" else "✗"
        print(f"    {marker} [{category}] {feature}" + (f"\n      → {note}" if status != "pass" else ""))

    try:
        source_conn.close()
    except Exception:
        pass

    failing = sum(1 for row in rows if str(row.get("status")).lower() != "pass")
    return rows, {
        "status": "executed",
        "target": target,
        "target_label": source_label,
        "family": family,
        "checks_total": len(rows),
        "failing_features": failing,
        "rows": rows,
    }


def _run_source_count_check(cur, sql: str, resolution: str) -> tuple[str, int | None, str]:
    try:
        cur.execute(sql)
        row = cur.fetchone()
        value = row[0] if row else 0
        count = _safe_int(value)
        if count is None:
            return "fail", None, f"non-numeric count result; resolution: {resolution}"
        if count > 0:
            return "fail", count, f"count={count}; resolution: {resolution}"
        return "pass", count, "count=0"
    except Exception as exc:
        msg = str(exc).strip().replace("\n", " ")[:150]
        return "fail", None, f"query failed: {msg}; resolution: {resolution}"


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _open_source_connection(comp_cfg: dict, family: str):
    if family == "mysql":
        import mysql.connector

        ssl_args = {"ssl_disabled": not bool(comp_cfg.get("ssl", False))}
        kwargs = {
            "host": comp_cfg["host"],
            "port": int(comp_cfg.get("port", 3306) or 3306),
            "user": comp_cfg["user"],
            "password": comp_cfg["password"],
            "connection_timeout": int(comp_cfg.get("connect_timeout_sec", 30) or 30),
            **ssl_args,
        }
        db = str(comp_cfg.get("database") or "").strip()
        if db:
            kwargs["database"] = db
        conn = mysql.connector.connect(**kwargs)
        conn.autocommit = True
        return conn

    if family == "postgres":
        import psycopg

        ssl_mode = "disable" if not bool(comp_cfg.get("ssl", False)) else str(comp_cfg.get("ssl_mode") or "require")
        kwargs = {
            "host": comp_cfg["host"],
            "port": int(comp_cfg.get("port", 5432) or 5432),
            "user": comp_cfg["user"],
            "password": comp_cfg["password"],
            "dbname": comp_cfg.get("database") or "postgres",
            "connect_timeout": int(comp_cfg.get("connect_timeout_sec", 30) or 30),
            "sslmode": ssl_mode,
        }
        conn = psycopg.connect(**kwargs)
        conn.autocommit = True
        return conn

    if family == "mssql":
        try:
            import pyodbc  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "SQL Server inventory requires pyodbc (pip install pyodbc) and an ODBC driver."
            ) from exc

        driver = str(comp_cfg.get("sqlserver_driver") or "ODBC Driver 18 for SQL Server")
        host = str(comp_cfg.get("host") or "")
        port = int(comp_cfg.get("port", 1433) or 1433)
        database = str(comp_cfg.get("database") or "master")
        timeout = int(comp_cfg.get("connect_timeout_sec", 30) or 30)
        encrypt = "yes" if bool(comp_cfg.get("sqlserver_encrypt", True)) else "no"
        trust = "yes" if bool(comp_cfg.get("sqlserver_trust_server_certificate", False)) else "no"
        mars = "yes" if bool(comp_cfg.get("sqlserver_mars", False)) else "no"
        packet_size = int(comp_cfg.get("sqlserver_packet_size", 4096) or 4096)

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={comp_cfg['user']};"
            f"PWD={comp_cfg['password']};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust};"
            f"MARS_Connection={mars};"
            f"Packet Size={packet_size};"
            f"Connection Timeout={timeout};"
        )
        return pyodbc.connect(conn_str, autocommit=True)

    raise RuntimeError(f"Unsupported source family: {family}")


def _write_source_summary(payload: dict):
    try:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        with open(SOURCE_SUMMARY_PATH, "w") as f:
            json.dump(payload or {}, f, indent=2)
    except Exception:
        pass


# Check runners for TiDB-side SQL compatibility

def _run_check(cur, conn, check):
    try:
        if callable(check):
            ok = check(cur)
            conn.rollback()
            return ("pass" if ok else "fail"), ""
        cur.execute(check)
        try:
            cur.fetchall()
        except Exception:
            pass
        conn.rollback()
        return "pass", ""
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return "fail", str(exc)[:120]


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
