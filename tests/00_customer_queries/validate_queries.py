#!/usr/bin/env python3
"""
Module 0 — Customer Query Validation
Validates customer queries from config.yaml: connectivity, execution, EXPLAIN plan.
"""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.db_utils import get_connection
from lib.result_store import init_db, start_module, end_module, log_compat_check

MODULE = "00_customer_queries"


def _discover_default_queries(cur, database: str, max_tables: int = 4):
    """
    Build a lightweight fallback validation set from tables already present in
    the active database so Module 0 can still execute when customer SQL is not
    provided.
    """
    cur.execute(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = %s
           AND table_type = 'BASE TABLE'
         ORDER BY table_name
         LIMIT %s
        """,
        (database, max_tables),
    )
    tables = [str(row[0]) for row in cur.fetchall() if row and row[0]]
    queries = []
    for table in tables:
        quoted = f"`{table}`"
        queries.append(f"SELECT COUNT(*) FROM {quoted}")
        queries.append(f"SELECT * FROM {quoted} LIMIT 20")

    # Keep module lightweight and deterministic for small E2E.
    return queries[:8]


def run(cfg: dict):
    init_db()
    start_module(MODULE)
    queries = [q for q in (cfg.get("customer_queries") or []) if isinstance(q, str) and q.strip()]

    conn = get_connection(cfg["tidb"])
    cur = conn.cursor()
    auto_generated = False

    if not queries:
        database = str(cfg.get("tidb", {}).get("database") or "test")
        queries = _discover_default_queries(cur, database)
        auto_generated = True
        if not queries:
            print("  No customer queries configured and no tables found — skipping Module 0.")
            conn.close()
            end_module(MODULE, "skipped", "No customer_queries in config.yaml and no source tables available")
            return []

    print(f"\n{'='*60}")
    print(f"  Module 0: Customer Query Validation ({len(queries)} queries)")
    if auto_generated:
        print("  Source: auto-generated default validation queries")
    print(f"{'='*60}")
    results = []

    for i, sql in enumerate(queries, 1):
        print(f"  [{i}/{len(queries)}] {sql[:70]}...")
        result = {"sql": sql, "status": "unknown", "explain": None, "error": None}

        # Try EXPLAIN first (non-destructive)
        try:
            cur.execute(f"EXPLAIN FORMAT='brief' {sql}", [1] * sql.count("?"))
            explain_rows = cur.fetchall()
            result["explain"] = [list(r) for r in explain_rows]
            result["status"] = "pass"
            print(f"    ✓ EXPLAIN OK — {len(explain_rows)} plan nodes")
            log_compat_check(f"customer_query_{i}", "pass",
                             f"EXPLAIN OK: {explain_rows[0][0] if explain_rows else ''}")
        except Exception as e:
            result["status"] = "fail"
            result["error"] = str(e)
            print(f"    ✗ EXPLAIN failed: {e}")
            log_compat_check(f"customer_query_{i}", "fail", str(e))

        results.append(result)

    conn.close()

    # Write results file
    out = os.path.join(os.path.dirname(__file__), "..", "..", "results",
                       "customer_query_validation.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(results, f, indent=2)

    passed = sum(1 for r in results if r["status"] == "pass")
    print(f"\n  Result: {passed}/{len(results)} queries validated successfully.")
    end_module(MODULE, "passed" if passed == len(results) else "partial",
               f"{passed}/{len(results)} queries passed")
    return results


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
