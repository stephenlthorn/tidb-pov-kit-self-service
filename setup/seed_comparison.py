#!/usr/bin/env python3
"""
seed_comparison.py - Seed the comparison database with core schemas + sample
data so the parallel benchmark can drive both engines without expecting the
customer to pre-populate matching tables.

Dispatches on the comparison target family:
  - mysql family   -> reuses generate_data.py's MySQL DDL + bulk_insert
  - postgres family-> uses dialect_ddl.SCHEMA_*_PG + a psycopg COPY-friendly
                       executemany path

Usage:
    python setup/seed_comparison.py --config config.yaml [--scale small]
                                    [--users 5000] [--accounts 7500]
                                    [--transactions 50000]
"""
from __future__ import annotations

import argparse
import os
import random
import string
import sys
import time
from typing import Iterable, List, Sequence

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.comparison_targets import normalize_comparison_cfg, target_family
from lib.db_utils import create_database_if_missing, family_of, get_connection
from setup.dialect_ddl import (
    SCHEMA_A_DDL_MYSQL_EXTRA,
    SCHEMA_A_PG,
    SCHEMA_B_DDL_MYSQL,
    SCHEMA_B_PG,
    SCHEMA_C_PG,
    split_statements,
)
from setup.generate_data import (
    SCHEMA_A_DDL_MYSQL,
    SCHEMA_C_DDL_MYSQL,
    gen_accounts,
    gen_audit_log,
    gen_events,
    gen_metrics,
    gen_sessions,
    gen_tenant_data,
    gen_tenant_users,
    gen_tenants,
    gen_transaction_items,
    gen_transactions,
    gen_users,
)


SMOKE_DEFAULTS = {
    "users": 5_000,
    "accounts": 7_500,
    "transactions": 50_000,
    "transaction_items": 100_000,
    "audit_log": 10_000,
    "events": 50_000,
    "metrics": 20_000,
    "sessions": 5_000,
    "tenants": 100,
    "tenant_users": 5_000,
    "tenant_data": 10_000,
}


def _quote_ident(name: str, family: str) -> str:
    if family == "postgres":
        return f'"{name}"'
    return f"`{name}`"


def _executemany(cur, sql: str, batch: List[Sequence]):
    cur.executemany(sql, batch)


def bulk_insert(conn, family: str, table: str, cols: List[str],
                gen: Iterable, total: int, label: str, batch_size: int = 1000) -> int:
    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(cols))
    quoted_table = _quote_ident(table, family)
    quoted_cols = ", ".join(_quote_ident(c, family) for c in cols)
    if family == "postgres":
        # Random generators can hit our unique constraints (e.g. accounts has
        # UNIQUE (user_id, type) to support the upsert_account workload).
        # ON CONFLICT DO NOTHING makes the seed idempotent and keeps the
        # psycopg executemany pipeline from aborting on duplicate keys.
        sql = f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    elif family == "mysql":
        # INSERT IGNORE for MySQL idempotency. Avoids ON DUPLICATE KEY UPDATE
        # on the auto-increment column, which MySQL 8 rejects with
        # "Auto-increment value in UPDATE conflicts with internally generated
        # values" when the no-op update target is the AI column.
        sql = f"INSERT IGNORE INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"
    else:
        sql = f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"
    inserted = 0
    batch: List[Sequence] = []
    t0 = time.time()
    for row in gen:
        batch.append(row)
        if len(batch) >= batch_size:
            _executemany(cur, sql, batch)
            if not getattr(conn, "autocommit", True):
                conn.commit()
            inserted += len(batch)
            batch = []
            elapsed = time.time() - t0
            rate = inserted / max(elapsed, 0.001)
            pct = inserted / max(total, 1) * 100
            print(f"  {label}: {inserted:,}/{total:,} ({pct:.0f}%) - {rate:.0f} rows/s", end="\r")
    if batch:
        _executemany(cur, sql, batch)
        if not getattr(conn, "autocommit", True):
            conn.commit()
        inserted += len(batch)
    print(f"  {label}: {inserted:,} rows in {time.time()-t0:.1f}s" + " " * 20)
    return inserted


def _sanitize_mysql_ddl(stmt: str) -> str:
    """Strip TiDB-only clauses out of statements taken from generate_data.py's
    MySQL-labeled DDL (which sneaks in AUTO_RANDOM and similar). The kit's
    original mysql_compatible schema_mode wasn't exercised against real MySQL
    so a few TiDB-isms leaked through."""
    import re
    out = re.sub(r"\bAUTO_RANDOM\b", "AUTO_INCREMENT", stmt, flags=re.IGNORECASE)
    out = re.sub(
        r"SHARD_ROW_ID_BITS\s*=\s*\d+|PRE_SPLIT_REGIONS\s*=\s*\d+|NONCLUSTERED",
        "",
        out,
        flags=re.IGNORECASE,
    )
    return out


def _exec_ddl(conn, statements: List[str], sanitize_for_mysql: bool = False):
    cur = conn.cursor()
    for stmt in statements:
        if sanitize_for_mysql:
            stmt = _sanitize_mysql_ddl(stmt)
        cur.execute(stmt)
    if not getattr(conn, "autocommit", True):
        conn.commit()


def _try_add_unique(conn, table: str, name: str, cols: str):
    """Idempotently add a UNIQUE constraint on MySQL. Swallows the error if
    the index already exists so the seeder can be re-run safely."""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` ADD UNIQUE KEY `{name}` ({cols})")
    except Exception as exc:
        msg = str(exc).lower()
        if "duplicate key name" in msg or "already exists" in msg:
            return
        raise


def _table_already_populated(conn, family: str, table: str) -> bool:
    cur = conn.cursor()
    quoted = _quote_ident(table, family)
    try:
        cur.execute(f"SELECT COUNT(*) FROM {quoted}")
        row = cur.fetchone()
        return bool(row and row[0] and int(row[0]) > 0)
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="Seed the comparison DB with core schemas + data")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--scale", default="smoke",
                        help="smoke|small (smoke is the default fast path for parallel-run validation)")
    parser.add_argument("--skip-if-exists", action="store_true",
                        help="Skip data generation if `users` table is already populated.")
    parser.add_argument("--users", type=int, default=None)
    parser.add_argument("--accounts", type=int, default=None)
    parser.add_argument("--transactions", type=int, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f) or {}

    comparison_cfg = normalize_comparison_cfg(cfg.get("comparison_db") or {})
    if not comparison_cfg.get("enabled"):
        raise SystemExit("comparison_db.enabled must be true to seed the comparison DB.")
    if not comparison_cfg.get("host"):
        raise SystemExit("comparison_db.host is required to seed the comparison DB.")
    target = comparison_cfg.get("target") or "mysql"
    family = target_family(target)
    if family not in ("mysql", "postgres"):
        raise SystemExit(f"Unsupported comparison family for seeding: {family} ({target}).")

    # The driver layer uses cfg["target"] to dispatch; comparison_cfg already
    # carries that key.
    counts = dict(SMOKE_DEFAULTS)
    if args.users is not None:
        counts["users"] = args.users
        counts["accounts"] = max(counts["accounts"], args.users)
        counts["audit_log"] = min(counts["audit_log"], args.users * 2)
    if args.accounts is not None:
        counts["accounts"] = args.accounts
    if args.transactions is not None:
        counts["transactions"] = args.transactions
        counts["transaction_items"] = args.transactions * 2

    print("=" * 60)
    print(f"  Comparison Seeder | target={target} family={family}")
    print(f"  host={comparison_cfg['host']}:{comparison_cfg['port']} db={comparison_cfg['database']}")
    print(f"  rows: users={counts['users']:,} accounts={counts['accounts']:,} "
          f"transactions={counts['transactions']:,}")
    print("=" * 60)

    # Database creation (best-effort - postgres will require user with CREATEDB)
    try:
        create_database_if_missing(comparison_cfg)
    except Exception as exc:
        print(f"  [warn] create_database_if_missing failed (continuing): {exc}")

    conn = get_connection(comparison_cfg, autocommit=True)

    print("\n[1/2] Creating schemas...")
    if family == "postgres":
        statements = (
            split_statements(SCHEMA_A_PG)
            + split_statements(SCHEMA_B_PG)
            + split_statements(SCHEMA_C_PG)
        )
    else:
        statements = (
            split_statements(SCHEMA_A_DDL_MYSQL)
            + split_statements(SCHEMA_B_DDL_MYSQL)
            + split_statements(SCHEMA_C_DDL_MYSQL)
        )
    _exec_ddl(conn, statements, sanitize_for_mysql=(family == "mysql"))
    # MySQL needs the accounts unique constraint added separately because
    # ALTER TABLE ... ADD UNIQUE is not idempotent without IF NOT EXISTS.
    if family == "mysql":
        _try_add_unique(conn, "accounts", "uq_accounts_user_type", "user_id, type")
    print(f"  Created/verified {len(statements)} statements.")

    if args.skip_if_exists and _table_already_populated(conn, family, "users"):
        print("  `users` already populated; skipping data generation.")
        conn.close()
        return

    print("\n[2/2] Inserting data...")
    t_start = time.time()

    bulk_insert(conn, family, "users",
                ["external_id", "email", "name", "status"],
                gen_users(counts["users"]), counts["users"], "users")

    bulk_insert(conn, family, "accounts",
                ["user_id", "type", "balance", "currency"],
                gen_accounts(counts["accounts"], counts["users"]),
                counts["accounts"], "accounts")

    bulk_insert(conn, family, "transactions",
                ["account_id", "type", "amount", "status", "reference_id"],
                gen_transactions(counts["transactions"], counts["accounts"]),
                counts["transactions"], "transactions")

    bulk_insert(conn, family, "transaction_items",
                ["transaction_id", "description", "amount", "quantity"],
                gen_transaction_items(counts["transaction_items"], counts["transactions"]),
                counts["transaction_items"], "transaction_items")

    bulk_insert(conn, family, "audit_log",
                ["entity_type", "entity_id", "action", "actor_id", "payload"],
                gen_audit_log(counts["audit_log"], counts["users"]),
                counts["audit_log"], "audit_log")

    bulk_insert(conn, family, "sessions",
                ["user_id", "duration_sec", "page_views"],
                gen_sessions(counts["sessions"], counts["users"]),
                counts["sessions"], "sessions")

    bulk_insert(conn, family, "events",
                ["source", "event_type", "user_id", "session_id", "properties"],
                gen_events(counts["events"], counts["users"], counts["sessions"]),
                counts["events"], "events")

    bulk_insert(conn, family, "metrics",
                ["host", "metric_name", "value", "tags"],
                gen_metrics(counts["metrics"]), counts["metrics"], "metrics")

    bulk_insert(conn, family, "tenants",
                ["name", "plan", "status"],
                gen_tenants(counts["tenants"]), counts["tenants"], "tenants")

    bulk_insert(conn, family, "tenant_users",
                ["tenant_id", "email", "role"],
                gen_tenant_users(counts["tenant_users"], counts["tenants"]),
                counts["tenant_users"], "tenant_users")

    bulk_insert(conn, family, "tenant_data",
                ["tenant_id", "data_type", "payload"],
                gen_tenant_data(counts["tenant_data"], counts["tenants"]),
                counts["tenant_data"], "tenant_data")

    conn.close()
    print(f"\n  Seeding complete in {time.time() - t_start:.1f}s.")


if __name__ == "__main__":
    main()
