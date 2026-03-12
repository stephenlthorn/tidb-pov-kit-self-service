#!/usr/bin/env python3
"""
generate_data.py — Synthetic data generator for the TiDB Cloud PoV Kit.

Creates three schemas:
  Schema A: OLTP/payments (users, accounts, transactions, transaction_items, audit_log)
  Schema B: Time-series/events (events, metrics, sessions)
  Schema C: Multi-tenant SaaS (tenants, tenant_users, tenant_data)

Usage:
    python setup/generate_data.py --config config.yaml [--skip-if-exists]
"""

from __future__ import annotations
import argparse
import os
import sys
import time
import json
import random
import string

import yaml
from faker import Faker

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.db_utils import get_connection, create_database_if_missing
from lib.industry_profiles import INDUSTRY_DEFAULT, resolve_industry_from_cfg
from setup.industry_data import industry_primary_table, industry_schema_sql, industry_seed_specs

fake = Faker()
random.seed(42)

# ─── Row counts per scale ─────────────────────────────────────────────────────
SCALE_CONFIG = {
    "small": {
        "users": 10_000,
        "accounts": 15_000,
        "transactions": 250_000,
        "transaction_items": 500_000,
        "audit_log": 100_000,
        "events": 250_000,
        "metrics": 100_000,
        "sessions": 50_000,
        "tenants": 200,
        "tenant_users": 10_000,
        "tenant_data": 100_000,
    },
    "medium": {
        "users": 500_000,
        "accounts": 750_000,
        "transactions": 50_000_000,
        "transaction_items": 100_000_000,
        "audit_log": 20_000_000,
        "events": 50_000_000,
        "metrics": 20_000_000,
        "sessions": 5_000_000,
        "tenants": 10_000,
        "tenant_users": 500_000,
        "tenant_data": 10_000_000,
    },
    "large": {
        "users": 2_000_000,
        "accounts": 3_000_000,
        "transactions": 200_000_000,
        "transaction_items": 400_000_000,
        "audit_log": 80_000_000,
        "events": 200_000_000,
        "metrics": 80_000_000,
        "sessions": 20_000_000,
        "tenants": 50_000,
        "tenant_users": 2_000_000,
        "tenant_data": 40_000_000,
    },
}

DURATION_MULTIPLIER = {
    "small": 1.0,
    "medium": 1.5,
    "large": 2.0,
}

BATCH = 1000  # rows per INSERT


# ─── DDL ─────────────────────────────────────────────────────────────────────

SCHEMA_MODE_DEFAULT = "tidb_optimized"
SCHEMA_MODES = {"tidb_optimized", "mysql_compatible"}
RUN_MODE_DEFAULT = "validation"
RUN_MODES = {"validation", "performance"}

SCHEMA_A_DDL_MYSQL = """
CREATE TABLE IF NOT EXISTS users (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    external_id VARCHAR(36) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    name        VARCHAR(255),
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_email (email),
    INDEX idx_status (status),
    INDEX idx_created (created_at)
);

CREATE TABLE IF NOT EXISTS accounts (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    type        VARCHAR(50) NOT NULL,
    balance     DECIMAL(18,4) DEFAULT 0,
    currency    CHAR(3) DEFAULT 'USD',
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_status_type (status, type)
);

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    type            VARCHAR(50) NOT NULL,
    amount          DECIMAL(18,4) NOT NULL,
    currency        CHAR(3) DEFAULT 'USD',
    status          VARCHAR(20) DEFAULT 'completed',
    reference_id    VARCHAR(64),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_account_created (account_id, created_at),
    INDEX idx_status (status),
    INDEX idx_created (created_at)
);

CREATE TABLE IF NOT EXISTS transaction_items (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id  BIGINT NOT NULL,
    description     VARCHAR(255),
    amount          DECIMAL(18,4) NOT NULL,
    quantity        INT DEFAULT 1,
    INDEX idx_txn (transaction_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity_type VARCHAR(50),
    entity_id   BIGINT,
    action      VARCHAR(50),
    actor_id    BIGINT,
    payload     JSON,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_entity (entity_type, entity_id),
    INDEX idx_created (created_at)
);
"""

SCHEMA_A_DDL_TIDB = """
CREATE TABLE IF NOT EXISTS users (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    external_id VARCHAR(36) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    name        VARCHAR(255),
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_email (email),
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS accounts (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    user_id     BIGINT NOT NULL,
    type        VARCHAR(50) NOT NULL,
    balance     DECIMAL(18,4) DEFAULT 0,
    currency    CHAR(3) DEFAULT 'USD',
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_status_type (status, type)
) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    account_id      BIGINT NOT NULL,
    type            VARCHAR(50) NOT NULL,
    amount          DECIMAL(18,4) NOT NULL,
    currency        CHAR(3) DEFAULT 'USD',
    status          VARCHAR(20) DEFAULT 'completed',
    reference_id    VARCHAR(64),
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_account_created (account_id, created_at),
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS transaction_items (
    id              BIGINT AUTO_RANDOM PRIMARY KEY,
    transaction_id  BIGINT NOT NULL,
    description     VARCHAR(255),
    amount          DECIMAL(18,4) NOT NULL,
    quantity        INT DEFAULT 1,
    INDEX idx_txn (transaction_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGINT AUTO_RANDOM PRIMARY KEY,
    entity_type VARCHAR(50),
    entity_id   BIGINT,
    action      VARCHAR(50),
    actor_id    BIGINT,
    payload     JSON,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_entity (entity_type, entity_id),
    INDEX idx_created (created_at)
);
"""

SCHEMA_B_DDL = """
CREATE TABLE IF NOT EXISTS events (
    id          BIGINT AUTO_RANDOM PRIMARY KEY,
    source      VARCHAR(100),
    event_type  VARCHAR(100),
    user_id     BIGINT,
    session_id  BIGINT,
    properties  JSON,
    ts          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_type_ts (event_type, ts),
    INDEX idx_user_ts (user_id, ts)
);

CREATE TABLE IF NOT EXISTS metrics (
    id          BIGINT AUTO_RANDOM PRIMARY KEY,
    host        VARCHAR(100),
    metric_name VARCHAR(100),
    value       DOUBLE,
    tags        JSON,
    ts          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_name_ts (metric_name, ts),
    INDEX idx_host_ts (host, ts)
);

CREATE TABLE IF NOT EXISTS sessions (
    id              BIGINT AUTO_RANDOM PRIMARY KEY,
    user_id         BIGINT,
    started_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    ended_at        DATETIME,
    duration_sec    INT,
    page_views      INT DEFAULT 0,
    INDEX idx_user (user_id),
    INDEX idx_started (started_at)
);
"""

SCHEMA_C_DDL_MYSQL = """
CREATE TABLE IF NOT EXISTS tenants (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    plan        VARCHAR(50) DEFAULT 'starter',
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_plan (plan)
);

CREATE TABLE IF NOT EXISTS tenant_users (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    email       VARCHAR(255) NOT NULL,
    role        VARCHAR(50) DEFAULT 'member',
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_email (email)
);

CREATE TABLE IF NOT EXISTS tenant_data (
    id          BIGINT AUTO_RANDOM PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    data_type   VARCHAR(100),
    payload     JSON,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_type (tenant_id, data_type)
);
"""

SCHEMA_C_DDL_TIDB = """
CREATE TABLE IF NOT EXISTS tenants (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    name        VARCHAR(255) NOT NULL,
    plan        VARCHAR(50) DEFAULT 'starter',
    status      TINYINT DEFAULT 1,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_plan (plan)
) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS tenant_users (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED,
    tenant_id   BIGINT NOT NULL,
    email       VARCHAR(255) NOT NULL,
    role        VARCHAR(50) DEFAULT 'member',
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant (tenant_id),
    INDEX idx_email (email)
) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS tenant_data (
    id          BIGINT AUTO_RANDOM PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    data_type   VARCHAR(100),
    payload     JSON,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_tenant_type (tenant_id, data_type)
);
"""


def resolve_schema_mode(test_cfg: dict) -> str:
    raw = str((test_cfg or {}).get("schema_mode", SCHEMA_MODE_DEFAULT)).strip().lower()
    if raw not in SCHEMA_MODES:
        return SCHEMA_MODE_DEFAULT
    return raw


def resolve_run_mode(test_cfg: dict) -> str:
    raw = str((test_cfg or {}).get("run_mode", RUN_MODE_DEFAULT)).strip().lower()
    if raw not in RUN_MODES:
        return RUN_MODE_DEFAULT
    return raw


def schema_ddls(schema_mode: str) -> tuple[str, str, str]:
    if schema_mode == "mysql_compatible":
        return SCHEMA_A_DDL_MYSQL, SCHEMA_B_DDL, SCHEMA_C_DDL_MYSQL
    return SCHEMA_A_DDL_TIDB, SCHEMA_B_DDL, SCHEMA_C_DDL_TIDB


# ─── Data generators ─────────────────────────────────────────────────────────

def _rand_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))


def gen_users(n):
    for i in range(n):
        yield (
            fake.uuid4(), fake.unique.email(),
            fake.name(), random.randint(0, 1),
        )


def gen_accounts(n, user_count):
    types = ["checking", "savings", "credit", "investment"]
    for _ in range(n):
        yield (
            random.randint(1, user_count),
            random.choice(types),
            round(random.uniform(0, 100_000), 4),
            "USD",
        )


def gen_transactions(n, account_count):
    types = ["payment", "transfer", "deposit", "withdrawal", "refund"]
    statuses = ["completed", "pending", "failed"]
    for _ in range(n):
        yield (
            random.randint(1, account_count),
            random.choice(types),
            round(random.uniform(0.01, 5000), 4),
            random.choice(statuses),
            _rand_str(16),
        )


def gen_transaction_items(n, txn_count):
    descs = ["Service fee", "Product", "Subscription", "Tax", "Shipping"]
    for _ in range(n):
        yield (
            random.randint(1, txn_count),
            random.choice(descs),
            round(random.uniform(0.01, 500), 4),
            random.randint(1, 5),
        )


def gen_audit_log(n, user_count):
    actions = ["create", "update", "delete", "login", "logout"]
    entities = ["user", "account", "transaction"]
    import json as _json
    for _ in range(n):
        yield (
            random.choice(entities),
            random.randint(1, 100_000),
            random.choice(actions),
            random.randint(1, user_count),
            _json.dumps({"ip": fake.ipv4(), "ua": fake.user_agent()[:80]}),
        )


def gen_events(n, user_count, session_count):
    sources = ["web", "mobile", "api", "batch"]
    evts = ["page_view", "click", "purchase", "signup", "error"]
    import json as _json
    for _ in range(n):
        yield (
            random.choice(sources), random.choice(evts),
            random.randint(1, user_count),
            random.randint(1, max(1, session_count)),
            _json.dumps({"v": random.randint(1, 100)}),
        )


def gen_metrics(n):
    hosts = [f"host-{i}" for i in range(1, 21)]
    names = ["cpu_pct", "mem_pct", "disk_iops", "net_rx_mb", "req_latency_ms"]
    import json as _json
    for _ in range(n):
        yield (
            random.choice(hosts), random.choice(names),
            round(random.uniform(0, 100), 4),
            _json.dumps({"env": random.choice(["prod", "staging"])}),
        )


def gen_sessions(n, user_count):
    for _ in range(n):
        dur = random.randint(10, 3600)
        yield (random.randint(1, user_count), dur, random.randint(1, 50))


def gen_tenants(n):
    plans = ["starter", "essential", "premium", "enterprise"]
    for i in range(n):
        yield (f"Tenant-{i}", random.choice(plans), 1)


def gen_tenant_users(n, tenant_count):
    roles = ["owner", "admin", "member", "viewer"]
    for _ in range(n):
        yield (random.randint(1, tenant_count), fake.email(), random.choice(roles))


def gen_tenant_data(n, tenant_count):
    types = ["config", "record", "report", "log", "asset"]
    import json as _json
    for _ in range(n):
        yield (
            random.randint(1, tenant_count),
            random.choice(types),
            _json.dumps({"key": _rand_str(8), "value": random.randint(1, 10000)}),
        )


# ─── Bulk insert helper ───────────────────────────────────────────────────────

def bulk_insert(conn, table, cols, gen, total, label=""):
    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{table}` ({', '.join(cols)}) VALUES ({placeholders})"
    inserted = 0
    batch = []
    t0 = time.time()
    for row in gen:
        batch.append(row)
        if len(batch) >= BATCH:
            cur.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)
            batch = []
            elapsed = time.time() - t0
            rate = inserted / elapsed
            pct = inserted / total * 100
            print(f"  {label}: {inserted:,}/{total:,} ({pct:.0f}%) — {rate:.0f} rows/s", end="\r")
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)
    print(f"  {label}: {inserted:,} rows in {time.time()-t0:.1f}s" + " " * 20)
    return inserted


def table_exists(conn, table):
    cur = conn.cursor()
    cur.execute(f"SHOW TABLES LIKE '{table}'")
    return cur.fetchone() is not None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic PoV data")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--skip-if-exists", action="store_true",
                        help="Skip table population if tables already exist")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f) or {}

    test_cfg = cfg.get("test") or {}
    run_mode = resolve_run_mode(test_cfg)
    schema_mode = resolve_schema_mode(test_cfg)
    scale = str(test_cfg.get("data_scale", "small")).strip().lower()
    if scale not in SCALE_CONFIG:
        print(f"  Unknown data_scale '{scale}', defaulting to small.")
        scale = "small"
    counts = SCALE_CONFIG[scale]
    industry = resolve_industry_from_cfg(cfg)
    industry_key = str(industry.get("key", INDUSTRY_DEFAULT))
    tidb_cfg = cfg.get("tidb") or {}
    if not tidb_cfg:
        raise ValueError("Missing required 'tidb' config block.")

    print(f"\n{'='*60}")
    print(f"  TiDB Cloud PoV Kit — Data Generator")
    print(f"  Scale: {scale.upper()} | DB: {tidb_cfg['database']}")
    print(f"  Industry: {industry.get('label', industry_key)}")
    print(f"  Run Mode: {run_mode} | Schema Mode: {schema_mode}")
    print(f"{'='*60}\n")

    print("[1/3] Creating database and schemas...")
    create_database_if_missing(tidb_cfg)
    conn = get_connection(tidb_cfg, autocommit=True)
    cur = conn.cursor()
    schema_a_ddl, schema_b_ddl, schema_c_ddl = schema_ddls(schema_mode)

    # Schema B is always present because write-contention and telemetry tests use it.
    for stmt in schema_b_ddl.strip().split(";"):
        s = stmt.strip()
        if s:
            cur.execute(s)

    if industry_key == INDUSTRY_DEFAULT:
        for stmt in schema_a_ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        for stmt in schema_c_ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
    else:
        for stmt in industry_schema_sql(industry_key, schema_mode):
            s = stmt.strip()
            if s:
                cur.execute(s)
    print("  Schemas created.")

    # Capture cluster metadata for report
    try:
        from lib.db_utils import capture_cluster_info
        capture_cluster_info(tidb_cfg)
    except Exception:
        pass

    primary_table = "users" if industry_key == INDUSTRY_DEFAULT else industry_primary_table(industry_key)
    if args.skip_if_exists and table_exists(conn, primary_table):
        cur.execute(f"SELECT COUNT(*) FROM {primary_table}")
        n = cur.fetchone()[0]
        if n > 0:
            print(f"  Tables already populated ({n:,} rows in {primary_table}). Skipping data generation.")
            conn.close()
            _write_manifest(
                scale,
                counts,
                schema_mode=schema_mode,
                run_mode=run_mode,
                industry=industry_key,
            )
            return

    print("\n[2/3] Inserting data...")
    t_start = time.time()

    user_count = counts["users"]
    acct_count = counts["accounts"]
    txn_count = counts["transactions"]
    tenant_count = counts["tenants"]
    session_count = counts["sessions"]

    manifest_counts = dict(counts)

    if industry_key == INDUSTRY_DEFAULT:
        bulk_insert(conn, "users",
                    ["external_id", "email", "name", "status"],
                    gen_users(user_count), user_count, "users")

        bulk_insert(conn, "accounts",
                    ["user_id", "type", "balance", "currency"],
                    gen_accounts(acct_count, user_count), acct_count, "accounts")

        bulk_insert(conn, "transactions",
                    ["account_id", "type", "amount", "status", "reference_id"],
                    gen_transactions(txn_count, acct_count), txn_count, "transactions")

        bulk_insert(conn, "transaction_items",
                    ["transaction_id", "description", "amount", "quantity"],
                    gen_transaction_items(counts["transaction_items"], txn_count),
                    counts["transaction_items"], "transaction_items")

        bulk_insert(conn, "audit_log",
                    ["entity_type", "entity_id", "action", "actor_id", "payload"],
                    gen_audit_log(counts["audit_log"], user_count),
                    counts["audit_log"], "audit_log")
    else:
        industry_counts, seed_specs = industry_seed_specs(industry_key, counts, fake)
        manifest_counts.update(industry_counts)
        for spec in seed_specs:
            bulk_insert(
                conn,
                spec["table"],
                spec["cols"],
                spec["gen"],
                spec["total"],
                spec["label"],
            )

    bulk_insert(conn, "sessions",
                ["user_id", "duration_sec", "page_views"],
                gen_sessions(session_count, user_count), session_count, "sessions")

    bulk_insert(conn, "events",
                ["source", "event_type", "user_id", "session_id", "properties"],
                gen_events(counts["events"], user_count, session_count),
                counts["events"], "events")

    bulk_insert(conn, "metrics",
                ["host", "metric_name", "value", "tags"],
                gen_metrics(counts["metrics"]), counts["metrics"], "metrics")

    bulk_insert(conn, "tenants",
                ["name", "plan", "status"],
                gen_tenants(tenant_count), tenant_count, "tenants")

    bulk_insert(conn, "tenant_users",
                ["tenant_id", "email", "role"],
                gen_tenant_users(counts["tenant_users"], tenant_count),
                counts["tenant_users"], "tenant_users")

    bulk_insert(conn, "tenant_data",
                ["tenant_id", "data_type", "payload"],
                gen_tenant_data(counts["tenant_data"], tenant_count),
                counts["tenant_data"], "tenant_data")

    # Update optimizer statistics for all populated tables
    print("\n[3/3] Analyzing tables for optimal query plans...")
    tables_to_analyze = []
    if industry_key == INDUSTRY_DEFAULT:
        tables_to_analyze = ["users", "accounts", "transactions", "transaction_items", "audit_log"]
    else:
        tables_to_analyze = [spec["table"] for spec in seed_specs] if 'seed_specs' in dir() else []
    tables_to_analyze += ["sessions", "events", "metrics", "tenants", "tenant_users", "tenant_data"]

    for tbl in tables_to_analyze:
        try:
            print(f"  ANALYZE TABLE {tbl}...", end=" ")
            cur.execute(f"ANALYZE TABLE `{tbl}`")
            print("done")
        except Exception as e:
            print(f"warning: {e}")

    conn.close()
    total_time = time.time() - t_start
    print(f"\n  All data inserted in {total_time:.1f}s")
    _write_manifest(
        scale,
        manifest_counts,
        total_time,
        schema_mode=schema_mode,
        run_mode=run_mode,
        industry=industry_key,
    )


def _write_manifest(
    scale,
    counts,
    duration_sec=0,
    schema_mode=SCHEMA_MODE_DEFAULT,
    run_mode=RUN_MODE_DEFAULT,
    industry=INDUSTRY_DEFAULT,
):
    manifest = {
        "scale": scale,
        "schema_mode": schema_mode,
        "run_mode": run_mode,
        "industry": industry,
        "counts": counts,
        "generation_duration_sec": round(duration_sec, 1),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    path = os.path.join(os.path.dirname(__file__), "..", "results", "data_manifest.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Manifest written to results/data_manifest.json")


if __name__ == "__main__":
    main()
