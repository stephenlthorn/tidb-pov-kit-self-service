"""
dialect_ddl.py - Per-dialect DDL builders for the comparison data plane.

The repository's primary generate_data.py emits MySQL/TiDB dialect DDL for the
TiDB cluster. When a comparison target is a PostgreSQL flavor, the workload
runner expects matching schemas (users, accounts, transactions, events,
metrics, sessions, plus per-industry tables) but with PG-native types and
without TiDB-specific clauses.

This module produces the PostgreSQL CREATE TABLE statements for the core
schemas used by the baseline OLTP, analytical, and write-contention workloads,
along with the UNIQUE constraints expected by the PG workload upsert queries
in load.postgres_workload_definitions.
"""
from __future__ import annotations

from typing import List


# Schema A - OLTP / payments
SCHEMA_A_PG = """
CREATE TABLE IF NOT EXISTS users (
    id          BIGSERIAL PRIMARY KEY,
    external_id VARCHAR(36) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    name        VARCHAR(255),
    status      SMALLINT DEFAULT 1,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_users_email UNIQUE (email)
);
CREATE INDEX IF NOT EXISTS idx_users_status ON users (status);
CREATE INDEX IF NOT EXISTS idx_users_created ON users (created_at);

CREATE TABLE IF NOT EXISTS accounts (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    type        VARCHAR(50) NOT NULL,
    balance     NUMERIC(18,4) DEFAULT 0,
    currency    CHAR(3) DEFAULT 'USD',
    status      SMALLINT DEFAULT 1,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_accounts_user_type UNIQUE (user_id, type)
);
CREATE INDEX IF NOT EXISTS idx_accounts_user ON accounts (user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status_type ON accounts (status, type);

CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    type            VARCHAR(50) NOT NULL,
    amount          NUMERIC(18,4) NOT NULL,
    currency        CHAR(3) DEFAULT 'USD',
    status          VARCHAR(20) DEFAULT 'completed',
    reference_id    VARCHAR(64),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transactions_account_created ON transactions (account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions (status);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions (created_at);

CREATE TABLE IF NOT EXISTS transaction_items (
    id              BIGSERIAL PRIMARY KEY,
    transaction_id  BIGINT NOT NULL,
    description     VARCHAR(255),
    amount          NUMERIC(18,4) NOT NULL,
    quantity        INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_transaction_items_txn ON transaction_items (transaction_id);

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    entity_type VARCHAR(50),
    entity_id   BIGINT,
    action      VARCHAR(50),
    actor_id    BIGINT,
    payload     JSONB,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log (created_at);
"""

# Schema B - time-series / events. Unique constraint on metrics
# (host, metric_name) matches the upsert_metric_sequential workload query.
SCHEMA_B_PG = """
CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    source      VARCHAR(100),
    event_type  VARCHAR(100),
    user_id     BIGINT,
    session_id  BIGINT,
    properties  JSONB,
    ts          TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events (event_type, ts);
CREATE INDEX IF NOT EXISTS idx_events_user_ts ON events (user_id, ts);

CREATE TABLE IF NOT EXISTS metrics (
    id          BIGSERIAL PRIMARY KEY,
    host        VARCHAR(100),
    metric_name VARCHAR(100),
    value       DOUBLE PRECISION,
    tags        JSONB,
    ts          TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_metrics_host_name UNIQUE (host, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_metrics_name_ts ON metrics (metric_name, ts);
CREATE INDEX IF NOT EXISTS idx_metrics_host_ts ON metrics (host, ts);

CREATE TABLE IF NOT EXISTS sessions (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT,
    started_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at        TIMESTAMP,
    duration_sec    INTEGER,
    page_views      INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions (started_at);
"""

# Schema C - multi-tenant
SCHEMA_C_PG = """
CREATE TABLE IF NOT EXISTS tenants (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    plan        VARCHAR(50) DEFAULT 'starter',
    status      SMALLINT DEFAULT 1,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tenants_plan ON tenants (plan);

CREATE TABLE IF NOT EXISTS tenant_users (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    email       VARCHAR(255) NOT NULL,
    role        VARCHAR(50) DEFAULT 'member',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tenant_users_tenant ON tenant_users (tenant_id);
CREATE INDEX IF NOT EXISTS idx_tenant_users_email ON tenant_users (email);

CREATE TABLE IF NOT EXISTS tenant_data (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    data_type   VARCHAR(100),
    payload     JSONB,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tenant_data_tenant_type ON tenant_data (tenant_id, data_type);
"""


# Schema B - time-series, MySQL-native (AUTO_INCREMENT in place of TiDB's
# AUTO_RANDOM; unique constraint on metrics matches the workload upsert).
SCHEMA_B_DDL_MYSQL = """
CREATE TABLE IF NOT EXISTS events (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    host        VARCHAR(100),
    metric_name VARCHAR(100),
    value       DOUBLE,
    tags        JSON,
    ts          DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uq_metrics_host_name (host, metric_name),
    INDEX idx_name_ts (metric_name, ts),
    INDEX idx_host_ts (host, ts)
);

CREATE TABLE IF NOT EXISTS sessions (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id         BIGINT,
    started_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    ended_at        DATETIME,
    duration_sec    INT,
    page_views      INT DEFAULT 0,
    INDEX idx_user (user_id),
    INDEX idx_started (started_at)
);
"""

# Schema A and C MySQL variants are reused from setup/generate_data.py, but
# the accounts UNIQUE (user_id, type) constraint is added here because the
# workload's upsert_account query targets it.
SCHEMA_A_DDL_MYSQL_EXTRA = """
ALTER TABLE accounts ADD UNIQUE KEY uq_accounts_user_type (user_id, type);
"""


def split_statements(ddl: str) -> List[str]:
    return [s.strip() for s in ddl.split(";") if s.strip()]


def core_schemas_pg() -> List[str]:
    """All CREATE TABLE / CREATE INDEX statements needed for the default
    industry profile's OLTP, analytics, hotspot, and multi-tenant workloads."""
    out: List[str] = []
    out.extend(split_statements(SCHEMA_A_PG))
    out.extend(split_statements(SCHEMA_B_PG))
    out.extend(split_statements(SCHEMA_C_PG))
    return out
