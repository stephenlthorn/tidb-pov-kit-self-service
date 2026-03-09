"""Industry-specific schema and seed planners for PoV synthetic data."""

from __future__ import annotations

import random
from typing import Dict, List


def _derive_sizes(base_counts: Dict) -> Dict[str, int]:
    dim = max(5_000, min(int(base_counts.get("users", 50_000)), 250_000))
    mid = max(10_000, min(int(base_counts.get("accounts", 75_000)), 500_000))
    fact = max(50_000, min(int(base_counts.get("transactions", 5_000_000)) // 10, 5_000_000))
    return {"dim": dim, "mid": mid, "fact": fact}


def _pk_dim(schema_mode: str) -> tuple[str, str]:
    if schema_mode == "tidb_optimized":
        return "BIGINT AUTO_INCREMENT PRIMARY KEY NONCLUSTERED", " SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4"
    return "BIGINT AUTO_INCREMENT PRIMARY KEY", ""


def _pk_fact(schema_mode: str) -> str:
    if schema_mode == "tidb_optimized":
        return "BIGINT AUTO_RANDOM PRIMARY KEY"
    return "BIGINT AUTO_INCREMENT PRIMARY KEY"


def _table_bundle(industry_key: str) -> Dict[str, str]:
    bundles = {
        "banking": {"a": "bank_customers", "b": "bank_accounts", "c": "bank_payments"},
        "healthcare": {"a": "hc_patients", "b": "hc_encounters", "c": "hc_claims"},
        "gaming": {"a": "gm_players", "b": "gm_sessions", "c": "gm_purchases"},
        "retail_ecommerce": {"a": "rt_customers", "b": "rt_orders", "c": "rt_order_items"},
        "saas": {"a": "saas_tenants", "b": "saas_users", "c": "saas_usage_events"},
        "iot_telemetry": {"a": "iot_devices", "b": "iot_alerts", "c": "iot_telemetry"},
        "adtech": {"a": "ad_campaigns", "b": "ad_clicks", "c": "ad_impressions"},
        "logistics": {"a": "lg_shipments", "b": "lg_stops", "c": "lg_tracking_events"},
    }
    return bundles[industry_key]


def industry_primary_table(industry_key: str) -> str:
    return _table_bundle(industry_key)["a"]


def industry_schema_sql(industry_key: str, schema_mode: str) -> List[str]:
    t = _table_bundle(industry_key)
    dim_pk, dim_suffix = _pk_dim(schema_mode)
    fact_pk = _pk_fact(schema_mode)

    if industry_key == "banking":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                full_name   VARCHAR(255),
                segment     VARCHAR(40),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id          {dim_pk},
                customer_id BIGINT NOT NULL,
                balance     DECIMAL(18,4) DEFAULT 0,
                currency    CHAR(3) DEFAULT 'USD',
                status      VARCHAR(20) DEFAULT 'open',
                updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_customer (customer_id)
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                account_id      BIGINT NOT NULL,
                payment_type    VARCHAR(40),
                amount          DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'completed',
                reference_id    VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_acct_created (account_id, created_at),
                INDEX idx_status (status)
            )
            """,
        ]

    if industry_key == "healthcare":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                full_name   VARCHAR(255),
                region      VARCHAR(40),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                patient_id      BIGINT NOT NULL,
                provider_id     BIGINT NOT NULL,
                encounter_type  VARCHAR(40),
                charge          DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'open',
                claim_ref       VARCHAR(64),
                occurred_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_patient_time (patient_id, occurred_at)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                encounter_id    BIGINT NOT NULL,
                payer           VARCHAR(80),
                claim_amount    DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'submitted',
                claim_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_status (status)
            )
            """,
        ]

    if industry_key == "gaming":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                username    VARCHAR(80),
                tier        VARCHAR(20),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                player_id       BIGINT NOT NULL,
                region          VARCHAR(20),
                duration_sec    INT,
                status          VARCHAR(20) DEFAULT 'active',
                started_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at        DATETIME,
                INDEX idx_player (player_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                player_id       BIGINT NOT NULL,
                item_sku        VARCHAR(80),
                amount          DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'settled',
                order_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_player_time (player_id, created_at)
            )
            """,
        ]

    if industry_key == "retail_ecommerce":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                email       VARCHAR(255),
                segment     VARCHAR(40),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_email (email)
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                customer_id     BIGINT NOT NULL,
                status          VARCHAR(20) DEFAULT 'created',
                total_amount    DECIMAL(18,4),
                order_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_customer_time (customer_id, created_at)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                order_id        BIGINT NOT NULL,
                sku             VARCHAR(80),
                qty             INT,
                unit_price      DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'active',
                reference_id    VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_order (order_id)
            )
            """,
        ]

    if industry_key == "saas":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                name        VARCHAR(255),
                plan        VARCHAR(40),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id          {dim_pk},
                tenant_id   BIGINT NOT NULL,
                email       VARCHAR(255),
                role        VARCHAR(40),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tenant (tenant_id)
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                tenant_id       BIGINT NOT NULL,
                user_id         BIGINT NOT NULL,
                event_type      VARCHAR(80),
                units           INT,
                billable_amount DECIMAL(18,4),
                status          VARCHAR(20) DEFAULT 'open',
                event_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_tenant_time (tenant_id, created_at)
            )
            """,
        ]

    if industry_key == "iot_telemetry":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id              {dim_pk},
                customer_id     BIGINT NOT NULL,
                device_type     VARCHAR(60),
                status          VARCHAR(20) DEFAULT 'online',
                firmware_version VARCHAR(40),
                last_seen_at    DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                device_id       BIGINT NOT NULL,
                alert_type      VARCHAR(60),
                status          VARCHAR(20) DEFAULT 'open',
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_device (device_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                device_id       BIGINT NOT NULL,
                metric_name     VARCHAR(80),
                metric_value    DOUBLE,
                severity        VARCHAR(20),
                status          VARCHAR(20) DEFAULT 'new',
                event_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_device_time (device_id, created_at)
            )
            """,
        ]

    if industry_key == "adtech":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id          {dim_pk},
                advertiser_id BIGINT NOT NULL,
                name        VARCHAR(255),
                budget      DECIMAL(18,4),
                status      VARCHAR(20) DEFAULT 'active',
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                impression_id   BIGINT,
                campaign_id     BIGINT NOT NULL,
                cost            DECIMAL(18,6),
                status          VARCHAR(20) DEFAULT 'charged',
                click_ref       VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_campaign (campaign_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                campaign_id     BIGINT NOT NULL,
                user_id         BIGINT NOT NULL,
                cost            DECIMAL(18,6),
                status          VARCHAR(20) DEFAULT 'served',
                bid_ref         VARCHAR(64),
                impressed_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_campaign_time (campaign_id, impressed_at)
            )
            """,
        ]

    if industry_key == "logistics":
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {t['a']} (
                id              {dim_pk},
                customer_id     BIGINT NOT NULL,
                origin          VARCHAR(80),
                destination     VARCHAR(80),
                status          VARCHAR(20) DEFAULT 'created',
                tracking_ref    VARCHAR(64),
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
            ){dim_suffix}
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['b']} (
                id              {fact_pk},
                shipment_id     BIGINT NOT NULL,
                stop_seq        INT,
                location        VARCHAR(120),
                status          VARCHAR(20) DEFAULT 'planned',
                eta_at          DATETIME,
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_shipment (shipment_id)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {t['c']} (
                id              {fact_pk},
                shipment_id     BIGINT NOT NULL,
                event_type      VARCHAR(60),
                event_status    VARCHAR(20),
                detail_ref      VARCHAR(64),
                status          VARCHAR(20) DEFAULT 'ok',
                event_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_ship_time (shipment_id, event_at)
            )
            """,
        ]

    raise ValueError(f"Unsupported industry key: {industry_key}")


def industry_seed_specs(industry_key: str, base_counts: Dict, fake) -> tuple[Dict[str, int], List[Dict]]:
    sizes = _derive_sizes(base_counts)
    t = _table_bundle(industry_key)
    dim_n, mid_n, fact_n = sizes["dim"], sizes["mid"], sizes["fact"]
    counts = {t["a"]: dim_n, t["b"]: mid_n, t["c"]: fact_n}

    if industry_key == "banking":
        specs = [
            {
                "table": t["a"],
                "cols": ["full_name", "segment", "status"],
                "gen": ((fake.name(), random.choice(["retail", "wealth", "business"]), "active") for _ in range(dim_n)),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["customer_id", "balance", "currency", "status"],
                "gen": ((random.randint(1, dim_n), round(random.uniform(0, 500000), 2), "USD", "open") for _ in range(mid_n)),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["account_id", "payment_type", "amount", "status", "reference_id"],
                "gen": (
                    (
                        random.randint(1, mid_n),
                        random.choice(["transfer", "payment", "withdrawal", "deposit"]),
                        round(random.uniform(1, 5000), 2),
                        random.choice(["completed", "pending", "failed"]),
                        f"bp-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "healthcare":
        specs = [
            {
                "table": t["a"],
                "cols": ["full_name", "region", "status"],
                "gen": ((fake.name(), random.choice(["east", "west", "north", "south"]), "active") for _ in range(dim_n)),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["patient_id", "provider_id", "encounter_type", "charge", "status", "claim_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.randint(1, 10000),
                        random.choice(["visit", "lab", "telehealth", "er"]),
                        round(random.uniform(50, 2000), 2),
                        random.choice(["open", "closed"]),
                        f"enc-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["encounter_id", "payer", "claim_amount", "status", "claim_ref"],
                "gen": (
                    (
                        random.randint(1, mid_n),
                        random.choice(["payer_a", "payer_b", "payer_c"]),
                        round(random.uniform(80, 6000), 2),
                        random.choice(["submitted", "paid", "denied"]),
                        f"cl-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "gaming":
        specs = [
            {
                "table": t["a"],
                "cols": ["username", "tier", "status"],
                "gen": ((f"user_{random.randint(100000, 999999)}", random.choice(["bronze", "silver", "gold"]), "active") for _ in range(dim_n)),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["player_id", "region", "duration_sec", "status"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["na", "eu", "apac"]),
                        random.randint(60, 3600),
                        random.choice(["active", "ended"]),
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["player_id", "item_sku", "amount", "status", "order_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["skin_a", "skin_b", "pass", "xp_boost"]),
                        round(random.uniform(1, 60), 2),
                        random.choice(["settled", "pending"]),
                        f"gm-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "retail_ecommerce":
        specs = [
            {
                "table": t["a"],
                "cols": ["email", "segment", "status"],
                "gen": ((fake.email(), random.choice(["consumer", "vip", "wholesale"]), "active") for _ in range(dim_n)),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["customer_id", "status", "total_amount", "order_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["created", "paid", "shipped", "delivered"]),
                        round(random.uniform(10, 500), 2),
                        f"rt-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["order_id", "sku", "qty", "unit_price", "status", "reference_id"],
                "gen": (
                    (
                        random.randint(1, mid_n),
                        random.choice(["sku-1", "sku-2", "sku-3", "sku-4"]),
                        random.randint(1, 5),
                        round(random.uniform(2, 180), 2),
                        "active",
                        f"ri-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "saas":
        specs = [
            {
                "table": t["a"],
                "cols": ["name", "plan", "status"],
                "gen": ((f"Tenant-{i}", random.choice(["starter", "essential", "premium"]), "active") for i in range(dim_n)),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["tenant_id", "email", "role", "status"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        fake.email(),
                        random.choice(["owner", "admin", "member", "viewer"]),
                        "active",
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["tenant_id", "user_id", "event_type", "units", "billable_amount", "status", "event_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.randint(1, mid_n),
                        random.choice(["api_call", "job_run", "sync", "storage_write"]),
                        random.randint(1, 100),
                        round(random.uniform(0.01, 20), 4),
                        random.choice(["open", "invoiced", "settled"]),
                        f"sa-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "iot_telemetry":
        specs = [
            {
                "table": t["a"],
                "cols": ["customer_id", "device_type", "status", "firmware_version"],
                "gen": (
                    (
                        random.randint(1, 50000),
                        random.choice(["sensor", "gateway", "camera", "meter"]),
                        random.choice(["online", "offline", "degraded"]),
                        random.choice(["1.0", "1.1", "2.0"]),
                    )
                    for _ in range(dim_n)
                ),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["device_id", "alert_type", "status"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["temp_high", "battery_low", "disconnect", "tamper"]),
                        random.choice(["open", "ack", "resolved"]),
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["device_id", "metric_name", "metric_value", "severity", "status", "event_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["temp_c", "humidity", "pressure", "cpu_pct"]),
                        round(random.uniform(0, 100), 3),
                        random.choice(["info", "warn", "critical"]),
                        random.choice(["new", "processed"]),
                        f"iot-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "adtech":
        specs = [
            {
                "table": t["a"],
                "cols": ["advertiser_id", "name", "budget", "status"],
                "gen": (
                    (
                        random.randint(1, 100000),
                        f"Campaign-{random.randint(1, 999999)}",
                        round(random.uniform(1000, 100000), 2),
                        random.choice(["active", "paused"]),
                    )
                    for _ in range(dim_n)
                ),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["impression_id", "campaign_id", "cost", "status", "click_ref"],
                "gen": (
                    (
                        random.randint(1, fact_n),
                        random.randint(1, dim_n),
                        round(random.uniform(0.01, 1.0), 4),
                        random.choice(["charged", "invalid"]),
                        f"clk-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["campaign_id", "user_id", "cost", "status", "bid_ref"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.randint(1, 10_000_000),
                        round(random.uniform(0.0001, 0.02), 6),
                        random.choice(["served", "skipped"]),
                        f"ad-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    if industry_key == "logistics":
        specs = [
            {
                "table": t["a"],
                "cols": ["customer_id", "origin", "destination", "status", "tracking_ref"],
                "gen": (
                    (
                        random.randint(1, 100000),
                        random.choice(["nyc", "lax", "mia", "dfw"]),
                        random.choice(["sea", "chi", "atl", "bos"]),
                        random.choice(["created", "in_transit", "delivered", "exception"]),
                        f"trk-{random.randint(1000000, 9999999)}",
                    )
                    for _ in range(dim_n)
                ),
                "total": dim_n,
                "label": t["a"],
            },
            {
                "table": t["b"],
                "cols": ["shipment_id", "stop_seq", "location", "status", "eta_at"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.randint(1, 4),
                        random.choice(["hub_a", "hub_b", "hub_c", "hub_d"]),
                        random.choice(["planned", "arrived", "departed"]),
                        fake.date_time_this_year(),
                    )
                    for _ in range(mid_n)
                ),
                "total": mid_n,
                "label": t["b"],
            },
            {
                "table": t["c"],
                "cols": ["shipment_id", "event_type", "event_status", "detail_ref", "status"],
                "gen": (
                    (
                        random.randint(1, dim_n),
                        random.choice(["picked", "in_transit", "arrival", "delivery"]),
                        random.choice(["ok", "delay", "exception"]),
                        f"lg-{random.randint(1000000, 9999999)}",
                        "ok",
                    )
                    for _ in range(fact_n)
                ),
                "total": fact_n,
                "label": t["c"],
            },
        ]
        return counts, specs

    raise ValueError(f"Unsupported industry key: {industry_key}")

