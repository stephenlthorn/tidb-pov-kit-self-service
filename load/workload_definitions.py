"""
workload_definitions.py — Query templates and selectors for workload families.

All queries use %s placeholders (mysql-connector style).
Each definition is a dict: {sql, params_fn, query_type, weight}
"""
import random
import time
from typing import Dict, List

from lib.industry_profiles import INDUSTRY_DEFAULT, normalize_industry_key


def _rnd(n): return random.randint(1, max(1, n))


def _now_offset():
    return time.strftime("%Y-%m-%d %H:%M:%S",
                         time.gmtime(time.time() - random.randint(0, 86400 * 30)))


def _count(counts: dict, key: str, default: int) -> int:
    try:
        raw = int(counts.get(key, default))
    except (TypeError, ValueError):
        raw = default
    return max(1, raw)


# ─── Schema A — OLTP/Payments ─────────────────────────────────────────────────

def schema_a_workload(counts: dict) -> list:
    user_count = _count(counts, "users", 500_000)
    acct_count = _count(counts, "accounts", 750_000)

    return [
        # Point reads (most common in OLTP)
        {
            "query_type": "select_account",
            "sql": "SELECT id, balance, status FROM accounts WHERE id = %s",
            "params_fn": lambda c: (_rnd(acct_count),),
            "weight": 25,
        },
        {
            "query_type": "select_user",
            "sql": "SELECT id, email, name, status FROM users WHERE id = %s",
            "params_fn": lambda c: (_rnd(user_count),),
            "weight": 15,
        },
        # Range reads
        {
            "query_type": "select_transactions_range",
            "sql": ("SELECT id, amount, status, created_at FROM transactions "
                    "WHERE account_id = %s AND created_at > %s LIMIT 20"),
            "params_fn": lambda c: (_rnd(acct_count), _now_offset()),
            "weight": 15,
        },
        {
            "query_type": "select_transaction_count",
            "sql": "SELECT COUNT(*) FROM transactions WHERE account_id = %s",
            "params_fn": lambda c: (_rnd(acct_count),),
            "weight": 5,
        },
        # Writes
        {
            "query_type": "update_balance",
            "sql": "UPDATE accounts SET balance = balance - %s WHERE id = %s AND balance >= %s",
            "params_fn": lambda c: (
                round(random.uniform(0.01, 100), 2),
                _rnd(acct_count),
                round(random.uniform(0.01, 100), 2),
            ),
            "weight": 20,
        },
        {
            "query_type": "insert_transaction",
            "sql": ("INSERT INTO transactions (account_id, type, amount, status, reference_id) "
                    "VALUES (%s, %s, %s, 'completed', %s)"),
            "params_fn": lambda c: (
                _rnd(acct_count),
                random.choice(["payment", "transfer", "deposit"]),
                round(random.uniform(0.01, 5000), 4),
                f"ref-{random.randint(1000000, 9999999)}",
            ),
            "weight": 15,
        },
        {
            "query_type": "upsert_account",
            "sql": ("INSERT INTO accounts (user_id, type, balance, currency) "
                    "VALUES (%s, %s, %s, 'USD') "
                    "ON DUPLICATE KEY UPDATE balance = balance + %s"),
            "params_fn": lambda c: (
                _rnd(user_count),
                random.choice(["checking", "savings"]),
                round(random.uniform(100, 10000), 4),
                round(random.uniform(0.01, 100), 4),
            ),
            "weight": 5,
        },
    ]


# ─── Schema B — Time-series (hotspot write pattern) ──────────────────────────

def schema_b_hotspot_workload(counts: dict) -> list:
    """Uses AUTO_INCREMENT-style sequential inserts — creates hot Regions."""
    user_count = _count(counts, "users", 500_000)

    return [
        {
            "query_type": "insert_event_sequential",
            "sql": ("INSERT INTO events (source, event_type, user_id, session_id, properties) "
                    "VALUES (%s, %s, %s, %s, %s)"),
            "params_fn": lambda c: (
                random.choice(["web", "mobile", "api"]),
                random.choice(["click", "page_view", "purchase"]),
                _rnd(user_count),
                _rnd(5_000_000),
                '{"v": 1}',
            ),
            "weight": 60,
        },
        {
            "query_type": "upsert_metric_sequential",
            "sql": ("INSERT INTO metrics (host, metric_name, value, tags) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON DUPLICATE KEY UPDATE value = %s"),
            "params_fn": lambda c: (
                f"host-{random.randint(1,20)}",
                random.choice(["cpu_pct", "mem_pct", "disk_iops"]),
                round(random.uniform(0, 100), 4),
                '{"env":"prod"}',
                round(random.uniform(0, 100), 4),
            ),
            "weight": 40,
        },
    ]


def schema_b_autorand_workload(counts: dict) -> list:
    """Same semantics but against AUTO_RANDOM tables — avoids hotspots."""
    user_count = _count(counts, "users", 500_000)

    return [
        {
            "query_type": "insert_event_autorand",
            "sql": ("INSERT INTO events (source, event_type, user_id, session_id, properties) "
                    "VALUES (%s, %s, %s, %s, %s)"),
            "params_fn": lambda c: (
                random.choice(["web", "mobile", "api"]),
                random.choice(["click", "page_view", "purchase"]),
                _rnd(user_count),
                _rnd(5_000_000),
                '{"v": 1}',
            ),
            "weight": 100,
        },
    ]


# ─── Analytical queries (TiFlash) ────────────────────────────────────────────

def analytical_workload(counts: dict) -> list:
    return [
        {
            "query_type": "analytics_daily_volume",
            "sql": ("SELECT DATE(created_at) AS day, SUM(amount) AS total, COUNT(*) AS cnt "
                    "FROM transactions "
                    "WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                    "GROUP BY day ORDER BY day"),
            "params_fn": lambda c: (),
            "weight": 30,
        },
        {
            "query_type": "analytics_top_accounts",
            "sql": ("SELECT account_id, SUM(amount) AS total "
                    "FROM transactions "
                    "WHERE status = 'completed' "
                    "GROUP BY account_id ORDER BY total DESC LIMIT 10"),
            "params_fn": lambda c: (),
            "weight": 20,
        },
        {
            "query_type": "analytics_status_breakdown",
            "sql": ("SELECT status, COUNT(*) AS cnt, AVG(amount) AS avg_amt "
                    "FROM transactions GROUP BY status"),
            "params_fn": lambda c: (),
            "weight": 25,
        },
        {
            "query_type": "analytics_user_activity",
            "sql": ("SELECT u.id, u.email, COUNT(t.id) AS txn_count, SUM(t.amount) AS total "
                    "FROM users u "
                    "JOIN accounts a ON a.user_id = u.id "
                    "JOIN transactions t ON t.account_id = a.id "
                    "WHERE t.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) "
                    "GROUP BY u.id, u.email "
                    "ORDER BY total DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 25,
        },
    ]


def build_weighted_pool(workload: list) -> list:
    """Expand workload definitions into a weighted pool for random sampling."""
    pool = []
    for w in workload:
        pool.extend([w] * w.get("weight", 10))
    return pool


def classify_query_kind(query_type: str) -> str:
    q = (query_type or "").lower()
    if q.startswith("select") or q.startswith("analytics"):
        return "read"
    if q.startswith("insert") or q.startswith("update") or q.startswith("upsert") or q.startswith("delete"):
        return "write"
    return "other"


def apply_workload_profile(
    workload: List[Dict],
    mix: str = "mixed",
    read_multiplier: float = 1.0,
    write_multiplier: float = 1.0,
) -> List[Dict]:
    """
    Return a copy of workload with tuned weights.

    mix:
      - mixed: balanced defaults
      - read_heavy: increase read weights, soften writes
      - write_heavy: increase write weights, soften reads
    read_multiplier / write_multiplier:
      Extra multipliers applied after mix scaling.
    """
    mix = (mix or "mixed").lower()
    read_mix = 1.0
    write_mix = 1.0
    if mix == "read_heavy":
        read_mix = 1.6
        write_mix = 0.7
    elif mix == "write_heavy":
        read_mix = 0.7
        write_mix = 1.6

    read_multiplier = max(0.1, float(read_multiplier or 1.0))
    write_multiplier = max(0.1, float(write_multiplier or 1.0))

    tuned = []
    for item in workload:
        row = dict(item)
        kind = classify_query_kind(str(row.get("query_type", "")))
        base_weight = float(row.get("weight", 1))
        if kind == "read":
            weight = base_weight * read_mix * read_multiplier
        elif kind == "write":
            weight = base_weight * write_mix * write_multiplier
        else:
            weight = base_weight
        row["weight"] = max(1, int(round(weight)))
        tuned.append(row)
    return tuned


def resolve_industry_key(cfg: Dict | None) -> str:
    cfg = cfg or {}
    return normalize_industry_key((cfg.get("industry") or {}).get("selected"))


def transactional_workload_for_cfg(cfg: Dict | None, counts: Dict) -> List[Dict]:
    industry = resolve_industry_key(cfg)
    return INDUSTRY_OLTP_BUILDERS.get(industry, schema_a_workload)(counts)


def analytical_workload_for_cfg(cfg: Dict | None, counts: Dict) -> List[Dict]:
    industry = resolve_industry_key(cfg)
    return INDUSTRY_ANALYTICS_BUILDERS.get(industry, analytical_workload)(counts)


def banking_workload(counts: Dict) -> List[Dict]:
    customer_n = _count(counts, "bank_customers", _count(counts, "users", 500_000))
    account_n = _count(counts, "bank_accounts", _count(counts, "accounts", 750_000))
    return [
        {
            "query_type": "bank_select_account",
            "sql": "SELECT id, balance, status, currency FROM bank_accounts WHERE id = %s",
            "params_fn": lambda c: (_rnd(account_n),),
            "weight": 24,
        },
        {
            "query_type": "bank_select_customer",
            "sql": "SELECT id, full_name, segment, status FROM bank_customers WHERE id = %s",
            "params_fn": lambda c: (_rnd(customer_n),),
            "weight": 14,
        },
        {
            "query_type": "bank_payment_range",
            "sql": ("SELECT id, amount, payment_type, status FROM bank_payments "
                    "WHERE account_id = %s AND created_at > %s ORDER BY id DESC LIMIT 30"),
            "params_fn": lambda c: (_rnd(account_n), _now_offset()),
            "weight": 18,
        },
        {
            "query_type": "bank_update_balance",
            "sql": "UPDATE bank_accounts SET balance = balance - %s WHERE id = %s AND balance >= %s",
            "params_fn": lambda c: (
                round(random.uniform(1, 200), 2),
                _rnd(account_n),
                round(random.uniform(1, 200), 2),
            ),
            "weight": 22,
        },
        {
            "query_type": "bank_insert_payment",
            "sql": ("INSERT INTO bank_payments "
                    "(account_id, payment_type, amount, status, reference_id) "
                    "VALUES (%s, %s, %s, 'completed', %s)"),
            "params_fn": lambda c: (
                _rnd(account_n),
                random.choice(["transfer", "payment", "withdrawal", "deposit"]),
                round(random.uniform(1, 5000), 2),
                f"bp-{random.randint(1000000, 9999999)}",
            ),
            "weight": 22,
        },
    ]


def banking_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "bank_analytics_daily_volume",
            "sql": ("SELECT DATE(created_at) AS d, SUM(amount) AS total_amount, COUNT(*) AS payment_count "
                    "FROM bank_payments WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                    "GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 60,
        },
        {
            "query_type": "bank_analytics_top_accounts",
            "sql": ("SELECT account_id, SUM(amount) AS total FROM bank_payments "
                    "WHERE status = 'completed' GROUP BY account_id ORDER BY total DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 40,
        },
    ]


def healthcare_workload(counts: Dict) -> List[Dict]:
    patient_n = _count(counts, "hc_patients", _count(counts, "users", 500_000))
    encounter_n = _count(counts, "hc_encounters", _count(counts, "accounts", 750_000))
    return [
        {
            "query_type": "hc_select_patient",
            "sql": "SELECT id, full_name, region, status FROM hc_patients WHERE id = %s",
            "params_fn": lambda c: (_rnd(patient_n),),
            "weight": 18,
        },
        {
            "query_type": "hc_recent_encounters",
            "sql": ("SELECT id, encounter_type, charge, status FROM hc_encounters "
                    "WHERE patient_id = %s AND occurred_at > %s ORDER BY id DESC LIMIT 20"),
            "params_fn": lambda c: (_rnd(patient_n), _now_offset()),
            "weight": 18,
        },
        {
            "query_type": "hc_insert_encounter",
            "sql": ("INSERT INTO hc_encounters "
                    "(patient_id, provider_id, encounter_type, charge, status, claim_ref) "
                    "VALUES (%s, %s, %s, %s, 'open', %s)"),
            "params_fn": lambda c: (
                _rnd(patient_n),
                random.randint(1, 10000),
                random.choice(["visit", "lab", "telehealth", "er"]),
                round(random.uniform(50, 2000), 2),
                f"hc-{random.randint(1000000, 9999999)}",
            ),
            "weight": 24,
        },
        {
            "query_type": "hc_update_claim_status",
            "sql": "UPDATE hc_claims SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["submitted", "paid", "denied"]), _rnd(_count(counts, "hc_claims", encounter_n))),
            "weight": 20,
        },
        {
            "query_type": "hc_insert_claim",
            "sql": ("INSERT INTO hc_claims (encounter_id, payer, claim_amount, status, claim_ref) "
                    "VALUES (%s, %s, %s, 'submitted', %s)"),
            "params_fn": lambda c: (
                _rnd(encounter_n),
                random.choice(["payer_a", "payer_b", "payer_c"]),
                round(random.uniform(80, 6000), 2),
                f"cl-{random.randint(1000000, 9999999)}",
            ),
            "weight": 20,
        },
    ]


def healthcare_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "hc_analytics_claim_status",
            "sql": "SELECT status, COUNT(*) AS cnt, SUM(claim_amount) AS total FROM hc_claims GROUP BY status",
            "params_fn": lambda c: (),
            "weight": 60,
        },
        {
            "query_type": "hc_analytics_daily_charges",
            "sql": ("SELECT DATE(occurred_at) AS d, SUM(charge) AS total_charge "
                    "FROM hc_encounters WHERE occurred_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                    "GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 40,
        },
    ]


def gaming_workload(counts: Dict) -> List[Dict]:
    player_n = _count(counts, "gm_players", _count(counts, "users", 500_000))
    session_n = _count(counts, "gm_sessions", _count(counts, "sessions", 5_000_000))
    return [
        {
            "query_type": "gm_select_player",
            "sql": "SELECT id, username, tier, status FROM gm_players WHERE id = %s",
            "params_fn": lambda c: (_rnd(player_n),),
            "weight": 15,
        },
        {
            "query_type": "gm_recent_sessions",
            "sql": ("SELECT id, region, duration_sec, ended_at FROM gm_sessions "
                    "WHERE player_id = %s ORDER BY id DESC LIMIT 25"),
            "params_fn": lambda c: (_rnd(player_n),),
            "weight": 15,
        },
        {
            "query_type": "gm_insert_session",
            "sql": ("INSERT INTO gm_sessions "
                    "(player_id, region, duration_sec, status) VALUES (%s, %s, %s, 'active')"),
            "params_fn": lambda c: (
                _rnd(player_n),
                random.choice(["na", "eu", "apac"]),
                random.randint(60, 3600),
            ),
            "weight": 25,
        },
        {
            "query_type": "gm_insert_purchase",
            "sql": ("INSERT INTO gm_purchases "
                    "(player_id, item_sku, amount, status, order_ref) VALUES (%s, %s, %s, 'settled', %s)"),
            "params_fn": lambda c: (
                _rnd(player_n),
                random.choice(["skin_a", "skin_b", "battle_pass", "xp_boost"]),
                round(random.uniform(1, 60), 2),
                f"gm-{random.randint(1000000, 9999999)}",
            ),
            "weight": 30,
        },
        {
            "query_type": "gm_update_session_status",
            "sql": "UPDATE gm_sessions SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["active", "ended"]), _rnd(session_n)),
            "weight": 15,
        },
    ]


def gaming_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "gm_analytics_revenue_daily",
            "sql": ("SELECT DATE(created_at) AS d, SUM(amount) AS rev FROM gm_purchases "
                    "WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 55,
        },
        {
            "query_type": "gm_analytics_top_spenders",
            "sql": ("SELECT player_id, SUM(amount) AS total FROM gm_purchases "
                    "GROUP BY player_id ORDER BY total DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 45,
        },
    ]


def retail_workload(counts: Dict) -> List[Dict]:
    customer_n = _count(counts, "rt_customers", _count(counts, "users", 500_000))
    order_n = _count(counts, "rt_orders", _count(counts, "transactions", 50_000_000))
    return [
        {
            "query_type": "rt_select_order",
            "sql": "SELECT id, status, total_amount FROM rt_orders WHERE id = %s",
            "params_fn": lambda c: (_rnd(order_n),),
            "weight": 18,
        },
        {
            "query_type": "rt_customer_order_history",
            "sql": ("SELECT id, status, total_amount FROM rt_orders "
                    "WHERE customer_id = %s AND created_at > %s ORDER BY id DESC LIMIT 20"),
            "params_fn": lambda c: (_rnd(customer_n), _now_offset()),
            "weight": 20,
        },
        {
            "query_type": "rt_insert_order",
            "sql": ("INSERT INTO rt_orders (customer_id, status, total_amount, order_ref) "
                    "VALUES (%s, 'created', %s, %s)"),
            "params_fn": lambda c: (
                _rnd(customer_n),
                round(random.uniform(10, 500), 2),
                f"rt-{random.randint(1000000, 9999999)}",
            ),
            "weight": 24,
        },
        {
            "query_type": "rt_update_order_status",
            "sql": "UPDATE rt_orders SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["created", "paid", "shipped", "delivered"]), _rnd(order_n)),
            "weight": 20,
        },
        {
            "query_type": "rt_insert_order_item",
            "sql": "INSERT INTO rt_order_items (order_id, sku, qty, unit_price) VALUES (%s, %s, %s, %s)",
            "params_fn": lambda c: (
                _rnd(order_n),
                random.choice(["sku-1", "sku-2", "sku-3", "sku-4"]),
                random.randint(1, 5),
                round(random.uniform(2, 180), 2),
            ),
            "weight": 18,
        },
    ]


def retail_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "rt_analytics_daily_gmv",
            "sql": ("SELECT DATE(created_at) AS d, SUM(total_amount) AS gmv, COUNT(*) AS orders "
                    "FROM rt_orders WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                    "GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 65,
        },
        {
            "query_type": "rt_analytics_top_skus",
            "sql": ("SELECT sku, SUM(qty) AS units FROM rt_order_items "
                    "GROUP BY sku ORDER BY units DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 35,
        },
    ]


def saas_workload(counts: Dict) -> List[Dict]:
    tenant_n = _count(counts, "saas_tenants", _count(counts, "tenants", 10_000))
    user_n = _count(counts, "saas_users", _count(counts, "tenant_users", 500_000))
    event_n = _count(counts, "saas_usage_events", _count(counts, "tenant_data", 10_000_000))
    return [
        {
            "query_type": "saas_select_tenant",
            "sql": "SELECT id, name, plan, status FROM saas_tenants WHERE id = %s",
            "params_fn": lambda c: (_rnd(tenant_n),),
            "weight": 16,
        },
        {
            "query_type": "saas_tenant_users",
            "sql": ("SELECT id, email, role, status FROM saas_users "
                    "WHERE tenant_id = %s ORDER BY id DESC LIMIT 25"),
            "params_fn": lambda c: (_rnd(tenant_n),),
            "weight": 20,
        },
        {
            "query_type": "saas_insert_usage_event",
            "sql": ("INSERT INTO saas_usage_events "
                    "(tenant_id, user_id, event_type, units, billable_amount, status, event_ref) "
                    "VALUES (%s, %s, %s, %s, %s, 'open', %s)"),
            "params_fn": lambda c: (
                _rnd(tenant_n),
                _rnd(user_n),
                random.choice(["api_call", "job_run", "storage_write", "workspace_edit"]),
                random.randint(1, 100),
                round(random.uniform(0.01, 20), 4),
                f"sa-{random.randint(1000000, 9999999)}",
            ),
            "weight": 30,
        },
        {
            "query_type": "saas_update_usage_status",
            "sql": "UPDATE saas_usage_events SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["open", "invoiced", "settled"]), _rnd(event_n)),
            "weight": 18,
        },
        {
            "query_type": "saas_update_plan",
            "sql": "UPDATE saas_tenants SET plan = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["starter", "essential", "premium", "enterprise"]), _rnd(tenant_n)),
            "weight": 16,
        },
    ]


def saas_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "saas_analytics_usage_daily",
            "sql": ("SELECT DATE(created_at) AS d, SUM(units) AS units, SUM(billable_amount) AS billable "
                    "FROM saas_usage_events WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
                    "GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 60,
        },
        {
            "query_type": "saas_analytics_top_tenants",
            "sql": ("SELECT tenant_id, SUM(billable_amount) AS spend "
                    "FROM saas_usage_events GROUP BY tenant_id ORDER BY spend DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 40,
        },
    ]


def iot_workload(counts: Dict) -> List[Dict]:
    device_n = _count(counts, "iot_devices", _count(counts, "accounts", 750_000))
    telemetry_n = _count(counts, "iot_telemetry", _count(counts, "events", 50_000_000))
    alert_n = _count(counts, "iot_alerts", _count(counts, "metrics", 20_000_000))
    return [
        {
            "query_type": "iot_select_device",
            "sql": "SELECT id, device_type, status, firmware_version FROM iot_devices WHERE id = %s",
            "params_fn": lambda c: (_rnd(device_n),),
            "weight": 14,
        },
        {
            "query_type": "iot_recent_telemetry",
            "sql": ("SELECT id, metric_name, metric_value, severity FROM iot_telemetry "
                    "WHERE device_id = %s ORDER BY id DESC LIMIT 30"),
            "params_fn": lambda c: (_rnd(device_n),),
            "weight": 16,
        },
        {
            "query_type": "iot_insert_telemetry",
            "sql": ("INSERT INTO iot_telemetry "
                    "(device_id, metric_name, metric_value, severity, status, event_ref) "
                    "VALUES (%s, %s, %s, %s, 'new', %s)"),
            "params_fn": lambda c: (
                _rnd(device_n),
                random.choice(["temp_c", "cpu_pct", "humidity", "pressure"]),
                round(random.uniform(0, 100), 3),
                random.choice(["info", "warn", "critical"]),
                f"iot-{random.randint(1000000, 9999999)}",
            ),
            "weight": 36,
        },
        {
            "query_type": "iot_update_alert",
            "sql": "UPDATE iot_alerts SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["open", "ack", "resolved"]), _rnd(alert_n)),
            "weight": 18,
        },
        {
            "query_type": "iot_update_last_seen",
            "sql": "UPDATE iot_devices SET status = %s, last_seen_at = NOW() WHERE id = %s",
            "params_fn": lambda c: (random.choice(["online", "offline", "degraded"]), _rnd(device_n)),
            "weight": 16,
        },
    ]


def iot_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "iot_analytics_metrics",
            "sql": ("SELECT metric_name, ROUND(AVG(metric_value), 3) AS avg_val "
                    "FROM iot_telemetry WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY) "
                    "GROUP BY metric_name ORDER BY avg_val DESC"),
            "params_fn": lambda c: (),
            "weight": 55,
        },
        {
            "query_type": "iot_analytics_alerts",
            "sql": "SELECT status, COUNT(*) AS cnt FROM iot_alerts GROUP BY status",
            "params_fn": lambda c: (),
            "weight": 45,
        },
    ]


def adtech_workload(counts: Dict) -> List[Dict]:
    campaign_n = _count(counts, "ad_campaigns", _count(counts, "accounts", 750_000))
    impression_n = _count(counts, "ad_impressions", _count(counts, "events", 50_000_000))
    return [
        {
            "query_type": "ad_select_campaign",
            "sql": "SELECT id, name, budget, status FROM ad_campaigns WHERE id = %s",
            "params_fn": lambda c: (_rnd(campaign_n),),
            "weight": 16,
        },
        {
            "query_type": "ad_recent_impressions",
            "sql": ("SELECT id, cost, status FROM ad_impressions "
                    "WHERE campaign_id = %s AND impressed_at > %s ORDER BY id DESC LIMIT 30"),
            "params_fn": lambda c: (_rnd(campaign_n), _now_offset()),
            "weight": 18,
        },
        {
            "query_type": "ad_insert_impression",
            "sql": ("INSERT INTO ad_impressions "
                    "(campaign_id, user_id, cost, status, bid_ref) VALUES (%s, %s, %s, 'served', %s)"),
            "params_fn": lambda c: (
                _rnd(campaign_n),
                random.randint(1, 10_000_000),
                round(random.uniform(0.0001, 0.02), 6),
                f"ad-{random.randint(1000000, 9999999)}",
            ),
            "weight": 28,
        },
        {
            "query_type": "ad_insert_click",
            "sql": ("INSERT INTO ad_clicks "
                    "(impression_id, campaign_id, cost, status, click_ref) VALUES (%s, %s, %s, 'charged', %s)"),
            "params_fn": lambda c: (
                _rnd(impression_n),
                _rnd(campaign_n),
                round(random.uniform(0.01, 1.2), 4),
                f"clk-{random.randint(1000000, 9999999)}",
            ),
            "weight": 22,
        },
        {
            "query_type": "ad_update_campaign_budget",
            "sql": "UPDATE ad_campaigns SET budget = budget - %s WHERE id = %s AND budget >= %s",
            "params_fn": lambda c: (
                round(random.uniform(1, 50), 2),
                _rnd(campaign_n),
                round(random.uniform(1, 50), 2),
            ),
            "weight": 16,
        },
    ]


def adtech_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "ad_analytics_campaign_spend",
            "sql": ("SELECT campaign_id, SUM(cost) AS spend FROM ad_impressions "
                    "WHERE impressed_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) "
                    "GROUP BY campaign_id ORDER BY spend DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 55,
        },
        {
            "query_type": "ad_analytics_ctr",
            "sql": ("SELECT i.campaign_id, COUNT(c.id) / NULLIF(COUNT(i.id),0) AS ctr "
                    "FROM ad_impressions i LEFT JOIN ad_clicks c ON c.impression_id = i.id "
                    "GROUP BY i.campaign_id ORDER BY ctr DESC LIMIT 20"),
            "params_fn": lambda c: (),
            "weight": 45,
        },
    ]


def logistics_workload(counts: Dict) -> List[Dict]:
    shipment_n = _count(counts, "lg_shipments", _count(counts, "transactions", 50_000_000))
    return [
        {
            "query_type": "lg_select_shipment",
            "sql": "SELECT id, status, origin, destination FROM lg_shipments WHERE id = %s",
            "params_fn": lambda c: (_rnd(shipment_n),),
            "weight": 18,
        },
        {
            "query_type": "lg_tracking_history",
            "sql": ("SELECT id, event_type, event_status FROM lg_tracking_events "
                    "WHERE shipment_id = %s ORDER BY id DESC LIMIT 30"),
            "params_fn": lambda c: (_rnd(shipment_n),),
            "weight": 18,
        },
        {
            "query_type": "lg_insert_tracking_event",
            "sql": ("INSERT INTO lg_tracking_events "
                    "(shipment_id, event_type, event_status, detail_ref) VALUES (%s, %s, %s, %s)"),
            "params_fn": lambda c: (
                _rnd(shipment_n),
                random.choice(["picked", "in_transit", "customs", "arrived", "delivered"]),
                random.choice(["ok", "delay", "exception"]),
                f"lg-{random.randint(1000000, 9999999)}",
            ),
            "weight": 26,
        },
        {
            "query_type": "lg_update_shipment_status",
            "sql": "UPDATE lg_shipments SET status = %s WHERE id = %s",
            "params_fn": lambda c: (random.choice(["created", "in_transit", "delivered", "exception"]), _rnd(shipment_n)),
            "weight": 22,
        },
        {
            "query_type": "lg_update_stop_status",
            "sql": "UPDATE lg_stops SET status = %s WHERE shipment_id = %s AND stop_seq = %s",
            "params_fn": lambda c: (
                random.choice(["planned", "arrived", "departed", "skipped"]),
                _rnd(shipment_n),
                random.randint(1, 4),
            ),
            "weight": 16,
        },
    ]


def logistics_analytics_workload(counts: Dict) -> List[Dict]:
    return [
        {
            "query_type": "lg_analytics_status",
            "sql": "SELECT status, COUNT(*) AS cnt FROM lg_shipments GROUP BY status",
            "params_fn": lambda c: (),
            "weight": 55,
        },
        {
            "query_type": "lg_analytics_event_trend",
            "sql": ("SELECT DATE(event_at) AS d, COUNT(*) AS events FROM lg_tracking_events "
                    "WHERE event_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) GROUP BY d ORDER BY d"),
            "params_fn": lambda c: (),
            "weight": 45,
        },
    ]


INDUSTRY_OLTP_BUILDERS = {
    INDUSTRY_DEFAULT: schema_a_workload,
    "banking": banking_workload,
    "healthcare": healthcare_workload,
    "gaming": gaming_workload,
    "retail_ecommerce": retail_workload,
    "saas": saas_workload,
    "iot_telemetry": iot_workload,
    "adtech": adtech_workload,
    "logistics": logistics_workload,
}

INDUSTRY_ANALYTICS_BUILDERS = {
    INDUSTRY_DEFAULT: analytical_workload,
    "banking": banking_analytics_workload,
    "healthcare": healthcare_analytics_workload,
    "gaming": gaming_analytics_workload,
    "retail_ecommerce": retail_analytics_workload,
    "saas": saas_analytics_workload,
    "iot_telemetry": iot_analytics_workload,
    "adtech": adtech_analytics_workload,
    "logistics": logistics_analytics_workload,
}


def sample_query(pool: list):
    """Return a random (sql, params_fn, query_type) from the pool."""
    entry = random.choice(pool)
    return entry["sql"], entry["params_fn"], entry["query_type"]
