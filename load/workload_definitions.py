"""
workload_definitions.py — Query templates for each workload type.
All queries use %s placeholders (mysql-connector style).
Each definition is a dict: {sql, params_fn, query_type}
params_fn(counts) returns a tuple of bind parameters.
"""
import random
import time
from typing import Dict, List


def _rnd(n): return random.randint(1, max(1, n))


def _now_offset():
    return time.strftime("%Y-%m-%d %H:%M:%S",
                         time.gmtime(time.time() - random.randint(0, 86400 * 30)))


# ─── Schema A — OLTP/Payments ─────────────────────────────────────────────────

def schema_a_workload(counts: dict) -> list:
    user_count = counts.get("users", 500_000)
    acct_count = counts.get("accounts", 750_000)
    txn_count  = counts.get("transactions", 50_000_000)

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
    user_count = counts.get("users", 500_000)

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
    user_count = counts.get("users", 500_000)

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
    acct_count = counts.get("accounts", 750_000)

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


def sample_query(pool: list):
    """Return a random (sql, params_fn, query_type) from the pool."""
    entry = random.choice(pool)
    return entry["sql"], entry["params_fn"], entry["query_type"]
