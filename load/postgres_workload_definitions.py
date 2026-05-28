"""
postgres_workload_definitions.py - PostgreSQL-dialect workload queries.

This module mirrors the surface area of load.workload_definitions but rewrites
each query's SQL into PostgreSQL dialect. It reuses the same params_fn / weight
/ query_type structure so the load runner can swap pools transparently based
on the target database family.

Translations applied:
- DATE_SUB(NOW(), INTERVAL N DAY)  -> NOW() - INTERVAL 'N days'
- ON DUPLICATE KEY UPDATE x = y    -> ON CONFLICT (<keys>) DO UPDATE SET x = y
- NOW() (returns TIMESTAMP)        -> NOW() (works as-is in PG)
- DATE(col)                        -> DATE(col) (works as-is in PG)

The two upsert queries (accounts, metrics) require explicit conflict targets;
those are overridden by name after translation. Other queries are pure SQL
strings and translate cleanly with regex.
"""
from __future__ import annotations

import re
from typing import Callable, Dict, List

from load import workload_definitions as _mysql
from load.workload_definitions import (  # re-export untouched helpers
    apply_workload_profile,
    build_weighted_pool,
    classify_query_kind,
    resolve_industry_key,
)


_DATE_SUB_RE = re.compile(
    r"DATE_SUB\(\s*NOW\(\)\s*,\s*INTERVAL\s+(\d+)\s+(DAY|HOUR|MINUTE|SECOND)\s*\)",
    re.IGNORECASE,
)


def _pg_interval(match: re.Match) -> str:
    n, unit = match.group(1), match.group(2).lower()
    # postgres canonical units are pluralized
    plural = unit if unit.endswith("s") else f"{unit}s"
    return f"(NOW() - INTERVAL '{n} {plural}')"


def _translate_sql(sql: str) -> str:
    return _DATE_SUB_RE.sub(_pg_interval, sql)


# Per-query_type overrides for queries that can't be regex-translated cleanly.
# The override receives the original (already regex-translated) SQL plus the
# query def and returns the final PG SQL. Conflict targets here MUST match
# the unique constraints set up by the PG schema in setup/generate_data.py.
# The original MySQL params_fn passes (user_id, type, balance, increment) for
# upsert_account and (host, metric_name, value, tags, new_value) for
# upsert_metric_sequential. The PG override must consume the same number of
# placeholders so the workload's params_fn stays compatible.
_QUERY_OVERRIDES: Dict[str, str] = {
    # accounts: UNIQUE (user_id, type) is created by the PG DDL.
    "upsert_account": (
        "INSERT INTO accounts (user_id, type, balance, currency) "
        "VALUES (%s, %s, %s, 'USD') "
        "ON CONFLICT (user_id, type) DO UPDATE "
        "SET balance = accounts.balance + %s"
    ),
    # metrics: UNIQUE (host, metric_name) is created by the PG DDL.
    "upsert_metric_sequential": (
        "INSERT INTO metrics (host, metric_name, value, tags) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (host, metric_name) DO UPDATE SET value = %s"
    ),
}


def translate_pool(workload: List[Dict]) -> List[Dict]:
    """Public alias: translate a MySQL-dialect workload list into PG dialect."""
    return _translate_workload(workload)


def _translate_workload(workload: List[Dict]) -> List[Dict]:
    out: List[Dict] = []
    for item in workload:
        new_item = dict(item)
        qt = str(new_item.get("query_type", ""))
        if qt in _QUERY_OVERRIDES:
            new_item["sql"] = _QUERY_OVERRIDES[qt]
        else:
            new_item["sql"] = _translate_sql(str(new_item.get("sql", "")))
        out.append(new_item)
    return out


def transactional_workload_for_cfg(cfg: Dict | None, counts: Dict) -> List[Dict]:
    return _translate_workload(_mysql.transactional_workload_for_cfg(cfg, counts))


def analytical_workload_for_cfg(cfg: Dict | None, counts: Dict) -> List[Dict]:
    return _translate_workload(_mysql.analytical_workload_for_cfg(cfg, counts))


def schema_a_workload(counts: dict) -> list:
    return _translate_workload(_mysql.schema_a_workload(counts))


def schema_b_hotspot_workload(counts: dict) -> list:
    return _translate_workload(_mysql.schema_b_hotspot_workload(counts))


def schema_b_autorand_workload(counts: dict) -> list:
    return _translate_workload(_mysql.schema_b_autorand_workload(counts))


def analytical_workload(counts: dict) -> list:
    return _translate_workload(_mysql.analytical_workload(counts))


# Sampling helper - delegate to base so behavior stays identical.
def sample_query(pool: List[Dict]):
    return _mysql.sample_query(pool)


__all__ = [
    "transactional_workload_for_cfg",
    "analytical_workload_for_cfg",
    "schema_a_workload",
    "schema_b_hotspot_workload",
    "schema_b_autorand_workload",
    "analytical_workload",
    "apply_workload_profile",
    "build_weighted_pool",
    "classify_query_kind",
    "resolve_industry_key",
    "sample_query",
    "translate_pool",
]
