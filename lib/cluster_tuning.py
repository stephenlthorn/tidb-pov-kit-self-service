"""
cluster_tuning.py — TiDB cluster tuning profiles for PoV Kit.

Applies global and per-module session variables based on the PoC Configuration
template. All tuning is SQL-level only (SET GLOBAL / SET SESSION). TiKV, PD,
and TiFlash server-config changes require support tickets for managed tiers.

Reference: PoC Configuration.docx.md (Kernel team template)
"""
from __future__ import annotations

import contextlib
from typing import Dict, Generator, List, Optional, Tuple


# ─── Global tuning (applied once at PoV start) ──────────────────────────────

GLOBAL_SYSTEM_VARIABLES: List[Tuple[str, str]] = [
    ("tidb_prepared_plan_cache_size", "200"),
    ("tidb_enable_non_prepared_plan_cache", "ON"),
    ("tidb_ignore_prepared_cache_close_stmt", "ON"),
    ("tidb_enable_inl_join_inner_multi_pattern", "ON"),
    ("tidb_opt_derive_topn", "ON"),
    ("tidb_opt_ordering_index_selectivity_threshold", "0.0001"),
    ("tidb_runtime_filter_mode", "LOCAL"),
    ("tidb_opt_enable_mpp_shared_cte_execution", "ON"),
    ("tidb_rc_read_check_ts", "ON"),
    ("tidb_guarantee_linearizability", "OFF"),
    ("tidb_enable_historical_stats", "OFF"),
    ("tidb_analyze_skip_column_types", "json,blob,mediumblob,longblob,mediumtext,longtext"),
    ("tidb_opt_prefer_range_scan", "ON"),
]

GLOBAL_FIX_CONTROL = "44262:ON,44389:ON,44823:2000000,44830:ON,44855:ON,45132:10,52869:ON"

# Instance plan cache — only available on Dedicated/BYOC (v8.4.0+)
INSTANCE_PLAN_CACHE_TIERS = {"dedicated", "byoc"}
INSTANCE_PLAN_CACHE_VARS: List[Tuple[str, str]] = [
    ("tidb_enable_instance_plan_cache", "ON"),
    ("tidb_instance_plan_cache_max_size", "2GiB"),
]


# ─── Per-module session tuning ───────────────────────────────────────────────

MODULE_SESSION_PROFILES: Dict[str, Dict[str, str]] = {
    "01_baseline_perf": {
        "tidb_max_chunk_size": "128",
    },
    "01b_user_growth": {
        "tidb_max_chunk_size": "128",
    },
    "02_elastic_scale": {
        "tidb_max_chunk_size": "128",
    },
    "03_high_availability": {
        "tidb_max_chunk_size": "128",
    },
    "03b_write_contention": {
        "tidb_max_chunk_size": "128",
        "tidb_txn_mode": "pessimistic",
    },
    "04_htap_concurrent": {
        "tidb_max_chunk_size": "4096",
        "tidb_opt_agg_push_down": "ON",
        "tidb_opt_distinct_agg_push_down": "ON",
    },
    "05_online_ddl": {
        "tidb_max_chunk_size": "128",
    },
    "07_data_import": {
        "tidb_dml_type": "bulk",
    },
    "08_vector_search": {
        "tidb_max_chunk_size": "128",
    },
}


def apply_global_tuning(conn, tier: str = "serverless") -> Dict[str, str]:
    """Apply global system variables and optimizer fix controls.

    Returns a dict of {variable: status} where status is 'ok' or error message.
    Failures are logged but never block the PoV run.
    """
    tier = (tier or "serverless").strip().lower()
    results: Dict[str, str] = {}
    cur = conn.cursor()

    for var, val in GLOBAL_SYSTEM_VARIABLES:
        try:
            cur.execute(f"SET GLOBAL {var} = %s", (val,))
            results[var] = "ok"
        except Exception as e:
            results[var] = f"skipped: {e}"

    try:
        cur.execute("SET GLOBAL tidb_opt_fix_control = %s", (GLOBAL_FIX_CONTROL,))
        results["tidb_opt_fix_control"] = "ok"
    except Exception as e:
        results["tidb_opt_fix_control"] = f"skipped: {e}"

    if tier in INSTANCE_PLAN_CACHE_TIERS:
        for var, val in INSTANCE_PLAN_CACHE_VARS:
            try:
                cur.execute(f"SET GLOBAL {var} = %s", (val,))
                results[var] = "ok"
            except Exception as e:
                results[var] = f"skipped: {e}"

    cur.close()
    return results


@contextlib.contextmanager
def module_tuning(conn, module: str) -> Generator[None, None, None]:
    """Context manager that applies per-module session variables and reverts on exit.

    Usage:
        with module_tuning(conn, "04_htap_concurrent"):
            # run HTAP workload with OLAP-optimized session vars
        # session vars reverted automatically
    """
    profile = MODULE_SESSION_PROFILES.get(module, {})
    if not profile:
        yield
        return

    cur = conn.cursor()
    original_values: Dict[str, Optional[str]] = {}

    for var, val in profile.items():
        try:
            cur.execute(f"SELECT @@SESSION.{var}")
            row = cur.fetchone()
            original_values[var] = str(row[0]) if row else None
        except Exception:
            original_values[var] = None

        try:
            cur.execute(f"SET SESSION {var} = %s", (val,))
        except Exception:
            pass

    cur.close()

    try:
        yield
    finally:
        revert_cur = conn.cursor()
        for var, orig in original_values.items():
            if orig is not None:
                try:
                    revert_cur.execute(f"SET SESSION {var} = %s", (orig,))
                except Exception:
                    pass
        revert_cur.close()


def apply_module_session_vars(conn, module: str) -> Dict[str, str]:
    """Apply per-module session variables without automatic revert.

    Use this when the connection is created per-module and discarded after.
    Returns a dict of {variable: status}.
    """
    profile = MODULE_SESSION_PROFILES.get(module, {})
    if not profile:
        return {}

    results: Dict[str, str] = {}
    cur = conn.cursor()
    for var, val in profile.items():
        try:
            cur.execute(f"SET SESSION {var} = %s", (val,))
            results[var] = "ok"
        except Exception as e:
            results[var] = f"skipped: {e}"
    cur.close()
    return results


def print_tuning_report(results: Dict[str, str], label: str = "Global tuning") -> None:
    """Print a formatted tuning application report."""
    applied = sum(1 for v in results.values() if v == "ok")
    skipped = len(results) - applied
    print(f"\n  {label}: {applied} applied, {skipped} skipped")
    for var, status in results.items():
        marker = "+" if status == "ok" else "-"
        print(f"    [{marker}] {var} = {status}")


# ─── Tier / load-size validation ─────────────────────────────────────────────

TIER_LOAD_SIZE_LIMITS: Dict[str, List[str]] = {
    "starter": ["small"],
    "serverless": ["small"],
    "essential": ["small", "medium", "large"],
    "premium": ["small", "medium", "large"],
    "dedicated": ["small", "medium", "large"],
    "byoc": ["small", "medium", "large"],
}

TIER_WARNINGS: Dict[str, str] = {
    "starter": (
        "WARNING: Starter (free) tier has limited RUs and 5GB storage.\n"
        "  Running 'medium' or 'large' load sizes WILL exceed free-tier limits.\n"
        "  You must add a payment method on TiDB Cloud or the PoV will fail.\n"
        "  Use --force to continue anyway, or switch to load_size: small."
    ),
    "serverless": (
        "WARNING: Serverless tier has limited RUs and 5GB storage on the free plan.\n"
        "  Running 'medium' or 'large' load sizes may exceed free-tier limits.\n"
        "  Ensure your cluster has sufficient RU budget or the PoV may fail.\n"
        "  Use --force to continue anyway, or switch to load_size: small."
    ),
}


def validate_tier_load_size(tier: str, load_size: str) -> Tuple[bool, str]:
    """Check if the load_size is valid for the given tier.

    Returns (ok, message). If ok is False, the message explains why.
    """
    tier = (tier or "serverless").strip().lower()
    load_size = (load_size or "small").strip().lower()

    allowed = TIER_LOAD_SIZE_LIMITS.get(tier, ["small", "medium", "large"])
    if load_size in allowed:
        return True, ""

    warning = TIER_WARNINGS.get(tier, "")
    if warning:
        return False, warning

    return False, (
        f"Load size '{load_size}' is not recommended for tier '{tier}'.\n"
        f"  Allowed sizes for {tier}: {', '.join(allowed)}\n"
        f"  Use --force to override."
    )
