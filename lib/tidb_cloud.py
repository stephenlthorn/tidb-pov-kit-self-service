"""
tidb_cloud.py - TiDB Cloud specific validation helpers.
"""

from __future__ import annotations

from typing import Dict

PREFIX_REQUIRED_TIERS = {"starter", "serverless", "essential", "premium"}


def normalize_tier(raw_tier: str | None) -> str:
    return str(raw_tier or "").strip().lower()


def is_tidb_cloud_host(host: str | None) -> bool:
    h = str(host or "").strip().lower()
    if not h:
        return False
    return h.endswith("tidbcloud.com") or ".tidbcloud.com" in h


def requires_username_prefix(host: str | None, tier: str | None = None) -> bool:
    if not is_tidb_cloud_host(host):
        return False
    t = normalize_tier(tier)
    if not t:
        return True
    if t in {"dedicated", "byoc"}:
        return False
    return t in PREFIX_REQUIRED_TIERS


def has_prefixed_username(user: str | None) -> bool:
    u = str(user or "").strip()
    if "." not in u:
        return False
    if u.startswith(".") or u.endswith("."):
        return False
    return True


def validate_tidb_cloud_username(tidb_cfg: Dict, tier: str | None = None) -> str | None:
    """
    Return a validation message when TiDB Cloud username formatting is likely wrong.
    Returns None when the username format looks acceptable for this host/tier.
    """
    host = str((tidb_cfg or {}).get("host") or "").strip()
    user = str((tidb_cfg or {}).get("user") or "").strip()

    if not host or not user:
        return None
    if not requires_username_prefix(host, tier=tier):
        return None
    if has_prefixed_username(user):
        return None
    return (
        "TiDB Cloud username format looks invalid for this tier. "
        "Use the exact TiDB Cloud username with prefix, for example: <prefix>.root"
    )

