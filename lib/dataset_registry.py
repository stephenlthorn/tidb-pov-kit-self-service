"""Shared dataset bootstrap registry helpers.

This module resolves which S3 dataset profile should be used for PoV bootstrap
import and provides a lightweight manifest selection utility.
"""

from __future__ import annotations

from typing import Dict, List

from lib.industry_profiles import INDUSTRY_DEFAULT, INDUSTRY_KEYS, normalize_industry_key

DEFAULT_DATASET_PROFILE = INDUSTRY_DEFAULT


def normalize_dataset_profile_key(raw: str | None) -> str:
    value = str(raw or DEFAULT_DATASET_PROFILE).strip().lower()
    if value in INDUSTRY_KEYS:
        return value
    return DEFAULT_DATASET_PROFILE


def resolve_dataset_profile_from_cfg(cfg: Dict | None) -> str:
    cfg = cfg or {}
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    profile = ds_cfg.get("profile_key")
    if profile:
        return normalize_dataset_profile_key(profile)
    industry_cfg = cfg.get("industry") or {}
    return normalize_dataset_profile_key(industry_cfg.get("selected"))


def dataset_bootstrap_enabled(cfg: Dict | None) -> bool:
    cfg = cfg or {}
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    return bool(ds_cfg.get("enabled", False))


def dataset_bootstrap_required(cfg: Dict | None) -> bool:
    cfg = cfg or {}
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    return bool(ds_cfg.get("required", False))


def dataset_skip_synthetic_generation(cfg: Dict | None) -> bool:
    cfg = cfg or {}
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    return bool(ds_cfg.get("skip_synthetic_generation", False))


def resolve_manifest_entry(manifest: Dict, profile_key: str) -> Dict:
    """Return dataset entry from manifest for the requested profile key.

    Supported manifest layouts:
      1) {"datasets": {"banking": {...}, ...}}
      2) {"datasets": [{ "key": "banking", ...}, ...]}
    """
    profile = normalize_dataset_profile_key(profile_key)
    datasets = manifest.get("datasets") if isinstance(manifest, dict) else {}

    if isinstance(datasets, dict):
        entry = datasets.get(profile) or datasets.get(DEFAULT_DATASET_PROFILE)
        if isinstance(entry, dict):
            out = dict(entry)
            out.setdefault("key", profile if profile in datasets else DEFAULT_DATASET_PROFILE)
            return out
        return {}

    if isinstance(datasets, list):
        by_key = {}
        for row in datasets:
            if not isinstance(row, dict):
                continue
            key = normalize_dataset_profile_key(row.get("key"))
            by_key[key] = row
        entry = by_key.get(profile) or by_key.get(DEFAULT_DATASET_PROFILE)
        return dict(entry) if isinstance(entry, dict) else {}

    return {}


def as_csv_uris(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        s = value.strip()
        return [s] if s else []
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return out
    return []

