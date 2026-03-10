#!/usr/bin/env python3
"""Bootstrap TiDB with pre-staged OLTP/OLAP datasets from S3.

This script is designed for the first execution step in PoV runs:
  1) Resolve dataset profile (industry-aware).
  2) Load dataset manifest from S3/local JSON.
  3) Fast-load OLTP + OLAP seed tables via IMPORT INTO.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
from typing import Dict, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.dataset_registry import (  # noqa: E402
    as_csv_uris,
    dataset_bootstrap_enabled,
    dataset_bootstrap_required,
    resolve_dataset_profile_from_cfg,
    resolve_manifest_entry,
)
from lib.db_utils import create_database_if_missing, get_connection  # noqa: E402

RESULTS_PATH = os.path.join(os.path.dirname(__file__), "..", "results", "dataset_bootstrap.json")


def _parse_args():
    parser = argparse.ArgumentParser(description="Bootstrap PoV dataset from S3 manifest.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--strict", action="store_true", help="Fail hard on any bootstrap error.")
    return parser.parse_args()


def _is_s3_uri(uri: str) -> bool:
    return str(uri or "").strip().lower().startswith("s3://")


def _split_s3_uri(uri: str) -> Tuple[str, str]:
    raw = str(uri or "").strip()
    if not raw.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    body = raw[len("s3://") :]
    if "/" not in body:
        return body, ""
    bucket, key = body.split("/", 1)
    return bucket, key


def _manifest_uri(cfg: Dict) -> str:
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    explicit = str(ds_cfg.get("manifest_uri") or os.environ.get("POV_DATASET_MANIFEST_URI") or "").strip()
    if explicit:
        return explicit
    bucket = str(ds_cfg.get("s3_bucket") or os.environ.get("POV_DATASET_BUCKET") or "").strip()
    prefix = str(ds_cfg.get("s3_prefix") or os.environ.get("POV_DATASET_PREFIX") or "tidb-pov/datasets").strip().strip("/")
    if bucket:
        return f"s3://{bucket}/{prefix}/manifest.json"
    return ""


def _load_manifest(cfg: Dict) -> Dict:
    uri = _manifest_uri(cfg)
    if not uri:
        return {}

    if not _is_s3_uri(uri):
        if os.path.exists(uri):
            with open(uri) as f:
                return json.load(f)
        raise FileNotFoundError(f"Manifest file not found: {uri}")

    import boto3

    region = (
        str((cfg.get("dataset_bootstrap") or {}).get("aws_region") or "").strip()
        or str(os.environ.get("AWS_REGION", "")).strip()
        or str(os.environ.get("POV_S3_REGION", "")).strip()
        or None
    )
    bucket, key = _split_s3_uri(uri)
    kwargs = {}
    if region:
        kwargs["region_name"] = region
    client = boto3.client("s3", **kwargs)
    obj = client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_result(payload: Dict):
    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(payload, f, indent=2)


def _ensure_tables(cur, oltp_table: str, olap_table: str):
    cur.execute(f"DROP TABLE IF EXISTS `{oltp_table}`")
    cur.execute(
        f"""
        CREATE TABLE `{oltp_table}` (
            order_id BIGINT PRIMARY KEY NONCLUSTERED,
            customer_id BIGINT NOT NULL,
            account_id BIGINT NOT NULL,
            status VARCHAR(32) NOT NULL,
            amount DECIMAL(18,2) NOT NULL,
            currency CHAR(3) NOT NULL,
            created_at DATETIME(3) NOT NULL,
            updated_at DATETIME(3) NOT NULL,
            INDEX idx_customer_created(customer_id, created_at),
            INDEX idx_status_created(status, created_at)
        ) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4
        """
    )
    cur.execute(f"DROP TABLE IF EXISTS `{olap_table}`")
    cur.execute(
        f"""
        CREATE TABLE `{olap_table}` (
            event_ts DATETIME(3) NOT NULL,
            dimension_a VARCHAR(64) NOT NULL,
            dimension_b VARCHAR(64) NOT NULL,
            metric_value DOUBLE NOT NULL,
            session_count INT NOT NULL,
            revenue DECIMAL(18,2) NOT NULL,
            INDEX idx_event_ts(event_ts),
            INDEX idx_dims_ts(dimension_a, dimension_b, event_ts)
        ) SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4
        """
    )


def _run_import(cfg: Dict, table: str, columns: list[str], uris: list[str], label: str) -> Dict:
    conn = get_connection(cfg["tidb"])
    cur = conn.cursor()
    imported_uris = []
    start = time.time()
    effective_uris = [_augment_s3_uri_auth(uri, cfg) for uri in uris]
    for uri in effective_uris:
        sql = (
            f"IMPORT INTO `{table}` ({', '.join(columns)}) "
            f"FROM '{uri}' FORMAT 'CSV'"
        )
        cur.execute(sql)
        try:
            cur.fetchall()
        except Exception:
            pass
        imported_uris.append(_redact_uri(uri))
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    row = cur.fetchone()
    conn.close()
    elapsed = max(0.001, time.time() - start)
    count = int(row[0] if row else 0)
    return {
        "label": label,
        "table": table,
        "uris": imported_uris,
        "rows": count,
        "duration_sec": round(elapsed, 2),
        "rows_per_sec": round(count / elapsed, 2),
    }


def _enable_tiflash(cfg: Dict, table: str):
    conn = get_connection(cfg["tidb"])
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE `{table}` SET TIFLASH REPLICA 1")
    finally:
        conn.close()


def _augment_s3_uri_auth(uri: str, cfg: Dict) -> str:
    raw = str(uri or "").strip()
    if not raw.lower().startswith("s3://"):
        return raw
    parts = urlsplit(raw)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    if any(k in params for k in ("access-key", "secret-access-key", "role-arn")):
        return raw

    ds_cfg = cfg.get("dataset_bootstrap") or {}
    role_arn = str(ds_cfg.get("s3_role_arn") or os.environ.get("POV_DATASET_S3_ROLE_ARN") or "").strip()
    external_id = str(ds_cfg.get("s3_external_id") or os.environ.get("POV_DATASET_S3_EXTERNAL_ID") or "").strip()
    access_key = str(
        ds_cfg.get("s3_access_key_id")
        or os.environ.get("POV_DATASET_S3_ACCESS_KEY_ID")
        or os.environ.get("AWS_ACCESS_KEY_ID")
        or ""
    ).strip()
    secret_key = str(
        ds_cfg.get("s3_secret_access_key")
        or os.environ.get("POV_DATASET_S3_SECRET_ACCESS_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
        or ""
    ).strip()
    session_token = str(
        ds_cfg.get("s3_session_token")
        or os.environ.get("POV_DATASET_S3_SESSION_TOKEN")
        or os.environ.get("AWS_SESSION_TOKEN")
        or ""
    ).strip()

    if role_arn:
        params["role-arn"] = role_arn
        if external_id:
            params["external-id"] = external_id
    elif access_key and secret_key:
        params["access-key"] = access_key
        params["secret-access-key"] = secret_key
        if session_token:
            params["session-token"] = session_token
    else:
        return raw

    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))


def _redact_uri(uri: str) -> str:
    parts = urlsplit(str(uri or ""))
    if not parts.query:
        return str(uri or "")
    redacted = []
    for k, _v in parse_qsl(parts.query, keep_blank_values=True):
        redacted.append((k, "***"))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(redacted), parts.fragment))


def run(cfg: Dict, strict: bool = False) -> Dict:
    enabled = dataset_bootstrap_enabled(cfg)
    required = dataset_bootstrap_required(cfg) or strict
    if not enabled:
        out = {
            "status": "skipped",
            "reason": "dataset_bootstrap.enabled=false",
            "required": required,
        }
        _write_result(out)
        print("Dataset bootstrap skipped (disabled).")
        return out

    manifest = _load_manifest(cfg)
    profile = resolve_dataset_profile_from_cfg(cfg)
    entry = resolve_manifest_entry(manifest, profile)
    if not entry:
        msg = f"No dataset entry found for profile '{profile}'."
        out = {"status": "failed" if required else "skipped", "reason": msg, "required": required}
        _write_result(out)
        if required:
            raise RuntimeError(msg)
        print(f"Dataset bootstrap skipped: {msg}")
        return out

    oltp_cfg = entry.get("oltp", {}) if isinstance(entry.get("oltp"), dict) else {}
    olap_cfg = entry.get("olap", {}) if isinstance(entry.get("olap"), dict) else {}
    oltp_uris = as_csv_uris(oltp_cfg.get("uris"))
    olap_uris = as_csv_uris(olap_cfg.get("uris"))
    if not oltp_uris or not olap_uris:
        msg = "Manifest entry is missing oltp/olap URIs."
        out = {"status": "failed" if required else "skipped", "reason": msg, "required": required, "profile": profile}
        _write_result(out)
        if required:
            raise RuntimeError(msg)
        print(f"Dataset bootstrap skipped: {msg}")
        return out

    ds_cfg = cfg.get("dataset_bootstrap") or {}
    oltp_table = str(ds_cfg.get("oltp_table") or "poc_seed_oltp")
    olap_table = str(ds_cfg.get("olap_table") or "poc_seed_olap")

    try:
        create_database_if_missing(cfg["tidb"])
        conn = get_connection(cfg["tidb"])
        cur = conn.cursor()
        _ensure_tables(cur, oltp_table, olap_table)
        conn.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut_oltp = ex.submit(
                _run_import,
                cfg,
                oltp_table,
                ["order_id", "customer_id", "account_id", "status", "amount", "currency", "created_at", "updated_at"],
                oltp_uris,
                "oltp",
            )
            fut_olap = ex.submit(
                _run_import,
                cfg,
                olap_table,
                ["event_ts", "dimension_a", "dimension_b", "metric_value", "session_count", "revenue"],
                olap_uris,
                "olap",
            )
            oltp_res = fut_oltp.result()
            olap_res = fut_olap.result()

        tiflash_requested = bool(ds_cfg.get("enable_tiflash_for_olap", True))
        tiflash_error = ""
        if tiflash_requested:
            try:
                _enable_tiflash(cfg, olap_table)
            except Exception as e:
                tiflash_error = str(e)

        out = {
            "status": "passed",
            "required": required,
            "profile": profile,
            "manifest_source": _manifest_uri(cfg),
            "oltp": oltp_res,
            "olap": olap_res,
            "tiflash_requested": tiflash_requested,
            "tiflash_error": tiflash_error,
            "skip_synthetic_generation": bool(ds_cfg.get("skip_synthetic_generation", False)),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        _write_result(out)
        print(
            "Dataset bootstrap complete: "
            f"OLTP {oltp_res['rows']:,} rows @ {oltp_res['rows_per_sec']:.0f}/s, "
            f"OLAP {olap_res['rows']:,} rows @ {olap_res['rows_per_sec']:.0f}/s"
        )
        if tiflash_error:
            print(f"TiFlash replica request warning: {tiflash_error}")
        return out
    except Exception as e:
        out = {
            "status": "failed",
            "required": required,
            "profile": profile,
            "reason": str(e),
            "manifest_source": _manifest_uri(cfg),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        _write_result(out)
        if required:
            raise
        print(f"Dataset bootstrap warning: {e}")
        return out


def main():
    args = _parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f) or {}
    result = run(cfg, strict=args.strict)
    if result.get("status") == "failed" and result.get("required"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
