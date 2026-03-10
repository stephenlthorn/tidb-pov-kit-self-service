#!/usr/bin/env python3
"""Generate and publish pluggable PoV OLTP/OLAP seed datasets to S3.

Creates one manifest containing dataset packs for each supported industry.
Each pack includes:
  - OLTP CSV shard URIs
  - OLAP CSV shard URIs
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import tempfile
import time
from decimal import Decimal, ROUND_HALF_UP

import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.industry_profiles import INDUSTRY_KEYS, INDUSTRY_PROFILES  # noqa: E402


def _args():
    p = argparse.ArgumentParser(description="Publish PoV dataset packs to S3.")
    p.add_argument("--bucket", required=True, help="Destination S3 bucket.")
    p.add_argument("--prefix", default="tidb-pov/datasets", help="Destination prefix root.")
    p.add_argument("--region", default="", help="AWS region override.")
    p.add_argument(
        "--industries",
        default="all",
        help="Comma list of industries or 'all' (default).",
    )
    p.add_argument(
        "--target-gb-per-family",
        type=float,
        default=0.10,
        help="Approximate data size in GB for each family (oltp and olap) per industry.",
    )
    p.add_argument("--shards", type=int, default=8, help="CSV shard files per family.")
    p.add_argument("--seed", type=int, default=42, help="Random seed for deterministic output.")
    return p.parse_args()


def _selected_industries(raw: str) -> list[str]:
    value = str(raw or "all").strip().lower()
    if value == "all":
        return list(INDUSTRY_KEYS)
    wanted = []
    for token in value.split(","):
        key = token.strip().lower()
        if key in INDUSTRY_KEYS:
            wanted.append(key)
    return wanted or list(INDUSTRY_KEYS)


def _row_count(target_gb: float, approx_row_bytes: int, min_rows: int = 100_000) -> int:
    bytes_target = max(1, int(target_gb * (1024**3)))
    n = max(min_rows, bytes_target // max(1, approx_row_bytes))
    return int(n)


def _dt_text(epoch_base: int, i: int) -> str:
    ts = epoch_base + (i % 31_536_000)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))


def _gen_oltp_row(i: int, industry_key: str) -> list[str]:
    statuses = {
        "banking": ["posted", "pending", "reversed"],
        "healthcare": ["open", "adjudicated", "denied"],
        "gaming": ["paid", "pending", "refunded"],
        "retail_ecommerce": ["placed", "shipped", "returned"],
        "saas": ["active", "invoiced", "overdue"],
        "iot_telemetry": ["ingested", "queued", "dropped"],
        "adtech": ["won", "delivered", "invalid"],
        "logistics": ["in_transit", "arrived", "exception"],
        "general_auto": ["ok", "pending", "retry"],
    }
    currency_map = {
        "banking": "USD",
        "healthcare": "USD",
        "gaming": "USD",
        "retail_ecommerce": "USD",
        "saas": "USD",
        "iot_telemetry": "USD",
        "adtech": "USD",
        "logistics": "USD",
        "general_auto": "USD",
    }
    st = random.choice(statuses.get(industry_key, statuses["general_auto"]))
    amount = Decimal(random.uniform(1.0, 5000.0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return [
        str(i + 1),
        str(random.randint(1, 5_000_000)),
        str(random.randint(1, 5_000_000)),
        st,
        f"{amount}",
        currency_map.get(industry_key, "USD"),
        _dt_text(1_700_000_000, i),
        _dt_text(1_700_000_000, i + random.randint(1, 2000)),
    ]


def _gen_olap_row(i: int, industry_key: str) -> list[str]:
    dim_a_pool = {
        "banking": ["retail", "commercial", "wealth"],
        "healthcare": ["claims", "encounters", "pharmacy"],
        "gaming": ["pc", "console", "mobile"],
        "retail_ecommerce": ["checkout", "catalog", "fulfillment"],
        "saas": ["billing", "workspace", "integrations"],
        "iot_telemetry": ["edge", "gateway", "fleet"],
        "adtech": ["search", "social", "display"],
        "logistics": ["air", "ground", "ocean"],
        "general_auto": ["a", "b", "c"],
    }
    dim_b_pool = {
        "banking": ["us", "eu", "apac"],
        "healthcare": ["payer_a", "payer_b", "payer_c"],
        "gaming": ["ranked", "casual", "events"],
        "retail_ecommerce": ["new", "repeat", "vip"],
        "saas": ["free", "pro", "enterprise"],
        "iot_telemetry": ["ok", "warn", "critical"],
        "adtech": ["campaign_1", "campaign_2", "campaign_3"],
        "logistics": ["priority", "standard", "economy"],
        "general_auto": ["x", "y", "z"],
    }
    dim_a = random.choice(dim_a_pool.get(industry_key, dim_a_pool["general_auto"]))
    dim_b = random.choice(dim_b_pool.get(industry_key, dim_b_pool["general_auto"]))
    metric = Decimal(random.uniform(0.0, 1000.0)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    revenue = Decimal(random.uniform(0.0, 20000.0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return [
        _dt_text(1_700_000_000, i),
        dim_a,
        dim_b,
        f"{metric}",
        str(random.randint(1, 5000)),
        f"{revenue}",
    ]


def _write_family_csvs(base_dir: str, family: str, rows: int, shards: int, row_fn):
    os.makedirs(base_dir, exist_ok=True)
    shard_rows = max(1, rows // max(1, shards))
    outputs = []
    total_bytes = 0
    seq = 0
    for shard_idx in range(max(1, shards)):
        path = os.path.join(base_dir, f"{family}_part_{shard_idx:03d}.csv")
        rows_this = shard_rows if shard_idx < shards - 1 else (rows - seq)
        rows_this = max(0, rows_this)
        with open(path, "w", newline="") as fh:
            writer = csv.writer(fh)
            for _ in range(rows_this):
                writer.writerow(row_fn(seq))
                seq += 1
        sz = os.path.getsize(path)
        total_bytes += sz
        outputs.append((path, rows_this, sz))
    return outputs, total_bytes


def _upload_files(s3, bucket: str, prefix: str, files: list[tuple[str, int, int]]):
    uris = []
    for path, _rows, _sz in files:
        key = f"{prefix}/{os.path.basename(path)}"
        s3.upload_file(path, bucket, key)
        uris.append(f"s3://{bucket}/{key}")
    return uris


def main():
    args = _args()
    random.seed(args.seed)
    industries = _selected_industries(args.industries)
    import boto3
    from botocore.config import Config as BotoConfig

    kwargs = {}
    if args.region:
        kwargs["region_name"] = args.region
    kwargs["config"] = BotoConfig(signature_version="s3v4")
    s3 = boto3.client("s3", **kwargs)

    manifest = {
        "version": "1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target_gb_per_family": args.target_gb_per_family,
        "shards_per_family": args.shards,
        "datasets": {},
    }

    with tempfile.TemporaryDirectory(prefix="pov_dataset_publish_") as tmp:
        for industry in industries:
            label = INDUSTRY_PROFILES.get(industry, {}).get("label", industry)
            print(f"Publishing dataset pack for {industry} ({label})...")
            root_prefix = f"{args.prefix.strip().strip('/')}/{industry}"
            industry_dir = os.path.join(tmp, industry)
            oltp_dir = os.path.join(industry_dir, "oltp")
            olap_dir = os.path.join(industry_dir, "olap")

            oltp_rows = _row_count(args.target_gb_per_family, approx_row_bytes=100, min_rows=120_000)
            olap_rows = _row_count(args.target_gb_per_family, approx_row_bytes=80, min_rows=120_000)

            oltp_files, oltp_bytes = _write_family_csvs(
                oltp_dir,
                family="oltp",
                rows=oltp_rows,
                shards=args.shards,
                row_fn=lambda i, k=industry: _gen_oltp_row(i, k),
            )
            olap_files, olap_bytes = _write_family_csvs(
                olap_dir,
                family="olap",
                rows=olap_rows,
                shards=args.shards,
                row_fn=lambda i, k=industry: _gen_olap_row(i, k),
            )

            oltp_uris = _upload_files(s3, args.bucket, f"{root_prefix}/oltp", oltp_files)
            olap_uris = _upload_files(s3, args.bucket, f"{root_prefix}/olap", olap_files)

            manifest["datasets"][industry] = {
                "key": industry,
                "label": label,
                "oltp": {
                    "uris": oltp_uris,
                    "rows": int(oltp_rows),
                    "approx_size_gb": round(oltp_bytes / (1024**3), 4),
                },
                "olap": {
                    "uris": olap_uris,
                    "rows": int(olap_rows),
                    "approx_size_gb": round(olap_bytes / (1024**3), 4),
                },
            }
            print(
                f"  OLTP rows={oltp_rows:,} size={oltp_bytes / (1024**3):.3f}GB, "
                f"OLAP rows={olap_rows:,} size={olap_bytes / (1024**3):.3f}GB"
            )

        manifest_path = os.path.join(tmp, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        manifest_key = f"{args.prefix.strip().strip('/')}/manifest.json"
        s3.upload_file(manifest_path, args.bucket, manifest_key)
        print(f"Manifest uploaded: s3://{args.bucket}/{manifest_key}")


if __name__ == "__main__":
    main()
