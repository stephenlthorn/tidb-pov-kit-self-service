#!/usr/bin/env python3
"""Upload PoV run artifacts to S3 for durable access."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import socket
import urllib.parse
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError

DEFAULT_FILES = [
    "tidb_pov_report.pdf",
    "metrics_summary.json",
    "results.db",
    "web_ui_run.log",
    "pre_poc_checklist.md",
    "pre_poc_intake.json",
    "config.resolved.yaml",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload TiDB PoV result artifacts to S3")
    p.add_argument("--results-dir", default="results", help="Local results directory")
    p.add_argument("--runs-dir", default="runs", help="Local runs directory (optional)")
    p.add_argument("--bucket", default=os.environ.get("POV_S3_BUCKET") or os.environ.get("S3_BUCKET") or "")
    p.add_argument("--prefix", default=os.environ.get("POV_S3_PREFIX") or os.environ.get("S3_PREFIX") or "tidb-pov")
    p.add_argument("--project", default=os.environ.get("POV_S3_PROJECT") or os.environ.get("S3_ARTIFACTS_PROJECT") or "default")
    p.add_argument("--run-tag", default=os.environ.get("POV_RUN_TAG") or "")
    p.add_argument("--region", default=os.environ.get("POV_S3_REGION") or os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or "")
    p.add_argument(
        "--expected-bucket-owner",
        default=os.environ.get("POV_S3_EXPECTED_BUCKET_OWNER") or os.environ.get("S3_EXPECTED_BUCKET_OWNER") or "",
        help="Expected AWS account ID that owns the bucket",
    )
    p.add_argument(
        "--kms-key-id",
        default=os.environ.get("POV_S3_KMS_KEY_ID") or os.environ.get("S3_KMS_KEY_ID") or "",
        help="Optional KMS key ARN/ID for SSE-KMS",
    )
    p.add_argument(
        "--presign-seconds",
        type=int,
        default=int(
            os.environ.get("POV_S3_PRESIGN_SECONDS")
            or os.environ.get("S3_PRESIGN_SECONDS")
            or "604800"
        ),
        help="Signed URL TTL in seconds (default 7 days, set 0 to disable).",
    )
    p.add_argument("--check-only", action="store_true", help="Only validate S3 read/write access and exit")
    return p.parse_args()


def build_run_tag(given: str) -> str:
    if given:
        return given
    stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    host = socket.gethostname().split(".", 1)[0]
    safe_host = "".join(c if c.isalnum() or c in "-_" else "-" for c in host).strip("-") or "host"
    return f"{stamp}_{safe_host}"


def upload_file(
    s3,
    bucket: str,
    local_path: Path,
    key: str,
    expected_bucket_owner: str = "",
    kms_key_id: str = "",
) -> Dict:
    content_type = "application/octet-stream"
    if local_path.suffix.lower() == ".pdf":
        content_type = "application/pdf"
    elif local_path.suffix.lower() == ".json":
        content_type = "application/json"
    elif local_path.suffix.lower() in {".yaml", ".yml", ".md", ".log", ".txt"}:
        content_type = "text/plain"
    put_args = {
        "Bucket": bucket,
        "Key": key,
        "ContentType": content_type,
    }
    if expected_bucket_owner:
        put_args["ExpectedBucketOwner"] = expected_bucket_owner
    if kms_key_id:
        put_args["ServerSideEncryption"] = "aws:kms"
        put_args["SSEKMSKeyId"] = kms_key_id

    with local_path.open("rb") as f:
        s3.put_object(Body=f, **put_args)
    return {
        "local": str(local_path),
        "key": key,
        "s3_uri": f"s3://{bucket}/{key}",
        "size_bytes": local_path.stat().st_size,
    }


def build_download_url(s3, bucket: str, key: str, region: str, presign_seconds: int) -> str:
    if presign_seconds > 0:
        try:
            return s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=presign_seconds,
            )
        except Exception:
            pass
    key_enc = urllib.parse.quote(key, safe="/")
    reg = region or "us-east-1"
    return f"https://s3.console.aws.amazon.com/s3/object/{bucket}?region={reg}&prefix={key_enc}"


def probe_bucket_access(
    s3,
    bucket: str,
    prefix: str,
    project: str,
    expected_bucket_owner: str = "",
    kms_key_id: str = "",
) -> None:
    probe_key = (
        f"{prefix}/{project}/healthchecks/"
        f"probe_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{socket.gethostname()}.txt"
    )
    probe_body = f"tidb-pov-s3-probe {dt.datetime.utcnow().isoformat()}Z".encode("utf-8")
    put_args = {
        "Bucket": bucket,
        "Key": probe_key,
        "Body": probe_body,
        "ContentType": "text/plain",
    }
    if expected_bucket_owner:
        put_args["ExpectedBucketOwner"] = expected_bucket_owner
    if kms_key_id:
        put_args["ServerSideEncryption"] = "aws:kms"
        put_args["SSEKMSKeyId"] = kms_key_id

    s3.put_object(**put_args)

    get_args = {"Bucket": bucket, "Key": probe_key}
    if expected_bucket_owner:
        get_args["ExpectedBucketOwner"] = expected_bucket_owner
    got = s3.get_object(**get_args)
    body = got["Body"].read()
    if body != probe_body:
        raise RuntimeError("S3 read-back probe failed (content mismatch)")
    del_args = {"Bucket": bucket, "Key": probe_key}
    if expected_bucket_owner:
        del_args["ExpectedBucketOwner"] = expected_bucket_owner
    try:
        s3.delete_object(**del_args)
    except ClientError as e:
        # Deletion is optional; some least-privilege roles intentionally deny it.
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code not in {"AccessDenied", "UnauthorizedOperation"}:
            raise


def main() -> int:
    args = parse_args()
    if not args.bucket:
        print("[upload] missing --bucket (or POV_S3_BUCKET / S3_BUCKET env)")
        return 2

    results_dir = Path(args.results_dir).resolve()
    runs_dir = Path(args.runs_dir).resolve()
    run_tag = build_run_tag(args.run_tag)
    prefix = args.prefix.strip("/")
    project = args.project.strip("/")

    s3_kwargs = {}
    if args.region:
        s3_kwargs["region_name"] = args.region
    s3 = boto3.client("s3", **s3_kwargs)
    try:
        probe_bucket_access(
            s3,
            args.bucket,
            prefix,
            project,
            expected_bucket_owner=args.expected_bucket_owner,
            kms_key_id=args.kms_key_id,
        )
    except Exception as e:
        print(f"[upload] s3 probe failed: {e}")
        return 2

    if args.check_only:
        print(f"[upload] s3 probe ok for s3://{args.bucket}/{prefix}/{project}/healthchecks/")
        return 0

    uploaded: List[Dict] = []
    skipped: List[str] = []

    for name in DEFAULT_FILES:
        path = results_dir / name
        if not path.exists():
            skipped.append(str(path))
            continue
        key = f"{prefix}/{project}/runs/{run_tag}/results/{name}"
        row = upload_file(
            s3,
            args.bucket,
            path,
            key,
            expected_bucket_owner=args.expected_bucket_owner,
            kms_key_id=args.kms_key_id,
        )
        row["download_url"] = build_download_url(
            s3=s3,
            bucket=args.bucket,
            key=key,
            region=args.region,
            presign_seconds=args.presign_seconds,
        )
        uploaded.append(row)

    if runs_dir.exists() and runs_dir.is_dir():
        run_dirs = sorted([p for p in runs_dir.iterdir() if p.is_dir()])
        if run_dirs:
            latest = run_dirs[-1]
            for file_name in ("summary.json", "summary.md", "chart_data.json", "commands.json", "resolved_config.yaml", "validation.json"):
                p = latest / file_name
                if p.exists():
                    key = f"{prefix}/{project}/runs/{run_tag}/workload/{file_name}"
                    row = upload_file(
                        s3,
                        args.bucket,
                        p,
                        key,
                        expected_bucket_owner=args.expected_bucket_owner,
                        kms_key_id=args.kms_key_id,
                    )
                    row["download_url"] = build_download_url(
                        s3=s3,
                        bucket=args.bucket,
                        key=key,
                        region=args.region,
                        presign_seconds=args.presign_seconds,
                    )
                    uploaded.append(row)

    manifest_key = f"{prefix}/{project}/runs/{run_tag}/manifest.json"
    manifest = {
        "uploaded_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "bucket": args.bucket,
        "prefix": prefix,
        "project": project,
        "run_tag": run_tag,
        "presign_seconds": args.presign_seconds,
        "manifest_s3": f"s3://{args.bucket}/{manifest_key}",
        "manifest_download_url": build_download_url(
            s3=s3,
            bucket=args.bucket,
            key=manifest_key,
            region=args.region,
            presign_seconds=args.presign_seconds,
        ),
        "uploaded_count": len(uploaded),
        "uploaded": uploaded,
        "skipped": skipped,
    }

    manifest_path = results_dir / f"s3_upload_manifest_{run_tag}.json"
    results_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    upload_file(
        s3,
        args.bucket,
        manifest_path,
        manifest_key,
        expected_bucket_owner=args.expected_bucket_owner,
        kms_key_id=args.kms_key_id,
    )

    print(f"[upload] uploaded_count={len(uploaded)}")
    print(f"[upload] manifest={manifest_path}")
    print(f"[upload] manifest_s3=s3://{args.bucket}/{manifest_key}")
    report = next((r for r in uploaded if str(r.get("key", "")).endswith("/results/tidb_pov_report.pdf")), None)
    metrics = next((r for r in uploaded if str(r.get("key", "")).endswith("/results/metrics_summary.json")), None)
    if report:
        print(f"[upload] report_download={report.get('download_url')}")
    if metrics:
        print(f"[upload] metrics_download={metrics.get('download_url')}")
    print(f"[upload] manifest_download={manifest.get('manifest_download_url')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
