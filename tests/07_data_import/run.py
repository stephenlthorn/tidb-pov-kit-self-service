#!/usr/bin/env python3
"""
Module 7 — Data Import Speed
Tests TiDB's bulk data ingestion throughput using:
  Method A: IMPORT INTO (TiDB native bulk loader — fastest path)
  Method B: LOAD DATA LOCAL INFILE (MySQL-compatible path)
  Method C: Batched INSERT (baseline reference)

Generates a local CSV of configurable size, then imports it three ways
and reports rows/sec and GB/min for each method.

Note: IMPORT INTO requires TiDB >= 7.2 and S3/GCS or a local file path
accessible to TiDB server.  The test gracefully falls back to LOAD DATA
or INSERT-only if IMPORT INTO is unavailable.
"""
from __future__ import annotations
import sys, os, time, csv, io, random, tempfile, math, re, json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.industry_profiles import resolve_industry_from_cfg
from lib.result_store import init_db, start_module, end_module, log_import_stat
from lib.db_utils import get_connection

MODULE    = "07_data_import"
DEFAULT_BATCH_SZ = 5_000   # rows per INSERT batch (Method C)

# Target import sizes — adjust via config test.import_rows
DEFAULT_ROWS = 1_000_000   # ~100 MB CSV with our schema


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    test_cfg = cfg.get("test", {})
    try:
        import_rows = int(test_cfg.get("import_rows", DEFAULT_ROWS))
    except (TypeError, ValueError):
        import_rows = DEFAULT_ROWS
    import_rows = max(10_000, import_rows)

    try:
        batch_size = max(100, int(test_cfg.get("import_batch_size", DEFAULT_BATCH_SZ)))
    except (TypeError, ValueError):
        batch_size = DEFAULT_BATCH_SZ
    methods = test_cfg.get("import_methods", ["batched_insert", "load_data_infile", "import_into"])
    if not isinstance(methods, list):
        methods = ["batched_insert", "load_data_infile", "import_into"]
    methods = [str(m).strip().lower() for m in methods]
    if not methods:
        methods = ["batched_insert", "load_data_infile", "import_into"]
    import_into_source_uri = str(test_cfg.get("import_into_source_uri", "") or "").strip()
    import_source_size_gb = float(test_cfg.get("import_source_size_gb", 0.0) or 0.0)
    try:
        import_into_threads = max(0, int(test_cfg.get("import_into_threads", 0) or 0))
    except (TypeError, ValueError):
        import_into_threads = 0

    print(f"\n{'='*60}")
    print(f"  Module 7: Data Import Speed")
    industry = resolve_industry_from_cfg(cfg)
    industry_key = str(industry.get("key", "general_auto"))
    print(f"  Industry: {industry.get('label', industry_key)}")
    print(f"  Target rows: {import_rows:,}")
    print(f"  Batched INSERT size: {batch_size:,}")
    print(f"  Enabled methods: {', '.join(methods)}")
    print(f"{'='*60}")

    if "import_into" in methods and (
        not import_into_source_uri or import_into_source_uri == "__AUTO_DATASET_OLTP__"
    ):
        auto_uri, auto_size, auto_note = _resolve_auto_import_source(cfg, industry_key)
        if auto_uri:
            import_into_source_uri = auto_uri
            if import_source_size_gb <= 0 and auto_size > 0:
                import_source_size_gb = auto_size
            print(f"  Auto import source ({auto_note}): {_redact_uri(import_into_source_uri)}")
        else:
            print("  Auto import source not resolved; falling back to local CSV for import methods.")

    conn = get_connection(cfg["tidb"])
    cur  = conn.cursor()

    need_local_csv = any(m in methods for m in ["batched_insert", "load_data_infile"])
    if "import_into" in methods and not import_into_source_uri:
        need_local_csv = True

    csv_path = None
    file_size_gb = import_source_size_gb
    if need_local_csv:
        print(f"\n  Generating {import_rows:,} row CSV...")
        csv_path, file_size_gb = _generate_csv(import_rows, industry_key)
        print(f"    CSV size: {file_size_gb*1024:.1f} MB at {csv_path}")
    elif import_into_source_uri:
        print(f"\n  Using remote import source URI: {import_into_source_uri}")
        if import_source_size_gb <= 0:
            print("    Note: import_source_size_gb not set; GB/min will be 0 for this method.")

    results = {}

    # ── Method C: Batched INSERT (always available, baseline) ─────────────────
    if "batched_insert" in methods:
        print(f"\n  Method C — Batched INSERT (batch={batch_size:,})...")
        _drop_and_create(cur, conn, f"import_test_insert_{industry_key}")
        t0 = time.time()
        if csv_path:
            rows_c = _batched_insert(cur, conn, f"import_test_insert_{industry_key}", csv_path, import_rows, batch_size)
            dur_c = time.time() - t0
            results["batched_insert"] = _metrics(rows_c, file_size_gb, dur_c)
            log_import_stat(rows_c, file_size_gb, dur_c,
                            file_size_gb / dur_c * 60 if dur_c > 0 else 0)
            _print_result("Batched INSERT", results["batched_insert"])
        else:
            results["batched_insert"] = {"skipped": True, "reason": "local CSV not generated"}
            print("    Skipped: local CSV not available.")
    else:
        print("\n  Method C — Batched INSERT skipped by config.")
        results["batched_insert"] = {"skipped": True, "reason": "disabled"}

    # ── Method B: LOAD DATA LOCAL INFILE ─────────────────────────────────────
    if "load_data_infile" in methods:
        print(f"\n  Method B — LOAD DATA LOCAL INFILE...")
        _drop_and_create(cur, conn, f"import_test_load_{industry_key}")
        try:
            t0 = time.time()
            if not csv_path:
                raise RuntimeError("local CSV not available")
            rows_b = _load_data_infile(cfg["tidb"], f"import_test_load_{industry_key}", csv_path)
            dur_b = time.time() - t0
            results["load_data_infile"] = _metrics(rows_b, file_size_gb, dur_b)
            log_import_stat(rows_b, file_size_gb, dur_b,
                            file_size_gb / dur_b * 60 if dur_b > 0 else 0)
            _print_result("LOAD DATA INFILE", results["load_data_infile"])
        except Exception as e:
            print(f"    Skipped: {e}")
            results["load_data_infile"] = {"skipped": True, "reason": str(e)}
    else:
        print("\n  Method B — LOAD DATA LOCAL INFILE skipped by config.")
        results["load_data_infile"] = {"skipped": True, "reason": "disabled"}

    # ── Method A: IMPORT INTO ─────────────────────────────────────────────────
    if "import_into" in methods:
        print(f"\n  Method A — IMPORT INTO (TiDB native loader)...")
        _drop_and_create(cur, conn, f"import_test_native_{industry_key}")
        source_uri = import_into_source_uri
        if not source_uri:
            source_uri = f"file://{csv_path}"
        try:
            t0 = time.time()
            rows_a = _import_into(
                cur,
                conn,
                f"import_test_native_{industry_key}",
                source_uri,
                cfg=cfg,
                import_threads=import_into_threads,
            )
            dur_a = time.time() - t0
            import_size_gb = max(file_size_gb, import_source_size_gb)
            results["import_into"] = _metrics(rows_a, import_size_gb, dur_a)
            log_import_stat(rows_a, import_size_gb, dur_a,
                            import_size_gb / dur_a * 60 if dur_a > 0 else 0)
            _print_result("IMPORT INTO", results["import_into"])
        except Exception as e:
            msg = str(e)
            if _is_s3_auth_required_error(msg) and source_uri.lower().startswith("s3://"):
                print("    IMPORT INTO missing S3 auth fields for TiDB; falling back to runner-side S3 download + LOAD DATA LOCAL INFILE.")
                t0 = time.time()
                rows_a = _load_data_infile_from_s3_fallback(
                    cfg,
                    f"import_test_native_{industry_key}",
                    source_uri,
                )
                dur_a = time.time() - t0
                import_size_gb = max(file_size_gb, import_source_size_gb)
                results["import_into"] = _metrics(rows_a, import_size_gb, dur_a)
                results["import_into"]["note"] = "fallback_load_data_local_infile"
                log_import_stat(rows_a, import_size_gb, dur_a,
                                import_size_gb / dur_a * 60 if dur_a > 0 else 0)
                _print_result("IMPORT INTO (fallback)", results["import_into"])
            else:
                print(f"    Skipped (requires TiDB >= 7.2 or accessible file path): {e}")
                results["import_into"] = {"skipped": True, "reason": msg}
    else:
        print("\n  Method A — IMPORT INTO skipped by config.")
        results["import_into"] = {"skipped": True, "reason": "disabled"}

    # Cleanup
    for t in [
        f"import_test_insert_{industry_key}",
        f"import_test_load_{industry_key}",
        f"import_test_native_{industry_key}",
    ]:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()
    if csv_path:
        try:
            os.unlink(csv_path)
        except Exception:
            pass

    # Best method
    best = max(
        [(k, v) for k, v in results.items() if not v.get("skipped")],
        key=lambda x: x[1].get("rows_per_sec", 0),
        default=(None, {}),
    )
    best_name, best_metrics = best

    end_module(MODULE, "passed",
               f"Best: {best_name} at {best_metrics.get('gbpm',0):.2f} GB/min")
    return {"methods": results, "best_method": best_name, "best_metrics": best_metrics}


# ── Import method implementations ─────────────────────────────────────────────

def _generate_csv(n: int, industry_key: str) -> tuple:
    """Write n rows to a temp CSV. Returns (path, size_gb)."""
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="tidb_import_")
    sources, evt_types = _import_value_sets(industry_key)
    with os.fdopen(fd, "w", newline="") as fh:
        w = csv.writer(fh)
        for _ in range(n):
            w.writerow([
                random.choice(sources),
                random.choice(evt_types),
                random.randint(1, 500_000),
                random.randint(1, 5_000_000),
            ])
    size_gb = os.path.getsize(path) / 1024**3
    return path, size_gb


def _drop_and_create(cur, conn, table: str):
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(f"""
        CREATE TABLE {table} (
            id         BIGINT AUTO_RANDOM PRIMARY KEY,
            source     VARCHAR(100),
            event_type VARCHAR(100),
            user_id    BIGINT,
            session_id BIGINT
        )
    """)
    conn.commit()


def _import_value_sets(industry_key: str) -> tuple[list, list]:
    mapping = {
        "banking": (["atm", "mobile", "branch", "api"], ["transfer", "payment", "withdrawal", "deposit"]),
        "healthcare": (["ehr", "portal", "lab", "api"], ["claim", "encounter", "lab_result", "prior_auth"]),
        "gaming": (["game_client", "matchmaker", "store", "api"], ["session", "purchase", "match", "reward"]),
        "retail_ecommerce": (["web", "mobile", "marketplace", "batch"], ["cart", "checkout", "order", "refund"]),
        "saas": (["workspace", "api", "scheduler", "billing"], ["api_call", "job_run", "sync", "invoice"]),
        "iot_telemetry": (["edge", "gateway", "collector", "api"], ["telemetry", "alert", "heartbeat", "status"]),
        "adtech": (["rtb", "exchange", "sdk", "api"], ["impression", "click", "conversion", "budget_update"]),
        "logistics": (["scanner", "fleet_app", "ops_console", "api"], ["pickup", "in_transit", "arrival", "delivery"]),
    }
    return mapping.get(industry_key, (["web", "mobile", "api", "batch"], ["event", "update", "action", "status"]))


def _batched_insert(cur, conn, table: str, csv_path: str, total: int, batch_size: int) -> int:
    inserted = 0
    batch    = []
    sql = (f"INSERT INTO {table} (source, event_type, user_id, session_id) "
           f"VALUES (%s, %s, %s, %s)")
    with open(csv_path) as fh:
        reader = csv.reader(fh)
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()
                inserted += len(batch)
                batch = []
                pct = inserted / total * 100
                print(f"    {inserted:,}/{total:,} ({pct:.0f}%)", end="\r")
    if batch:
        cur.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)
    print(f"    {inserted:,} rows inserted" + " " * 20)
    return inserted


def _load_data_infile(tidb_cfg: dict, table: str, csv_path: str) -> int:
    """Use LOAD DATA LOCAL INFILE — requires allow_local_infile=True."""
    import mysql.connector
    conn = mysql.connector.connect(
        host=tidb_cfg["host"],
        port=tidb_cfg.get("port", 4000),
        user=tidb_cfg["user"],
        password=tidb_cfg["password"],
        database=tidb_cfg.get("database", "pov_test"),
        allow_local_infile=True,
        ssl_disabled=not tidb_cfg.get("ssl", False),
    )
    cur = conn.cursor()
    sql = f"""
        LOAD DATA LOCAL INFILE '{csv_path}'
        INTO TABLE {table}
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        (source, event_type, user_id, session_id)
    """
    cur.execute(sql)
    conn.commit()
    rows = cur.rowcount
    conn.close()
    return rows


def _import_into(cur, conn, table: str, source_uri: str, cfg: dict, import_threads: int = 0) -> int:
    """
    Use TiDB IMPORT INTO statement (TiDB >= 7.2).
    Requires the CSV to be accessible from TiDB server or an S3/GCS URI.
    Falls back gracefully if not supported.
    """
    resolved_uri = _augment_s3_uri_auth(source_uri, cfg)
    active_threads = import_threads if import_threads > 0 else None
    while True:
        sql = (
            f"IMPORT INTO {table} (source, event_type, user_id, session_id) "
            f"FROM '{resolved_uri}' FORMAT 'CSV'"
        )
        if active_threads:
            sql += f" WITH thread={int(active_threads)}"
        try:
            cur.execute(sql)
            break
        except Exception as e:
            fallback = _derive_cpu_safe_threads(str(e))
            if fallback and (active_threads is None or fallback < active_threads):
                active_threads = fallback
                print(f"    IMPORT INTO retry with WITH thread={fallback} (cluster cpu guardrail detected).")
                continue
            raise
    # IMPORT INTO is async in some versions — poll for completion
    try:
        result = cur.fetchone()
    except Exception:
        result = None

    # Count rows actually imported
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    return row[0] if row else 0


def _derive_cpu_safe_threads(err_text: str) -> int | None:
    msg = str(err_text or "")
    m = re.search(r"task concurrency\((\d+)\)\s+larger than cpu count\((\d+)\)", msg, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        cpu = int(m.group(2))
    except (TypeError, ValueError):
        return None
    return max(1, cpu)


def _is_s3_auth_required_error(err_text: str) -> bool:
    msg = str(err_text or "")
    if re.search(r"access to the data source has been denied", msg, flags=re.IGNORECASE):
        return True
    if re.search(r"access\\s*key.*required", msg, flags=re.IGNORECASE):
        return True
    if re.search(r"role\\s*arn.*external\\s*id.*required", msg, flags=re.IGNORECASE):
        return True
    return False


def _load_data_infile_from_s3_fallback(cfg: dict, table: str, source_uri: str) -> int:
    import boto3
    from botocore.config import Config as BotoConfig

    region = (
        str((cfg.get("dataset_bootstrap") or {}).get("aws_region") or "").strip()
        or str(os.environ.get("POV_S3_REGION", "")).strip()
        or str(os.environ.get("AWS_REGION", "")).strip()
    )
    kwargs = {"config": BotoConfig(signature_version="s3v4")}
    if region:
        kwargs["region_name"] = region
    s3 = boto3.client("s3", **kwargs)

    uris = _expand_s3_source_uris(source_uri, s3)
    if not uris:
        raise RuntimeError(f"No S3 CSV objects matched source URI: {source_uri}")

    loaded = 0
    with tempfile.TemporaryDirectory(prefix="tidb_import_s3_fallback_") as tmp:
        for idx, uri in enumerate(uris):
            local_path = _download_s3_uri(uri, tmp, idx, s3)
            rows = _load_data_infile(cfg["tidb"], table, local_path)
            if rows and rows > 0:
                loaded += int(rows)

    if loaded > 0:
        return loaded
    conn = get_connection(cfg["tidb"])
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    conn.close()
    return int(row[0] if row else 0)


def _expand_s3_source_uris(source_uri: str, s3_client) -> list[str]:
    raw = str(source_uri or "").strip()
    parts = urlsplit(raw)
    bucket = str(parts.netloc or "").strip()
    key = str(parts.path or "").lstrip("/")
    if not bucket or not key:
        return []
    if "*" not in key:
        return [f"s3://{bucket}/{key}"]

    prefix = key.split("*", 1)[0]
    paginator = s3_client.get_paginator("list_objects_v2")
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []) or []:
            obj_key = str(item.get("Key") or "")
            if not obj_key.lower().endswith(".csv"):
                continue
            out.append(f"s3://{bucket}/{obj_key}")
    return sorted(out)


def _download_s3_uri(uri: str, tmp_dir: str, idx: int, s3_client) -> str:
    parts = urlsplit(str(uri or ""))
    bucket = str(parts.netloc or "").strip()
    key = str(parts.path or "").lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri}")
    out = os.path.join(tmp_dir, f"part_{idx:04d}.csv")
    s3_client.download_file(bucket, key, out)
    return out


def _resolve_auto_import_source(cfg: dict, industry_key: str) -> tuple[str, float, str]:
    ds_cfg = cfg.get("dataset_bootstrap") or {}
    bucket = str(ds_cfg.get("s3_bucket") or "").strip()
    prefix = str(ds_cfg.get("s3_prefix") or "tidb-pov/datasets").strip().strip("/")
    profile = str(ds_cfg.get("profile_key") or industry_key or "general_auto").strip().lower()
    if not profile:
        profile = "general_auto"

    manifest_uri = str(ds_cfg.get("manifest_uri") or "").strip()
    if not manifest_uri and bucket:
        manifest_uri = f"s3://{bucket}/{prefix}/manifest.json"

    manifest = _load_manifest_from_uri(manifest_uri, cfg)
    if manifest:
        datasets = manifest.get("datasets") if isinstance(manifest.get("datasets"), dict) else {}
        for key in [profile, industry_key, "general_auto"]:
            entry = datasets.get(key) if isinstance(datasets, dict) else None
            if not isinstance(entry, dict):
                continue
            oltp = entry.get("oltp") if isinstance(entry.get("oltp"), dict) else {}
            uris = oltp.get("uris") if isinstance(oltp.get("uris"), list) else []
            if not uris:
                continue
            first = str(uris[0]).split("?", 1)[0]
            base = first.rsplit("/", 1)[0] if "/" in first else first
            wildcard = f"{base}/*.csv"
            try:
                approx = float(oltp.get("approx_size_gb") or 0.0)
            except (TypeError, ValueError):
                approx = 0.0
            return wildcard, max(0.0, approx), f"manifest:{key}"

    if bucket:
        return f"s3://{bucket}/{prefix}/{profile}/oltp/oltp_part_*.csv", 0.0, "derived"
    return "", 0.0, "none"


def _load_manifest_from_uri(uri: str, cfg: dict) -> dict:
    uri = str(uri or "").strip()
    if not uri:
        return {}
    if not uri.lower().startswith("s3://"):
        if not os.path.exists(uri):
            return {}
        try:
            with open(uri, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except Exception:
        return {}

    parts = urlsplit(uri)
    bucket = str(parts.netloc or "").strip()
    key = str(parts.path or "").lstrip("/")
    if not bucket or not key:
        return {}

    region = (
        str((cfg.get("dataset_bootstrap") or {}).get("aws_region") or "").strip()
        or str(os.environ.get("AWS_REGION", "")).strip()
        or str(os.environ.get("POV_S3_REGION", "")).strip()
        or None
    )
    kwargs = {"config": BotoConfig(signature_version="s3v4")}
    if region:
        kwargs["region_name"] = region
    try:
        client = boto3.client("s3", **kwargs)
        obj = client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return {}


def _augment_s3_uri_auth(uri: str, cfg: dict) -> str:
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


# ── Utility ───────────────────────────────────────────────────────────────────

def _metrics(rows: int, gb: float, dur_sec: float) -> dict:
    return {
        "rows_imported":  rows,
        "gb_imported":    round(gb, 4),
        "duration_sec":   round(dur_sec, 1),
        "rows_per_sec":   round(rows / dur_sec, 0) if dur_sec > 0 else 0,
        "gbpm":           round(gb / dur_sec * 60, 3) if dur_sec > 0 else 0,
    }


def _print_result(label, m):
    if m.get("skipped"):
        return
    print(f"    {label}: {m['rows_imported']:,} rows in {m['duration_sec']:.1f}s "
          f"→ {m['rows_per_sec']:,.0f} rows/s | {m['gbpm']:.3f} GB/min")


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
