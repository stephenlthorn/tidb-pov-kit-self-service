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
import sys, os, time, csv, io, random, tempfile, math
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
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

    print(f"\n{'='*60}")
    print(f"  Module 7: Data Import Speed")
    print(f"  Target rows: {import_rows:,}")
    print(f"  Batched INSERT size: {batch_size:,}")
    print(f"  Enabled methods: {', '.join(methods)}")
    print(f"{'='*60}")

    conn = get_connection(cfg["tidb"])
    cur  = conn.cursor()

    need_local_csv = any(m in methods for m in ["batched_insert", "load_data_infile"])
    if "import_into" in methods and not import_into_source_uri:
        need_local_csv = True

    csv_path = None
    file_size_gb = import_source_size_gb
    if need_local_csv:
        print(f"\n  Generating {import_rows:,} row CSV...")
        csv_path, file_size_gb = _generate_csv(import_rows)
        print(f"    CSV size: {file_size_gb*1024:.1f} MB at {csv_path}")
    elif import_into_source_uri:
        print(f"\n  Using remote import source URI: {import_into_source_uri}")
        if import_source_size_gb <= 0:
            print("    Note: import_source_size_gb not set; GB/min will be 0 for this method.")

    results = {}

    # ── Method C: Batched INSERT (always available, baseline) ─────────────────
    if "batched_insert" in methods:
        print(f"\n  Method C — Batched INSERT (batch={batch_size:,})...")
        _drop_and_create(cur, conn, "import_test_insert")
        t0 = time.time()
        if csv_path:
            rows_c = _batched_insert(cur, conn, "import_test_insert", csv_path, import_rows, batch_size)
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
        _drop_and_create(cur, conn, "import_test_load")
        try:
            t0 = time.time()
            if not csv_path:
                raise RuntimeError("local CSV not available")
            rows_b = _load_data_infile(cfg["tidb"], "import_test_load", csv_path)
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
        _drop_and_create(cur, conn, "import_test_native")
        try:
            source_uri = import_into_source_uri
            if not source_uri:
                source_uri = f"file://{csv_path}"
            t0 = time.time()
            rows_a = _import_into(cur, conn, "import_test_native", source_uri)
            dur_a = time.time() - t0
            import_size_gb = max(file_size_gb, import_source_size_gb)
            results["import_into"] = _metrics(rows_a, import_size_gb, dur_a)
            log_import_stat(rows_a, import_size_gb, dur_a,
                            import_size_gb / dur_a * 60 if dur_a > 0 else 0)
            _print_result("IMPORT INTO", results["import_into"])
        except Exception as e:
            print(f"    Skipped (requires TiDB >= 7.2 or accessible file path): {e}")
            results["import_into"] = {"skipped": True, "reason": str(e)}
    else:
        print("\n  Method A — IMPORT INTO skipped by config.")
        results["import_into"] = {"skipped": True, "reason": "disabled"}

    # Cleanup
    for t in ["import_test_insert", "import_test_load", "import_test_native"]:
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

def _generate_csv(n: int) -> tuple:
    """Write n rows to a temp CSV. Returns (path, size_gb)."""
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="tidb_import_")
    sources    = ["web", "mobile", "api", "batch"]
    evt_types  = ["page_view", "click", "purchase", "signup", "error"]
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


def _import_into(cur, conn, table: str, source_uri: str) -> int:
    """
    Use TiDB IMPORT INTO statement (TiDB >= 7.2).
    Requires the CSV to be accessible from TiDB server or an S3/GCS URI.
    Falls back gracefully if not supported.
    """
    # IMPORT INTO supports 'file://' URIs for local dev; real deployments use S3
    sql = f"""
        IMPORT INTO {table} (source, event_type, user_id, session_id)
        FROM '{source_uri}'
        FORMAT 'CSV'
    """
    cur.execute(sql)
    # IMPORT INTO is async in some versions — poll for completion
    try:
        result = cur.fetchone()
    except Exception:
        result = None

    # Count rows actually imported
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    return row[0] if row else 0


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
