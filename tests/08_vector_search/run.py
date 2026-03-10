#!/usr/bin/env python3
"""
Module 8 — Vector Search (AI Track) — Optional
Tests TiDB's built-in vector search capability (TiDB >= 8.4 / TiDB Cloud).

Demonstrates:
  - Storing embedding vectors in a VECTOR column
  - Building a vector index (HNSW)
  - ANN (Approximate Nearest Neighbour) search with cosine / L2 distance
  - Combining vector similarity with SQL filters (hybrid search)
  - Measuring QPS and p99 latency for vector queries at various concurrencies

If TiDB vector support is unavailable, the module logs a skip and exits cleanly.
"""
import sys, os, time, random, math
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import yaml
from lib.result_store import init_db, start_module, end_module, log_result, get_latency_stats
from lib.db_utils import get_connection

MODULE        = "08_vector_search"
VECTOR_DIM    = 256          # embedding dimensions
DOC_COUNT     = 50_000       # documents to index
QUERY_COUNT   = 500          # number of ANN queries per concurrency level
CONCURRENCIES = [1, 4, 8, 16]


def run(cfg: dict):
    init_db()
    start_module(MODULE)

    # Check if vector module is requested in config
    if not cfg.get("modules", {}).get("vector_search", True):
        print("\n  Module 8: Vector Search — disabled in config, skipping.")
        end_module(MODULE, "skipped", "Disabled in config")
        return {"skipped": True, "reason": "disabled in config"}

    print(f"\n{'='*60}")
    print(f"  Module 8: Vector Search (AI Track)")
    print(f"  Vectors: {DOC_COUNT:,} × {VECTOR_DIM}d | Queries: {QUERY_COUNT}")
    print(f"{'='*60}")

    conn = get_connection(cfg["tidb"])
    cur  = conn.cursor()

    # ── Check vector support ───────────────────────────────────────────────────
    supported, note = _check_vector_support(cur)
    if not supported:
        print(f"\n  Vector search not available: {note}")
        print("  (Requires TiDB Cloud Serverless or TiDB >= 8.4 with TiFlash)")
        conn.close()
        end_module(MODULE, "skipped", f"Vector not supported: {note}")
        return {"skipped": True, "reason": note}

    print(f"\n  Vector support confirmed: {note}")

    # ── Setup: create table and index ─────────────────────────────────────────
    print(f"\n  Creating vector table ({DOC_COUNT:,} docs × {VECTOR_DIM}d)...")
    _setup_vector_table(cur, conn)

    # ── Insert synthetic embeddings ────────────────────────────────────────────
    print("  Inserting embeddings...")
    t0 = time.time()
    _insert_embeddings(cur, conn, DOC_COUNT, VECTOR_DIM)
    insert_sec = time.time() - t0
    print(f"    Inserted {DOC_COUNT:,} vectors in {insert_sec:.1f}s "
          f"({DOC_COUNT/insert_sec:.0f} vecs/s)")

    # ── Build HNSW index ──────────────────────────────────────────────────────
    print("  Building HNSW vector index...")
    t0 = time.time()
    try:
        cur.execute("""
            ALTER TABLE vector_docs
            ADD VECTOR INDEX vidx_cosine ((VEC_COSINE_DISTANCE(embedding)))
            USING HNSW
        """)
        conn.commit()
        index_sec = time.time() - t0
        print(f"    HNSW index built in {index_sec:.1f}s")
    except Exception as e:
        index_sec = 0
        print(f"    HNSW index (best-effort): {e}")

    # ── ANN search at various concurrencies ───────────────────────────────────
    concurrency_results = []
    for conc in CONCURRENCIES:
        print(f"\n  ANN search @ concurrency={conc}...")
        _run_ann_queries(cfg["tidb"], VECTOR_DIM, QUERY_COUNT, conc)
        stats = get_latency_stats(MODULE, phase=f"ann_conc{conc}")
        concurrency_results.append({
            "concurrency": conc,
            "stats": stats,
        })
        print(f"    p50={stats.get('p50_ms',0):.1f}ms  "
              f"p99={stats.get('p99_ms',0):.1f}ms  "
              f"QPS={stats.get('tps',0):.0f}")

    # ── Hybrid search (vector + SQL filter) ───────────────────────────────────
    print("\n  Hybrid search (vector similarity + SQL category filter)...")
    hybrid_stats = _run_hybrid_queries(cfg["tidb"], VECTOR_DIM, 100)
    print(f"    p99={hybrid_stats.get('p99_ms',0):.1f}ms  "
          f"QPS={hybrid_stats.get('tps',0):.0f}")

    # Cleanup
    cur.execute("DROP TABLE IF EXISTS vector_docs")
    conn.commit()
    conn.close()

    end_module(MODULE, "passed",
               f"ANN p99 @ conc=1: {concurrency_results[0]['stats'].get('p99_ms',0):.1f}ms")
    return {
        "doc_count":    DOC_COUNT,
        "vector_dim":   VECTOR_DIM,
        "insert_sec":   round(insert_sec, 1),
        "index_sec":    round(index_sec, 1),
        "ann_by_concurrency": concurrency_results,
        "hybrid_search": hybrid_stats,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _check_vector_support(cur) -> tuple:
    """Return (supported: bool, note: str)."""
    try:
        cur.execute("SELECT TIDB_VERSION()")
        row = cur.fetchone()
        version_str = row[0] if row else ""

        # Try creating a minimal vector column
        cur.execute("DROP TABLE IF EXISTS _vec_check")
        cur.execute("CREATE TABLE _vec_check (v VECTOR(4))")
        cur.execute("DROP TABLE IF EXISTS _vec_check")
        return True, version_str[:80]
    except Exception as e:
        return False, str(e)[:120]


def _setup_vector_table(cur, conn):
    cur.execute("DROP TABLE IF EXISTS vector_docs")
    cur.execute(f"""
        CREATE TABLE vector_docs (
            id        BIGINT AUTO_RANDOM PRIMARY KEY,
            title     VARCHAR(255),
            category  VARCHAR(50),
            embedding VECTOR({VECTOR_DIM}) NOT NULL,
            INDEX idx_category (category)
        )
    """)
    conn.commit()


def _random_vector(dim: int) -> str:
    """Return a normalised random vector as a TiDB VECTOR literal string."""
    v = [random.gauss(0, 1) for _ in range(dim)]
    norm = math.sqrt(sum(x*x for x in v)) or 1.0
    v = [x / norm for x in v]
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def _insert_embeddings(cur, conn, n: int, dim: int):
    categories = ["finance", "retail", "healthcare", "tech", "legal"]
    batch_size = 200
    sql = ("INSERT INTO vector_docs (title, category, embedding) "
           "VALUES (%s, %s, %s)")
    batch = []
    for i in range(n):
        batch.append((
            f"Document {i}",
            random.choice(categories),
            _random_vector(dim),
        ))
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            conn.commit()
            batch = []
    if batch:
        cur.executemany(sql, batch)
        conn.commit()


def _run_ann_queries(tidb_cfg, dim, query_count, concurrency):
    """Run ANN queries concurrently, log to results.db."""
    import concurrent.futures
    from lib.result_store import log_results_batch
    import time as _time

    phase = f"ann_conc{concurrency}"
    max_connect_retries = 3

    def worker(_):
        results = []
        per_worker = max(1, query_count // concurrency)
        conn = None
        cur = None
        last_err = None
        for _attempt in range(max_connect_retries):
            try:
                conn = get_connection(tidb_cfg)
                cur = conn.cursor()
                break
            except Exception as e:
                last_err = e
                _time.sleep(0.2)

        if conn is None or cur is None:
            err_msg = str(last_err)[:120] if last_err else "connection failed"
            now_ts = _time.time()
            for _ in range(per_worker):
                results.append({
                    "module": MODULE, "phase": phase, "db_label": "tidb",
                    "ts": now_ts, "query_type": "ann_cosine",
                    "latency_ms": 0.0, "success": 0,
                    "retries": max_connect_retries, "error": err_msg,
                })
            log_results_batch(results)
            return

        for _ in range(per_worker):
            qvec = _random_vector(dim)
            t0   = _time.perf_counter()
            try:
                cur.execute(f"""
                    SELECT id, title,
                           VEC_COSINE_DISTANCE(embedding, '{qvec}') AS dist
                    FROM vector_docs
                    ORDER BY dist
                    LIMIT 10
                """)
                cur.fetchall()
                lat_ms = (_time.perf_counter() - t0) * 1000
                results.append({
                    "module": MODULE, "phase": phase, "db_label": "tidb",
                    "ts": _time.time(), "query_type": "ann_cosine",
                    "latency_ms": lat_ms, "success": 1,
                    "retries": 0, "error": None,
                })
            except Exception as e:
                lat_ms = (_time.perf_counter() - t0) * 1000
                results.append({
                    "module": MODULE, "phase": phase, "db_label": "tidb",
                    "ts": _time.time(), "query_type": "ann_cosine",
                    "latency_ms": lat_ms, "success": 0,
                    "retries": 0, "error": str(e)[:120],
                })
        log_results_batch(results)
        try:
            conn.close()
        except Exception:
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        list(ex.map(worker, range(concurrency)))


def _run_hybrid_queries(tidb_cfg, dim, query_count) -> dict:
    """Vector similarity + SQL predicate filter (hybrid search)."""
    from lib.result_store import log_results_batch, get_latency_stats
    import time as _time

    categories = ["finance", "retail", "healthcare", "tech", "legal"]
    conn = get_connection(tidb_cfg)
    cur  = conn.cursor()
    results = []

    for _ in range(query_count):
        qvec = _random_vector(dim)
        cat  = random.choice(categories)
        t0   = _time.perf_counter()
        try:
            cur.execute(f"""
                SELECT id, title,
                       VEC_COSINE_DISTANCE(embedding, '{qvec}') AS dist
                FROM vector_docs
                WHERE category = %s
                ORDER BY dist
                LIMIT 5
            """, (cat,))
            cur.fetchall()
            lat_ms = (_time.perf_counter() - t0) * 1000
            results.append({
                "module": MODULE, "phase": "hybrid", "db_label": "tidb",
                "ts": _time.time(), "query_type": "ann_hybrid",
                "latency_ms": lat_ms, "success": 1,
                "retries": 0, "error": None,
            })
        except Exception as e:
            lat_ms = (_time.perf_counter() - t0) * 1000
            results.append({
                "module": MODULE, "phase": "hybrid", "db_label": "tidb",
                "ts": _time.time(), "query_type": "ann_hybrid",
                "latency_ms": lat_ms, "success": 0,
                "retries": 0, "error": str(e)[:120],
            })

    log_results_batch(results)
    conn.close()
    return get_latency_stats(MODULE, phase="hybrid")


if __name__ == "__main__":
    with open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml") as f:
        cfg = yaml.safe_load(f)
    run(cfg)
