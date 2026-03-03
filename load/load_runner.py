"""
load_runner.py — Core load generator for the TiDB Cloud PoV Kit.

Runs a timed concurrent workload against TiDB (and optionally a comparison DB),
logging every transaction result to results/results.db.

Usage:
    from load.load_runner import LoadRunner
    runner = LoadRunner(tidb_cfg, counts, module="01_baseline_perf")
    runner.run(workload_pool, concurrency=64, duration_sec=300, phase="c64")
"""

import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from lib.db_utils import get_connection
from lib.result_store import log_results_batch

FLUSH_EVERY = 500   # rows buffered before batch insert to SQLite


class LoadRunner:
    def __init__(self, tidb_cfg: dict, counts: dict, module: str,
                 comparison_cfg: dict = None, comparison_label: str = "comparison"):
        self.tidb_cfg = tidb_cfg
        self.comparison_cfg = comparison_cfg
        self.comparison_label = comparison_label
        self.counts = counts
        self.module = module
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self, workload_pool: list, concurrency: int, duration_sec: int,
            phase: str = None, ramp_to: int = None, ramp_sec: int = None,
            customer_queries: list = None, customer_ratio: float = 0.3) -> dict:
        """
        Run the workload.

        ramp_to / ramp_sec: if set, linearly increase concurrency from current
        value to ramp_to over ramp_sec seconds. Used for elastic scale test.
        """
        self._stop_event.clear()
        print(f"\n  [{self.module}] phase={phase or '-'} | concurrency={concurrency} | "
              f"duration={duration_sec}s")

        # Merge customer queries into pool
        if customer_queries:
            from load.workload_definitions import build_weighted_pool
            cq_pool = [
                {"query_type": "customer_query", "sql": q,
                 "params_fn": lambda c: (), "weight": 10}
                for q in customer_queries
            ]
            n_cq = max(1, int(len(workload_pool) * customer_ratio))
            workload_pool = workload_pool + cq_pool * n_cq

        result_buf = []
        buf_lock = threading.Lock()
        flush_lock = threading.Lock()
        start_ts = time.time()
        end_ts = start_ts + duration_sec

        def flush_buffer(force=False):
            with buf_lock:
                if not result_buf:
                    return
                if not force and len(result_buf) < FLUSH_EVERY:
                    return
                rows = result_buf.copy()
                result_buf.clear()
            with flush_lock:
                log_results_batch(rows)

        def worker(db_label: str, cfg: dict):
            import random
            from load.workload_definitions import sample_query
            from lib.db_utils import execute_timed, get_connection

            conn = get_connection(cfg)
            cur = conn.cursor()
            local_buf = []

            while not self._stop_event.is_set() and time.time() < end_ts:
                sql, params_fn, qt = sample_query(workload_pool)
                try:
                    params = params_fn(self.counts)
                except Exception:
                    params = ()
                result = execute_timed(cur, sql, params)
                local_buf.append({
                    "module": self.module,
                    "phase": phase,
                    "db_label": db_label,
                    "ts": time.time(),
                    "query_type": qt,
                    "latency_ms": result["latency_ms"],
                    "success": int(result["success"]),
                    "retries": result.get("retries", 0),
                    "error": result.get("error"),
                })
                if len(local_buf) >= 100:
                    with buf_lock:
                        result_buf.extend(local_buf)
                    local_buf.clear()
                    flush_buffer()

            with buf_lock:
                result_buf.extend(local_buf)
            flush_buffer(force=True)
            try:
                conn.close()
            except Exception:
                pass

        # Launch workers
        futures = []
        with ThreadPoolExecutor(max_workers=concurrency * (2 if self.comparison_cfg else 1)) as ex:
            for _ in range(concurrency):
                futures.append(ex.submit(worker, "tidb", self.tidb_cfg))
            if self.comparison_cfg:
                for _ in range(concurrency):
                    futures.append(ex.submit(worker, self.comparison_label, self.comparison_cfg))

            # Progress ticker
            while time.time() < end_ts and not self._stop_event.is_set():
                elapsed = time.time() - start_ts
                remaining = max(0, duration_sec - elapsed)
                with buf_lock:
                    buffered = len(result_buf)
                print(f"    elapsed={elapsed:.0f}s remaining={remaining:.0f}s "
                      f"buffered={buffered}", end="\r")
                time.sleep(5)
                flush_buffer()

        self._stop_event.set()
        flush_buffer(force=True)
        print()

        # Return quick summary
        from lib.result_store import get_latency_stats
        stats = get_latency_stats(self.module, phase=phase)
        print(f"    TiDB: TPS={stats.get('tps',0)} p99={stats.get('p99_ms',0)}ms "
              f"count={stats.get('count',0)}")
        return stats


class RampRunner(LoadRunner):
    """Variant that linearly increases concurrency over time."""

    def run_ramp(self, workload_pool: list, start_concurrency: int,
                 end_concurrency: int, ramp_sec: int,
                 sustain_sec: int = 300, phase: str = "ramp") -> list:
        """
        Gradually adds threads over ramp_sec, then sustains at end_concurrency.
        Returns list of (elapsed_sec, active_threads) annotations.
        """
        self._stop_event.clear()
        annotations = []
        end_ts = time.time() + ramp_sec + sustain_sec

        # Use run() at increasing concurrency steps
        steps = 5
        step_dur = ramp_sec // steps
        for i in range(steps + 1):
            if self._stop_event.is_set():
                break
            c = start_concurrency + int((end_concurrency - start_concurrency) * i / steps)
            elapsed = time.time() - (end_ts - ramp_sec - sustain_sec)
            annotations.append({"elapsed_sec": elapsed, "concurrency": c, "event": "ramp_step"})
            step_phase = f"{phase}_step{i}"
            self.run(workload_pool, concurrency=c,
                     duration_sec=step_dur if i < steps else sustain_sec,
                     phase=step_phase)

        return annotations
