#!/usr/bin/env python3
"""Workload Generator controller utilities.

This module wraps TiUP Bench command planning/execution for the web UI Workload Lab.
It focuses on high-throughput rawsql first, with tpcc/ycsb support as additional modes.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote, unquote, urlparse

import yaml

ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = ROOT / "runs"
LAST_RUN_FILE = ROOT / "results" / "blaster_last_run.txt"

MODES = ("rawsql", "tpcc", "ycsb")
TXN_MODES = ("autocommit", "explicit_txn")
YCSB_WORKLOADS = ("A", "B", "C", "D", "F")


def _to_int(value, default: int, minimum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = int(default)
    if minimum is not None:
        out = max(minimum, out)
    return out


def _to_float(value, default: float, minimum: float | None = None) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        out = float(default)
    if minimum is not None:
        out = max(minimum, out)
    return out


def _split_csv(raw: str | Iterable[str] | None) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        parts = [str(v).strip() for v in raw]
    else:
        parts = re.split(r"[\n,]", str(raw))
    return [p for p in (part.strip() for part in parts) if p]


def _slug(value: str, default: str = "run") -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip()).strip("-_.")
    return s[:60] if s else default


def _shell_join(parts: List[str]) -> str:
    return " ".join(shlex.quote(str(p)) for p in parts)


def _safe_host_label(host: str) -> str:
    return _slug(host.replace("@", "_at_"), "host")


def _is_local_host(host: str) -> bool:
    h = host.strip().lower()
    if h in {"", "localhost", "127.0.0.1", "::1"}:
        return True
    try:
        local = socket.gethostname().lower()
        fqdn = socket.getfqdn().lower()
        return h in {local, fqdn}
    except Exception:
        return False


def _host_ref(host: str, default_user: str) -> Tuple[str, str]:
    h = str(host or "").strip()
    if "@" in h:
        user, addr = h.split("@", 1)
        return user.strip() or default_user, addr.strip()
    return default_user, h


def _distribution(total: int, slots: int) -> List[int]:
    total = max(0, int(total))
    slots = max(1, int(slots))
    base = total // slots
    rem = total % slots
    return [base + (1 if i < rem else 0) for i in range(slots)]


def parse_tidb_dsn(dsn: str) -> Dict:
    raw = str(dsn or "").strip()
    if not raw:
        raise ValueError("TiDB DSN is empty.")
    parsed = urlparse(raw)
    if parsed.scheme not in {"mysql", "tidb", "postgres", "postgresql"}:
        raise ValueError(f"Unsupported DSN scheme: {parsed.scheme}")
    if not parsed.hostname:
        raise ValueError("TiDB DSN is missing host.")

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    database = (parsed.path or "/test").lstrip("/") or "test"
    port = int(parsed.port or (5432 if parsed.scheme.startswith("postgres") else 4000))

    return {
        "scheme": parsed.scheme,
        "host": parsed.hostname,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }


def dsn_from_tidb_cfg(tidb_cfg: Dict) -> str:
    host = str((tidb_cfg or {}).get("host") or "").strip()
    if not host:
        return ""
    port = _to_int((tidb_cfg or {}).get("port"), 4000, 1)
    user = str((tidb_cfg or {}).get("user") or "root")
    password = str((tidb_cfg or {}).get("password") or "")
    database = str((tidb_cfg or {}).get("database") or "test")
    user_enc = quote(user, safe="")
    pass_enc = quote(password, safe="")
    auth = user_enc if not password else f"{user_enc}:{pass_enc}"
    return f"mysql://{auth}@{host}:{port}/{database}"


def default_blaster_config(tidb_cfg: Dict | None = None) -> Dict:
    rawsql_file = ROOT / "load" / "sql" / "rawsql_mix.sql"
    return {
        "mode": "rawsql",
        "tag": "poc",
        "cluster": {
            "tidb_dsn": dsn_from_tidb_cfg(tidb_cfg or {}),
            "tidb_hosts": [],
        },
        "loadgen": {
            "hosts": ["localhost"],
            "ssh_user": "",
            "ssh_key_path": "",
            "max_domains_concurrent": 8,
        },
        "rawsql": {
            "sql_file": str(rawsql_file),
            "duration_sec": 120,
            "warmup_sec": 15,
            "cooldown_sec": 10,
            "threads_total": 64,
            "connections_total": 128,
            "qps_target": 0,
            "statement_mix": "",
            "txn_mode": "autocommit",
        },
        "tpcc": {
            "warehouses": 100,
            "threads_total": 128,
            "duration_sec": 180,
        },
        "ycsb": {
            "workload": "A",
            "recordcount": 1000000,
            "operationcount": 5000000,
            "threads_total": 128,
        },
    }


def normalize_blaster_config(raw_cfg: Dict | None, tidb_cfg: Dict | None = None, mode: str | None = None, tag: str | None = None) -> Dict:
    base = default_blaster_config(tidb_cfg or {})
    cfg = dict(raw_cfg or {})

    # shallow/deep merge for top-level sections
    for section in ("cluster", "loadgen", "rawsql", "tpcc", "ycsb"):
        incoming = cfg.get(section)
        if isinstance(incoming, dict):
            base[section].update(incoming)

    if "mode" in cfg:
        base["mode"] = str(cfg.get("mode") or "rawsql").strip().lower()
    if "tag" in cfg:
        base["tag"] = str(cfg.get("tag") or "poc").strip()

    if mode:
        base["mode"] = str(mode).strip().lower()
    if tag:
        base["tag"] = str(tag).strip() or base["tag"]

    if base["mode"] not in MODES:
        base["mode"] = "rawsql"

    if not base["cluster"].get("tidb_dsn"):
        base["cluster"]["tidb_dsn"] = dsn_from_tidb_cfg(tidb_cfg or {})

    hosts = _split_csv(base["loadgen"].get("hosts"))
    base["loadgen"]["hosts"] = hosts or ["localhost"]
    base["loadgen"]["ssh_user"] = str(base["loadgen"].get("ssh_user") or "").strip()
    base["loadgen"]["ssh_key_path"] = str(base["loadgen"].get("ssh_key_path") or "").strip()
    base["loadgen"]["max_domains_concurrent"] = _to_int(base["loadgen"].get("max_domains_concurrent"), 8, 1)

    r = base["rawsql"]
    r["sql_file"] = str(r.get("sql_file") or "").strip()
    r["duration_sec"] = _to_int(r.get("duration_sec"), 120, 10)
    r["warmup_sec"] = _to_int(r.get("warmup_sec"), 15, 0)
    r["cooldown_sec"] = _to_int(r.get("cooldown_sec"), 10, 0)
    r["threads_total"] = _to_int(r.get("threads_total"), 64, 1)
    r["connections_total"] = _to_int(r.get("connections_total"), max(2, r["threads_total"]), 1)
    r["qps_target"] = _to_int(r.get("qps_target"), 0, 0)
    r["statement_mix"] = str(r.get("statement_mix") or "").strip()
    r["txn_mode"] = str(r.get("txn_mode") or "autocommit").strip().lower()
    if r["txn_mode"] not in TXN_MODES:
        r["txn_mode"] = "autocommit"

    t = base["tpcc"]
    t["warehouses"] = _to_int(t.get("warehouses"), 100, 1)
    t["threads_total"] = _to_int(t.get("threads_total"), 128, 1)
    t["duration_sec"] = _to_int(t.get("duration_sec"), 180, 10)

    y = base["ycsb"]
    y["workload"] = str(y.get("workload") or "A").strip().upper()
    if y["workload"] not in YCSB_WORKLOADS:
        y["workload"] = "A"
    y["recordcount"] = _to_int(y.get("recordcount"), 1_000_000, 1)
    y["operationcount"] = _to_int(y.get("operationcount"), 5_000_000, 1)
    y["threads_total"] = _to_int(y.get("threads_total"), 128, 1)

    return base


def create_run_dir(mode: str, tag: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / f"{stamp}_{_slug(mode, 'rawsql')}_{_slug(tag, 'run')}"
    (run_dir / "loadgens").mkdir(parents=True, exist_ok=True)
    return run_dir


def write_run_metadata(run_dir: Path, resolved_cfg: Dict, commands: List[Dict], validation: Dict | None = None) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "resolved_config.yaml").write_text(yaml.safe_dump(resolved_cfg, sort_keys=False), encoding="utf-8")
    (run_dir / "commands.json").write_text(json.dumps(commands, indent=2), encoding="utf-8")
    if validation is not None:
        (run_dir / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")


def _scan_json_metrics(output: str) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    def walk(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = str(k).lower()
                if isinstance(v, (int, float)):
                    if "qps" in key:
                        metrics["qps"] = float(v)
                    if "tps" in key:
                        metrics["tps"] = float(v)
                    if "p95" in key and "lat" in key:
                        metrics["p95_ms"] = float(v)
                    if "p99" in key and "lat" in key:
                        metrics["p99_ms"] = float(v)
                    if "error" in key and "rate" in key:
                        metrics["error_rate"] = float(v)
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except Exception:
            continue
        walk(parsed)

    return metrics


def parse_metrics_from_output(output: str) -> Dict[str, float]:
    metrics = _scan_json_metrics(output)

    regexes = {
        "qps": [r"(?i)\bqps\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", r"(?i)([0-9]+(?:\.[0-9]+)?)\s*qps"],
        "tps": [r"(?i)\btps\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)", r"(?i)([0-9]+(?:\.[0-9]+)?)\s*tps"],
        "p95_ms": [r"(?i)\bp95\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)"],
        "p99_ms": [r"(?i)\bp99\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)"],
        "error_rate": [r"(?i)\berror(?:\s+rate)?\b\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)"],
    }

    for key, pats in regexes.items():
        if key in metrics:
            continue
        for pat in pats:
            m = re.search(pat, output)
            if not m:
                continue
            try:
                metrics[key] = float(m.group(1))
                break
            except Exception:
                continue

    return metrics


def _ssh_prefix(ssh_key_path: str) -> List[str]:
    out = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
    if ssh_key_path:
        out.extend(["-i", ssh_key_path])
    return out


def _scp_prefix(ssh_key_path: str) -> List[str]:
    out = ["scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
    if ssh_key_path:
        out.extend(["-i", ssh_key_path])
    return out


def _render_rawsql_command(dsn: Dict, mode_cfg: Dict, threads: int, sql_file: str) -> List[str]:
    cmd = [
        "tiup",
        "bench",
        "rawsql",
        "run",
        "-d",
        "mysql",
        "-H",
        str(dsn["host"]),
        "-P",
        str(dsn["port"]),
        "-U",
        str(dsn["user"]),
        "-p",
        str(dsn["password"]),
        "-D",
        str(dsn["database"]),
        "-T",
        str(max(1, threads)),
        "--time",
        f"{_to_int(mode_cfg.get('duration_sec'), 120, 10)}s",
        "--interval",
        "5s",
        "--output",
        "json",
        "--query-files",
        sql_file,
    ]

    if _to_int(mode_cfg.get("qps_target"), 0, 0) > 0:
        # TiUP Bench rawsql does not expose direct qps-target gating.
        # Keep target as metadata and rely on thread/connections tuning.
        pass

    if str(mode_cfg.get("txn_mode") or "autocommit") == "explicit_txn":
        cmd.extend(["--conn-params", "autocommit=0"])

    return cmd


def _render_tpcc_command(dsn: Dict, mode_cfg: Dict, threads: int) -> List[str]:
    return [
        "tiup",
        "bench",
        "tpcc",
        "run",
        "-d",
        "mysql",
        "-H",
        str(dsn["host"]),
        "-P",
        str(dsn["port"]),
        "-U",
        str(dsn["user"]),
        "-p",
        str(dsn["password"]),
        "-D",
        str(dsn["database"]),
        "-T",
        str(max(1, threads)),
        "--warehouses",
        str(_to_int(mode_cfg.get("warehouses"), 100, 1)),
        "--time",
        f"{_to_int(mode_cfg.get('duration_sec'), 180, 10)}s",
        "--interval",
        "5s",
        "--output",
        "json",
    ]


def _render_ycsb_command(dsn: Dict, mode_cfg: Dict, threads: int) -> List[str]:
    workload = str(mode_cfg.get("workload") or "A").strip().upper()
    if workload not in YCSB_WORKLOADS:
        workload = "A"

    return [
        "tiup",
        "bench",
        "ycsb",
        "run",
        "mysql",
        "--threads",
        str(max(1, threads)),
        "--interval",
        "5",
        "-P",
        f"workloads/workload{workload.lower()}",
        "-p",
        f"mysql.host={dsn['host']}",
        "-p",
        f"mysql.port={dsn['port']}",
        "-p",
        f"mysql.user={dsn['user']}",
        "-p",
        f"mysql.passwd={dsn['password']}",
        "-p",
        f"mysql.db={dsn['database']}",
        "-p",
        f"recordcount={_to_int(mode_cfg.get('recordcount'), 1000000, 1)}",
        "-p",
        f"operationcount={_to_int(mode_cfg.get('operationcount'), 5000000, 1)}",
    ]


def plan_commands(resolved_cfg: Dict, run_dir: Path | None = None) -> List[Dict]:
    mode = str(resolved_cfg.get("mode", "rawsql"))
    dsn = parse_tidb_dsn(str(resolved_cfg.get("cluster", {}).get("tidb_dsn", "")))

    hosts = list(resolved_cfg.get("loadgen", {}).get("hosts") or ["localhost"])
    threads_total = _to_int(
        (resolved_cfg.get(mode, {}) or {}).get("threads_total"),
        64 if mode == "rawsql" else 128,
        1,
    )
    conn_total = _to_int((resolved_cfg.get("rawsql", {}) or {}).get("connections_total"), 128, 1)

    thread_split = _distribution(threads_total, len(hosts))
    conn_split = _distribution(conn_total, len(hosts))

    sql_source = str(resolved_cfg.get("rawsql", {}).get("sql_file") or "").strip()
    sql_source_abs = str((ROOT / sql_source).resolve()) if sql_source and not Path(sql_source).is_absolute() else sql_source

    plan: List[Dict] = []
    for idx, host in enumerate(hosts):
        user, addr = _host_ref(host, str(resolved_cfg.get("loadgen", {}).get("ssh_user") or ""))
        is_local = _is_local_host(addr)
        remote_sql_path = f"/tmp/workload-generator/{run_dir.name if run_dir else 'preview'}/rawsql.sql"
        sql_for_cmd = sql_source_abs
        if mode == "rawsql" and not is_local:
            sql_for_cmd = remote_sql_path

        if mode == "rawsql":
            cmd = _render_rawsql_command(dsn, resolved_cfg.get("rawsql", {}), thread_split[idx], sql_for_cmd)
        elif mode == "tpcc":
            cmd = _render_tpcc_command(dsn, resolved_cfg.get("tpcc", {}), thread_split[idx])
        else:
            cmd = _render_ycsb_command(dsn, resolved_cfg.get("ycsb", {}), thread_split[idx])

        plan.append(
            {
                "host": host,
                "ssh_user": user,
                "address": addr,
                "is_local": is_local,
                "threads": thread_split[idx],
                "connections": conn_split[idx],
                "sql_source": sql_source_abs,
                "sql_remote": remote_sql_path,
                "cmd": cmd,
                "command": _shell_join(cmd),
            }
        )

    return plan


def _validate_dsn_connectivity(dsn: Dict) -> Tuple[bool, str]:
    try:
        import mysql.connector  # type: ignore
    except Exception:
        return False, "mysql-connector-python not available in environment."

    try:
        conn = mysql.connector.connect(
            host=dsn["host"],
            port=dsn["port"],
            user=dsn["user"],
            password=dsn["password"],
            database=dsn["database"],
            connection_timeout=8,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        conn.close()
        return True, "Connected to TiDB DSN and executed SELECT 1."
    except Exception as exc:
        return False, str(exc)


def validate_resolved_config(resolved_cfg: Dict, plan: List[Dict] | None = None) -> Dict:
    checks = []
    mode = str(resolved_cfg.get("mode", "rawsql"))

    try:
        dsn = parse_tidb_dsn(str(resolved_cfg.get("cluster", {}).get("tidb_dsn", "")))
        checks.append({"name": "tidb_dsn", "ok": True, "detail": f"{dsn['host']}:{dsn['port']} db={dsn['database']}"})
    except Exception as exc:
        return {"ok": False, "checks": [{"name": "tidb_dsn", "ok": False, "detail": str(exc)}]}

    tiup_path = shutil.which("tiup")
    checks.append(
        {
            "name": "tiup_local",
            "ok": bool(tiup_path),
            "detail": tiup_path or "tiup is not installed on controller host.",
        }
    )

    ok_conn, detail_conn = _validate_dsn_connectivity(dsn)
    checks.append({"name": "tidb_connectivity", "ok": ok_conn, "detail": detail_conn})

    if mode == "rawsql":
        sql_file = str(resolved_cfg.get("rawsql", {}).get("sql_file") or "").strip()
        sql_path = (ROOT / sql_file).resolve() if sql_file and not Path(sql_file).is_absolute() else Path(sql_file)
        checks.append(
            {
                "name": "rawsql_sql_file",
                "ok": bool(sql_file and sql_path.exists()),
                "detail": str(sql_path) if sql_file else "Missing sql_file.",
            }
        )

    check_plan = plan or plan_commands(resolved_cfg)
    ssh_key = str(resolved_cfg.get("loadgen", {}).get("ssh_key_path") or "").strip()
    for host_plan in check_plan:
        if host_plan.get("is_local"):
            checks.append(
                {
                    "name": f"loadgen:{host_plan['host']}",
                    "ok": True,
                    "detail": "Local host",
                }
            )
            continue

        user = str(host_plan.get("ssh_user") or "").strip()
        addr = str(host_plan.get("address") or "").strip()
        if not user:
            checks.append(
                {
                    "name": f"loadgen:{host_plan['host']}",
                    "ok": False,
                    "detail": "Missing SSH user for remote host.",
                }
            )
            continue

        ssh_cmd = _ssh_prefix(ssh_key) + [f"{user}@{addr}", "command -v tiup >/dev/null && echo ok || echo missing"]
        try:
            proc = subprocess.run(ssh_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
            out = (proc.stdout or "").strip()
            ok = proc.returncode == 0 and "ok" in out.lower() and "missing" not in out.lower()
            checks.append(
                {
                    "name": f"loadgen:{host_plan['host']}",
                    "ok": ok,
                    "detail": out or f"exit={proc.returncode}",
                }
            )
        except Exception as exc:
            checks.append({"name": f"loadgen:{host_plan['host']}", "ok": False, "detail": str(exc)})

    overall = all(bool(c.get("ok")) for c in checks)
    return {"ok": overall, "checks": checks}


def _run_subprocess(cmd: List[str], cwd: Path, timeout_sec: int | None = None) -> Dict:
    started = dt.datetime.now(dt.timezone.utc)
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_sec,
        )
        output = proc.stdout or ""
        rc = proc.returncode
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + "\n[TIMEOUT] process exceeded configured timeout."
        rc = 124
        timed_out = True

    finished = dt.datetime.now(dt.timezone.utc)
    duration = max(0.0, (finished - started).total_seconds())
    return {
        "return_code": rc,
        "timed_out": timed_out,
        "output": output,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_sec": round(duration, 2),
    }


def _run_host_plan(host_plan: Dict, run_dir: Path, ssh_key: str, timeout_sec: int | None = None) -> Dict:
    host = str(host_plan["host"])
    host_file = run_dir / "loadgens" / f"{_safe_host_label(host)}.log"
    host_file.parent.mkdir(parents=True, exist_ok=True)

    upload_result = {"ok": True, "detail": ""}

    if not host_plan.get("is_local") and host_plan.get("sql_source"):
        sql_source = str(host_plan.get("sql_source") or "").strip()
        if sql_source:
            source_path = Path(sql_source)
            if source_path.exists():
                user = str(host_plan.get("ssh_user") or "").strip()
                addr = str(host_plan.get("address") or "").strip()
                remote_sql = str(host_plan.get("sql_remote") or "")
                remote_dir = str(Path(remote_sql).parent)

                mkdir_cmd = _ssh_prefix(ssh_key) + [f"{user}@{addr}", f"mkdir -p {shlex.quote(remote_dir)}"]
                mkdir_proc = _run_subprocess(mkdir_cmd, ROOT, timeout_sec=20)
                if mkdir_proc["return_code"] == 0:
                    scp_cmd = _scp_prefix(ssh_key) + [str(source_path), f"{user}@{addr}:{remote_sql}"]
                    scp_proc = _run_subprocess(scp_cmd, ROOT, timeout_sec=30)
                    if scp_proc["return_code"] != 0:
                        upload_result = {"ok": False, "detail": scp_proc["output"]}
                else:
                    upload_result = {"ok": False, "detail": mkdir_proc["output"]}
            else:
                upload_result = {"ok": False, "detail": f"SQL file not found: {source_path}"}

    cmd = list(host_plan["cmd"])
    display_cmd = host_plan.get("command") or _shell_join(cmd)

    if not host_plan.get("is_local"):
        user = str(host_plan.get("ssh_user") or "").strip()
        addr = str(host_plan.get("address") or "").strip()
        remote_cmd = _shell_join(cmd)
        run_cmd = _ssh_prefix(ssh_key) + [f"{user}@{addr}", remote_cmd]
    else:
        run_cmd = cmd

    if not upload_result["ok"]:
        output = f"[UPLOAD FAILED]\n{upload_result['detail']}"
        host_file.write_text(output + "\n", encoding="utf-8")
        return {
            "host": host,
            "command": display_cmd,
            "return_code": 2,
            "duration_sec": 0.0,
            "timed_out": False,
            "metrics": {},
            "log_file": str(host_file),
            "upload_ok": False,
            "upload_detail": upload_result["detail"],
        }

    result = _run_subprocess(run_cmd, ROOT, timeout_sec=timeout_sec)
    host_file.write_text(result["output"] + "\n", encoding="utf-8")
    metrics = parse_metrics_from_output(result["output"])

    return {
        "host": host,
        "command": display_cmd,
        "return_code": result["return_code"],
        "duration_sec": result["duration_sec"],
        "timed_out": result["timed_out"],
        "metrics": metrics,
        "log_file": str(host_file),
        "upload_ok": upload_result["ok"],
        "upload_detail": upload_result["detail"],
    }


def aggregate_summary(resolved_cfg: Dict, run_dir: Path, host_results: List[Dict], validation: Dict | None = None) -> Dict:
    mode = str(resolved_cfg.get("mode", "rawsql"))
    success_hosts = [r for r in host_results if int(r.get("return_code", 1)) == 0]
    failed_hosts = [r for r in host_results if int(r.get("return_code", 1)) != 0]

    total_qps = sum(_to_float(r.get("metrics", {}).get("qps"), 0.0) for r in success_hosts)
    total_tps = sum(_to_float(r.get("metrics", {}).get("tps"), 0.0) for r in success_hosts)
    p99_candidates = [_to_float(r.get("metrics", {}).get("p99_ms"), 0.0) for r in success_hosts]
    p95_candidates = [_to_float(r.get("metrics", {}).get("p95_ms"), 0.0) for r in success_hosts]

    mean_p99 = round(sum(v for v in p99_candidates if v > 0) / max(1, len([v for v in p99_candidates if v > 0])), 2)
    mean_p95 = round(sum(v for v in p95_candidates if v > 0) / max(1, len([v for v in p95_candidates if v > 0])), 2)
    avg_error_rate = round(
        sum(_to_float(r.get("metrics", {}).get("error_rate"), 0.0) for r in success_hosts) / max(1, len(success_hosts)),
        4,
    )

    status = "completed" if not failed_hosts else "failed"
    if not host_results:
        status = "dry_run"

    summary = {
        "run_dir": str(run_dir),
        "mode": mode,
        "tag": str(resolved_cfg.get("tag") or ""),
        "status": status,
        "loadgens_total": len(host_results),
        "loadgens_success": len(success_hosts),
        "loadgens_failed": len(failed_hosts),
        "achieved_qps": round(total_qps, 2),
        "achieved_tps": round(total_tps, 2),
        "p95_ms": mean_p95,
        "p99_ms": mean_p99,
        "error_rate": avg_error_rate,
        "per_loadgen": host_results,
        "validation_ok": bool((validation or {}).get("ok", True)),
        "validation": validation or {},
    }

    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    chart_data = {
        "totals": {
            "qps": summary["achieved_qps"],
            "tps": summary["achieved_tps"],
            "p99_ms": summary["p99_ms"],
            "error_rate": summary["error_rate"],
        },
        "per_loadgen": [
            {
                "host": row.get("host"),
                "qps": _to_float(row.get("metrics", {}).get("qps"), 0.0),
                "tps": _to_float(row.get("metrics", {}).get("tps"), 0.0),
                "p99_ms": _to_float(row.get("metrics", {}).get("p99_ms"), 0.0),
                "return_code": row.get("return_code"),
            }
            for row in host_results
        ],
    }
    (run_dir / "chart_data.json").write_text(json.dumps(chart_data, indent=2), encoding="utf-8")

    md_lines = [
        f"# Workload Generator Summary ({mode})",
        "",
        f"- Status: **{summary['status']}**",
        f"- Tag: `{summary['tag']}`",
        f"- Run Dir: `{run_dir}`",
        f"- Load Generators: {summary['loadgens_success']} success / {summary['loadgens_total']} total",
        f"- Achieved QPS: **{summary['achieved_qps']}**",
        f"- Achieved TPS: **{summary['achieved_tps']}**",
        f"- P95 Latency (ms): {summary['p95_ms']}",
        f"- P99 Latency (ms): {summary['p99_ms']}",
        f"- Error Rate: {summary['error_rate']}",
        "",
        "## Per Loadgen",
        "",
        "| Host | Return Code | QPS | TPS | P99 (ms) | Log |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in host_results:
        md_lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("host") or "n/a"),
                    str(row.get("return_code", "n/a")),
                    str(_to_float(row.get("metrics", {}).get("qps"), 0.0)),
                    str(_to_float(row.get("metrics", {}).get("tps"), 0.0)),
                    str(_to_float(row.get("metrics", {}).get("p99_ms"), 0.0)),
                    str(row.get("log_file") or ""),
                ]
            )
            + " |"
        )

    (run_dir / "summary.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN_FILE.write_text(str(run_dir), encoding="utf-8")
    return summary


def run_blaster(resolved_cfg: Dict, run_dir: Path, execute: bool = True) -> Dict:
    try:
        plan = plan_commands(resolved_cfg, run_dir)
    except Exception as exc:
        validation = {"ok": False, "checks": [{"name": "plan_build", "ok": False, "detail": str(exc)}]}
        write_run_metadata(run_dir, resolved_cfg, [], validation)
        return aggregate_summary(resolved_cfg, run_dir, [], validation)

    validation = validate_resolved_config(resolved_cfg, plan)
    write_run_metadata(run_dir, resolved_cfg, plan, validation)

    if not execute:
        return aggregate_summary(resolved_cfg, run_dir, [], validation)

    max_fanout = _to_int(resolved_cfg.get("loadgen", {}).get("max_domains_concurrent"), 8, 1)
    ssh_key = str(resolved_cfg.get("loadgen", {}).get("ssh_key_path") or "").strip()

    mode = str(resolved_cfg.get("mode") or "rawsql")
    duration = _to_int((resolved_cfg.get(mode, {}) or {}).get("duration_sec"), 120, 10)
    timeout_sec = max(30, duration + 120)

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=min(max_fanout, len(plan) or 1)) as pool:
        future_map = {
            pool.submit(_run_host_plan, host_plan, run_dir, ssh_key, timeout_sec): host_plan.get("host")
            for host_plan in plan
        }
        for future in as_completed(future_map):
            host = future_map[future]
            try:
                row = future.result()
            except Exception as exc:
                row = {
                    "host": host,
                    "command": "",
                    "return_code": 1,
                    "duration_sec": 0.0,
                    "timed_out": False,
                    "metrics": {},
                    "log_file": "",
                    "upload_ok": False,
                    "upload_detail": str(exc),
                }
            results.append(row)

    results.sort(key=lambda r: str(r.get("host", "")))
    return aggregate_summary(resolved_cfg, run_dir, results, validation)


def regenerate_report(run_dir: Path) -> Dict:
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    resolved_path = run_dir / "resolved_config.yaml"
    if not resolved_path.exists():
        raise FileNotFoundError(f"Missing resolved config for run: {run_dir}")
    resolved_cfg = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}

    host_results = []
    for log_file in sorted((run_dir / "loadgens").glob("*.log")):
        text = log_file.read_text(encoding="utf-8", errors="replace")
        host = log_file.stem.replace("_at_", "@").replace("-", ".")
        host_results.append(
            {
                "host": host,
                "command": "",
                "return_code": 0,
                "duration_sec": 0.0,
                "timed_out": False,
                "metrics": parse_metrics_from_output(text),
                "log_file": str(log_file),
                "upload_ok": True,
                "upload_detail": "",
            }
        )

    validation_path = run_dir / "validation.json"
    validation = None
    if validation_path.exists():
        validation = json.loads(validation_path.read_text(encoding="utf-8"))

    return aggregate_summary(resolved_cfg, run_dir, host_results, validation)


def list_recent_runs(limit: int = 12) -> List[Dict]:
    if not RUNS_DIR.exists():
        return []

    out = []
    for path in sorted([p for p in RUNS_DIR.iterdir() if p.is_dir()], reverse=True):
        summary_path = path / "summary.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                summary = {}
        else:
            summary = {}

        parts = path.name.split("_", 2)
        ts = parts[0] + ("_" + parts[1] if len(parts) > 1 else "")
        mode = summary.get("mode") or (parts[2].split("_")[0] if len(parts) > 2 else "rawsql")

        out.append(
            {
                "name": path.name,
                "run_dir": str(path),
                "timestamp": ts,
                "mode": mode,
                "status": summary.get("status") or "unknown",
                "qps": summary.get("achieved_qps", 0),
                "tps": summary.get("achieved_tps", 0),
                "p99_ms": summary.get("p99_ms", 0),
                "error_rate": summary.get("error_rate", 0),
            }
        )

    return out[: max(1, limit)]


def latest_run_dir() -> Path | None:
    if LAST_RUN_FILE.exists():
        p = Path(LAST_RUN_FILE.read_text(encoding="utf-8").strip())
        if p.exists() and p.is_dir():
            return p

    runs = list_recent_runs(limit=1)
    if not runs:
        return None
    return Path(runs[0]["run_dir"])
