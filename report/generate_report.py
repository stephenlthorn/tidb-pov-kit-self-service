#!/usr/bin/env python3
"""
generate_report.py — Produces a professional PDF PoV results report.

Layout:
  Page 1:  Cover + Executive Summary (KPI cards)
  Page 2:  Module status table
  Page 3:  Data population snapshot
  Page 4:  Baseline OLTP performance charts (latency + TPS by concurrency)
  Page 5:  Warm workload stability chart
  Page 6:  Elastic scale time-series chart
  Page 7:  HA recovery chart (RTO visualisation)
  Page 8:  Write contention comparison (sequential vs AUTO_RANDOM)
  Page 9:  HTAP chart (OLTP-only vs HTAP p99)
  Page 10: MySQL compatibility heatmap
  Page 11: Data import comparison bar chart
  Page 12: TCO model (3-year cost comparison)
  Page 13: (Optional) Vector search QPS chart
  Page 14: Appendix — raw latency table

Usage:
    python report/generate_report.py [config.yaml]
"""
import sys, os, json, time, io, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import yaml
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mtick
import numpy as np
from fpdf import FPDF
from fpdf.enums import XPos, YPos

from report.collect_metrics import collect
from report.tco_model import compute as compute_tco, make_chart as make_tco_chart

# ── Brand colours ─────────────────────────────────────────────────────────────
RED        = (220, 50,  47)
BLUE       = (31,  119, 180)
GREEN      = (44,  160, 44)
ORANGE     = (255, 127, 14)
PURPLE     = (148, 103, 189)
LIGHT_GREY = (245, 245, 245)
DARK_GREY  = (80,  80,  80)
WHITE      = (255, 255, 255)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")

MODULE_DISPLAY = {
    "00_customer_queries":    "M0 — Customer Query Validation",
    "01_baseline_perf":       "M1 — Baseline OLTP Performance",
    "02_elastic_scale":       "M2 — Elastic Auto-Scaling",
    "03_high_availability":   "M3 — High Availability & RTO",
    "03b_write_contention":   "M3b — Write Contention / Hot Region",
    "04_htap_concurrent":     "M4 — HTAP Concurrent Workload",
    "05_online_ddl":          "M5 — Online DDL",
    "06_mysql_compat":        "M6 — MySQL Compatibility",
    "07_data_import":         "M7 — Data Import Speed",
    "08_vector_search":       "M8 — Vector Search (AI Track)",
}

KPI_THRESHOLDS = {
    "01_baseline_perf": {"p99_ms_warn": 80.0, "p99_ms_fail": 200.0, "tps_warn": 500.0},
    "02_elastic_scale": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0, "tps_warn": 300.0},
    "03_high_availability": {"p99_ms_warn": 200.0, "p99_ms_fail": 500.0},
    "03b_write_contention": {"p99_ms_warn": 150.0, "p99_ms_fail": 400.0},
    "04_htap_concurrent": {"p99_ms_warn": 150.0, "p99_ms_fail": 400.0},
    "05_online_ddl": {"p99_ms_warn": 200.0, "p99_ms_fail": 500.0},
    "07_data_import": {"tps_warn": 100.0},
}


# ── PDF helper class ──────────────────────────────────────────────────────────

class PoVReport(FPDF):
    def __init__(self, customer_name=""):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.customer_name = customer_name
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(18, 18, 18)

    def normalize_text(self, text):
        """Normalize Unicode punctuation to core-font-safe characters.

        The default Helvetica core font in fpdf only supports Latin-1. This
        keeps report generation robust even when copy includes smart punctuation
        (for example em dash, curly quotes, bullets, or ellipsis).
        """
        if text is None:
            text = ""
        if not isinstance(text, str):
            text = str(text)

        safe_map = str.maketrans(
            {
                "—": "-",
                "–": "-",
                "−": "-",
                "“": '"',
                "”": '"',
                "‘": "'",
                "’": "'",
                "•": "-",
                "…": "...",
                "\u00a0": " ",
            }
        )
        normalized = text.translate(safe_map)

        try:
            return super().normalize_text(normalized)
        except Exception:
            return normalized.encode("latin-1", "replace").decode("latin-1")

    def header(self):
        if self.page_no() == 1:
            return
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*DARK_GREY)
        self.cell(0, 6, f"TiDB Cloud PoV Results — {self.customer_name}", align="L",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*RED)
        self.set_line_width(0.4)
        self.line(self.l_margin, self.get_y(), 210 - self.r_margin, self.get_y())
        self.ln(3)

    def footer(self):
        self.set_y(-14)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*DARK_GREY)
        self.cell(0, 5,
                  f"Generated {time.strftime('%B %Y')} | Page {self.page_no()}",
                  align="C")

    def section_title(self, text):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*RED)
        self.cell(0, 8, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_draw_color(*RED)
        self.set_line_width(0.5)
        self.line(self.l_margin, self.get_y(), 210 - self.r_margin, self.get_y())
        self.ln(4)
        self.set_text_color(0, 0, 0)

    def sub_title(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*DARK_GREY)
        self.cell(0, 6, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.set_text_color(0, 0, 0)

    def body_text(self, text, size=9):
        self.set_font("Helvetica", "", size)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def kpi_card(self, x, y, w, h, label, value, unit="", color=BLUE):
        self.set_fill_color(*LIGHT_GREY)
        self.set_draw_color(*color)
        self.set_line_width(0.8)
        self.rect(x, y, w, h, style="FD")
        # Label
        self.set_xy(x + 2, y + 2)
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*DARK_GREY)
        self.cell(w - 4, 5, label)
        # Value
        self.set_xy(x + 2, y + 8)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*color)
        self.cell(w - 4, 8, str(value))
        # Unit
        self.set_xy(x + 2, y + 17)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(*DARK_GREY)
        self.cell(w - 4, 4, unit)
        self.set_text_color(0, 0, 0)

    def embed_figure(self, fig, w=174, h=None):
        """Save a matplotlib figure to PNG buffer, then embed in PDF."""
        buf = io.BytesIO()
        aspect = fig.get_figheight() / fig.get_figwidth()
        if h is None:
            h = w * aspect
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        x = self.l_margin
        self.image(buf, x=x, y=self.get_y(), w=w, h=h)
        self.ln(h + 4)


# ── Chart generators ──────────────────────────────────────────────────────────

def _chart_baseline(metrics) -> plt.Figure:
    mod  = metrics["modules"].get("01_baseline_perf", {})
    tidb = mod.get("tidb", {})
    comp_label = metrics.get("comparison_label") or "Comparison DB"
    phase_rows = []
    for ph, stats in tidb.items():
        if not isinstance(stats, dict):
            continue
        if re.fullmatch(r"c\d+", str(ph)):
            phase_rows.append((int(str(ph)[1:]), str(ph), stats))
    phase_rows.sort(key=lambda x: x[0])

    if not phase_rows:
        reason, actions = _module_missing_reason(metrics, "01_baseline_perf", "No baseline OLTP phase data")
        return _empty_chart("Baseline OLTP data unavailable", reason=reason, actions=actions)

    phases = [row[1] for row in phase_rows]
    concs  = []
    p99s   = []
    tpss   = []
    for conc, _phase, s in phase_rows:
        concs.append(conc)
        p99s.append(s.get("p99_ms", 0))
        tpss.append(s.get("tps", 0))

    comp_p99s = []
    comp_tpss = []
    if metrics.get("comparison_enabled"):
        comp = mod.get("comparison", {})
        for ph in phases:
            s = comp.get(ph, {})
            comp_p99s.append(s.get("p99_ms", 0))
            comp_tpss.append(s.get("tps", 0))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))
    x = np.arange(len(concs))

    ax1.plot(concs, p99s, "o-", color=_rgb(BLUE), lw=2, markersize=7, label="TiDB Cloud")
    if comp_p99s:
        ax1.plot(concs, comp_p99s, "s--", color=_rgb(ORANGE), lw=2, markersize=7,
                 label=comp_label)
    ax1.set_xlabel("Concurrency")
    ax1.set_ylabel("p99 Latency (ms)")
    ax1.set_title("p99 Latency vs Concurrency")
    ax1.legend()
    ax1.grid(alpha=0.3)

    ax2.bar(x - 0.2, tpss, 0.35, label="TiDB Cloud", color=_rgb(BLUE))
    if comp_tpss:
        ax2.bar(x + 0.2, comp_tpss, 0.35, label=comp_label, color=_rgb(ORANGE))
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(c) for c in concs])
    ax2.set_xlabel("Concurrency")
    ax2.set_ylabel("Transactions / sec")
    ax2.set_title("Throughput (TPS) vs Concurrency")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    return fig


def _chart_warm_steady(metrics) -> plt.Figure:
    mod = metrics["modules"].get("01_baseline_perf", {})
    tidb = mod.get("tidb", {})
    warm_stats = tidb.get("warm_steady", {}) if isinstance(tidb, dict) else {}
    ts = mod.get("time_series", {}).get("warm_steady", []) if isinstance(mod.get("time_series"), dict) else []

    if ts:
        elapsed = [r.get("elapsed_sec", 0) for r in ts]
        p99 = [r.get("p99_ms", 0) for r in ts]
        tps = [r.get("tps", 0) for r in ts]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 6), sharex=True)
        ax1.plot(elapsed, p99, color=_rgb(BLUE), lw=1.7)
        ax1.set_ylabel("p99 Latency (ms)")
        ax1.set_title("Warm Workload: Latency Stability Over Time")
        ax1.grid(alpha=0.3)

        ax2.plot(elapsed, tps, color=_rgb(GREEN), lw=1.7)
        ax2.fill_between(elapsed, tps, alpha=0.2, color=_rgb(GREEN))
        ax2.set_xlabel("Elapsed (seconds)")
        ax2.set_ylabel("TPS")
        ax2.set_title("Warm Workload: Throughput Over Time")
        ax2.grid(alpha=0.3)

        fig.tight_layout()
        return fig

    if warm_stats:
        labels = ["p50", "p95", "p99"]
        vals = [
            warm_stats.get("p50_ms", 0),
            warm_stats.get("p95_ms", 0),
            warm_stats.get("p99_ms", 0),
        ]
        fig, ax = plt.subplots(figsize=(8, 4))
        bars = ax.bar(labels, vals, color=[_rgb(BLUE), _rgb(ORANGE), _rgb(RED)])
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{val:.1f}", ha="center", va="bottom")
        ax.set_ylabel("Latency (ms)")
        ax.set_title("Warm Workload Steady-State Latency")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        return fig

    reason, actions = _module_missing_reason(metrics, "01_baseline_perf", "No warm workload data")
    return _empty_chart("Warm workload data unavailable", reason=reason, actions=actions)


def _chart_data_population(metrics) -> plt.Figure:
    manifest = metrics.get("data_manifest", {}) or {}
    counts = manifest.get("counts", {}) if isinstance(manifest, dict) else {}
    if not isinstance(counts, dict) or not counts:
        return _empty_chart(
            "Data population unavailable",
            reason="No data manifest file was found for this run.",
            actions=[
                "Run data generation (full run or setup/generate_data.py) before building the report.",
                "Confirm results/data_manifest.json is present in local results or S3 artifacts.",
            ],
        )

    rows = [(str(k), int(v)) for k, v in counts.items() if isinstance(v, (int, float))]
    if not rows:
        return _empty_chart(
            "Data population unavailable",
            reason="Manifest exists, but row-count values are missing.",
            actions=["Re-run data generation with a valid test.data_scale value (small/medium/large)."],
        )
    rows.sort(key=lambda x: x[1], reverse=True)

    labels = [k.replace("_", " ") for k, _ in rows]
    vals = [v for _, v in rows]
    y = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(12, max(4.5, len(labels) * 0.35)))
    bars = ax.barh(y, vals, color=_rgb(BLUE), alpha=0.8)
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Rows")
    ax.set_title("Data Population by Table")
    ax.grid(axis="x", alpha=0.3)

    if max(vals) / max(min(vals), 1) > 50:
        ax.set_xscale("log")
        ax.set_xlabel("Rows (log scale)")

    for bar, val in zip(bars, vals):
        ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, f" {val:,}", va="center", fontsize=8)

    fig.tight_layout()
    return fig


def _chart_scale(metrics) -> plt.Figure:
    mod  = metrics["modules"].get("02_elastic_scale", {})
    ts   = mod.get("time_series", {})
    all_ts = []
    for ph in ["ramp_up", "sustain", "ramp_down"]:
        if ph in ts:
            all_ts.extend(ts[ph])
    if not all_ts:
        reason, actions = _module_missing_reason(metrics, "02_elastic_scale", "No elastic scale data")
        return _empty_chart("Elastic scale data unavailable", reason=reason, actions=actions)

    elapsed = [r["elapsed_sec"] for r in all_ts]
    p99     = [r["p99_ms"]      for r in all_ts]
    tps     = [r["tps"]         for r in all_ts]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 6), sharex=True)
    ax1.plot(elapsed, p99, color=_rgb(RED), lw=1.5)
    ax1.set_ylabel("p99 Latency (ms)")
    ax1.set_title("Elastic Auto-Scaling: Latency & Throughput over Time")
    ax1.grid(alpha=0.3)
    ax2.fill_between(elapsed, tps, alpha=0.3, color=_rgb(BLUE))
    ax2.plot(elapsed, tps, color=_rgb(BLUE), lw=1.5)
    ax2.set_xlabel("Elapsed (seconds)")
    ax2.set_ylabel("TPS")
    ax2.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def _chart_ha(metrics) -> plt.Figure:
    mod = metrics["modules"].get("03_high_availability", {})
    ts  = mod.get("time_series", {})
    all_ts = []
    for ph in ["warmup", "failure", "recovery"]:
        if ph == "failure" and ph not in ts and "during_failure" in ts:
            all_ts.extend(ts["during_failure"])
            continue
        if ph in ts:
            all_ts.extend(ts[ph])
    if not all_ts:
        reason, actions = _module_missing_reason(metrics, "03_high_availability", "No HA data")
        return _empty_chart("High-availability data unavailable", reason=reason, actions=actions)

    elapsed = [r["elapsed_sec"] for r in all_ts]
    p99     = [r["p99_ms"]      for r in all_ts]

    fig, ax = plt.subplots(figsize=(13, 4))
    ax.plot(elapsed, p99, color=_rgb(BLUE), lw=1.5, label="p99 Latency")
    ax.axvline(x=30, color=_rgb(RED), linestyle="--", label="Failure injected")
    ax.set_xlabel("Elapsed (seconds)")
    ax.set_ylabel("p99 Latency (ms)")
    ax.set_title("High Availability: RTO Visualisation")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    return fig


def _chart_hotspot(metrics) -> plt.Figure:
    mod  = metrics["modules"].get("03b_write_contention", {})
    tidb = mod.get("tidb", {})
    seq  = tidb.get("sequential", {})
    ar   = tidb.get("autorand",   {})
    if not seq or not ar:
        reason, actions = _module_missing_reason(metrics, "03b_write_contention", "No write contention data")
        return _empty_chart("Write-contention data unavailable", reason=reason, actions=actions)

    categories = ["p50", "p95", "p99", "max"]
    seq_vals   = [seq.get(f"{k}_ms", 0) for k in categories]
    ar_vals    = [ar.get(f"{k}_ms",  0) for k in categories]
    x = np.arange(len(categories))

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(x - 0.2, seq_vals, 0.35, label="AUTO_INCREMENT (hotspot)", color=_rgb(RED))
    ax.bar(x + 0.2, ar_vals,  0.35, label="AUTO_RANDOM (mitigated)",  color=_rgb(GREEN))
    ax.set_xticks(x)
    ax.set_xticklabels([c.upper() for c in categories])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Write Contention: Sequential vs AUTO_RANDOM Keys")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def _chart_htap(metrics) -> plt.Figure:
    mod  = metrics["modules"].get("04_htap_concurrent", {})
    tidb = mod.get("tidb", {})
    only = tidb.get("oltp_only", {})
    htap = tidb.get("htap", {})
    if not only or not htap:
        reason, actions = _module_missing_reason(metrics, "04_htap_concurrent", "No HTAP data")
        return _empty_chart("HTAP data unavailable", reason=reason, actions=actions)

    labels = ["p50", "p95", "p99"]
    only_v = [only.get(f"{k}_ms", 0) for k in labels]
    htap_v = [htap.get(f"{k}_ms", 0) for k in labels]
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(x - 0.2, only_v, 0.35, label="OLTP only",           color=_rgb(BLUE))
    ax.bar(x + 0.2, htap_v, 0.35, label="OLTP + Analytics",    color=_rgb(PURPLE))
    ax.set_xticks(x)
    ax.set_xticklabels([k.upper() for k in labels])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("HTAP: OLTP Latency with and without Concurrent Analytics")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def _chart_compat(compat_data) -> plt.Figure:
    details = compat_data.get("details", [])
    if not details:
        return _empty_chart(
            "Compatibility data unavailable",
            reason="No compatibility checks were captured.",
            actions=[
                "Enable module M6 (MySQL compatibility) and run again.",
                "Confirm compat_checks entries exist in results.db before report build.",
            ],
        )

    normalized = []
    for row in details:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or row.get("group") or row.get("module") or "Uncategorized")
        raw_status = str(row.get("status") or row.get("result") or "").strip().lower()
        if raw_status in {"pass", "passed", "ok", "success", "true", "1"}:
            status = "pass"
        elif raw_status in {"fail", "failed", "error", "false", "0"}:
            status = "fail"
        else:
            status = "fail"
        normalized.append({"category": category, "status": status})

    if not normalized:
        return _empty_chart(
            "Compatibility data unavailable",
            reason="Compatibility rows were present but did not include usable pass/fail status.",
            actions=["Re-run M6 and ensure checks log status values to results.db."],
        )

    cats = sorted(set(r["category"] for r in normalized))
    pass_cnt = {c: sum(1 for r in normalized if r["category"] == c and r["status"] == "pass")
                for c in cats}
    fail_cnt = {c: sum(1 for r in normalized if r["category"] == c and r["status"] != "pass")
                for c in cats}
    x = np.arange(len(cats))

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(x, [pass_cnt[c] for c in cats], label="Pass", color=_rgb(GREEN))
    ax.bar(x, [fail_cnt[c] for c in cats], bottom=[pass_cnt[c] for c in cats],
           label="Fail", color=_rgb(RED))
    ax.set_xticks(x)
    ax.set_xticklabels(cats, rotation=20, ha="right")
    ax.set_ylabel("Check count")
    ax.set_title("MySQL Compatibility by Category")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def _chart_import(metrics) -> plt.Figure:
    imp = metrics.get("import_stats", [])
    if not imp:
        reason, actions = _module_missing_reason(metrics, "07_data_import", "No import data")
        return _empty_chart("Import data unavailable", reason=reason, actions=actions)

    # We log one row per method; use last 3 rows
    rows  = imp[-3:]
    labels = ["Batched INSERT", "LOAD DATA INFILE", "IMPORT INTO"][:len(rows)]
    gbpms  = [r.get("throughput_gbpm", 0) for r in rows]

    colors = [_rgb(BLUE), _rgb(GREEN), _rgb(RED)]
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(labels, gbpms, color=colors[:len(rows)])
    for bar, val in zip(bars, gbpms):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.002,
                f"{val:.3f}", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Throughput (GB/min)")
    ax.set_title("Data Import: Method Comparison")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def _empty_chart(title: str, reason: str = "", actions: list[str] | None = None) -> plt.Figure:
    actions = actions or []
    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.axis("off")

    ax.text(0.02, 0.92, title, transform=ax.transAxes, fontsize=14, fontweight="bold", color="#334155")
    ax.text(0.02, 0.80, "Data not available for this section.", transform=ax.transAxes, fontsize=11, color="#64748b")
    if reason:
        ax.text(0.02, 0.66, f"Reason: {reason}", transform=ax.transAxes, fontsize=10, color="#475569")
    if actions:
        ax.text(0.02, 0.52, "Next run guidance:", transform=ax.transAxes, fontsize=10, color="#334155", fontweight="bold")
        y = 0.43
        for item in actions[:4]:
            ax.text(0.03, y, f"- {item}", transform=ax.transAxes, fontsize=9, color="#475569")
            y -= 0.10
    ax.add_patch(plt.Rectangle((0.01, 0.04), 0.98, 0.90, fill=False, lw=1.0, ec="#cbd5e1", transform=ax.transAxes))
    ax.axis("off")
    return fig


def _module_data_points(mod_entry: dict) -> int:
    tidb = mod_entry.get("tidb", {}) if isinstance(mod_entry, dict) else {}
    total = 0
    if isinstance(tidb, dict):
        for stats in tidb.values():
            if isinstance(stats, dict):
                try:
                    total += int(stats.get("count", 0) or 0)
                except Exception:
                    pass
    return total


def _module_missing_reason(metrics: dict, module_key: str, fallback: str) -> tuple[str, list[str]]:
    mod = (metrics.get("modules", {}) or {}).get(module_key, {}) or {}
    status = str(mod.get("status") or "not_run").lower()
    notes = str(mod.get("notes") or "").strip()
    if status == "not_run":
        reason = f"{MODULE_DISPLAY.get(module_key, module_key)} was not selected for this run."
    elif status == "failed":
        reason = f"{MODULE_DISPLAY.get(module_key, module_key)} failed during execution."
    elif status == "skipped":
        reason = f"{MODULE_DISPLAY.get(module_key, module_key)} was skipped."
    else:
        reason = fallback
    if notes:
        reason = f"{reason} Notes: {notes}"
    actions = [
        f"Enable {MODULE_DISPLAY.get(module_key, module_key)} in test selection and rerun.",
        "Use small defaults first, then increase duration/concurrency after a clean baseline.",
        "Check results/run_all.log for module-level errors before rebuilding report.",
    ]
    return reason, actions


def _add_run_coverage_page(pdf, metrics):
    pdf.add_page()
    pdf.section_title("Run Coverage and Data Completeness")
    summary = metrics.get("summary", {}) or {}
    run_context = metrics.get("run_context", {}) or {}
    manifest = metrics.get("data_manifest", {}) or {}
    rows_generated = 0
    if isinstance(manifest.get("counts"), dict):
        rows_generated = sum(v for v in manifest.get("counts", {}).values() if isinstance(v, (int, float)))

    pdf.body_text(
        f"Run mode: {run_context.get('run_mode', 'n/a')} | "
        f"Schema mode: {run_context.get('schema_mode', 'n/a')} | "
        f"Industry: {run_context.get('industry', 'general_auto')} | "
        f"Modules passed: {summary.get('modules_passed', 0)}/{summary.get('modules_run', 0)} | "
        f"Generated rows: {rows_generated:,}" if rows_generated else
        f"Run mode: {run_context.get('run_mode', 'n/a')} | "
        f"Schema mode: {run_context.get('schema_mode', 'n/a')} | "
        f"Industry: {run_context.get('industry', 'general_auto')} | "
        f"Modules passed: {summary.get('modules_passed', 0)}/{summary.get('modules_run', 0)}"
    , size=8)

    col_widths = [74, 24, 18, 18, 42]
    headers = ["Module", "Status", "Secs", "Points", "Interpretation"]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*RED)
    pdf.set_text_color(*WHITE)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 6, h, border=1, fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)

    for key in MODULE_DISPLAY:
        mod = (metrics.get("modules", {}) or {}).get(key, {}) or {}
        status = str(mod.get("status") or "not_run")
        dur = f"{float(mod.get('duration_sec') or 0):.0f}"
        points = str(_module_data_points(mod))
        if status == "passed":
            interp = "Usable evidence captured."
            color = GREEN
        elif status == "failed":
            interp = "Execution failed; chart may be unavailable."
            color = RED
        elif status == "skipped":
            interp = "Explicitly skipped by test selection."
            color = ORANGE
        else:
            interp = "Not selected in this run."
            color = DARK_GREY
        pdf.set_fill_color(*LIGHT_GREY)
        pdf.cell(col_widths[0], 6, MODULE_DISPLAY[key], border=1, fill=True)
        pdf.set_text_color(*color)
        pdf.cell(col_widths[1], 6, status.upper(), border=1, fill=True)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(col_widths[2], 6, dur, border=1, fill=True)
        pdf.cell(col_widths[3], 6, points, border=1, fill=True)
        pdf.cell(col_widths[4], 6, interp[:60], border=1, fill=True)
        pdf.ln()

    pdf.ln(4)
    pdf.body_text(
        "Charts in this report always display either measured data or a clear reason and action guidance block.",
        size=8,
    )


def _kpi_eval(module_key: str, stats: dict) -> tuple[str, str]:
    rule = KPI_THRESHOLDS.get(module_key, {})
    p99 = _maybe_float(stats.get("p99_ms"))
    tps = _maybe_float(stats.get("tps"))

    if p99 is not None:
        fail_cut = _maybe_float(rule.get("p99_ms_fail"))
        warn_cut = _maybe_float(rule.get("p99_ms_warn"))
        if fail_cut is not None and p99 >= fail_cut:
            return "FAIL", f"p99 {p99:.1f}ms >= {fail_cut:.0f}ms"
        if warn_cut is not None and p99 >= warn_cut:
            return "WARN", f"p99 {p99:.1f}ms >= {warn_cut:.0f}ms"

    if tps is not None:
        warn_tps = _maybe_float(rule.get("tps_warn"))
        if warn_tps is not None and tps < warn_tps:
            return "WARN", f"TPS {tps:.0f} < {warn_tps:.0f}"

    if p99 is None and tps is None:
        return "NO DATA", "No latency/throughput points captured"
    return "PASS", "Within baseline target band"


def _add_kpi_appendix_page(pdf, metrics):
    pdf.add_page()
    pdf.section_title("Appendix — KPI Threshold Evaluation")
    pdf.body_text(
        "Thresholds are PoV guidance bands for quick interpretation, not strict SLA commitments. "
        "Use this page to identify phases that need rerun or tuning.",
        size=8,
    )

    col_w = [48, 24, 18, 18, 18, 18, 20, 36]
    hdrs = ["Module / Phase", "Status", "Count", "p50", "p95", "p99", "TPS", "Evaluation"]
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(*RED)
    pdf.set_text_color(*WHITE)
    for w, h in zip(col_w, hdrs):
        pdf.cell(w, 6, h, border=1, fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)

    for mod_key in MODULE_DISPLAY:
        mod = (metrics.get("modules", {}) or {}).get(mod_key, {}) or {}
        tidb = mod.get("tidb", {}) if isinstance(mod.get("tidb"), dict) else {}
        if not tidb:
            pdf.set_fill_color(*LIGHT_GREY)
            row = [mod_key, "NO DATA", "-", "-", "-", "-", "-", "Module not run or no rows"]
            for w, cell in zip(col_w, row):
                pdf.cell(w, 5, str(cell)[:60], border=1, fill=True)
            pdf.ln()
            continue

        for phase, s in tidb.items():
            if not isinstance(s, dict):
                continue
            verdict, note = _kpi_eval(mod_key, s)
            if verdict == "PASS":
                color = GREEN
            elif verdict == "WARN":
                color = ORANGE
            elif verdict == "FAIL":
                color = RED
            else:
                color = DARK_GREY

            pdf.set_fill_color(*LIGHT_GREY)
            pdf.cell(col_w[0], 5, f"{mod_key[:14]}/{str(phase)[:20]}", border=1, fill=True)
            pdf.set_text_color(*color)
            pdf.cell(col_w[1], 5, verdict, border=1, fill=True)
            pdf.set_text_color(0, 0, 0)
            pdf.cell(col_w[2], 5, str(int(s.get("count", 0) or 0)), border=1, fill=True)
            pdf.cell(col_w[3], 5, f"{float(s.get('p50_ms', 0) or 0):.1f}", border=1, fill=True)
            pdf.cell(col_w[4], 5, f"{float(s.get('p95_ms', 0) or 0):.1f}", border=1, fill=True)
            pdf.cell(col_w[5], 5, f"{float(s.get('p99_ms', 0) or 0):.1f}", border=1, fill=True)
            pdf.cell(col_w[6], 5, f"{float(s.get('tps', 0) or 0):.0f}", border=1, fill=True)
            pdf.cell(col_w[7], 5, note[:80], border=1, fill=True)
            pdf.ln()


def _rgb(color_tuple):
    return tuple(c / 255 for c in color_tuple)


def _maybe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


# ── Main report assembler ─────────────────────────────────────────────────────

def generate(cfg: dict = None, out_path: str = None) -> str:
    cfg = cfg or {}
    metrics   = collect()
    tco_data  = compute_tco(cfg)
    summary   = metrics.get("summary", {})
    customer  = cfg.get("report", {}).get("customer_name", "Customer")
    se_name   = cfg.get("report", {}).get("se_name", "PingCAP Sales Engineering")

    if out_path is None:
        out_path = os.path.join(RESULTS_DIR, "tidb_pov_report.pdf")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    pdf = PoVReport(customer_name=customer)

    # ── Page 1: Cover ─────────────────────────────────────────────────────────
    pdf.add_page()
    pdf.set_fill_color(*RED)
    pdf.rect(0, 0, 210, 60, style="F")
    pdf.set_xy(18, 18)
    pdf.set_font("Helvetica", "B", 26)
    pdf.set_text_color(*WHITE)
    pdf.cell(0, 12, "TiDB Cloud", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_xy(18, 32)
    pdf.set_font("Helvetica", "", 14)
    pdf.cell(0, 8, "Proof of Value — Results Report")

    pdf.set_xy(18, 70)
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, f"Prepared for: {customer}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 7, f"Prepared by:  {se_name}",  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 7, f"Date: {time.strftime('%B %d, %Y')}",
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # KPI cards
    pdf.ln(10)
    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 7, "Executive Summary", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(3)

    def _fmt(v, decimals=0, na="—"):
        if v is None:
            return na
        return f"{v:,.{decimals}f}"

    display_latency = summary.get("warm_p99_ms")
    display_tps = summary.get("warm_tps")
    latency_label = "Warm p99 Latency"
    tps_label = "Warm Throughput"
    if summary.get("run_mode") == "performance":
        if display_latency is None:
            display_latency = summary.get("workload_p99_ms")
        if display_tps is None:
            display_tps = summary.get("workload_tps")
        latency_label = "Current Run p99"
        tps_label = "Current Run Throughput"

    cards = [
        ("Run Mode",            str(summary.get("run_mode") or "validation"), "",       (0, 128, 128)),
        ("Schema Mode",         str(summary.get("schema_mode") or "tidb_optimized"), "", (100, 100, 180)),
        ("Industry",            str(summary.get("industry") or "general_auto"), "", (120, 120, 160)),
        (latency_label,         _fmt(display_latency, 1), "ms",       BLUE),
        (tps_label,             _fmt(display_tps,    0), "TPS",      GREEN),
        ("Best Observed p99",   _fmt(summary.get("best_observed_p99_ms", summary.get("best_p99_ms")), 1), "ms", BLUE),
        ("Peak Throughput",     _fmt(summary.get("best_tps"),    0), "TPS",      GREEN),
        ("HA Recovery (RTO)",   _fmt(summary.get("rto_sec"),     1), "seconds",  ORANGE),
        ("Hotspot Reduction",   _fmt(summary.get("hotspot_improvement_pct"), 0), "%",   RED),
        ("MySQL Compat",        _fmt(summary.get("mysql_compat_pct"), 0), "%",   PURPLE),
        ("Modules Passed",
         f"{summary.get('modules_passed','—')}/{summary.get('modules_run','—')}",
         "tests", (0, 128, 128)),
    ]
    col_w = 28
    col_h = 26
    cols_per_row = 6
    start_x = pdf.l_margin
    y0 = pdf.get_y()
    for i, (label, value, unit, color) in enumerate(cards):
        col = i % cols_per_row
        row = i // cols_per_row
        pdf.kpi_card(start_x + col * (col_w + 3), y0 + row * (col_h + 4),
                     col_w, col_h, label, value, unit, color)
    row_count = max(1, (len(cards) + cols_per_row - 1) // cols_per_row)
    pdf.set_y(y0 + row_count * (col_h + 4) + 2)

    # Intro paragraph
    intro = (
        "This report summarises the results of a self-service Proof of Value "
        "conducted on TiDB Cloud. The tests were executed automatically using "
        "the TiDB Cloud PoV Kit and cover OLTP performance (including warm steady-state), elastic auto-scaling, "
        "high availability, write contention, HTAP, online DDL, MySQL compatibility, "
        "data import speed, and total cost of ownership. "
        f"Run mode: {summary.get('run_mode', 'validation')}. "
        f"Schema mode: {summary.get('schema_mode', 'tidb_optimized')}. "
        f"Industry: {summary.get('industry', 'general_auto')}."
    )
    if summary.get("workload_status"):
        intro += (
            f" Workload Generator: {summary.get('workload_status')} "
            f"({summary.get('workload_mode') or 'rawsql'})"
        )
        if summary.get("workload_qps") is not None:
            intro += f", achieved QPS {summary.get('workload_qps'):.2f}"
        if summary.get("workload_p99_ms") is not None:
            intro += f", p99 {summary.get('workload_p99_ms'):.2f} ms"
        intro += "."
    pdf.body_text(intro, size=9)

    # ── Page 2: Module status table ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Test Module Status")
    col_widths = [80, 22, 22, 50]
    headers    = ["Module", "Status", "Duration", "Notes"]
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(*RED)
    pdf.set_text_color(*WHITE)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)

    for mod_key, mod_label in MODULE_DISPLAY.items():
        mod  = metrics["modules"].get(mod_key, {})
        stat = mod.get("status", "not_run")
        dur  = f"{mod.get('duration_sec', 0):.0f}s" if mod.get("duration_sec") else "—"
        note = (mod.get("notes") or "")[:55]
        color = GREEN if stat == "passed" else (ORANGE if stat == "skipped" else RED)
        pdf.set_fill_color(*LIGHT_GREY)
        pdf.set_font("Helvetica", "", 8)
        pdf.cell(col_widths[0], 6, mod_label, border=1, fill=True)
        pdf.set_text_color(*color)
        pdf.cell(col_widths[1], 6, stat.upper(), border=1, fill=True)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(col_widths[2], 6, dur, border=1, fill=True)
        pdf.cell(col_widths[3], 6, note, border=1, fill=True)
        pdf.ln()
    pdf.ln(5)

    # ── Page 3: Coverage details ─────────────────────────────────────────────
    _add_run_coverage_page(pdf, metrics)

    # ── Pages 4+: Charts ──────────────────────────────────────────────────────
    manifest = metrics.get("data_manifest", {}) or {}
    _add_chart_page(
        pdf,
        "Data Population Snapshot",
        _chart_data_population(metrics),
        f"Scale: {manifest.get('scale', 'n/a')} | "
        f"Rows generated across schemas: {sum((manifest.get('counts') or {}).values()) if isinstance(manifest.get('counts'), dict) else 'n/a'} | "
        f"Generation time: {manifest.get('generation_duration_sec', 'n/a')} sec",
    )

    _add_chart_page(pdf, "Baseline OLTP Performance",
                    _chart_baseline(metrics),
                    "OLTP workload across configured concurrency levels. "
                    "Shows p99 latency and transactions per second.")

    _add_chart_page(
        pdf,
        "Warm Workload Stability",
        _chart_warm_steady(metrics),
        "Steady-state warm workload after data load. This phase reflects customer-expected latency drift and TPS consistency over time.",
    )

    _add_chart_page(pdf, "Elastic Auto-Scaling",
                    _chart_scale(metrics),
                    "Load ramped from baseline to 4× peak. TiDB Cloud auto-scales "
                    "compute horizontally, keeping p99 stable throughout.")

    _add_chart_page(pdf, "High Availability — RTO",
                    _chart_ha(metrics),
                    "A node failure was simulated during steady-state load. "
                    "The chart shows recovery time (RTO) and latency impact.")

    _add_chart_page(pdf, "Write Contention — AUTO_RANDOM vs Sequential Keys",
                    _chart_hotspot(metrics),
                    "Sequential (AUTO_INCREMENT) PKs concentrate writes on a single "
                    "region leader (hot region). AUTO_RANDOM distributes writes evenly.")

    _add_chart_page(pdf, "HTAP — Concurrent Transactional & Analytical Workload",
                    _chart_htap(metrics),
                    "TiFlash columnar replicas serve analytical queries without "
                    "interfering with TiKV row-store OLTP writes.")

    _add_chart_page(pdf, "MySQL Compatibility",
                    _chart_compat(metrics.get("compat_checks", {})),
                    f"{metrics.get('compat_checks',{}).get('passed','—')} / "
                    f"{metrics.get('compat_checks',{}).get('total','—')} checks passed "
                    f"({metrics.get('compat_checks',{}).get('pct','—')}% compatible).")

    _add_chart_page(pdf, "Data Import Speed",
                    _chart_import(metrics),
                    "Bulk load throughput comparison: Batched INSERT, "
                    "LOAD DATA LOCAL INFILE, and IMPORT INTO (TiDB native loader).")

    # TCO page
    pdf.add_page()
    pdf.section_title("3-Year Total Cost of Ownership")
    tco_fig = make_tco_chart(tco_data)
    pdf.embed_figure(tco_fig, w=174)
    npv = tco_data["npv"]
    pdf.body_text(
        f"3-year TCO: Aurora MySQL + Sharding ${npv['aurora_3yr']:,} vs "
        f"TiDB Cloud ${npv['tidb_3yr']:,}. "
        f"Projected savings: ${npv['savings']:,} ({npv['savings_pct']:.0f}%). "
        "Engineering overhead of maintaining manual sharding accounts for a "
        "significant portion of Aurora's total cost.",
        size=9,
    )

    # Vector search (if available)
    mod8 = metrics["modules"].get("08_vector_search", {})
    if mod8.get("status") == "passed":
        ann = mod8.get("tidb", {})
        _add_chart_page(pdf, "Vector Search (AI Track)",
                        _chart_vector(ann),
                        "ANN search latency (cosine distance, HNSW index) "
                        "at increasing concurrency levels.")

    # ── Appendix: raw latency table ───────────────────────────────────────────
    _add_kpi_appendix_page(pdf, metrics)

    # ── Appendix: raw latency table ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Appendix — Raw Latency Statistics")
    col_w = [55, 22, 18, 18, 18, 18, 18, 18]
    hdrs  = ["Module / Phase", "Count", "Avg ms", "p50", "p95", "p99", "Max", "TPS"]
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_fill_color(*RED)
    pdf.set_text_color(*WHITE)
    for w, h in zip(col_w, hdrs):
        pdf.cell(w, 6, h, border=1, fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 7)

    for mod_key in MODULE_DISPLAY:
        mod  = metrics["modules"].get(mod_key, {})
        tidb = mod.get("tidb", {})
        for phase, s in tidb.items():
            pdf.set_fill_color(*LIGHT_GREY)
            row = [
                f"{mod_key[:18]}/{phase[:15]}",
                str(s.get("count", "")),
                f"{s.get('avg_ms',0):.1f}",
                f"{s.get('p50_ms',0):.1f}",
                f"{s.get('p95_ms',0):.1f}",
                f"{s.get('p99_ms',0):.1f}",
                f"{s.get('max_ms',0):.1f}",
                f"{s.get('tps',0):.0f}",
            ]
            for w, cell in zip(col_w, row):
                pdf.cell(w, 5, cell, border=1, fill=True)
            pdf.ln()

    pdf.output(out_path)
    print(f"  Report written to: {out_path}")
    return out_path


def _add_chart_page(pdf, title, fig, caption=""):
    pdf.add_page()
    pdf.section_title(title)
    pdf.embed_figure(fig, w=174)
    if caption:
        pdf.body_text(caption, size=8)


def _chart_vector(ann_data) -> plt.Figure:
    concs = []
    p99s  = []
    qpss  = []
    for k, v in sorted(ann_data.items()):
        if k.startswith("ann_conc") and isinstance(v, dict):
            try:
                concs.append(int(k.replace("ann_conc", "")))
                p99s.append(v.get("p99_ms", 0))
                qpss.append(v.get("tps", 0))
            except Exception:
                pass
    if not concs:
        return _empty_chart("No vector search data")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
    ax1.plot(concs, p99s, "o-", color=_rgb(PURPLE), lw=2, markersize=7)
    ax1.set_xlabel("Concurrency")
    ax1.set_ylabel("p99 Latency (ms)")
    ax1.set_title("ANN Search p99 Latency")
    ax1.grid(alpha=0.3)

    ax2.bar(concs, qpss, color=_rgb(PURPLE))
    ax2.set_xlabel("Concurrency")
    ax2.set_ylabel("QPS")
    ax2.set_title("ANN Search QPS")
    ax2.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    return fig


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    cfg = {}
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
    out = generate(cfg)
    print(f"PDF report: {out}")
