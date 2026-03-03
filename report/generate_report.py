#!/usr/bin/env python3
"""
generate_report.py — Produces a professional PDF PoV results report.

Layout:
  Page 1:  Cover + Executive Summary (KPI cards)
  Page 2:  Module status table
  Page 3:  Baseline OLTP performance charts (latency + TPS by concurrency)
  Page 4:  Elastic scale time-series chart
  Page 5:  HA recovery chart (RTO visualisation)
  Page 6:  Write contention comparison (sequential vs AUTO_RANDOM)
  Page 7:  HTAP chart (OLTP-only vs HTAP p99)
  Page 8:  MySQL compatibility heatmap
  Page 9:  Data import comparison bar chart
  Page 10: TCO model (3-year cost comparison)
  Page 11: (Optional) Vector search QPS chart
  Page 12: Appendix — raw latency table

Usage:
    python report/generate_report.py [config.yaml]
"""
import sys, os, json, time, io
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
                  f"Confidential | {time.strftime('%B %Y')} | Page {self.page_no()}",
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
    phases = sorted(tidb.keys())
    concs  = []
    p99s   = []
    tpss   = []
    for ph in phases:
        s = tidb[ph]
        try:
            concs.append(int(ph.lstrip("c")))
        except Exception:
            concs.append(ph)
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
                 label="Comparison DB")
    ax1.set_xlabel("Concurrency")
    ax1.set_ylabel("p99 Latency (ms)")
    ax1.set_title("p99 Latency vs Concurrency")
    ax1.legend()
    ax1.grid(alpha=0.3)

    ax2.bar(x - 0.2, tpss, 0.35, label="TiDB Cloud", color=_rgb(BLUE))
    if comp_tpss:
        ax2.bar(x + 0.2, comp_tpss, 0.35, label="Comparison DB", color=_rgb(ORANGE))
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(c) for c in concs])
    ax2.set_xlabel("Concurrency")
    ax2.set_ylabel("Transactions / sec")
    ax2.set_title("Throughput (TPS) vs Concurrency")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

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
        return _empty_chart("No elastic scale data")

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
    for ph in ["warmup", "during_failure", "recovery"]:
        if ph in ts:
            all_ts.extend(ts[ph])
    if not all_ts:
        return _empty_chart("No HA data")

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
        return _empty_chart("No write contention data")

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
        return _empty_chart("No HTAP data")

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
        return _empty_chart("No compatibility data")

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
        return _empty_chart("No compatibility data")

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
        return _empty_chart("No import data")

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


def _empty_chart(title: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.text(0.5, 0.5, f"No data — {title}", ha="center", va="center",
            transform=ax.transAxes, fontsize=12, color="grey")
    ax.axis("off")
    return fig


def _rgb(color_tuple):
    return tuple(c / 255 for c in color_tuple)


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

    cards = [
        ("Best p99 Latency",    _fmt(summary.get("best_p99_ms"), 1), "ms",       BLUE),
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
    start_x = pdf.l_margin
    y0 = pdf.get_y()
    for i, (label, value, unit, color) in enumerate(cards):
        col = i % 6
        row = i // 6
        pdf.kpi_card(start_x + col * (col_w + 3), y0 + row * (col_h + 4),
                     col_w, col_h, label, value, unit, color)
    pdf.set_y(y0 + col_h + 10)

    # Intro paragraph
    pdf.body_text(
        "This report summarises the results of a self-service Proof of Value "
        "conducted on TiDB Cloud. The tests were executed automatically using "
        "the TiDB Cloud PoV Kit and cover OLTP performance, elastic auto-scaling, "
        "high availability, write contention, HTAP, online DDL, MySQL compatibility, "
        "data import speed, and total cost of ownership.",
        size=9,
    )

    # ── Page 2: Module status table ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Test Module Status")
    module_display = {
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
    col_widths = [80, 22, 22, 50]
    headers    = ["Module", "Status", "Duration", "Notes"]
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_fill_color(*RED)
    pdf.set_text_color(*WHITE)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, fill=True)
    pdf.ln()
    pdf.set_text_color(0, 0, 0)

    for mod_key, mod_label in module_display.items():
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

    # ── Pages 3-10: Charts ────────────────────────────────────────────────────
    _add_chart_page(pdf, "Baseline OLTP Performance",
                    _chart_baseline(metrics),
                    "OLTP workload across concurrency levels (c8–c64). "
                    "Shows p99 latency and transactions per second.")

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

    for mod_key in module_display:
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
