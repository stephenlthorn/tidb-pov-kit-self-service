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
  Page 10: SQL compatibility heatmap
  Page 11: Data import comparison bar chart
  Page 12: TCO model (3-year cost comparison)
  Page 13: (Optional) Vector search QPS chart
  Page 14: Appendix — raw latency table

Usage:
    python report/generate_report.py [config.yaml]
"""
import sys, os, json, time, io, re, runpy
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
from lib.industry_profiles import get_industry_profile

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
    "06_mysql_compat":        "M6 — SQL Compatibility",
    "07_data_import":         "M7 — Data Import Speed",
    "08_vector_search":       "M8 — Vector Search (AI Track)",
}

MODULE_SCOPE_SUMMARY = {
    "00_customer_queries": "Runs customer SQL samples with EXPLAIN checks to validate immediate query portability.",
    "01_baseline_perf": "Measures warm OLTP p99 latency and throughput across configured concurrency levels.",
    "02_elastic_scale": "Ramps load and records latency/throughput response under demand changes.",
    "03_high_availability": "Runs an application-level failure-window drill to estimate recovery behavior.",
    "03b_write_contention": "Compares sequential keys vs AUTO_RANDOM under write contention.",
    "04_htap_concurrent": "Runs transactional and analytical workloads concurrently to validate HTAP isolation.",
    "05_online_ddl": "Executes schema changes while workload traffic continues.",
    "06_mysql_compat": "Executes SQL compatibility checks and reports failed statements with fix guidance.",
    "07_data_import": "Compares bulk ingest paths and reports best observed import throughput.",
    "08_vector_search": "Measures ANN query behavior for the AI/vector workload track.",
}

KPI_THRESHOLDS_BY_TIER = {
    # Starter / Serverless: lowest baseline expectation.
    "serverless": {
        "01_baseline_perf": {"p99_ms_warn": 120.0, "p99_ms_fail": 280.0, "tps_warn": 250.0},
        "02_elastic_scale": {"p99_ms_warn": 180.0, "p99_ms_fail": 380.0, "tps_warn": 200.0},
        "03_high_availability": {"p99_ms_warn": 240.0, "p99_ms_fail": 550.0},
        "03b_write_contention": {"p99_ms_warn": 220.0, "p99_ms_fail": 450.0},
        "04_htap_concurrent": {"p99_ms_warn": 220.0, "p99_ms_fail": 450.0},
        "05_online_ddl": {"p99_ms_warn": 280.0, "p99_ms_fail": 600.0},
        "07_data_import": {"tps_warn": 60.0},
    },
    # Essential: balanced production baseline.
    "essential": {
        "01_baseline_perf": {"p99_ms_warn": 80.0, "p99_ms_fail": 200.0, "tps_warn": 500.0},
        "02_elastic_scale": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0, "tps_warn": 350.0},
        "03_high_availability": {"p99_ms_warn": 200.0, "p99_ms_fail": 500.0},
        "03b_write_contention": {"p99_ms_warn": 150.0, "p99_ms_fail": 400.0},
        "04_htap_concurrent": {"p99_ms_warn": 150.0, "p99_ms_fail": 400.0},
        "05_online_ddl": {"p99_ms_warn": 200.0, "p99_ms_fail": 500.0},
        "07_data_import": {"tps_warn": 100.0},
    },
    # Premium and higher: stricter expectations.
    "premium": {
        "01_baseline_perf": {"p99_ms_warn": 50.0, "p99_ms_fail": 140.0, "tps_warn": 800.0},
        "02_elastic_scale": {"p99_ms_warn": 90.0, "p99_ms_fail": 220.0, "tps_warn": 600.0},
        "03_high_availability": {"p99_ms_warn": 150.0, "p99_ms_fail": 380.0},
        "03b_write_contention": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "04_htap_concurrent": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "05_online_ddl": {"p99_ms_warn": 160.0, "p99_ms_fail": 420.0},
        "07_data_import": {"tps_warn": 150.0},
    },
    "dedicated": {
        "01_baseline_perf": {"p99_ms_warn": 35.0, "p99_ms_fail": 100.0, "tps_warn": 1200.0},
        "02_elastic_scale": {"p99_ms_warn": 70.0, "p99_ms_fail": 180.0, "tps_warn": 900.0},
        "03_high_availability": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "03b_write_contention": {"p99_ms_warn": 90.0, "p99_ms_fail": 220.0},
        "04_htap_concurrent": {"p99_ms_warn": 90.0, "p99_ms_fail": 220.0},
        "05_online_ddl": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "07_data_import": {"tps_warn": 220.0},
    },
    # BYOC follows dedicated-like expectations in this model.
    "byoc": {
        "01_baseline_perf": {"p99_ms_warn": 35.0, "p99_ms_fail": 100.0, "tps_warn": 1200.0},
        "02_elastic_scale": {"p99_ms_warn": 70.0, "p99_ms_fail": 180.0, "tps_warn": 900.0},
        "03_high_availability": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "03b_write_contention": {"p99_ms_warn": 90.0, "p99_ms_fail": 220.0},
        "04_htap_concurrent": {"p99_ms_warn": 90.0, "p99_ms_fail": 220.0},
        "05_online_ddl": {"p99_ms_warn": 120.0, "p99_ms_fail": 300.0},
        "07_data_import": {"tps_warn": 220.0},
    },
}

COMPAT_FIX_GUIDANCE = {
    "DDL": "Review unsupported DDL syntax or option mismatch; prefer TiDB-supported DDL forms and rerun migration DDL lint.",
    "DML": "Check SQL mode and implicit casts; update statements to explicit types and deterministic upsert semantics.",
    "QUERY": "Validate optimizer hints/window usage and verify index coverage for the failing query shape.",
    "FUNCTION": "Replace unsupported function variants with TiDB-compatible equivalents or computed columns.",
    "JSON": "Validate JSON path syntax and return-type expectations; normalize JSON extraction logic.",
    "TRANSACTION": "Align transaction isolation and retry handling with TiDB optimistic/pessimistic behavior.",
    "PREPARED_STMT": "Ensure driver/server-side prepare compatibility and avoid unsupported multi-statement prepare patterns.",
    "INFORMATION_SCHEMA": "Map metadata queries to TiDB information_schema differences.",
    "SHOW": "Adjust SHOW output expectations; some variables/status fields differ across engines.",
    "EXPLAIN": "Use TiDB EXPLAIN/EXPLAIN ANALYZE formats and update plan parser assumptions.",
    "SOURCE_MYSQL": "Inventory and remediate MySQL source features not directly supported in TiDB before migration cutover.",
    "SOURCE_POSTGRES": "Inventory PostgreSQL-specific objects and plan app/schema rewrites for TiDB compatibility.",
    "SOURCE_MSSQL": "Inventory SQL Server-specific features and replace them with TiDB-compatible schema + application patterns.",
    "UNCATEGORIZED": "Review failing SQL text and error note; patch SQL/driver behavior and retest.",
}
_COMPAT_NAME_TO_CATEGORY = None


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

    def _fit_single_line_text(self, text: str, max_width: float, style: str, start_size: float, min_size: float = 7.0):
        size = start_size
        self.set_font("Helvetica", style, size)
        while size > min_size and self.get_string_width(text) > max_width:
            size -= 0.5
            self.set_font("Helvetica", style, size)
        if self.get_string_width(text) <= max_width:
            return text, size

        clip = text
        while len(clip) > 1:
            clip = clip[:-1]
            candidate = f"{clip}..."
            if self.get_string_width(candidate) <= max_width:
                return candidate, size
        return "...", size

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
        value_text = str(value)
        fitted_text, fitted_size = self._fit_single_line_text(
            value_text,
            max_width=max(6.0, w - 4),
            style="B",
            start_size=16.0,
            min_size=6.0,
        )
        self.set_font("Helvetica", "B", fitted_size)
        self.set_text_color(*color)
        self.cell(w - 4, 8, fitted_text)
        # Unit
        self.set_xy(x + 2, y + 17)
        unit_text, unit_size = self._fit_single_line_text(
            str(unit or ""),
            max_width=max(6.0, w - 4),
            style="I",
            start_size=7.0,
            min_size=6.0,
        )
        self.set_font("Helvetica", "I", unit_size)
        self.set_text_color(*DARK_GREY)
        self.cell(w - 4, 4, unit_text)
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


def _split_value_bullets(text: str, max_items: int = 4) -> list[str]:
    raw = str(text or "").replace("\n", " ").strip()
    if not raw:
        return ["No validated value captured in this run."]
    parts = [p.strip(" .") for p in re.split(r";|\.\s+", raw) if p and p.strip(" .")]
    if not parts:
        parts = [raw]
    return parts[:max_items]


def _safe_table_lines(pdf: PoVReport, text: str, width: float, line_h: float, style: str, size: float) -> list[str]:
    content = pdf.normalize_text(str(text or ""))
    pdf.set_font("Helvetica", style, size)
    usable_w = max(4.0, width)
    try:
        lines = pdf.multi_cell(usable_w, line_h, content, dry_run=True, output="LINES")
    except Exception:
        lines = [content]
    if not lines:
        return [""]
    return [pdf.normalize_text(str(line)) for line in lines]


def _draw_wrapped_table_row(
    pdf: PoVReport,
    col_widths: list[float],
    cells: list[str],
    *,
    styles: list[str] | None = None,
    font_sizes: list[float] | None = None,
    text_colors: list[tuple[int, int, int]] | None = None,
    aligns: list[str] | None = None,
    fill_color: tuple[int, int, int] = WHITE,
    line_h: float = 3.8,
    padding: float = 0.8,
) -> bool:
    n = len(col_widths)
    styles = list(styles or ([""] * n))
    font_sizes = list(font_sizes or ([7.0] * n))
    text_colors = list(text_colors or ([(0, 0, 0)] * n))
    aligns = list(aligns or (["L"] * n))
    if len(cells) < n:
        cells = list(cells) + ([""] * (n - len(cells)))

    wrapped = []
    max_lines = 1
    for idx, width in enumerate(col_widths):
        lines = _safe_table_lines(
            pdf,
            str(cells[idx]),
            width - (padding * 2),
            line_h,
            styles[idx] if idx < len(styles) else "",
            font_sizes[idx] if idx < len(font_sizes) else 7.0,
        )
        wrapped.append(lines)
        if len(lines) > max_lines:
            max_lines = len(lines)

    row_h = max_lines * line_h + (padding * 2)
    if pdf.get_y() + row_h > (pdf.h - pdf.b_margin):
        return False

    y0 = pdf.get_y()
    x0 = pdf.l_margin
    pdf.set_x(x0)
    for idx, width in enumerate(col_widths):
        x = pdf.get_x()
        pdf.set_fill_color(*fill_color)
        pdf.rect(x, y0, width, row_h, style="FD")
        pdf.set_xy(x + padding, y0 + padding)
        pdf.set_font(
            "Helvetica",
            styles[idx] if idx < len(styles) else "",
            font_sizes[idx] if idx < len(font_sizes) else 7.0,
        )
        color = text_colors[idx] if idx < len(text_colors) else (0, 0, 0)
        pdf.set_text_color(*color)
        pdf.multi_cell(
            width - (padding * 2),
            line_h,
            "\n".join(wrapped[idx]),
            border=0,
            align=aligns[idx] if idx < len(aligns) else "L",
            new_x=XPos.RIGHT,
            new_y=YPos.TOP,
        )
        pdf.set_xy(x + width, y0)

    pdf.set_xy(x0, y0 + row_h)
    pdf.set_text_color(0, 0, 0)
    return True


def _draw_wrapped_table_header(pdf: PoVReport, col_widths: list[float], headers: list[str]) -> bool:
    return _draw_wrapped_table_row(
        pdf,
        col_widths,
        headers,
        styles=["B"] * len(col_widths),
        font_sizes=[7.5] * len(col_widths),
        text_colors=[WHITE] * len(col_widths),
        aligns=["L"] * len(col_widths),
        fill_color=RED,
        line_h=3.9,
        padding=0.8,
    )


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


def _stitch_phase_series(ts_map: dict, phase_order: list[str], fallback_alias: dict | None = None) -> list[dict]:
    fallback_alias = fallback_alias or {}
    merged = []
    offset = 0.0
    for phase in phase_order:
        rows = ts_map.get(phase, [])
        if not rows and phase in fallback_alias:
            rows = ts_map.get(fallback_alias[phase], [])
        if not rows:
            continue
        local_elapsed = [float(r.get("elapsed_sec", 0) or 0) for r in rows]
        local_step = 0.0
        if len(local_elapsed) > 1:
            local_step = max(local_elapsed[1] - local_elapsed[0], 0.0)
        elif local_elapsed:
            local_step = max(local_elapsed[0], 0.0)
        for row in rows:
            merged.append({
                "elapsed_sec": offset + float(row.get("elapsed_sec", 0) or 0),
                "p99_ms": float(row.get("p99_ms", 0) or 0),
                "tps": float(row.get("tps", 0) or 0),
            })
        offset += max(local_elapsed[-1] if local_elapsed else 0.0, 0.0) + max(local_step, 1.0)
    return merged


def _chart_scale(metrics) -> plt.Figure:
    mod  = metrics["modules"].get("02_elastic_scale", {})
    ts   = mod.get("time_series", {})
    all_ts = _stitch_phase_series(ts, ["ramp_up", "sustain", "ramp_down"])
    if not all_ts and isinstance(ts, dict):
        dynamic_order = []
        for prefix in ("ramp_up", "sustain", "ramp_down"):
            keys = sorted(
                [
                    k for k, v in ts.items()
                    if isinstance(v, list) and (k == prefix or str(k).startswith(f"{prefix}_"))
                ]
            )
            dynamic_order.extend(keys)
        if dynamic_order:
            all_ts = _stitch_phase_series(ts, dynamic_order)
    if not all_ts:
        reason, actions = _module_missing_reason(metrics, "02_elastic_scale", "No elastic scale data")
        return _empty_chart("Elastic scale data unavailable", reason=reason, actions=actions)

    elapsed = [r["elapsed_sec"] for r in all_ts]
    p99     = [r["p99_ms"]      for r in all_ts]
    tps     = [r["tps"]         for r in all_ts]

    elapsed_np = np.array(elapsed, dtype=float)
    p99_np = np.array(p99, dtype=float)
    tps_np = np.array(tps, dtype=float)
    baseline_tps = max(float(np.percentile(tps_np, 20)) if len(tps_np) else 1.0, 1.0)
    inferred_capacity = np.maximum(1.0, tps_np / baseline_tps)
    # Guard short series: np.convolve(mode="same") returns max(M, N), which can
    # produce a length mismatch when the kernel is longer than the signal.
    window = min(3, int(len(inferred_capacity)))
    if window <= 1:
        smoothed_capacity = inferred_capacity.copy()
    else:
        smoothed_capacity = np.convolve(inferred_capacity, np.ones(window) / float(window), mode="same")
    if len(smoothed_capacity) != len(elapsed_np):
        if len(smoothed_capacity) > len(elapsed_np):
            smoothed_capacity = smoothed_capacity[: len(elapsed_np)]
        elif len(smoothed_capacity) > 0:
            pad_len = len(elapsed_np) - len(smoothed_capacity)
            smoothed_capacity = np.pad(smoothed_capacity, (0, pad_len), mode="edge")
        else:
            smoothed_capacity = np.ones_like(elapsed_np)
    if len(elapsed_np) > 1:
        step_sec = max(elapsed_np[1] - elapsed_np[0], 1.0)
    else:
        step_sec = 10.0
    cumulative_capacity_hours = np.cumsum(smoothed_capacity) * (step_sec / 3600.0)

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(13, 8), sharex=True)
    ax1.plot(elapsed, p99, color=_rgb(RED), lw=1.5)
    ax1.set_ylabel("p99 (ms)")
    ax1.set_title("Elastic Auto-Scaling: Performance and Pay-as-You-Grow Signal")
    ax1.grid(alpha=0.3)
    ax2.fill_between(elapsed, tps, alpha=0.3, color=_rgb(BLUE))
    ax2.plot(elapsed, tps, color=_rgb(BLUE), lw=1.5)
    ax2.set_ylabel("TPS")
    ax2.grid(alpha=0.3)

    ax3.step(elapsed, smoothed_capacity, where="mid", color=_rgb(GREEN), lw=1.8, label="Inferred compute index")
    ax3.fill_between(elapsed, smoothed_capacity, alpha=0.2, color=_rgb(GREEN))
    ax3_twin = ax3.twinx()
    ax3_twin.plot(elapsed, cumulative_capacity_hours, color=_rgb(ORANGE), lw=1.2, linestyle="--", label="Cumulative capacity-hours")
    ax3.set_xlabel("Elapsed (seconds)")
    ax3.set_ylabel("Capacity index")
    ax3_twin.set_ylabel("Capacity-hours")
    ax3.grid(alpha=0.25)
    h1, l1 = ax3.get_legend_handles_labels()
    h2, l2 = ax3_twin.get_legend_handles_labels()
    ax3.legend(h1 + h2, l1 + l2, loc="upper left")

    if len(tps_np):
        ax3.text(
            0.99,
            0.03,
            "Right axis is inferred from TPS time-series (not billing meter).",
            transform=ax3.transAxes,
            ha="right",
            va="bottom",
            fontsize=8,
            color="#475569",
        )
    fig.tight_layout()
    return fig


def _safe_chart_render(title: str, render_fn) -> plt.Figure:
    try:
        fig = render_fn()
        if fig is None:
            raise ValueError("renderer returned no figure")
        return fig
    except Exception as exc:
        return _empty_chart(
            f"{title} unavailable",
            reason=f"Chart renderer error: {type(exc).__name__}: {exc}",
            actions=[
                "Re-run report after metrics collection to refresh chart inputs.",
                "Review results/run_all.log for module-level chart input errors.",
            ],
        )


def _chart_ha(metrics) -> plt.Figure:
    mod = metrics["modules"].get("03_high_availability", {})
    ts  = mod.get("time_series", {})
    warm_rows = ts.get("warmup", []) if isinstance(ts, dict) else []
    warm_elapsed = [float(r.get("elapsed_sec", 0) or 0) for r in warm_rows]
    warm_step = max((warm_elapsed[1] - warm_elapsed[0]) if len(warm_elapsed) > 1 else 1.0, 1.0) if warm_elapsed else 1.0
    failure_marker_x = (max(warm_elapsed) + warm_step) if warm_elapsed else 30.0
    all_ts = _stitch_phase_series(ts, ["warmup", "failure", "recovery"], fallback_alias={"failure": "during_failure"})
    if not all_ts:
        reason, actions = _module_missing_reason(metrics, "03_high_availability", "No HA data")
        return _empty_chart("High-availability data unavailable", reason=reason, actions=actions)

    elapsed = [r["elapsed_sec"] for r in all_ts]
    p99     = [r["p99_ms"]      for r in all_ts]

    fig, ax = plt.subplots(figsize=(13, 4))
    ax.plot(elapsed, p99, color=_rgb(BLUE), lw=1.5, label="p99 Latency")
    ax.axvline(x=failure_marker_x, color=_rgb(RED), linestyle="--", label="Simulated failure window")
    ax.set_xlabel("Elapsed (seconds)")
    ax.set_ylabel("p99 Latency (ms)")
    ax.set_title("Availability Drill: Simulated Failure Window and Recovery")
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

    t_flash = tidb.get("analytics_tiflash", {}) if isinstance(tidb, dict) else {}
    t_kv = tidb.get("analytics_tikv", {}) if isinstance(tidb, dict) else {}
    has_engine_compare = bool(t_flash) and bool(t_kv)

    if has_engine_compare:
        fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))
    else:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax2 = None

    ax.bar(x - 0.2, only_v, 0.35, label="OLTP only", color=_rgb(BLUE))
    ax.bar(x + 0.2, htap_v, 0.35, label="OLTP + Analytics", color=_rgb(PURPLE))
    ax.set_xticks(x)
    ax.set_xticklabels([k.upper() for k in labels])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("OLTP Latency with Concurrent Analytics")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    if ax2 is not None:
        engine_labels = ["TiFlash", "TiKV"]
        p95_vals = [t_flash.get("p95_ms", 0), t_kv.get("p95_ms", 0)]
        p99_vals = [t_flash.get("p99_ms", 0), t_kv.get("p99_ms", 0)]
        tps_vals = [t_flash.get("tps", 0), t_kv.get("tps", 0)]
        xx = np.arange(len(engine_labels))
        ax2.bar(xx - 0.22, p95_vals, 0.22, label="p95 (ms)", color=_rgb(GREEN))
        ax2.bar(xx, p99_vals, 0.22, label="p99 (ms)", color=_rgb(ORANGE))
        ax2_twin = ax2.twinx()
        ax2_twin.plot(xx + 0.22, tps_vals, "o--", color=_rgb(BLUE), lw=1.8, label="TPS")
        ax2.set_xticks(xx)
        ax2.set_xticklabels(engine_labels)
        ax2.set_ylabel("Latency (ms)")
        ax2_twin.set_ylabel("TPS")
        ax2.set_title("OLAP Engine Comparison")
        ax2.grid(axis="y", alpha=0.3)
        h1, l1 = ax2.get_legend_handles_labels()
        h2, l2 = ax2_twin.get_legend_handles_labels()
        ax2.legend(h1 + h2, l1 + l2, loc="upper left")

    fig.tight_layout()
    return fig


def _chart_compat(compat_data) -> plt.Figure:
    details = compat_data.get("details", [])
    if not details:
        return _empty_chart(
            "Compatibility data unavailable",
            reason="No compatibility checks were captured.",
            actions=[
                "Enable module M6 (SQL compatibility) and run again.",
                "Confirm compat_checks entries exist in results.db before report build.",
            ],
        )

    normalized = []
    for row in details:
        if not isinstance(row, dict):
            continue
        category = _compat_category(row)
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
    display_labels = [c.replace("_", " ") for c in cats]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(x, [pass_cnt[c] for c in cats], label="Pass", color=_rgb(GREEN))
    ax.bar(x, [fail_cnt[c] for c in cats], bottom=[pass_cnt[c] for c in cats],
           label="Fail", color=_rgb(RED))
    ax.set_xticks(x)
    ax.set_xticklabels(display_labels, rotation=20, ha="right")
    ax.set_ylabel("Check count")
    ax.set_title("SQL Compatibility by Category")
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


def _compat_category(row: dict) -> str:
    if not isinstance(row, dict):
        return "UNCATEGORIZED"
    raw = str(row.get("category") or row.get("group") or row.get("module") or "").strip()
    if raw:
        return raw.upper().replace(" ", "_")
    name = str(row.get("check_name") or row.get("name") or "").strip()
    if name:
        cat_map = _load_compat_name_to_category()
        if name in cat_map:
            return str(cat_map[name]).upper().replace(" ", "_")
    if name and " - " in name:
        return name.split(" - ", 1)[0].strip().upper().replace(" ", "_")
    return "UNCATEGORIZED"


def _load_compat_name_to_category() -> dict:
    global _COMPAT_NAME_TO_CATEGORY
    if _COMPAT_NAME_TO_CATEGORY is not None:
        return _COMPAT_NAME_TO_CATEGORY
    mapping = {}
    try:
        path = os.path.join(os.path.dirname(__file__), "..", "tests", "06_mysql_compat", "run.py")
        loaded = runpy.run_path(path)
        checks = loaded.get("COMPAT_CHECKS") or []
        for row in checks:
            if isinstance(row, (list, tuple)) and len(row) >= 2:
                category = str(row[0] or "").strip()
                name = str(row[1] or "").strip()
                if category and name:
                    mapping[name] = category
    except Exception:
        mapping = {}
    _COMPAT_NAME_TO_CATEGORY = mapping
    return _COMPAT_NAME_TO_CATEGORY


def _compat_fix_text(category: str, note: str = "") -> str:
    key = str(category or "UNCATEGORIZED").upper()
    base = COMPAT_FIX_GUIDANCE.get(key, COMPAT_FIX_GUIDANCE["UNCATEGORIZED"])
    clean_note = str(note or "").strip()
    if clean_note:
        return f"{base} Last error: {clean_note[:90]}"
    return base


def _warm_stability_comment(metrics: dict) -> str:
    mod = (metrics.get("modules", {}) or {}).get("01_baseline_perf", {}) or {}
    ts = (mod.get("time_series", {}) or {}).get("warm_steady", []) if isinstance(mod.get("time_series"), dict) else []
    if not ts:
        return "Warm stability interpretation unavailable: no warm_steady time-series was captured."

    p99_vals = [float(r.get("p99_ms", 0) or 0) for r in ts if r.get("p99_ms") is not None]
    tps_vals = [float(r.get("tps", 0) or 0) for r in ts if r.get("tps") is not None]
    if len(p99_vals) < 3:
        return "Warm phase captured limited samples; rerun with longer warm duration for stronger confidence."

    p99_min = max(min(p99_vals), 0.1)
    p99_max = max(p99_vals)
    spike_ratio = p99_max / p99_min
    tps_avg = sum(tps_vals) / len(tps_vals) if tps_vals else 0.0
    tps_min = min(tps_vals) if tps_vals else 0.0
    tps_drop_pct = (1 - (tps_min / tps_avg)) * 100 if tps_avg > 0 else 0.0

    if spike_ratio <= 1.25 and tps_drop_pct <= 10:
        level = "stable"
        meaning = "good: latency variation stayed tight while throughput remained consistent."
    elif spike_ratio <= 1.8 and tps_drop_pct <= 20:
        level = "moderate spikes"
        meaning = "acceptable for PoV; usually caused by compaction, cache warming, or concurrent background work."
    else:
        level = "high spikes"
        meaning = "risk signal: sustained workload likely needs tier/concurrency/index tuning before production."

    return (
        f"Warm phase showed {level}. p99 min={p99_min:.1f}ms, max={p99_max:.1f}ms "
        f"({spike_ratio:.2f}x), lowest TPS dip={tps_drop_pct:.1f}%. This is {meaning}"
    )


def _module_interpretation(metrics: dict, module_key: str) -> tuple[str, str, str]:
    mod = (metrics.get("modules", {}) or {}).get(module_key, {}) or {}
    status = str(mod.get("status") or "not_run").lower()
    tidb = mod.get("tidb", {}) if isinstance(mod.get("tidb"), dict) else {}
    summary = metrics.get("summary", {}) if isinstance(metrics.get("summary"), dict) else {}

    if status != "passed":
        return (
            status.upper(),
            "No validated data captured in this run.",
            "No business conclusion for this module. Rerun or enable module to produce customer evidence.",
        )

    if module_key == "01_baseline_perf":
        warm = tidb.get("warm_steady", {}) if isinstance(tidb, dict) else {}
        p99 = warm.get("p99_ms")
        tps = warm.get("tps")
        max_qps = _maybe_float(summary.get("max_qps"))
        avg_qps = _maybe_float(summary.get("avg_qps"))
        qps_line = ""
        if max_qps is not None and avg_qps is not None:
            qps_line = f" | max/avg QPS={max_qps:.0f}/{avg_qps:.0f}"
        return (
            f"Warm p99={p99:.1f}ms, TPS={tps:.0f}{qps_line}" if p99 and tps else "Baseline phases captured",
            _warm_stability_comment(metrics),
            "Defines steady-state user experience and sets SLA-confidence baseline for migration approval.",
        )
    if module_key == "02_elastic_scale":
        ts = (mod.get("time_series", {}) or {})
        points = sum(len(v) for v in ts.values() if isinstance(v, list))
        return (
            f"{points} time buckets captured across ramp phases",
            "Load increased through ramp/sustain/ramp_down while tracking p99 and TPS to verify control under demand growth.",
            "Supports pay-as-you-grow positioning: scale when needed while keeping latency predictable.",
        )
    if module_key == "03_high_availability":
        notes = str(mod.get("notes") or "")
        rto = ""
        m = re.search(r"RTO[=: ]+([0-9.]+)", notes)
        if m:
            rto = f"Estimated simulated RTO {float(m.group(1)):.1f}s"
        return (
            rto or "Simulated failure-window drill completed",
            "This is a simulated connection-failure drill, not a physical TiKV node kill on cloud control plane.",
            "Shows application-side resilience behavior; use backup+restore drill for customer-facing RTO evidence.",
        )
    if module_key == "03b_write_contention":
        seq = tidb.get("sequential", {})
        ar = tidb.get("autorand", {})
        seq_p99 = float(seq.get("p99_ms", 0) or 0)
        ar_p99 = float(ar.get("p99_ms", 0) or 0)
        if seq_p99 > 0 and ar_p99 > 0:
            gain = (seq_p99 - ar_p99) / seq_p99 * 100
            signal = f"p99 improvement with AUTO_RANDOM: {gain:.1f}%"
        else:
            signal = "Sequential vs AUTO_RANDOM comparison captured"
        return (
            signal,
            "Compares hotspot-prone monotonic key pattern vs distributed key allocation to reduce leader pressure.",
            "Demonstrates schema-level tuning that delays expensive sharding/re-architecture work.",
        )
    if module_key == "04_htap_concurrent":
        htap = tidb.get("htap", {})
        only = tidb.get("oltp_only", {})
        t_flash = tidb.get("analytics_tiflash", {})
        t_kv = tidb.get("analytics_tikv", {})
        msg = "OLTP + analytics concurrent test completed"
        if only.get("p99_ms") and htap.get("p99_ms"):
            delta = (float(htap["p99_ms"]) - float(only["p99_ms"])) / max(float(only["p99_ms"]), 0.1) * 100
            msg = f"OLTP p99 delta under analytics: {delta:+.1f}%"
        if t_flash and t_kv and t_flash.get("p99_ms") and t_kv.get("p99_ms"):
            msg += f" | TiFlash vs TiKV OLAP p99: {float(t_flash['p99_ms']):.1f}ms vs {float(t_kv['p99_ms']):.1f}ms"
        return (
            msg,
            "Validates HTAP isolation and compares OLAP query service path on TiFlash versus TiKV.",
            "Enables mixed workload consolidation without forcing separate OLTP and analytics systems.",
        )
    if module_key == "05_online_ddl":
        return (
            "Online schema change workload executed",
            "Measures impact of schema evolution during active traffic.",
            "Reduces migration/change windows and operational downtime risk.",
        )
    if module_key == "06_mysql_compat":
        compat = metrics.get("compat_checks", {}) or {}
        failed = int(compat.get("failed", 0) or 0)
        total = int(compat.get("total", 0) or 0)
        return (
            f"{total - failed}/{total} checks passed" if total else "Compatibility checks executed",
            "Runs MySQL syntax/behavior checks and records exact failing statements with notes.",
            "Accelerates remediation planning by showing migration blockers and concrete fix paths.",
        )
    if module_key == "07_data_import":
        stats = metrics.get("import_stats", []) or []
        best = max((float(s.get("throughput_gbpm", 0) or 0) for s in stats), default=0.0)
        return (
            f"Best observed import throughput: {best:.3f} GB/min",
            "Compares ingest methods to find the fastest loading path for initial migration and bulk refresh.",
            "Directly impacts time-to-value by shrinking dataset onboarding time.",
        )
    if module_key == "08_vector_search":
        return (
            "Vector query profile captured",
            "Measures ANN query latency and throughput under concurrency.",
            "Supports AI search use cases on the same operational platform.",
        )

    return (
        "Module executed",
        "Performance evidence captured for this module.",
        "Provides workload-specific confidence for migration decision-making.",
    )


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


def _industry_display_label(industry_key: str | None) -> str:
    key = str(industry_key or "general_auto").strip().lower() or "general_auto"
    try:
        label = str(get_industry_profile(key).get("label") or "").strip()
        if label:
            return label
    except Exception:
        pass
    return key.replace("_", " ").title()


def _prospect_decision(summary: dict, metrics: dict) -> tuple[str, tuple[int, int, int], list[str], list[str]]:
    modules = (metrics.get("modules", {}) or {})
    failed = [
        MODULE_DISPLAY.get(k, k)
        for k, v in modules.items()
        if str((v or {}).get("status") or "").strip().lower() == "failed"
    ]
    modules_run = int(summary.get("modules_run", 0) or 0)
    modules_passed = int(summary.get("modules_passed", 0) or 0)
    warm_p99 = _maybe_float(summary.get("warm_p99_ms"))
    compat_pct = _maybe_float(summary.get("mysql_compat_pct"))

    findings = []
    actions = []

    if failed:
        findings.append(f"{len(failed)} module(s) failed: {', '.join(failed[:3])}")
        actions.append("Fix failed module(s) first, then rerun the same profile for clean evidence.")

    if compat_pct is not None and compat_pct < 95:
        findings.append(f"SQL compatibility below target ({compat_pct:.1f}% < 95%).")
        actions.append("Resolve failed compatibility checks and retest the affected SQL paths.")

    if warm_p99 is not None and warm_p99 > 40:
        findings.append(f"Warm p99 latency is elevated for first production gate ({warm_p99:.1f}ms).")
        actions.append("Tune indexes/concurrency and consider a higher tier before production sign-off.")

    if modules_run > 0 and modules_passed < modules_run:
        findings.append(f"Coverage incomplete ({modules_passed}/{modules_run} passed).")
        actions.append("Rerun incomplete modules or mark them out of scope with customer agreement.")

    if modules_run <= 2:
        findings.append("Limited module evidence was captured in this run.")
        actions.append("Run the full guided validation profile before migration decisions.")

    if findings:
        return (
            "Needs Tuning Before Next Stage",
            ORANGE,
            findings,
            actions[:4],
        )

    return (
        "Ready for Next PoC Stage",
        GREEN,
        [
            "Core validation modules passed with usable evidence.",
            "No critical blockers were detected in this run profile.",
        ],
        [
            "Proceed to customer workload replay or larger-scale tier validation.",
            "Track the same KPI set in the next stage to confirm consistency.",
        ],
    )


def _add_decision_summary_page(pdf: PoVReport, metrics: dict, cfg: dict):
    summary = metrics.get("summary", {}) or {}
    verdict, verdict_color, findings, actions = _prospect_decision(summary, metrics)
    run_context = metrics.get("run_context", {}) or {}
    source_cfg = (cfg or {}).get("comparison_db", {}) or {}

    pdf.add_page()
    pdf.section_title("Prospect Decision Summary")

    x = pdf.l_margin
    w = pdf.w - pdf.l_margin - pdf.r_margin
    y = pdf.get_y()
    h = 17
    pdf.set_fill_color(*LIGHT_GREY)
    pdf.set_draw_color(*verdict_color)
    pdf.set_line_width(0.8)
    pdf.rect(x, y, w, h, style="FD")
    pdf.set_xy(x + 2, y + 2)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*verdict_color)
    pdf.cell(0, 5, "Decision")
    pdf.set_xy(x + 2, y + 7.5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 7, verdict)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(h + 2)

    pdf.sub_title("First-Time Buyer Questions")
    pdf.body_text(
        "1. Will this workload perform consistently on TiDB Cloud?\n"
        "2. What migration blockers remain and how hard are they to fix?\n"
        "3. What operational risk is removed compared to the current path?",
        size=8,
    )

    pdf.sub_title("Run Evidence Highlights")
    for line in findings:
        _write_wrapped_bullet(pdf, line, size=8)
    if not findings:
        _write_wrapped_bullet(pdf, "No high-risk findings were detected.", size=8)

    pdf.sub_title("Recommended Next Action")
    for line in actions:
        _write_wrapped_bullet(pdf, line, size=8)

    pdf.sub_title("Context")
    tier = _normalize_tier_key((cfg.get("tier", {}) or {}).get("selected"))
    comparison = "enabled" if bool(source_cfg.get("enabled")) else "disabled"
    context_line = (
        f"Run mode: {run_context.get('run_mode', 'n/a')} | "
        f"Schema mode: {run_context.get('schema_mode', 'n/a')} | "
        f"Industry: {_industry_display_label(run_context.get('industry'))} | "
        f"Tier profile: {tier} | Source comparison: {comparison}"
    )
    pdf.body_text(context_line, size=8)


def _add_test_scope_page(pdf: PoVReport, metrics: dict, cfg: dict):
    modules = (metrics.get("modules", {}) or {})
    run_context = metrics.get("run_context", {}) or {}
    summary = metrics.get("summary", {}) or {}
    tidb_cfg = (cfg.get("tidb", {}) or {})
    aws_cfg = (cfg.get("aws_runner", {}) or {})

    pdf.add_page()
    pdf.section_title("What Was Tested")
    pdf.body_text(
        "This section lists exactly what was executed in this run so buyers can map evidence to their own deployment decision.",
        size=8,
    )

    env_bits = [
        f"TiDB host: {tidb_cfg.get('host', 'n/a')}",
        f"DB: {tidb_cfg.get('database', 'n/a')}",
        f"Tier profile: {_normalize_tier_key((cfg.get('tier', {}) or {}).get('selected'))}",
        f"Run mode: {run_context.get('run_mode', 'n/a')}",
        f"Schema mode: {run_context.get('schema_mode', 'n/a')}",
        f"Industry: {_industry_display_label(run_context.get('industry'))}",
    ]
    if aws_cfg.get("enabled"):
        env_bits.append(f"AWS runner region: {aws_cfg.get('aws_region', 'n/a')}")
        env_bits.append(f"AWS instance size profile: {aws_cfg.get('instance_size', 'n/a')}")
    pdf.body_text(" | ".join(env_bits), size=8)

    col_w = [52, 22, 98]
    _draw_wrapped_table_header(pdf, col_w, ["Module", "Status", "What It Tested"])
    for key in MODULE_DISPLAY:
        status = str((modules.get(key, {}) or {}).get("status") or "not_run").strip().lower()
        status_display = status.upper()
        if status == "passed":
            status_color = GREEN
        elif status == "failed":
            status_color = RED
        elif status == "skipped":
            status_color = ORANGE
        else:
            status_color = DARK_GREY
        scope = MODULE_SCOPE_SUMMARY.get(key, "Module scope summary unavailable.")
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_w,
            [MODULE_DISPLAY[key], status_display, scope],
            styles=["", "B", ""],
            font_sizes=[7.0, 7.0, 7.0],
            text_colors=[(0, 0, 0), status_color, (0, 0, 0)],
            aligns=["L", "C", "L"],
            fill_color=WHITE,
            line_h=3.6,
        )
        if not row_ok:
            pdf.add_page()
            pdf.section_title("What Was Tested (Continued)")
            _draw_wrapped_table_header(pdf, col_w, ["Module", "Status", "What It Tested"])
            _draw_wrapped_table_row(
                pdf,
                col_w,
                [MODULE_DISPLAY[key], status_display, scope],
                styles=["", "B", ""],
                font_sizes=[7.0, 7.0, 7.0],
                text_colors=[(0, 0, 0), status_color, (0, 0, 0)],
                aligns=["L", "C", "L"],
                fill_color=WHITE,
                line_h=3.6,
            )
    pdf.ln(2)
    pdf.body_text(
        f"Coverage summary: {summary.get('modules_passed', 0)}/{summary.get('modules_run', 0)} selected modules passed.",
        size=8,
    )


def _add_run_coverage_page(pdf, metrics):
    pdf.add_page()
    pdf.section_title("Run Coverage and Data Completeness")
    summary = metrics.get("summary", {}) or {}
    run_context = metrics.get("run_context", {}) or {}
    manifest = metrics.get("data_manifest", {}) or {}
    rows_generated = 0
    if isinstance(manifest.get("counts"), dict):
        rows_generated = sum(v for v in manifest.get("counts", {}).values() if isinstance(v, (int, float)))

    industry_label = _industry_display_label(run_context.get("industry"))
    pdf.body_text(
        f"Run mode: {run_context.get('run_mode', 'n/a')} | "
        f"Schema mode: {run_context.get('schema_mode', 'n/a')} | "
        f"Industry: {industry_label} | "
        f"Modules passed: {summary.get('modules_passed', 0)}/{summary.get('modules_run', 0)} | "
        f"Generated rows: {rows_generated:,}" if rows_generated else
        f"Run mode: {run_context.get('run_mode', 'n/a')} | "
        f"Schema mode: {run_context.get('schema_mode', 'n/a')} | "
        f"Industry: {industry_label} | "
        f"Modules passed: {summary.get('modules_passed', 0)}/{summary.get('modules_run', 0)}"
    , size=8)

    col_widths = [74, 24, 18, 18, 42]
    headers = ["Module", "Status", "Secs", "Points", "Interpretation"]
    def draw_header(title_suffix: str = ""):
        if title_suffix:
            pdf.section_title(f"Run Coverage and Data Completeness {title_suffix}")
        _draw_wrapped_table_header(pdf, col_widths, headers)

    draw_header()

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
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_widths,
            [MODULE_DISPLAY[key], status.upper(), dur, points, interp],
            styles=["", "B", "", "", ""],
            font_sizes=[7.0, 7.0, 7.0, 7.0, 7.0],
            text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0), (0, 0, 0)],
            aligns=["L", "C", "C", "C", "L"],
            fill_color=LIGHT_GREY,
            line_h=3.7,
        )
        if not row_ok:
            pdf.add_page()
            draw_header("(Continued)")
            _draw_wrapped_table_row(
                pdf,
                col_widths,
                [MODULE_DISPLAY[key], status.upper(), dur, points, interp],
                styles=["", "B", "", "", ""],
                font_sizes=[7.0, 7.0, 7.0, 7.0, 7.0],
                text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                aligns=["L", "C", "C", "C", "L"],
                fill_color=LIGHT_GREY,
                line_h=3.7,
            )

    pdf.ln(4)
    pdf.body_text(
        "Charts in this report always display either measured data or a clear reason and action guidance block.",
        size=8,
    )


def _add_module_interpretation_page(pdf, metrics):
    pdf.add_page()
    pdf.section_title("Module Interpretation — Technical and Business Value")
    pdf.body_text(
        "This page translates each executed module into technical meaning and business impact so non-DB stakeholders can action results quickly.",
        size=8,
    )

    block_w = pdf.w - pdf.l_margin - pdf.r_margin
    inner_w = block_w - 4

    for mod_key in MODULE_DISPLAY:
        status = str(((metrics.get("modules", {}) or {}).get(mod_key, {}) or {}).get("status") or "not_run").lower()
        if status == "not_run":
            continue
        signal, technical, business = _module_interpretation(metrics, mod_key)
        technical_points = _split_value_bullets(technical)
        business_points = _split_value_bullets(business)

        title_lines = _safe_table_lines(pdf, MODULE_DISPLAY[mod_key], inner_w, 4.2, "B", 8)
        observed_lines = _safe_table_lines(pdf, str(signal), inner_w, 4.0, "", 8)
        tech_lines = []
        for item in technical_points:
            tech_lines.extend(_safe_table_lines(pdf, f"- {item}", inner_w, 4.0, "", 8))
        biz_lines = []
        for item in business_points:
            biz_lines.extend(_safe_table_lines(pdf, f"- {item}", inner_w, 4.0, "", 8))

        line_count = (
            len(title_lines)
            + 1 + len(observed_lines)
            + 1 + len(tech_lines)
            + 1 + len(biz_lines)
        )
        block_h = line_count * 4.0 + 6
        if pdf.get_y() + block_h > (pdf.h - pdf.b_margin):
            pdf.add_page()
            pdf.section_title("Module Interpretation — Continued")

        x = pdf.l_margin
        y = pdf.get_y()
        pdf.set_fill_color(*LIGHT_GREY)
        pdf.set_draw_color(210, 210, 210)
        pdf.rect(x, y, block_w, block_h, style="FD")
        pdf.set_xy(x + 2, y + 2)

        pdf.set_text_color(*RED)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_x(x + 2)
        pdf.multi_cell(inner_w, 4.2, MODULE_DISPLAY[mod_key])
        pdf.set_text_color(0, 0, 0)

        pdf.set_font("Helvetica", "B", 8)
        pdf.set_x(x + 2)
        pdf.multi_cell(inner_w, 4.0, "Observed")
        pdf.set_font("Helvetica", "", 8)
        pdf.set_x(x + 2)
        pdf.multi_cell(inner_w, 4.0, str(signal))

        pdf.set_font("Helvetica", "B", 8)
        pdf.set_x(x + 2)
        pdf.multi_cell(inner_w, 4.0, "Technical Value")
        pdf.set_font("Helvetica", "", 8)
        for item in technical_points:
            pdf.set_x(x + 2)
            pdf.multi_cell(inner_w, 4.0, f"- {item}")

        pdf.set_font("Helvetica", "B", 8)
        pdf.set_x(x + 2)
        pdf.multi_cell(inner_w, 4.0, "Business Value")
        pdf.set_font("Helvetica", "", 8)
        for item in business_points:
            pdf.set_x(x + 2)
            pdf.multi_cell(inner_w, 4.0, f"- {item}")

        pdf.set_xy(pdf.l_margin, y + block_h + 1.2)


def _add_compat_index_page(pdf, metrics):
    compat = metrics.get("compat_checks", {}) or {}
    details = compat.get("details", []) or []
    if not details:
        return

    pdf.add_page()
    pdf.section_title("SQL Compatibility and Source Feature Index")
    passed = int(compat.get("passed", 0) or 0)
    total = int(compat.get("total", 0) or 0)
    failed = int(compat.get("failed", 0) or 0)
    pdf.body_text(
        f"Checks passed: {passed}/{total} ({compat.get('pct', 0)}%). Failed checks: {failed}. "
        "This index includes TiDB SQL checks and source-engine unsupported feature findings.",
        size=8,
    )

    failed_rows = []
    by_category = {}
    for row in details:
        if not isinstance(row, dict):
            continue
        category = _compat_category(row)
        status = str(row.get("status") or "").strip().lower()
        name = str(row.get("check_name") or row.get("name") or "Unnamed check")
        note = str(row.get("note") or "").strip()
        by_category.setdefault(category, {"pass": 0, "fail": 0})
        if status == "pass":
            by_category[category]["pass"] += 1
        else:
            by_category[category]["fail"] += 1
            failed_rows.append((category, name, note, _compat_fix_text(category, note)))

    pdf.sub_title("Category Summary")
    col_w = [36, 18, 18, 102]
    hdr = ["Category", "Pass", "Fail", "Recommended Fix Direction"]
    _draw_wrapped_table_header(pdf, col_w, hdr)
    for category in sorted(by_category.keys()):
        rec = _compat_fix_text(category, "")
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_w,
            [category, str(by_category[category]["pass"]), str(by_category[category]["fail"]), rec],
            styles=["", "", "", ""],
            font_sizes=[7.0, 7.0, 7.0, 7.0],
            text_colors=[(0, 0, 0)] * 4,
            aligns=["L", "C", "C", "L"],
            fill_color=WHITE,
            line_h=3.6,
        )
        if not row_ok:
            pdf.add_page()
            pdf.section_title("SQL Compatibility and Source Feature Index (Continued)")
            pdf.sub_title("Category Summary (Continued)")
            _draw_wrapped_table_header(pdf, col_w, hdr)
            _draw_wrapped_table_row(
                pdf,
                col_w,
                [category, str(by_category[category]["pass"]), str(by_category[category]["fail"]), rec],
                styles=["", "", "", ""],
                font_sizes=[7.0, 7.0, 7.0, 7.0],
                text_colors=[(0, 0, 0)] * 4,
                aligns=["L", "C", "C", "L"],
                fill_color=WHITE,
                line_h=3.6,
            )

    pdf.ln(3)
    pdf.sub_title("Failed Check Index")
    if not failed_rows:
        pdf.body_text("No failed checks in this run.", size=8)
        return

    col_w2 = [24, 54, 40, 56]
    hdr2 = ["Category", "Check", "Observed Error", "How to Fix"]
    _draw_wrapped_table_header(pdf, col_w2, hdr2)
    for category, name, note, fix in failed_rows:
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_w2,
            [category, name, note or "n/a", fix],
            styles=["", "", "", ""],
            font_sizes=[7.0, 7.0, 7.0, 7.0],
            text_colors=[(0, 0, 0)] * 4,
            aligns=["L", "L", "L", "L"],
            fill_color=WHITE,
            line_h=3.6,
        )
        if not row_ok:
            pdf.add_page()
            pdf.section_title("SQL Compatibility and Source Feature Index (Continued)")
            pdf.sub_title("Failed Check Index (Continued)")
            _draw_wrapped_table_header(pdf, col_w2, hdr2)
            _draw_wrapped_table_row(
                pdf,
                col_w2,
                [category, name, note or "n/a", fix],
                styles=["", "", "", ""],
                font_sizes=[7.0, 7.0, 7.0, 7.0],
                text_colors=[(0, 0, 0)] * 4,
                aligns=["L", "L", "L", "L"],
                fill_color=WHITE,
                line_h=3.6,
            )

    pdf.ln(3)
    pdf.sub_title("All Compatibility Checks (Full Index)")
    col_w3 = [24, 106, 22, 24]
    hdr3 = ["Category", "Check", "Status", "Fix Ref"]
    _draw_wrapped_table_header(pdf, col_w3, hdr3)

    for row in details:
        if not isinstance(row, dict):
            continue
        category = _compat_category(row)
        name = str(row.get("check_name") or row.get("name") or "Unnamed check")
        status = str(row.get("status") or "fail").upper()
        if status not in {"PASS", "FAIL"}:
            status = "FAIL"
        ref = "See failed index" if status == "FAIL" else "-"
        status_color = RED if status == "FAIL" else GREEN
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_w3,
            [category, name, status, ref],
            styles=["", "", "B", ""],
            font_sizes=[7.0, 7.0, 7.0, 7.0],
            text_colors=[(0, 0, 0), (0, 0, 0), status_color, (0, 0, 0)],
            aligns=["L", "L", "C", "L"],
            fill_color=WHITE,
            line_h=3.6,
        )
        if not row_ok:
            pdf.add_page()
            pdf.section_title("SQL Compatibility and Source Feature Index (Continued)")
            pdf.sub_title("All Compatibility Checks (Full Index) (Continued)")
            _draw_wrapped_table_header(pdf, col_w3, hdr3)
            _draw_wrapped_table_row(
                pdf,
                col_w3,
                [category, name, status, ref],
                styles=["", "", "B", ""],
                font_sizes=[7.0, 7.0, 7.0, 7.0],
                text_colors=[(0, 0, 0), (0, 0, 0), status_color, (0, 0, 0)],
                aligns=["L", "L", "C", "L"],
                fill_color=WHITE,
                line_h=3.6,
            )


def _normalize_tier_key(raw: str | None) -> str:
    t = str(raw or "serverless").strip().lower()
    if t == "starter":
        return "serverless"
    if t not in KPI_THRESHOLDS_BY_TIER:
        return "serverless"
    return t


def _kpi_eval(module_key: str, stats: dict, tier_key: str, thresholds_by_tier: dict) -> tuple[str, str]:
    tier_rules = thresholds_by_tier.get(_normalize_tier_key(tier_key), thresholds_by_tier["serverless"])
    rule = tier_rules.get(module_key, {})
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


def _coerce_numeric(value):
    if isinstance(value, (int, float)):
        return float(value)
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _effective_kpi_thresholds(cfg: dict) -> dict:
    merged = {}
    for tier, rule_map in KPI_THRESHOLDS_BY_TIER.items():
        merged[tier] = {m: dict(v) for m, v in rule_map.items()}

    report_cfg = (cfg or {}).get("report", {}) or {}
    overrides = report_cfg.get("kpi_threshold_overrides") or {}
    if not isinstance(overrides, dict):
        return merged

    # Global overrides applied to every tier.
    global_overrides = overrides.get("all_tiers") or {}
    if isinstance(global_overrides, dict):
        for tier in merged:
            for module_key, module_over in global_overrides.items():
                if not isinstance(module_over, dict):
                    continue
                merged[tier].setdefault(module_key, {})
                for k, v in module_over.items():
                    nv = _coerce_numeric(v)
                    if nv is not None:
                        merged[tier][module_key][str(k)] = nv

    # Tier-specific overrides.
    for raw_tier, tier_overrides in overrides.items():
        tkey = _normalize_tier_key(raw_tier)
        if tkey == "serverless" and str(raw_tier).strip().lower() not in {"serverless", "starter"}:
            continue
        if not isinstance(tier_overrides, dict):
            continue
        for module_key, module_over in tier_overrides.items():
            if not isinstance(module_over, dict):
                continue
            merged[tkey].setdefault(module_key, {})
            for k, v in module_over.items():
                nv = _coerce_numeric(v)
                if nv is not None:
                    merged[tkey][module_key][str(k)] = nv
    return merged


def _add_kpi_appendix_page(pdf, metrics, tier_key: str, thresholds_by_tier: dict):
    pdf.add_page()
    pdf.section_title("Appendix — KPI Threshold Evaluation")
    pdf.body_text(
        "Thresholds are PoV guidance bands for quick interpretation, not strict SLA commitments. "
        "Use this page to identify phases that need rerun or tuning.",
        size=8,
    )
    pdf.body_text(f"Threshold profile: {_normalize_tier_key(tier_key)}", size=8)

    col_w = [48, 24, 18, 18, 18, 18, 20, 36]
    hdrs = ["Module / Phase", "Status", "Count", "p50", "p95", "p99", "TPS", "Evaluation"]
    _draw_wrapped_table_header(pdf, col_w, hdrs)

    for mod_key in MODULE_DISPLAY:
        mod = (metrics.get("modules", {}) or {}).get(mod_key, {}) or {}
        tidb = mod.get("tidb", {}) if isinstance(mod.get("tidb"), dict) else {}
        if not tidb:
            # Some modules produce non-latency evidence. Render explicit rows so the appendix
            # does not incorrectly show "NO DATA" for successful compatibility/import checks.
            if mod_key == "00_customer_queries" and str(mod.get("status") or "").lower() == "passed":
                checks = str(mod.get("notes") or "Validation checks passed")
                row_ok = _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    [mod_key, "PASS", "-", "-", "-", "-", "-", checks],
                    styles=["", "B", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0), GREEN, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                    aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )
                if not row_ok:
                    pdf.add_page()
                    pdf.section_title("Appendix — KPI Threshold Evaluation (Continued)")
                    _draw_wrapped_table_header(pdf, col_w, hdrs)
                    _draw_wrapped_table_row(
                        pdf,
                        col_w,
                        [mod_key, "PASS", "-", "-", "-", "-", "-", checks],
                        styles=["", "B", "", "", "", "", "", ""],
                        font_sizes=[7.0] * 8,
                        text_colors=[(0, 0, 0), GREEN, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                        aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                        fill_color=LIGHT_GREY,
                        line_h=3.5,
                    )
                continue

            if mod_key == "06_mysql_compat" and str(mod.get("status") or "").lower() == "passed":
                compat = metrics.get("compat_checks", {}) or {}
                total = int(compat.get("total", 0) or 0)
                passed = int(compat.get("passed", 0) or 0)
                failed = int(compat.get("failed", 0) or 0)
                pct = _maybe_float(compat.get("pct"))
                verdict = "PASS" if failed == 0 else "WARN"
                verdict_color = GREEN if verdict == "PASS" else ORANGE
                note = f"{passed}/{total} checks passed"
                if pct is not None:
                    note += f" ({pct:.1f}%)"
                row_ok = _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    [mod_key, verdict, str(total), "-", "-", "-", "-", note],
                    styles=["", "B", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0), verdict_color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                    aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )
                if not row_ok:
                    pdf.add_page()
                    pdf.section_title("Appendix — KPI Threshold Evaluation (Continued)")
                    _draw_wrapped_table_header(pdf, col_w, hdrs)
                    _draw_wrapped_table_row(
                        pdf,
                        col_w,
                        [mod_key, verdict, str(total), "-", "-", "-", "-", note],
                        styles=["", "B", "", "", "", "", "", ""],
                        font_sizes=[7.0] * 8,
                        text_colors=[(0, 0, 0), verdict_color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                        aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                        fill_color=LIGHT_GREY,
                        line_h=3.5,
                    )
                continue

            if mod_key == "07_data_import" and str(mod.get("status") or "").lower() == "passed":
                import_stats = metrics.get("import_stats", []) or []
                best = max((float(r.get("throughput_gbpm", 0) or 0) for r in import_stats), default=0.0)
                verdict = "PASS" if best > 0 else "WARN"
                verdict_color = GREEN if verdict == "PASS" else ORANGE
                note = f"Best throughput {best:.3f} GB/min"
                row_ok = _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    [mod_key, verdict, str(len(import_stats)), "-", "-", "-", "-", note],
                    styles=["", "B", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0), verdict_color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                    aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )
                if not row_ok:
                    pdf.add_page()
                    pdf.section_title("Appendix — KPI Threshold Evaluation (Continued)")
                    _draw_wrapped_table_header(pdf, col_w, hdrs)
                    _draw_wrapped_table_row(
                        pdf,
                        col_w,
                        [mod_key, verdict, str(len(import_stats)), "-", "-", "-", "-", note],
                        styles=["", "B", "", "", "", "", "", ""],
                        font_sizes=[7.0] * 8,
                        text_colors=[(0, 0, 0), verdict_color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                        aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                        fill_color=LIGHT_GREY,
                        line_h=3.5,
                    )
                continue

            row_ok = _draw_wrapped_table_row(
                pdf,
                col_w,
                [mod_key, "NO DATA", "-", "-", "-", "-", "-", "Module not run or no rows"],
                styles=["", "B", "", "", "", "", "", ""],
                font_sizes=[7.0] * 8,
                text_colors=[(0, 0, 0), DARK_GREY, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                fill_color=LIGHT_GREY,
                line_h=3.5,
            )
            if not row_ok:
                pdf.add_page()
                pdf.section_title("Appendix — KPI Threshold Evaluation (Continued)")
                _draw_wrapped_table_header(pdf, col_w, hdrs)
                _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    [mod_key, "NO DATA", "-", "-", "-", "-", "-", "Module not run or no rows"],
                    styles=["", "B", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0), DARK_GREY, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                    aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )
            continue

        for phase, s in tidb.items():
            if not isinstance(s, dict):
                continue
            verdict, note = _kpi_eval(mod_key, s, tier_key, thresholds_by_tier)
            if verdict == "PASS":
                color = GREEN
            elif verdict == "WARN":
                color = ORANGE
            elif verdict == "FAIL":
                color = RED
            else:
                color = DARK_GREY

            row_ok = _draw_wrapped_table_row(
                pdf,
                col_w,
                [
                    f"{mod_key}/{str(phase)}",
                    verdict,
                    str(int(s.get("count", 0) or 0)),
                    f"{float(s.get('p50_ms', 0) or 0):.1f}",
                    f"{float(s.get('p95_ms', 0) or 0):.1f}",
                    f"{float(s.get('p99_ms', 0) or 0):.1f}",
                    f"{float(s.get('tps', 0) or 0):.0f}",
                    note,
                ],
                styles=["", "B", "", "", "", "", "", ""],
                font_sizes=[7.0] * 8,
                text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                fill_color=LIGHT_GREY,
                line_h=3.5,
            )
            if not row_ok:
                pdf.add_page()
                pdf.section_title("Appendix — KPI Threshold Evaluation (Continued)")
                _draw_wrapped_table_header(pdf, col_w, hdrs)
                _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    [
                        f"{mod_key}/{str(phase)}",
                        verdict,
                        str(int(s.get("count", 0) or 0)),
                        f"{float(s.get('p50_ms', 0) or 0):.1f}",
                        f"{float(s.get('p95_ms', 0) or 0):.1f}",
                        f"{float(s.get('p99_ms', 0) or 0):.1f}",
                        f"{float(s.get('tps', 0) or 0):.0f}",
                        note,
                    ],
                    styles=["", "B", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0)],
                    aligns=["L", "C", "C", "C", "C", "C", "C", "L"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )


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
    selected_tier = _normalize_tier_key((cfg.get("tier", {}) or {}).get("selected"))
    thresholds_by_tier = _effective_kpi_thresholds(cfg)
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
    display_max_qps = summary.get("max_qps")
    display_avg_qps = summary.get("avg_qps")
    latency_label = "Warm p99 Latency"
    tps_label = "Warm Throughput"
    run_mode_key = str(summary.get("run_mode") or "validation").strip().lower()
    schema_mode_key = str(summary.get("schema_mode") or "tidb_optimized").strip().lower()
    industry_key = str(summary.get("industry") or "general_auto").strip().lower()
    run_mode_display = "Performance" if run_mode_key == "performance" else "Validation"
    schema_mode_display = {
        "tidb_optimized": "TiDB Optimized",
        "mysql_compatible": "MySQL Compatible",
    }.get(schema_mode_key, schema_mode_key.replace("_", " ").title())
    industry_display = _industry_display_label(industry_key)
    if summary.get("run_mode") == "performance":
        if display_latency is None:
            display_latency = summary.get("workload_p99_ms")
        if display_tps is None:
            display_tps = summary.get("workload_tps")
        if display_max_qps is None:
            display_max_qps = summary.get("workload_qps")
        if display_avg_qps is None:
            display_avg_qps = summary.get("workload_qps")
        latency_label = "Current Run p99"
        tps_label = "Current Run Throughput"

    cards = [
        ("Run Mode",            run_mode_display, "",       (0, 128, 128)),
        ("Schema Mode",         schema_mode_display, "", (100, 100, 180)),
        ("Industry",            industry_display, "", (120, 120, 160)),
        (latency_label,         _fmt(display_latency, 1), "ms",       BLUE),
        (tps_label,             _fmt(display_tps,    0), "TPS",      GREEN),
        ("Max QPS",             _fmt(display_max_qps, 0), "QPS",      BLUE),
        ("Avg QPS",             _fmt(display_avg_qps, 0), "QPS",      BLUE),
        ("Best Observed p99",   _fmt(summary.get("best_observed_p99_ms", summary.get("best_p99_ms")), 1), "ms", BLUE),
        ("Peak Throughput",     _fmt(summary.get("best_tps"),    0), "TPS",      GREEN),
        ("Drill Recovery (sim)", _fmt(summary.get("rto_sec"),    1), "seconds",  ORANGE),
        ("Hotspot Reduction",   _fmt(summary.get("hotspot_improvement_pct"), 0), "%",   RED),
        ("SQL Compat",          _fmt(summary.get("mysql_compat_pct"), 0), "%",   PURPLE),
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
        "the TiDB Cloud PoV Kit. Charts and conclusions only include modules selected for this run. "
        f"Run mode: {summary.get('run_mode', 'validation')}. "
        f"Schema mode: {summary.get('schema_mode', 'tidb_optimized')}. "
        f"Industry: {industry_display}."
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

    compat = metrics.get("compat_checks", {}) or {}
    failed_compat = []
    for row in (compat.get("details") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip().lower() == "pass":
            continue
        cat = _compat_category(row)
        nm = str(row.get("check_name") or row.get("name") or "Unnamed check")
        nt = str(row.get("note") or "").strip()
        failed_compat.append((cat, nm, _compat_fix_text(cat, nt)))
    if failed_compat:
        pdf.sub_title("Top Compatibility Gaps to Fix")
        for cat, name, fix in failed_compat[:2]:
            concise_fix = fix.split(" Last error:", 1)[0].strip()
            _write_wrapped_bullet(pdf, f"[{cat}] {name} — Fix path: {concise_fix}", size=7.5)
    else:
        pdf.body_text("Compatibility summary: no SQL compatibility failures were observed in this run.", size=8)

    # ── Page 2/3: Buyer-facing summary pages ─────────────────────────────────
    _add_decision_summary_page(pdf, metrics, cfg)
    _add_test_scope_page(pdf, metrics, cfg)

    # ── Page 4: Module status table ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Test Module Status")
    col_widths = [80, 22, 22, 50]
    headers    = ["Module", "Status", "Duration", "Notes"]
    _draw_wrapped_table_header(pdf, col_widths, headers)

    for mod_key, mod_label in MODULE_DISPLAY.items():
        mod  = metrics["modules"].get(mod_key, {})
        stat = mod.get("status", "not_run")
        dur  = f"{mod.get('duration_sec', 0):.0f}s" if mod.get("duration_sec") else "—"
        note = mod.get("notes") or ""
        stat_l = str(stat).lower()
        if stat_l == "passed":
            color = GREEN
        elif stat_l == "skipped":
            color = ORANGE
        elif stat_l == "not_run":
            color = DARK_GREY
        else:
            color = RED
        row_ok = _draw_wrapped_table_row(
            pdf,
            col_widths,
            [mod_label, stat.upper(), dur, note],
            styles=["", "B", "", ""],
            font_sizes=[7.5, 7.5, 7.5, 7.5],
            text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0)],
            aligns=["L", "C", "C", "L"],
            fill_color=LIGHT_GREY,
            line_h=3.7,
        )
        if not row_ok:
            pdf.add_page()
            pdf.section_title("Test Module Status (Continued)")
            _draw_wrapped_table_header(pdf, col_widths, headers)
            _draw_wrapped_table_row(
                pdf,
                col_widths,
                [mod_label, stat.upper(), dur, note],
                styles=["", "B", "", ""],
                font_sizes=[7.5, 7.5, 7.5, 7.5],
                text_colors=[(0, 0, 0), color, (0, 0, 0), (0, 0, 0)],
                aligns=["L", "C", "C", "L"],
                fill_color=LIGHT_GREY,
                line_h=3.7,
            )
    pdf.ln(5)

    # ── Page 3: Coverage details ─────────────────────────────────────────────
    _add_run_coverage_page(pdf, metrics)
    _add_module_interpretation_page(pdf, metrics)

    # ── Chart pages (only for executed modules) ──────────────────────────────
    def _module_ran(key: str) -> bool:
        return str((metrics.get("modules", {}).get(key, {}) or {}).get("status") or "not_run").lower() != "not_run"

    manifest = metrics.get("data_manifest", {}) or {}
    _add_chart_page(
        pdf,
        "Data Population Snapshot",
        _safe_chart_render("Data Population Snapshot", lambda: _chart_data_population(metrics)),
        f"Scale: {manifest.get('scale', 'n/a')} | "
        f"Rows generated across schemas: {sum((manifest.get('counts') or {}).values()) if isinstance(manifest.get('counts'), dict) else 'n/a'} | "
        f"Generation time: {manifest.get('generation_duration_sec', 'n/a')} sec",
    )

    if _module_ran("01_baseline_perf"):
        baseline_caption = (
            "OLTP workload across configured concurrency levels. "
            "Shows p99 latency and transactions per second."
        )
        max_qps = _maybe_float(summary.get("max_qps"))
        avg_qps = _maybe_float(summary.get("avg_qps"))
        if max_qps is not None or avg_qps is not None:
            baseline_caption += (
                f" Max QPS: {max_qps:.0f}" if max_qps is not None else " Max QPS: n/a"
            )
            baseline_caption += (
                f" | Avg QPS: {avg_qps:.0f}." if avg_qps is not None else " | Avg QPS: n/a."
            )
        _add_chart_page(pdf, "Baseline OLTP Performance",
                        _safe_chart_render("Baseline OLTP Performance", lambda: _chart_baseline(metrics)),
                        baseline_caption)

        _add_chart_page(
            pdf,
            "Warm Workload Stability",
            _safe_chart_render("Warm Workload Stability", lambda: _chart_warm_steady(metrics)),
            "Steady-state warm workload after data load. This phase reflects customer-expected latency drift and TPS consistency over time. "
            + _warm_stability_comment(metrics),
        )

    if _module_ran("02_elastic_scale"):
        _add_chart_page(pdf, "Elastic Auto-Scaling",
                        _safe_chart_render("Elastic Auto-Scaling", lambda: _chart_scale(metrics)),
                        "Load ramped from baseline to peak. The bottom panel provides an inferred pay-as-you-grow control signal "
                        "(capacity index and cumulative capacity-hours) derived from measured TPS.")

    if _module_ran("03_high_availability"):
        _add_chart_page(pdf, "Availability Drill — Simulated Failure Window",
                        _safe_chart_render("Availability Drill — Simulated Failure Window", lambda: _chart_ha(metrics)),
                        "This module simulates a client-connection failure window and measures recovery behavior. "
                        "It is not a cloud control-plane node kill. For customer-grade RTO evidence, pair this with backup+restore drill timings.")

    if _module_ran("03b_write_contention"):
        _add_chart_page(pdf, "Write Contention — AUTO_RANDOM vs Sequential Keys",
                        _safe_chart_render("Write Contention — AUTO_RANDOM vs Sequential Keys", lambda: _chart_hotspot(metrics)),
                        "Sequential (AUTO_INCREMENT) PKs concentrate writes on a single "
                        "region leader (hot region). AUTO_RANDOM distributes writes evenly. "
                        "For a stronger delta, rerun with higher write concurrency and longer phase duration.")

    if _module_ran("04_htap_concurrent"):
        _add_chart_page(pdf, "HTAP — Concurrent Transactional & Analytical Workload",
                        _safe_chart_render("HTAP — Concurrent Transactional & Analytical Workload", lambda: _chart_htap(metrics)),
                        "TiFlash columnar replicas serve analytical queries without "
                        "interfering with TiKV row-store OLTP writes. This section also compares OLAP query behavior on TiFlash vs TiKV when captured.")

    if _module_ran("06_mysql_compat"):
        try:
            _add_sql_compat_page(pdf, metrics)
        except Exception as exc:
            _add_chart_page(
                pdf,
                "SQL Compatibility",
                _empty_chart(
                    "SQL Compatibility output unavailable",
                    reason=f"Renderer error: {type(exc).__name__}: {exc}",
                    actions=["Review results/metrics_summary.json compat_checks section and rerun report build."],
                ),
                "A rendering issue occurred while building compatibility details.",
            )
        try:
            _add_compat_index_page(pdf, metrics)
        except Exception as exc:
            _add_chart_page(
                pdf,
                "SQL Compatibility and Source Feature Index",
                _empty_chart(
                    "Compatibility index unavailable",
                    reason=f"Renderer error: {type(exc).__name__}: {exc}",
                    actions=["Rebuild report after validating compatibility detail rows in metrics_summary.json."],
                ),
                "A rendering issue occurred while building the full compatibility index.",
            )

    if _module_ran("07_data_import"):
        _add_chart_page(pdf, "Data Import Speed",
                        _safe_chart_render("Data Import Speed", lambda: _chart_import(metrics)),
                        "Bulk load throughput comparison: Batched INSERT, "
                        "LOAD DATA LOCAL INFILE, and IMPORT INTO (TiDB native loader). "
                        "For production-scale PoV, pre-stage industry data in S3 and prefer IMPORT INTO.")

    # TCO page
    pdf.add_page()
    pdf.section_title("3-Year Total Cost of Ownership")
    tco_fig = _safe_chart_render("3-Year Total Cost of Ownership", lambda: make_tco_chart(tco_data))
    pdf.embed_figure(tco_fig, w=174)
    npv = tco_data["npv"]
    tco_cfg = (cfg.get("tco", {}) or {})
    tco_mode = "customer-input" if bool(tco_cfg) else "illustrative-default"
    tco_prefix = "Illustrative model: " if tco_mode == "illustrative-default" else ""
    pdf.body_text(
        f"{tco_prefix}3-year TCO: Aurora MySQL + Sharding ${npv['aurora_3yr']:,} vs "
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
                        _safe_chart_render("Vector Search (AI Track)", lambda: _chart_vector(ann)),
                        "ANN search latency (cosine distance, HNSW index) "
                        "at increasing concurrency levels.")

    # ── Appendix: raw latency table ───────────────────────────────────────────
    _add_kpi_appendix_page(pdf, metrics, selected_tier, thresholds_by_tier)

    # ── Appendix: raw latency table ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("Appendix — Raw Latency Statistics")
    col_w = [55, 22, 18, 18, 18, 18, 18, 18]
    hdrs  = ["Module / Phase", "Count", "Avg ms", "p50", "p95", "p99", "Max", "TPS"]
    _draw_wrapped_table_header(pdf, col_w, hdrs)

    for mod_key in MODULE_DISPLAY:
        mod  = metrics["modules"].get(mod_key, {})
        tidb = mod.get("tidb", {})
        for phase, s in tidb.items():
            row = [
                f"{mod_key}/{phase}",
                str(s.get("count", "")),
                f"{s.get('avg_ms',0):.1f}",
                f"{s.get('p50_ms',0):.1f}",
                f"{s.get('p95_ms',0):.1f}",
                f"{s.get('p99_ms',0):.1f}",
                f"{s.get('max_ms',0):.1f}",
                f"{s.get('tps',0):.0f}",
            ]
            row_ok = _draw_wrapped_table_row(
                pdf,
                col_w,
                row,
                styles=["", "", "", "", "", "", "", ""],
                font_sizes=[7.0] * 8,
                text_colors=[(0, 0, 0)] * 8,
                aligns=["L", "C", "C", "C", "C", "C", "C", "C"],
                fill_color=LIGHT_GREY,
                line_h=3.5,
            )
            if not row_ok:
                pdf.add_page()
                pdf.section_title("Appendix — Raw Latency Statistics (Continued)")
                _draw_wrapped_table_header(pdf, col_w, hdrs)
                _draw_wrapped_table_row(
                    pdf,
                    col_w,
                    row,
                    styles=["", "", "", "", "", "", "", ""],
                    font_sizes=[7.0] * 8,
                    text_colors=[(0, 0, 0)] * 8,
                    aligns=["L", "C", "C", "C", "C", "C", "C", "C"],
                    fill_color=LIGHT_GREY,
                    line_h=3.5,
                )

    pdf.output(out_path)
    print(f"  Report written to: {out_path}")
    return out_path


def _add_chart_page(pdf, title, fig, caption=""):
    pdf.add_page()
    pdf.section_title(title)
    pdf.embed_figure(fig, w=174)
    if caption:
        pdf.body_text(caption, size=8)


def _write_wrapped_bullet(pdf: PoVReport, text: str, *, size: float = 7.5, line_h: float = 3.8):
    content = f"- {pdf.normalize_text(str(text or ''))}"
    lines = _safe_table_lines(pdf, content, pdf.w - pdf.l_margin - pdf.r_margin - 2, line_h, "", size)
    needed_h = len(lines) * line_h + 1.0
    if pdf.get_y() + needed_h > (pdf.h - pdf.b_margin):
        pdf.add_page()
        pdf.section_title("SQL Compatibility (Continued)")
    pdf.set_x(pdf.l_margin + 1)
    pdf.set_font("Helvetica", "", size)
    pdf.multi_cell(pdf.w - pdf.l_margin - pdf.r_margin - 2, line_h, "\n".join(lines))
    pdf.set_x(pdf.l_margin)


def _add_sql_compat_page(pdf: PoVReport, metrics: dict):
    compat = metrics.get("compat_checks", {}) or {}
    details = compat.get("details", []) or []
    source_inv = metrics.get("source_unsupported_inventory", {}) or {}

    pdf.add_page()
    pdf.section_title("SQL Compatibility")
    pdf.embed_figure(_chart_compat(compat), w=174)
    pdf.body_text(
        f"{compat.get('passed', '—')} / {compat.get('total', '—')} checks passed "
        f"({compat.get('pct', '—')}% compatible).",
        size=8,
    )

    failed_rows = []
    for row in details:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().lower()
        if status == "pass":
            continue
        category = _compat_category(row)
        name = str(row.get("check_name") or row.get("name") or "Unnamed check")
        note = str(row.get("note") or "").strip() or "Failed check"
        failed_rows.append((category, name, note))

    pdf.ln(1)
    pdf.sub_title("Results Output")
    if failed_rows:
        for category, name, note in failed_rows[:10]:
            _write_wrapped_bullet(pdf, f"[{category}] {name} -> {note}")
    else:
        pdf.body_text("No failed TiDB SQL compatibility checks in this run.", size=8)

    src_status = str(source_inv.get("status") or "").strip().lower()
    if src_status:
        pdf.ln(1)
        pdf.sub_title("Source Unsupported Feature Output")
        if src_status == "executed":
            target_label = str(source_inv.get("target_label") or source_inv.get("target") or "source")
            family = str(source_inv.get("family") or "unknown")
            checks_total = int(source_inv.get("checks_total") or 0)
            failing_features = int(source_inv.get("failing_features") or 0)
            pdf.body_text(
                f"{target_label} ({family}): {failing_features}/{checks_total} features require remediation.",
                size=8,
            )
            rows = source_inv.get("rows") or []
            shown = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("status") or "").strip().lower() == "pass":
                    continue
                feature = str(row.get("feature") or row.get("name") or "feature")
                note = str(row.get("note") or "").strip() or "needs review"
                _write_wrapped_bullet(pdf, f"{feature} -> {note}")
                shown += 1
                if shown >= 8:
                    break
            if shown == 0:
                pdf.body_text("No source unsupported features detected.", size=8)
        else:
            pdf.body_text(
                "Source feature inventory was not run in this profile. "
                "Enable source comparison to include source-engine unsupported-feature findings.",
                size=8,
            )


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
