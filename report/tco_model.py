#!/usr/bin/env python3
"""
tco_model.py — 3-Year Total Cost of Ownership comparison:
  TiDB Cloud (Dedicated) vs Aurora MySQL + Manual Sharding

Inputs (from config.yaml or defaults):
  - current_db_monthly_cost   : customer's existing DB monthly spend (USD)
  - peak_qps                  : peak queries per second
  - data_size_gb              : current dataset size in GB
  - annual_growth_pct         : expected data/traffic growth % per year
  - engineers_managing_shards : number of engineers maintaining sharding

Returns a dict with year-by-year cost breakdown and a 3-year NPV comparison.
Also generates a matplotlib figure (returns the Figure object).
"""
import sys, os

# ── Default assumptions (override via config) ────────────────────────────────
DEFAULTS = {
    # Aurora MySQL sharded setup
    "aurora_instance_monthly":   3_500,   # per shard (r6g.2xlarge Multi-AZ)
    "aurora_shards_year0":       4,        # shards needed today
    "aurora_shard_growth_rate":  1.0,      # new shards added per year
    "aurora_storage_per_gb_mo":  0.10,     # $/GB/month
    "aurora_data_transfer_mo":   200,      # cross-shard data transfer / month

    # Engineering overhead for sharding
    "engineer_annual_cost":      180_000,  # fully-loaded eng cost
    "sharding_eng_fraction":     0.25,     # fraction of 1 engineer's time
    "engineers_managing_shards": 2,        # number of engineers involved

    # TiDB Cloud Dedicated
    "tidb_node_type":            "8vCPU 16GB",
    "tidb_nodes_year0":          3,         # TiDB nodes
    "tikv_nodes_year0":          3,         # TiKV nodes
    "tidb_node_monthly":         290,       # TiDB node monthly (8vCPU)
    "tikv_node_monthly":         365,       # TiKV node monthly (8vCPU 64GB)
    "tidb_storage_per_gb_mo":    0.044,     # TiDB Cloud storage $/GB/month
    "tidb_support_monthly":      500,       # TiDB enterprise support
    "tidb_node_scale_rate":      0.5,       # additional nodes per year (fractional ok)

    # Shared inputs
    "data_size_gb":              1_000,
    "annual_growth_pct":         40,        # % data growth per year
    "years":                     3,
    "discount_rate":             0.08,      # NPV discount rate
}


def compute(cfg: dict = None) -> dict:
    """Return TCO model results dict."""
    p = dict(DEFAULTS)

    # Override with config values if provided
    tco_cfg = (cfg or {}).get("tco", {})
    for k, v in tco_cfg.items():
        if k in p:
            p[k] = v

    years = int(p["years"])
    aurora_rows = []
    tidb_rows   = []

    data_gb      = p["data_size_gb"]
    aurora_shards = p["aurora_shards_year0"]
    tidb_nodes   = p["tidb_nodes_year0"]
    tikv_nodes   = p["tikv_nodes_year0"]

    for yr in range(years + 1):   # year 0 = today (annualised)
        # Data growth
        if yr > 0:
            data_gb      *= (1 + p["annual_growth_pct"] / 100)
            aurora_shards = max(aurora_shards, p["aurora_shards_year0"] +
                                int(yr * p["aurora_shard_growth_rate"]))
            tidb_nodes   = max(tidb_nodes, p["tidb_nodes_year0"] +
                               int(yr * p["tidb_node_scale_rate"]))
            tikv_nodes   = max(tikv_nodes, p["tikv_nodes_year0"] +
                               int(yr * p["tidb_node_scale_rate"]))

        # Aurora annual cost
        aurora_compute  = aurora_shards * p["aurora_instance_monthly"] * 12
        aurora_storage  = data_gb * p["aurora_storage_per_gb_mo"] * 12
        aurora_xfer     = p["aurora_data_transfer_mo"] * 12
        aurora_eng      = (p["engineers_managing_shards"]
                           * p["sharding_eng_fraction"]
                           * p["engineer_annual_cost"])
        aurora_total    = aurora_compute + aurora_storage + aurora_xfer + aurora_eng

        # TiDB annual cost
        tidb_compute    = (tidb_nodes * p["tidb_node_monthly"] +
                           tikv_nodes * p["tikv_node_monthly"]) * 12
        tidb_storage    = data_gb * p["tidb_storage_per_gb_mo"] * 12
        tidb_support    = p["tidb_support_monthly"] * 12
        tidb_eng        = 0   # no sharding overhead
        tidb_total      = tidb_compute + tidb_storage + tidb_support + tidb_eng

        aurora_rows.append({
            "year":          yr,
            "shards":        aurora_shards,
            "compute":       round(aurora_compute),
            "storage":       round(aurora_storage),
            "data_transfer": round(aurora_xfer),
            "engineering":   round(aurora_eng),
            "total":         round(aurora_total),
        })
        tidb_rows.append({
            "year":          yr,
            "tidb_nodes":    tidb_nodes,
            "tikv_nodes":    tikv_nodes,
            "compute":       round(tidb_compute),
            "storage":       round(tidb_storage),
            "support":       round(tidb_support),
            "engineering":   0,
            "total":         round(tidb_total),
        })

    # 3-year NPV comparison (sum years 1..3 discounted)
    r = p["discount_rate"]
    aurora_npv = sum(
        aurora_rows[yr]["total"] / (1 + r)**yr
        for yr in range(1, years + 1)
    )
    tidb_npv = sum(
        tidb_rows[yr]["total"] / (1 + r)**yr
        for yr in range(1, years + 1)
    )
    savings   = aurora_npv - tidb_npv
    savings_pct = savings / aurora_npv * 100 if aurora_npv else 0

    return {
        "aurora": aurora_rows,
        "tidb":   tidb_rows,
        "npv": {
            "aurora_3yr": round(aurora_npv),
            "tidb_3yr":   round(tidb_npv),
            "savings":    round(savings),
            "savings_pct": round(savings_pct, 1),
        },
        "assumptions": p,
    }


def make_chart(tco_data: dict):
    """
    Build and return a matplotlib Figure with:
      - Bar chart: year-by-year cost (Aurora vs TiDB, stacked)
      - Annotation: 3-year savings
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick
    import numpy as np

    aurora = tco_data["aurora"]
    tidb   = tco_data["tidb"]
    npv    = tco_data["npv"]
    years  = [r["year"] for r in aurora if r["year"] > 0]

    aurora_totals = [r["total"] for r in aurora if r["year"] > 0]
    tidb_totals   = [r["total"] for r in tidb   if r["year"] > 0]

    # Stacked components
    aurora_compute = [r["compute"]      for r in aurora if r["year"] > 0]
    aurora_storage = [r["storage"]      for r in aurora if r["year"] > 0]
    aurora_eng     = [r["engineering"]  for r in aurora if r["year"] > 0]
    aurora_other   = [r["data_transfer"] for r in aurora if r["year"] > 0]

    tidb_compute   = [r["compute"]  for r in tidb if r["year"] > 0]
    tidb_storage   = [r["storage"]  for r in tidb if r["year"] > 0]
    tidb_support   = [r["support"]  for r in tidb if r["year"] > 0]

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("3-Year TCO: TiDB Cloud vs Aurora MySQL + Sharding",
                 fontsize=14, fontweight="bold", y=1.02)

    # ── Left: Stacked bar comparison ─────────────────────────────────────────
    ax = axes[0]
    x    = np.arange(len(years))
    w    = 0.35
    yr_labels = [f"Year {y}" for y in years]

    colors_aurora = ["#e74c3c", "#e67e22", "#f1c40f", "#c0392b"]
    colors_tidb   = ["#3498db", "#2ecc71", "#1abc9c"]

    # Aurora bars
    bot = np.zeros(len(years))
    for vals, label, color in [
        (aurora_compute, "Aurora Compute", colors_aurora[0]),
        (aurora_storage, "Aurora Storage", colors_aurora[1]),
        (aurora_eng,     "Engineering overhead", colors_aurora[2]),
        (aurora_other,   "Data transfer", colors_aurora[3]),
    ]:
        ax.bar(x - w/2, vals, w, bottom=bot, label=label, color=color)
        bot += np.array(vals)

    # TiDB bars
    bot = np.zeros(len(years))
    for vals, label, color in [
        (tidb_compute, "TiDB Compute", colors_tidb[0]),
        (tidb_storage, "TiDB Storage", colors_tidb[1]),
        (tidb_support, "TiDB Support", colors_tidb[2]),
    ]:
        ax.bar(x + w/2, vals, w, bottom=bot, label=label, color=color)
        bot += np.array(vals)

    ax.set_xticks(x)
    ax.set_xticklabels(yr_labels)
    ax.set_ylabel("Annual Cost (USD)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v/1000:.0f}K"))
    ax.set_title("Annual Cost Breakdown")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(axis="y", alpha=0.3)

    # ── Right: Cumulative savings ─────────────────────────────────────────────
    ax2 = axes[1]
    cumulative_aurora = np.cumsum(aurora_totals)
    cumulative_tidb   = np.cumsum(tidb_totals)
    cumulative_save   = cumulative_aurora - cumulative_tidb

    ax2.fill_between(years, cumulative_aurora, cumulative_tidb,
                     alpha=0.15, color="#27ae60", label="Savings area")
    ax2.plot(years, cumulative_aurora, "o-", color="#e74c3c",
             linewidth=2, markersize=7, label="Aurora (cumulative)")
    ax2.plot(years, cumulative_tidb, "o-", color="#3498db",
             linewidth=2, markersize=7, label="TiDB Cloud (cumulative)")

    # Annotate year-3 savings
    ax2.annotate(
        f"3-yr savings\n${npv['savings']:,.0f}\n({npv['savings_pct']:.0f}%)",
        xy=(years[-1], (cumulative_aurora[-1] + cumulative_tidb[-1]) / 2),
        xytext=(-80, 0), textcoords="offset points",
        fontsize=10, fontweight="bold", color="#27ae60",
        arrowprops=dict(arrowstyle="->", color="#27ae60"),
    )

    ax2.set_xticks(years)
    ax2.set_xticklabels(yr_labels)
    ax2.set_ylabel("Cumulative Cost (USD)")
    ax2.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"${v/1000:.0f}K"))
    ax2.set_title("Cumulative Cost Comparison")
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    fig.tight_layout()
    return fig


if __name__ == "__main__":
    import json
    result = compute()
    print(json.dumps(result, indent=2))
    fig = make_chart(result)
    out = os.path.join(os.path.dirname(__file__), "..", "results", "tco_chart.png")
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {out}")
