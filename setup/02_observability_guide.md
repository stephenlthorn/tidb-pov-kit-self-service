# TiDB Cloud Observability Guide

While the PoV kit runs, use TiDB Dashboard and Grafana to observe the cluster
in real time. This guide maps each test module to the most relevant panels.

---

## Accessing the Dashboards

### TiDB Dashboard
1. In the TiDB Cloud console, open your cluster.
2. Click **Monitoring → TiDB Dashboard**.
3. Log in with your DB credentials.

### Grafana
1. In the cluster page, click **Monitoring → Grafana**.
2. Navigate using the left sidebar.

---

## Module-by-Module Panel Reference

### M1 — Baseline OLTP Performance
**TiDB Dashboard**
- **Overview → QPS** — total queries/sec by statement type
- **Overview → Latency** — p99 query latency timeline

**Grafana → TiDB panel**
- `TiDB > Server > Query Duration (99th percentile)`
- `TiDB > Server > QPS`
- `TiKV > Server > CPU Usage` — watch for even CPU across all TiKV nodes

**What to watch:** QPS should scale linearly with concurrency. p99 should remain
flat until concurrency exceeds node capacity.

---

### M2 — Elastic Auto-Scaling
**TiDB Dashboard**
- **Overview → QPS** — should rise as concurrency ramps, then stabilise

**Grafana → TiDB panel**
- `TiDB > Server > Connection Count` — connection pressure per TiDB node
- `TiKV > Server > Region Count` — region distribution across nodes

**TiDB Cloud Console**
- **Scaling** tab — watch auto-scaling events fire as load peaks

**What to watch:** TiDB Cloud adds TiDB nodes automatically. After scaling,
p99 should recover even at higher concurrency.

---

### M3 — High Availability / RTO
**TiDB Dashboard**
- **Cluster Info → Stores** — watch for a TiKV node going down/recovering
- **Key Visualizer** → ensure traffic redistributes after failure

**Grafana → TiKV panel**
- `TiKV > Raft > Leader Changes` — spike when failure injected
- `TiKV > Raft > Region Heartbeat` — drops to 0 for failed node

**What to watch:** Raft leader re-election completes within 1–3 seconds.
Application-level errors spike briefly, then disappear as traffic re-routes.

---

### M3b — Write Contention / Hot Region
**TiDB Dashboard**
- **Key Visualizer** — look for a bright "hot stripe" during AUTO_INCREMENT phase;
  it should scatter into many colours during AUTO_RANDOM phase.

**Grafana → TiKV panel**
- `TiKV > Coprocessor > Hot Regions` — hot region count should drop in Phase B
- `TiKV > Server > CPU Usage` — single node spike in Phase A, even spread in Phase B

**What to watch:** The Key Visualizer is the money slide for this module —
screenshot it for both phases for the report.

---

### M4 — HTAP Concurrent Workload
**TiDB Dashboard**
- **Overview → QPS** — should show both OLTP and analytical query types

**Grafana → TiFlash panel**
- `TiFlash > Server > CPU Usage` — rises during analytics phase
- `TiKV > Server > CPU Usage` — should remain stable (isolation confirmed)

**Grafana → TiDB panel**
- `TiDB > Server > Query Duration` — compare p99 between OLTP-only and HTAP phases

**What to watch:** TiKV CPU is flat while TiFlash CPU rises — proving complete
resource isolation between OLTP and analytics workloads.

---

### M5 — Online DDL
**TiDB Dashboard**
- **Overview → DDL Jobs** — shows running DDL jobs and their progress %

**Grafana → TiDB panel**
- `TiDB > Server > QPS` — should remain non-zero throughout DDL
- `TiDB > Server > Query Duration` — p99 should stay within normal range

**What to watch:** QPS never drops to zero. DDL progress % moves from 0→100
while writes continue uninterrupted.

---

### M6 — MySQL Compatibility
No real-time monitoring needed for this module. Review the compat check
output table in the PDF report.

---

### M7 — Data Import Speed
**TiDB Dashboard**
- **Key Visualizer** — bulk import creates a distinctive write-heavy pattern
- **Cluster Info → Stores** — watch storage used grow across TiKV nodes

**Grafana → TiKV panel**
- `TiKV > Storage > Storage Size` — should rise during import
- `TiKV > Server > Write Flow` — MB/s write throughput

**What to watch:** For IMPORT INTO, writes bypass TiDB SQL layer and go
direct to TiKV — far higher MB/s than batched INSERT.

---

### M8 — Vector Search (AI Track)
**TiDB Dashboard**
- **Overview → QPS** — vector search queries appear as SELECT statements

**Grafana → TiFlash panel**
- `TiFlash > Server > CPU Usage` — HNSW index scan runs on TiFlash
- `TiFlash > Server > Memory` — watch for index memory footprint

**What to watch:** Latency stays sub-100ms at low concurrency. CPU rises
linearly with concurrent ANN searches on TiFlash.

---

## Tips

- **Screenshot Key Visualizer** at peak load for Module 3b — this is the most
  visually compelling slide in any TiDB PoV deck.
- **Export Grafana panels** using the Share button (camera icon) → PNG for
  inclusion in follow-up customer slides.
- **TiDB Dashboard slow query log** (`SQL Diagnose → Slow Queries`) — useful
  for validating customer queries in Module 0.
