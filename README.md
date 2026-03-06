# TiDB Cloud Self-Service PoV Kit

A fully automated Proof of Value toolkit for TiDB Cloud. Spin up a cluster,
edit one config file, and run a single command — the kit handles the rest and
produces a professional PDF report with charts, latency tables, and a 3-year
TCO comparison.

---

## What it tests

| Module | What it proves |
|--------|---------------|
| M0 — Customer Query Validation | Your SQL queries run on TiDB without changes |
| M1 — Baseline OLTP Performance | Raw throughput and latency under concurrent load |
| M2 — Elastic Auto-Scaling | TiDB Cloud adds capacity automatically; p99 stays flat |
| M3 — High Availability | Sub-30s RTO after a node failure with zero manual intervention |
| M3b — Write Contention | AUTO_RANDOM eliminates hot-region bottlenecks vs AUTO_INCREMENT |
| M4 — HTAP Concurrent | Analytics run on TiFlash without degrading OLTP on TiKV |
| M5 — Online DDL | Schema changes complete with zero application downtime |
| M6 — MySQL Compatibility | 95%+ MySQL syntax and semantic compatibility |
| M7 — Data Import | Bulk load throughput: IMPORT INTO vs LOAD DATA vs INSERT |
| M8 — Vector Search *(optional)* | ANN search with HNSW index (TiDB AI track) |

---

## Prerequisites

- Python 3.9+
- A TiDB Cloud account (free at [tidbcloud.com](https://tidbcloud.com))
- Network access to your TiDB cluster

---

## Quick Start

### Step 1 — Provision a cluster

Follow **`setup/00_provision.md`** and start with **Serverless (Starter)**
unless your requirements gate you to Essential/Premium/Dedicated/BYOC.

### Step 2 — Configure

```bash
cp config.yaml.example config.yaml
# Open config.yaml and fill in host, port, user, password
```

Optional: preload AWS runner defaults for cross-account execution in UI/config:

```bash
cp .env.example .env
# export vars from .env into your shell before launching web UI
```

Optionally add your own SQL queries under `customer_queries:` to validate them
in Module 0 and include them in the OLTP workload.

Vercel deployment note:
1. Import repo `stephenlthorn/tidb-pov-kit-self-service`
2. Set project root directory to repo root (`.`)
3. Vercel uses root Flask entrypoint `app.py`
4. Configure env vars from `.env.example` in Vercel project settings
5. Attach Vercel Postgres (or set `DATABASE_URL`) for persistent users/invites/config state
6. Configure S3 env vars (`S3_BUCKET`, `S3_PREFIX`, `S3_ARTIFACTS_ENABLED=true`) to persist PDF/metrics/log artifacts
7. For cross-account AWS runner launch, set control credentials that can call `sts:AssumeRole`:
   - preferred: `AWS_CONTROL_ACCESS_KEY_ID` + `AWS_CONTROL_SECRET_ACCESS_KEY`
   - fallback: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`

### Step 3 — Run

```bash
chmod +x run_all.sh
./run_all.sh
```

`run_all.sh` opens an interactive control panel by default in terminal sessions.
From that parent menu you can:
1. Run PoC with defaults
2. Choose cloud tier (including Dedicated)
3. Run security screener
4. Print/open PDF report (after completed PoC)
5. Clear PoC data with confirmation
6. Exit

The intake flow in `run_all.sh` supports:
1. Runs a tier decision tree
2. Captures a pre-PoC security/shared-responsibility checklist
3. Writes `results/pre_poc_checklist.md` + `results/pre_poc_intake.json`
4. Builds `results/config.resolved.yaml` and runs the kit automatically

Direct-run shortcuts:

```bash
./run_all.sh --no-menu --no-wizard
./run_all.sh --menu
./run_all.sh --web-ui
```

Dark web UI:
1. Open with `./run_all.sh --web-ui` (or `python setup/poc_web_ui.py`)
2. Use the Quickstart Wizard for guided setup + optional auto-run, or use full Configuration for advanced tuning
3. Use Test Planner to view per-module test insights and choose all/some suites before execution
4. Run security screener, run defaults, build report-only, and clear/reset data
5. In Manual Config -> AWS Runner, use:
   - `Validate AWS Runner` (AssumeRole + subnet/SG/AMI + DryRun check)
   - `Launch AWS Runner` (boots Amazon Linux if AMI is blank, installs deps, runs Workload Generator)
   - `Refresh Runner Status` / `Terminate Runner`

UI access control (invite-based):
1. First launch redirects to `/setup-admin` to create the first admin account.
2. Admin signs in and opens the `Admin` tab.
3. Admin can generate either:
   - email-scoped invite link/code (restricted to one email), or
   - open invite code (usable by any email, with max-use and expiry controls).
4. Invitees open `/signup`, enter email + password + invite code, and get user access.

Workload Lab (Workload Generator flow):
1. Open the `Workload Lab` tab.
2. Set mode: `rawsql`, `tpcc`, or `ycsb`.
3. Configure DSN, load generator hosts, SSH details, and mode-specific settings.
4. Use `Validate`, `Dry Run`, then `Run`.
5. Run outputs are written to `runs/<timestamp>_<mode>_<tag>/` with:
   - `resolved_config.yaml`
   - `commands.json`
   - `loadgens/*.log`
   - `summary.json`
   - `summary.md`
   - `chart_data.json`

Sample rawsql assets:
- `load/sql/rawsql_mix.sql` (high-frequency point reads + simple updates)
- `load/sql/rawsql_sample_schema.sql` (schema scaffold for quick prep)

That's it. The kit will:
1. Install Python dependencies
2. Generate synthetic data (3 schema archetypes, configurable scale)
3. Run all enabled test modules sequentially
4. Produce `results/tidb_pov_report.pdf`

To regenerate only the PDF from existing `results/` artifacts (no load/tests):

```bash
./run_all.sh --report-only
```

To regenerate only `results/metrics_summary.json` from existing artifacts:

```bash
./run_all.sh --report-json-only
```

---

## Configuration Reference (`config.yaml`)

```yaml
tidb:
  host:     "your-cluster.tidbcloud.com"
  port:     4000
  user:     "root"
  password: "your-password"
  database: "test"
  ssl:      true

# Optional: side-by-side comparison target (multi-engine config).
# Current automated runner support: Aurora MySQL, MySQL, RDS MySQL, SingleStore.
comparison_db:
  enabled:  false
  target:   "aurora_mysql"  # aurora_mysql | mysql | rds_mysql | postgres | rds_postgres | aurora_postgres | microsoft_sql_server | singlestore
  label:    "Aurora MySQL"
  host:     "aurora-cluster.us-west-2.rds.amazonaws.com"
  port:     3306
  user:     "admin"
  password: "your-password"
  database: "pov_test"
  schema:   "public"        # postgres: public, sql server: dbo
  ssl:      false
  ssl_mode: "require"       # postgres: disable | require | verify-ca | verify-full
  sqlserver_driver: "ODBC Driver 18 for SQL Server"
  sqlserver_encrypt: true
  sqlserver_trust_server_certificate: false

# Tier metadata (wizard updates this)
tier:
  selected: "serverless"   # serverless | essential | premium | dedicated | byoc

# Optional: launch workload generators in customer AWS account (AssumeRole)
aws_runner:
  enabled: false
  launch_mode: "customer_assume_role"
  connectivity_mode: "private_endpoint"   # private_endpoint | public_endpoint
  aws_region: "us-east-1"
  customer_account_id: "219248915861"
  customer_assume_role_arn: "arn:aws:iam::219248915861:role/TidbPovCustomerRunnerLaunchRole"
  external_id: "tidbpov-...generated..."
  vpc_id: "vpc-..."
  subnet_id: "subnet-..."
  security_group_id: "sg-..."
  runner_instance_profile_name: "TidbPovRunnerInstanceRole"
  runner_role_arn: "arn:aws:iam::219248915861:role/TidbPovRunnerInstanceRole"
  instance_size: "medium"                  # small | medium | large
  allowed_instance_types: ["c7i.2xlarge","c7i.4xlarge","c7i.8xlarge"]
  max_instances_per_run: 8
  summary_upload_only: true
  run_timeout_minutes: 180

test:
  data_scale:           "small"     # serverless default: small
  duration_seconds:     120         # seconds per phase
  concurrency_levels:   [8,16,32]
  ramp_duration_seconds: 300
  import_rows:          1000000

# Your production queries (optional — validated and replayed in M0/M1)
customer_queries:
  - name: "example_query"
    sql:  "SELECT * FROM users WHERE id = ?"
    params: [1]

# Toggle individual modules on/off
modules:
  customer_queries: true
  baseline_perf:    true
  elastic_scale:    true
  high_availability: false   # Dedicated/BYOC only by default
  write_contention: true
  htap:             false    # Enable when TiFlash is provisioned
  online_ddl:       true
  mysql_compat:     true
  data_import:      true
  vector_search:    false   # set true for AI track

report:
  customer_name: "Acme Corp"
  se_name:       "Jane Smith — PingCAP"

# Optional TCO model overrides
tco:
  data_size_gb:              1000
  annual_growth_pct:         40
  aurora_shards_year0:       4
  engineers_managing_shards: 2
```

---

## Data Scales

| Scale  | Schema A rows | Schema B events | Schema C tenants | Approx. size |
|--------|--------------|-----------------|------------------|-------------|
| small  | 100K users, 500K txns | 1M events | 100 tenants | ~2 GB |
| medium | 500K users, 5M txns | 10M events | 500 tenants | ~20 GB |
| large  | 2M users, 20M txns | 50M events | 2K tenants | ~100 GB |

---

## Output Files

```
results/
  tidb_pov_report.pdf       ← customer-ready PDF report
  results.db                ← SQLite database with all raw results
  metrics_summary.json      ← JSON summary used by the report generator
  data_manifest.json        ← Generated data row counts and schema info
  tco_chart.png             ← Standalone TCO chart (also embedded in PDF)
  run_all.log               ← Full run log
```

---

## Running Individual Modules

Each module can be run standalone for debugging:

```bash
python tests/01_baseline_perf/run.py config.yaml
python tests/04_htap_concurrent/run.py config.yaml
python report/generate_report.py config.yaml
```

---

## Side-by-Side Comparison Targets

Set `comparison_db.target` to one of:
- `aurora_mysql`
- `mysql`
- `rds_mysql`
- `postgres`
- `rds_postgres`
- `aurora_postgres`
- `microsoft_sql_server`
- `singlestore`

Automated side-by-side execution is currently supported for:
- `aurora_mysql`, `mysql`, `rds_mysql`, `singlestore`

PostgreSQL and Microsoft SQL Server targets are fully configurable in the UI and config, and are retained in project configuration for comparison planning. The current workload runner remains MySQL-dialect, so these targets are not yet executed automatically.

---

## Observability During the Run

Open **TiDB Dashboard** and **Grafana** in your browser while the kit runs.
See **`setup/02_observability_guide.md`** for the exact panels to watch for
each module — with descriptions of what to screenshot for customer slides.

---

## Troubleshooting

**Connection refused / authentication error**
→ Double-check `host`, `port`, `user`, `password` in `config.yaml`.
→ Ensure your IP is whitelisted under Security → Network Access.
→ `run_all.sh` will now prompt to update connection values and retry when this check fails in an interactive terminal.

**Checklist returns HOLD before tests start**
→ Open `results/pre_poc_checklist.md` and resolve blocking items.
→ Use `--allow-blocked` only for dry-runs where risk is explicitly accepted.

**Module skipped / `not_run` in report**
→ The module is disabled in `config.yaml` under `modules:`, or it was skipped
  because a prerequisite (e.g. TiFlash) isn't available on your cluster tier.

**TiFlash-related errors (M4, M8)**
→ TiFlash requires TiFlash nodes to be provisioned and replicated.
  Intake enables M4 by default only for Dedicated/BYOC tiers.
  Add at least 2 TiFlash nodes in the TiDB Cloud console and wait for
  replication to complete before running M4/M8.

**High Availability expectations on Serverless/Starter**
→ Full node-stop HA validation is a Dedicated/BYOC exercise.
  For lower tiers, keep M3 disabled or run only simulated HA mode.

**IMPORT INTO fails (M7)**
→ `IMPORT INTO` with `file://` URI requires TiDB >= 7.2 and the file to be
  accessible from the TiDB server. For TiDB Cloud, use an S3 URI instead.
  The module falls back to LOAD DATA and INSERT automatically.

**Out of memory during large data generation**
→ Reduce `data_scale` to `small` or `medium` in `config.yaml`.

---

## Project Structure

```
tidb-pov-kit/
├── config.yaml             ← Edit this first
├── run_all.sh              ← Run this to execute everything
├── requirements.txt
├── setup/
│   ├── 00_provision.md     ← Cluster setup guide
│   ├── 01_install_deps.sh  ← Dependency installer
│   ├── 02_observability_guide.md
│   ├── 03_pre_poc_checklist.md
│   ├── poc_control_panel.py← Parent interactive control panel
│   ├── poc_web_ui.py       ← Dark web UI for full configuration/workflow
│   ├── pre_poc_intake.py   ← Tier decision + security checklist wizard
│   ├── generate_data.py    ← Synthetic data generator
│   └── templates/
│       └── poc_web_ui.html ← Web UI template
├── lib/
│   ├── db_utils.py         ← MySQL connection helpers
│   └── result_store.py     ← SQLite results writer
├── load/
│   ├── workload_definitions.py  ← Query pools
│   └── load_runner.py           ← Concurrent load generator
├── tests/
│   ├── 00_customer_queries/
│   ├── 01_baseline_perf/
│   ├── 02_elastic_scale/
│   ├── 03_high_availability/
│   ├── 03b_write_contention/
│   ├── 04_htap_concurrent/
│   ├── 05_online_ddl/
│   ├── 06_mysql_compat/
│   ├── 07_data_import/
│   └── 08_vector_search/
├── report/
│   ├── collect_metrics.py  ← Aggregates results.db
│   ├── generate_report.py  ← Builds the PDF
│   └── tco_model.py        ← 3-year TCO calculator
└── results/                ← All output lives here (git-ignored)
```

---

*Built by PingCAP Sales Engineering. For questions, contact your PingCAP SE.*
