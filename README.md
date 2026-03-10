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
| M1 — Baseline OLTP Performance | Raw throughput and latency under concurrent load, including pre-warm + warm steady-state phase |
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

## Start Here (Easy)

### 1) Provision TiDB Cloud

Start with **Serverless/Starter** unless you already know you need Essential, Premium, Dedicated, or BYOC.
Provisioning guide: `setup/00_provision.md`.

### 2) Configure Connection

```bash
cp config.yaml.example config.yaml
# fill tidb.host, tidb.user, tidb.password, tidb.database
```

### 2.5) Optional: Pre-stage S3 Dataset Packs (recommended for demo/showcase runs)

This creates pluggable OLTP + OLAP seed packs for all industries and uploads them to S3:

```bash
python3 scripts/publish_dataset_packs_s3.py \
  --bucket <your-s3-bucket> \
  --prefix tidb-pov/datasets \
  --region us-east-1 \
  --industries all \
  --target-gb-per-family 0.25 \
  --shards 8
```

Then enable first-step bootstrap import in `config.yaml`:

```yaml
dataset_bootstrap:
  enabled: true
  required: true
  s3_bucket: "<your-s3-bucket>"
  s3_prefix: "tidb-pov/datasets"
  aws_region: "us-east-1"
  skip_synthetic_generation: false
```

`run_all.sh` will execute this bootstrap before synthetic generation and load OLTP + OLAP tables using `IMPORT INTO` from S3.

### 3) Choose How You Run

#### Path A — Web UI (recommended for first run)

```bash
bash scripts/bootstrap_cli.sh
./run_all.sh --web-ui
```

Then:
1. Use **Deployment Wizard** for guided setup (Industry + tests + tier).
2. Save and run.
3. Open report from `results/tidb_pov_report.pdf` (or S3 if enforced).

#### Path B — CLI (recommended for repeatable automation)

Local CLI run:

```bash
bash scripts/bootstrap_cli.sh
./run_all.sh config.yaml --no-menu --no-wizard
```

Safe small run (clean DB + EC2 cleanup before/after):

```bash
bash scripts/pov_safe_small_e2e.sh config.yaml
```

### 4) Enforce S3 Upload (recommended)

```bash
export POV_ENFORCE_S3_UPLOAD=true
export POV_S3_BUCKET=<bucket>
export POV_S3_PREFIX=tidb-pov
export POV_S3_PROJECT=<project-slug>
export POV_S3_REGION=us-east-1
```

With enforcement enabled, the run hard-fails if artifacts cannot be archived to S3.

### 5) Output Locations

Local:
- `results/tidb_pov_report.pdf`
- `results/metrics_summary.json`
- `results/results.db`
- `results/run_all.log`

S3:
- `s3://<bucket>/<prefix>/<project>/runs/<run_tag>/...`

### Useful Shortcuts

```bash
./run_all.sh --menu
./run_all.sh --report-only
./run_all.sh --report-json-only
```

---

## Advanced Runtime Paths

### Scripted Pull + Run + S3 Archive

```bash
export POV_S3_BUCKET=<your-bucket>
export POV_S3_PREFIX=tidb-pov
export POV_S3_PROJECT=<project-slug>
export POV_CONFIG_SOURCE=/absolute/path/to/config.yaml
curl -fsSL https://raw.githubusercontent.com/stephenlthorn/tidb-pov-kit-self-service/main/scripts/pov_pull_run_upload.sh | bash
```

### EC2 Script-Only Fast Path

```bash
sudo yum install -y git python3 || sudo apt-get update && sudo apt-get install -y git python3 python3-pip
git clone https://github.com/stephenlthorn/tidb-pov-kit-self-service.git ~/tidb-pov-kit-self-service-runner || true
cd ~/tidb-pov-kit-self-service-runner
git pull --ff-only origin main
export POV_ENV_FILE=~/pov_vm.env
bash scripts/pov_pull_run_upload.sh
```

See also:
- `docs/script_only_secure_s3_runner.md`
- `docs/aws/policies/pov_results_bucket_policy_template.json`
- `docs/aws/policies/pov_uploader_role_policy_template.json`

---

## Configuration Reference (`config.yaml`)

```yaml
tidb:
  host:     "your-cluster.tidbcloud.com"
  port:     4000
  user:     "<prefix>.root"
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

# Industry profile (Quickstart dropdown)
industry:
  selected: "general_auto"  # general_auto | banking | healthcare | gaming | retail_ecommerce | saas | iot_telemetry | adtech | logistics

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
  instance_size: "small"                   # small | medium | large
  allowed_instance_types: ["c7i.2xlarge","c7i.4xlarge","c7i.8xlarge"]
  max_instances_per_run: 8
  summary_upload_only: true
  run_timeout_minutes: 180

test:
  run_mode:            "validation"  # validation | performance
  schema_mode:         "tidb_optimized"  # tidb_optimized | mysql_compatible
  data_scale:           "small"     # serverless default: small
  duration_seconds:     120         # seconds per phase
  concurrency_levels:   [8,16,32]
  pre_warm_enabled:     true
  pre_warm_duration_seconds: 120
  pre_warm_concurrency: 16
  warm_phase_enabled:   true
  warm_phase_duration_seconds: 300
  warm_phase_concurrency: 32
  ramp_duration_seconds: 300
  import_rows:          1000000
  import_into_source_uri: ""        # optional s3://bucket/path/file.csv
  import_source_size_gb: 0.0        # optional, for GB/min with remote import

# Optional: first-step S3 dataset bootstrap (industry-pluggable OLTP/OLAP packs)
dataset_bootstrap:
  enabled: false
  required: false
  profile_key: ""                    # optional override, otherwise uses industry.selected
  manifest_uri: ""                   # optional s3://bucket/prefix/manifest.json
  s3_bucket: ""
  s3_prefix: "tidb-pov/datasets"
  aws_region: "us-east-1"
  oltp_table: "poc_seed_oltp"
  olap_table: "poc_seed_olap"
  enable_tiflash_for_olap: true
  skip_synthetic_generation: false

# Notes:
# - validation mode keeps broad self-service defaults.
# - performance mode is intended for high-throughput benchmarking workflows.
# - tidb_optimized schema mode applies TiDB-friendly key/table options for write-heavy paths.

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
  # Optional: override KPI threshold guidance used in PDF appendix.
  # Supported tier keys: all_tiers, serverless|starter, essential, premium, dedicated, byoc
  # kpi_threshold_overrides:
  #   all_tiers:
  #     01_baseline_perf:
  #       p99_ms_warn: 90
  #   premium:
  #     01_baseline_perf:
  #       p99_ms_warn: 45
  #       p99_ms_fail: 120

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
| small  | 50K users, 5M txns | 5M events | 1K tenants | multi-GB |
| medium | 500K users, 50M txns | 50M events | 10K tenants | tens of GB |
| large  | 2M users, 200M txns | 200M events | 50K tenants | 100GB+ |

## PoC Sizing Guidance (EC2)

Use these as practical starting points for load generators.

| Profile | EC2 instance type | Runner count | Throughput target envelope | Recommended starting knobs |
|--------|--------------------|--------------|----------------------------|-----------------------------|
| Small  | `c7i.2xlarge`      | 1            | up to ~50k QPS             | `data_scale=small`, `concurrency_levels=[8,16,32]`, `duration_seconds=120` |
| Medium | `c7i.4xlarge`      | 1-2          | up to ~200k QPS            | `data_scale=medium`, `concurrency_levels=[16,64,128]`, `duration_seconds=180-300` |
| Large  | `c7i.8xlarge`      | 2-4          | up to ~1M QPS              | `data_scale=large`, `concurrency_levels=[64,128,256]`, longer warm phase + Workload Generator |

Notes:
1. These are targets, not guarantees. Final QPS depends on query shape, latency, network path, and cluster tier.
2. For 500k+ QPS, use multi-loadgen Workload Generator mode (`rawsql`) and keep load generators in the same region/VPC path as TiDB.
3. Keep `POV_ENFORCE_S3_UPLOAD=true` so runs fail closed if S3 write/read is not available.

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

**Dataset bootstrap from S3 fails (Step 4)**
→ Verify `dataset_bootstrap.enabled=true`, valid manifest path
  (`manifest_uri` or `s3_bucket + s3_prefix`), AWS credentials, and TiDB Cloud
  `IMPORT INTO` access to the S3 objects.
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
