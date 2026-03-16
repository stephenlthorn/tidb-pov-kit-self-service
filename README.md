# TiDB Cloud Self-Service PoV Kit

A fully automated Proof of Value toolkit for TiDB Cloud. Spin up a cluster,
edit one config file, and run a single command — the kit handles the rest and
produces a professional PDF report with:
- a buyer-facing decision summary
- exact test scope and environment context
- module evidence charts for executed modules only
- SQL compatibility fix index
- KPI appendix and 3-year TCO comparison

---

## What it tests

| Module | What it proves |
|--------|---------------|
| M0 — Customer Query Validation | Your SQL queries run on TiDB without changes |
| M1 — Baseline OLTP Performance | Raw throughput and latency under concurrent load, including pre-warm, warm steady-state, and dedicated point-get lookup phase |
| M2 — Elastic Auto-Scaling | TiDB Cloud adds capacity automatically; p99 stays flat |
| M3 — High Availability | Sub-30s RTO after a node failure with zero manual intervention |
| M3b — Write Contention | AUTO_RANDOM eliminates hot-region bottlenecks vs AUTO_INCREMENT |
| M4 — HTAP Concurrent | Analytics run on TiFlash without degrading OLTP on TiKV |
| M5 — Online DDL | Schema changes complete with zero application downtime |
| M6 — SQL Compatibility | TiDB SQL compatibility checks + source unsupported-feature inventory (MySQL/PostgreSQL/SQL Server) |
| M7 — Data Import | Bulk load throughput: IMPORT INTO vs LOAD DATA vs INSERT |
| M8 — Vector Search *(optional)* | ANN search with HNSW index (TiDB AI track) |

M6 also writes source feature inventory output to:
- `results/compat_source_unsupported_summary.json`

If a module is not selected in the run, it is shown in coverage tables but
omitted from the chart section to keep the report concise.

---

## Prerequisites

- Python 3.10+
- A TiDB Cloud account (free at [tidbcloud.com](https://tidbcloud.com))
- Network access to your TiDB cluster

---

## How This Kit Works (For Report Readers)

If you received a PoV report and want to understand the methodology, here is a plain-English summary:

**What generates the workload?**
An EC2 instance (`c7i.2xlarge` by default) in the customer's own AWS account runs a purpose-built OLTP load generator against TiDB Cloud. This is *not* a replay of the customer's production queries — it is a synthetic benchmark calibrated to the chosen industry profile (banking, healthcare, gaming, retail, etc.). The EC2 instance and load generator are spun up automatically by the kit and torn down when the run completes.

**Where does the test data come from?**
Industry-specific seed datasets (~3 GB each, covering OLTP transactions and OLAP aggregation tables) are pre-staged in a PingCAP S3 bucket. TiDB Cloud's `IMPORT INTO` command pulls the data directly from S3 into the cluster before tests begin. No data leaves TiDB Cloud; the import is a one-way inbound load.

**Is the kit "pre-tuned" to inflate results?**
Each test module applies the same TiDB best practices that any production deployment should use: `AUTO_RANDOM` primary keys (eliminating hot-region write bottlenecks), column-store `TiFlash` replicas for analytics, and schema settings matched to the workload pattern. These are not artificial optimizations — they are the recommended production configuration. The goal is to show what TiDB actually achieves when set up correctly.

**How does this compare to a traditional PoC?**
A traditional PoC typically requires the customer to provision infrastructure, load their own data, write benchmark scripts, and interpret raw results — a process that can take weeks. This kit compresses that to hours: infrastructure is automated, data loads from S3, modules run sequentially, and the report is generated automatically. The output is a decision-ready PDF, not a spreadsheet of raw numbers.

**Report reading order:**
1. **Prospect Decision Summary** — clear decision + recommended next step
2. **What Was Tested** — exact executed scope and environment
3. **Executive Summary** — headline KPIs (warm latency, throughput, SQL compatibility)
4. **Module charts** — evidence for each executed module
5. **SQL Compatibility Index** — checks that passed/failed + fix directions
6. **KPI appendix** — full threshold evaluation table for technical review

---

## Getting Started from CLI

This section is written to be self-sufficient. Follow every step in order — it is designed so that anyone with a TiDB Cloud account and AWS access can run a complete PoV without assistance.

### Step 1 — Get your TiDB Cloud credentials

1. Log in at [tidbcloud.com](https://tidbcloud.com) and create or select a cluster.
2. In the cluster console, click **Connect** → **General** tab.
3. Copy the values for **Host**, **Username** (looks like `<prefix>.root`), and **Password**.

> Serverless / Starter tier: use the provided username as-is (e.g. `4FuxFdNpnGxBi9D.root`). Do not shorten it.

### Step 2 — Set up AWS authentication

The kit needs AWS credentials in two places:
- To upload run artifacts to the PingCAP results S3 bucket (from your local machine or EC2)
- So TiDB Cloud can run `IMPORT INTO` against the dataset S3 bucket

**Choose one of the following auth options:**

---

**Option A — EC2 with IAM Instance Profile (recommended for automated / SE-led runs)**

Attach the `TidbPovKitEC2Role` IAM instance profile to your EC2 instance. The kit will use it automatically — no credentials to manage.

The instance profile needs these permissions:
```json
{
  "s3:PutObject", "s3:GetObject", "s3:ListBucket"  on  arn:aws:s3:::pingcap-tidb-pov-results-219248915861/*
  "kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"  on the bucket KMS key
}
```

Leave all `s3_*` auth fields blank in `config.yaml`. The instance profile is used automatically.

---

**Option B — AWS SSO / IAM Identity Center (recommended for local laptop runs)**

```bash
# Log in (do this once per session — tokens expire after ~8 hours)
aws sso login --profile <your-profile>

# Verify it works
aws sts get-caller-identity --profile <your-profile>

# Export so all subprocesses (including TiDB IMPORT INTO) see the credentials
export AWS_PROFILE=<your-profile>
```

If `aws sso login` returns an error, re-run it with `--no-browser` and paste the code manually.

---

**Option C — Static IAM access keys (env vars)**

```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

For temporary credentials (STS AssumeRole or SSO-generated):
```bash
export AWS_ACCESS_KEY_ID=ASIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_DEFAULT_REGION=us-east-1
```

Verify before running:
```bash
aws sts get-caller-identity
aws s3 ls s3://pingcap-tidb-pov-results-219248915861/tidb-pov/ --region us-east-1
```

---

**Option D — Cross-account AssumeRole (for `IMPORT INTO` from a separate account)**

Set in `config.yaml`:
```yaml
dataset_bootstrap:
  s3_role_arn: "arn:aws:iam::<ACCOUNT_ID>:role/YourRole"
  s3_external_id: "your-external-id"    # if required by the role's trust policy
```

---

### Step 3 — Clone and configure

```bash
git clone https://github.com/stephenlthorn/tidb-pov-kit-self-service.git tidb-pov-kit
cd tidb-pov-kit
bash scripts/bootstrap_cli.sh          # install Python deps

cp config.yaml.example config.yaml
```

Edit `config.yaml` — fill in the three required fields:
```yaml
tidb:
  host:     "gateway01.us-east-1.prod.aws.tidbcloud.com"   # from Connect dialog
  user:     "4FuxFdNpnGxBi9D.root"                          # exact username from Connect
  password: "your-password"
```

Everything else has sensible defaults. The S3 dataset bucket is pre-configured.

Optionally set your industry:
```yaml
industry:
  selected: "banking"   # general_auto | banking | healthcare | gaming | retail_ecommerce | saas | iot_telemetry | adtech | logistics
```

### Step 4 — Run

**Option A — Web UI (recommended for first run, guided wizard)**

```bash
./run_all.sh --web-ui
```

Open `http://localhost:8787` in your browser. Use the **Deployment Wizard** to confirm settings, then click **Run**.

**Option B — CLI (no browser needed)**

```bash
./run_all.sh config.yaml --no-menu --no-wizard
```

**Option C — Safe small end-to-end (clean slate, validates everything first)**

```bash
bash scripts/pov_safe_small_e2e.sh config.yaml
```

This variant: resets the DB, runs S3 preflight, uploads the `general_auto` dataset pack, runs all modules at small scale, and cleans up on exit.

### Step 5 — Get results

Local output:
```
results/tidb_pov_report.pdf       ← customer-ready PDF
results/metrics_summary.json
results/run_all.log
```

S3 (auto-uploaded when `POV_ENFORCE_S3_UPLOAD=true`):
```
s3://pingcap-tidb-pov-results-219248915861/tidb-pov/<project>/runs/<run_tag>/
```

### Common problems and fixes

| Problem | Fix |
|---------|-----|
| `Token has expired and refresh failed` | Re-run `aws sso login --profile <profile>` |
| `Unable to locate credentials` | Set `AWS_ACCESS_KEY_ID` / `AWS_PROFILE`, or attach an instance profile |
| `AccessDenied: kms:GenerateDataKey` | Add KMS key permissions to your IAM role (see Option A above) |
| `Run blocked: S3 archival is required` | Set `S3_BUCKET` env var or run with `POV_ENFORCE_S3_UPLOAD=false` |
| `Missing user name prefix` | Use the full TiDB Cloud username from Connect (e.g. `4FuxFdNpnGxBi9D.root`) |
| `Connection refused` | Verify host/port; check IP allowlist under Security → Network Access |
| IMPORT INTO fails on Serverless | Set `dataset_bootstrap.import_threads: 1` in `config.yaml` |
| Run "stuck" at a module step | Normal — each module has pre-warm + ramp + test phases (~8–10 min total for M1). Let it run. |

### Useful shortcuts

```bash
./run_all.sh --menu              # interactive module picker
./run_all.sh --report-only       # regenerate PDF from existing results
./run_all.sh --report-json-only  # regenerate JSON summary only
```

---

## Advanced Runtime Paths

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
  point_get_phase_enabled: true
  point_get_duration_seconds: 120
  point_get_concurrency: 32
  ramp_duration_seconds: 300
  import_rows:          1000000
  import_batch_size:    5000
  import_into_threads:  0         # 0=auto; set 1 for tiny tiers
  import_into_source_uri: ""        # optional s3://bucket/path/file.csv
  import_source_size_gb: 0.0        # optional, for GB/min with remote import

# First-step S3 dataset bootstrap — loads industry-specific ~3 GB OLTP+OLAP packs.
# Pre-staged datasets live in the PingCAP-owned results bucket by default.
# Auth: leave all s3_* auth fields blank if running on EC2 with an instance profile.
dataset_bootstrap:
  enabled: true
  required: true
  profile_key: ""                    # optional override; defaults to industry.selected
  manifest_uri: ""                   # optional override; defaults to PingCAP bucket
  s3_bucket: "pingcap-tidb-pov-results-219248915861"
  s3_prefix: "tidb-pov/datasets"
  aws_region: "us-east-1"
  # Auth for TiDB IMPORT INTO from S3 (choose ONE path, or leave blank for instance profile):
  s3_role_arn: ""                    # Option B: cross-account AssumeRole ARN
  s3_external_id: ""
  s3_access_key_id: ""              # Option C: static access keys
  s3_secret_access_key: ""
  s3_session_token: ""              # required for temporary/session credentials
  import_threads: 0                 # 0=auto; set 1 on tiny tiers if IMPORT INTO hits CPU guardrail
  parallel_import_jobs: 2
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
4. To highlight low-latency API lookups, keep `point_get_phase_enabled=true` and run from EC2 in the same region/path as TiDB Cloud.

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
→ `pov_safe_small_e2e.sh` hard-fails early if TiDB Cloud username prefix format is invalid.

**`Missing user name prefix`**
→ Use the exact TiDB Cloud username from Connect (example: `<prefix>.root` for Starter/Essential/Premium/Serverless).
→ Dedicated/BYOC may not require prefix in the same format.

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

**`Token has expired and refresh failed` / SSO token errors**
→ Re-authenticate AWS SSO before running:
```bash
aws sso login --profile <your-profile> --no-browser
```
→ `pov_safe_small_e2e.sh` now attempts SSO re-auth automatically when it detects an SSO profile in an interactive terminal.

**`--no-wizard: command not found` when using `POV_ENV_FILE`**
→ Your env file contains non `KEY=VALUE` lines.
→ `scripts/pov_pull_run_upload.sh` now safely loads only valid env assignments and ignores other lines.

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
