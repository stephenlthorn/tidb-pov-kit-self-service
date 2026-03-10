#!/usr/bin/env bash
set -euo pipefail

# Safe small end-to-end PoV runner:
# 1) terminate any existing EC2 with tag tidb-pov-managed=true
# 2) drop + recreate TiDB database from config
# 3) enforce small defaults for run profile (general_auto + full module suite)
# 4) publish/upload general dataset pack to S3
# 5) run PoV
# 6) terminate leftover tagged EC2 again

CONFIG_PATH="${1:-config.small.yaml}"
TIER="${POV_TIER:-serverless}"
STRICT_AWS_CHECK="${POV_STRICT_AWS_CHECK:-true}"
OPEN_REPORT="${POV_OPEN_REPORT:-false}"
PUBLISH_GENERAL_DATASET="${POV_PUBLISH_GENERAL_DATASET:-true}"
DATASET_TARGET_GB="${POV_DATASET_TARGET_GB:-0.05}"
DATASET_SHARDS="${POV_DATASET_SHARDS:-8}"

is_true() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" || "${v}" == "on" ]]
}

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "[safe-e2e] config file not found: ${CONFIG_PATH}"
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[safe-e2e] python3 is required"
  exit 2
fi

if [[ ! -x "./run_all.sh" ]]; then
  chmod +x ./run_all.sh
fi

if ! command -v aws >/dev/null 2>&1; then
  if is_true "${STRICT_AWS_CHECK}"; then
    echo "[safe-e2e] aws CLI is required for strict EC2 cleanup checks."
    echo "           install AWS CLI or set POV_STRICT_AWS_CHECK=false"
    exit 2
  fi
fi

read_region_from_cfg() {
  python3 - "$1" <<'PY'
import sys, yaml
p = sys.argv[1]
cfg = yaml.safe_load(open(p)) or {}
runner = cfg.get("aws_runner") or {}
region = str(runner.get("aws_region") or "").strip()
print(region or "")
PY
}

read_dataset_s3_from_cfg() {
  python3 - "$1" <<'PY'
import sys, yaml
p = sys.argv[1]
cfg = yaml.safe_load(open(p)) or {}
ds = cfg.get("dataset_bootstrap") or {}
bucket = str(ds.get("s3_bucket") or "").strip()
prefix = str(ds.get("s3_prefix") or "tidb-pov/datasets").strip() or "tidb-pov/datasets"
region = str(ds.get("aws_region") or "").strip()
print(f"{bucket}|{prefix}|{region}")
PY
}

AWS_REGION_CFG="$(read_region_from_cfg "${CONFIG_PATH}")"
AWS_REGION="${AWS_REGION:-${POV_S3_REGION:-${AWS_REGION_CFG:-us-east-1}}}"
export AWS_REGION

aws_identity_check() {
  if ! command -v aws >/dev/null 2>&1; then
    return 0
  fi
  if ! aws sts get-caller-identity --region "${AWS_REGION}" >/dev/null 2>&1; then
    if is_true "${STRICT_AWS_CHECK}"; then
      echo "[safe-e2e] AWS identity check failed in region ${AWS_REGION}."
      echo "           run: aws sso login --profile <profile>  or export credentials"
      exit 2
    else
      echo "[safe-e2e] warning: AWS identity check failed; skipping EC2 cleanup checks."
    fi
  fi
}

terminate_tagged_instances() {
  if ! command -v aws >/dev/null 2>&1; then
    return 0
  fi
  local ids
  ids="$(
    aws ec2 describe-instances \
      --region "${AWS_REGION}" \
      --filters "Name=tag:tidb-pov-managed,Values=true" \
                "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query "Reservations[].Instances[].InstanceId" \
      --output text 2>/dev/null || true
  )"

  if [[ -n "${ids}" && "${ids}" != "None" ]]; then
    echo "[safe-e2e] terminating PoV-managed EC2: ${ids}"
    aws ec2 terminate-instances --region "${AWS_REGION}" --instance-ids ${ids} >/dev/null
    aws ec2 wait instance-terminated --region "${AWS_REGION}" --instance-ids ${ids}
    echo "[safe-e2e] EC2 termination complete."
  else
    echo "[safe-e2e] no active tidb-pov-managed EC2 instances found."
  fi
}

reset_tidb_database() {
  python3 - "${CONFIG_PATH}" <<'PY'
import sys, yaml
import mysql.connector

cfg = yaml.safe_load(open(sys.argv[1])) or {}
tidb = cfg.get("tidb") or {}
required = ["host", "user", "password"]
missing = [k for k in required if not str(tidb.get(k, "")).strip()]
if missing:
    raise SystemExit(f"[safe-e2e] missing tidb fields in config: {', '.join(missing)}")

db = str(tidb.get("database") or "test").strip() or "test"
conn = mysql.connector.connect(
    host=tidb["host"],
    port=int(tidb.get("port", 4000)),
    user=tidb["user"],
    password=tidb["password"],
    ssl_disabled=not bool(tidb.get("ssl", True)),
    connection_timeout=30,
)
cur = conn.cursor()
cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
cur.execute(f"CREATE DATABASE `{db}`")
conn.close()
print(f"[safe-e2e] reset TiDB database: {db}")
PY
}

enforce_small_defaults() {
  python3 - "${CONFIG_PATH}" <<'PY'
import sys, yaml
p = sys.argv[1]
cfg = yaml.safe_load(open(p)) or {}
test = cfg.setdefault("test", {})
runner = cfg.setdefault("aws_runner", {})
industry = cfg.setdefault("industry", {})
bootstrap = cfg.setdefault("dataset_bootstrap", {})
mods = cfg.setdefault("modules", {})

# Keep run in validation path so M0-M8 all execute.
test["run_mode"] = "validation"
test["data_scale"] = "small"
test["duration_seconds"] = int(test.get("duration_seconds", 120) or 120)
test["concurrency_levels"] = [8, 16, 32]
test["pre_warm_enabled"] = True
test["pre_warm_duration_seconds"] = int(test.get("pre_warm_duration_seconds", 60) or 60)
test["pre_warm_concurrency"] = int(test.get("pre_warm_concurrency", 8) or 8)
test["warm_phase_enabled"] = True
test["warm_phase_duration_seconds"] = int(test.get("warm_phase_duration_seconds", 120) or 120)
test["warm_phase_concurrency"] = int(test.get("warm_phase_concurrency", 16) or 16)
test["import_rows"] = int(test.get("import_rows", 250000) or 250000)
test["import_batch_size"] = int(test.get("import_batch_size", 2500) or 2500)

runner["instance_size"] = "small"
runner["auto_shutdown_minutes"] = int(runner.get("auto_shutdown_minutes", 10) or 10)
industry["selected"] = "general_auto"

bootstrap["enabled"] = True
bootstrap["required"] = True
bootstrap["profile_key"] = "general_auto"
bootstrap["s3_prefix"] = str(bootstrap.get("s3_prefix") or "tidb-pov/datasets")
bootstrap["aws_region"] = str(bootstrap.get("aws_region") or "us-east-1")
bootstrap["skip_synthetic_generation"] = False

# Force all standard modules on for full test sweep.
mods["customer_queries"] = True
mods["baseline_perf"] = True
mods["elastic_scale"] = True
mods["high_availability"] = True
mods["write_contention"] = True
mods["htap"] = True
mods["online_ddl"] = True
mods["mysql_compat"] = True
mods["data_import"] = True
mods["vector_search"] = True

with open(p, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)
print("[safe-e2e] enforced small defaults + general_auto dataset bootstrap + full module suite.")
PY
}

publish_general_dataset_pack() {
  if ! is_true "${PUBLISH_GENERAL_DATASET}"; then
    echo "[safe-e2e] dataset publish skipped (POV_PUBLISH_GENERAL_DATASET=false)."
    return 0
  fi

  local ds_cfg
  local ds_bucket
  local ds_prefix
  local ds_region
  ds_cfg="$(read_dataset_s3_from_cfg "${CONFIG_PATH}")"
  ds_bucket="${ds_cfg%%|*}"
  ds_prefix="${ds_cfg#*|}"
  ds_region="${ds_prefix##*|}"
  ds_prefix="${ds_prefix%|*}"

  ds_bucket="${POV_DATASET_BUCKET:-${POV_S3_BUCKET:-${ds_bucket}}}"
  ds_prefix="${POV_DATASET_PREFIX:-${POV_S3_PREFIX:-${ds_prefix}}}"
  ds_region="${POV_DATASET_REGION:-${POV_S3_REGION:-${ds_region:-${AWS_REGION}}}}"

  if [[ -z "${ds_bucket}" ]]; then
    echo "[safe-e2e] dataset S3 bucket missing."
    echo "           set dataset_bootstrap.s3_bucket in config or POV_DATASET_BUCKET / POV_S3_BUCKET."
    exit 2
  fi

  # Persist resolved dataset location so run_all/bootstrap_dataset consume the same path.
  python3 - "${CONFIG_PATH}" "${ds_bucket}" "${ds_prefix}" "${ds_region}" <<'PY'
import sys, yaml
p, bucket, prefix, region = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
cfg = yaml.safe_load(open(p)) or {}
ds = cfg.setdefault("dataset_bootstrap", {})
ds["s3_bucket"] = bucket
ds["s3_prefix"] = prefix
ds["aws_region"] = region
with open(p, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)
print(f"[safe-e2e] persisted dataset bootstrap location: s3://{bucket}/{prefix} ({region})")
PY

  echo "[safe-e2e] publishing general_auto dataset pack to s3://${ds_bucket}/${ds_prefix}"
  python3 scripts/publish_dataset_packs_s3.py \
    --bucket "${ds_bucket}" \
    --prefix "${ds_prefix}" \
    --region "${ds_region}" \
    --industries general_auto \
    --target-gb-per-family "${DATASET_TARGET_GB}" \
    --shards "${DATASET_SHARDS}"
}

echo "[safe-e2e] config: ${CONFIG_PATH}"
echo "[safe-e2e] region: ${AWS_REGION}"
aws_identity_check
terminate_tagged_instances
reset_tidb_database
enforce_small_defaults
publish_general_dataset_pack

echo "[safe-e2e] starting PoV run..."
./run_all.sh "${CONFIG_PATH}" --no-menu --no-wizard --tier "${TIER}"

echo "[safe-e2e] post-run EC2 cleanup..."
terminate_tagged_instances

if is_true "${OPEN_REPORT}"; then
  if command -v open >/dev/null 2>&1; then
    open results/tidb_pov_report.pdf || true
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open results/tidb_pov_report.pdf || true
  fi
fi

echo "[safe-e2e] done"
