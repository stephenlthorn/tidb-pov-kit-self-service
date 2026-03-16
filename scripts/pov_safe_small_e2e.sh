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
STRICT_AWS_CHECK="${POV_STRICT_AWS_CHECK:-true}"
OPEN_REPORT="${POV_OPEN_REPORT:-false}"
PUBLISH_GENERAL_DATASET="${POV_PUBLISH_GENERAL_DATASET:-true}"
DATASET_TARGET_GB="${POV_DATASET_TARGET_GB:-0.05}"
DATASET_SHARDS="${POV_DATASET_SHARDS:-8}"
CLEANUP_RAN="false"

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

profile_uses_sso() {
  local profile
  profile="${AWS_PROFILE:-default}"
  if ! command -v aws >/dev/null 2>&1; then
    return 1
  fi
  [[ -n "$(aws configure get sso_session --profile "${profile}" 2>/dev/null || true)" ]]
}

aws_reauth_if_sso() {
  local profile
  profile="${AWS_PROFILE:-default}"
  if ! profile_uses_sso; then
    return 1
  fi
  if [[ ! -t 0 ]]; then
    return 1
  fi
  echo "[safe-e2e] attempting AWS SSO re-auth for profile ${profile}..."
  aws sso login --profile "${profile}" --no-browser
}

aws_identity_check() {
  if ! command -v aws >/dev/null 2>&1; then
    return 0
  fi
  if ! aws sts get-caller-identity --region "${AWS_REGION}" >/dev/null 2>&1; then
    if aws_reauth_if_sso; then
      if aws sts get-caller-identity --region "${AWS_REGION}" >/dev/null 2>&1; then
        echo "[safe-e2e] AWS identity check recovered after SSO re-auth."
        return 0
      fi
    fi
    if is_true "${STRICT_AWS_CHECK}"; then
      echo "[safe-e2e] AWS identity check failed in region ${AWS_REGION}."
      echo "           run: aws sso login --profile ${AWS_PROFILE:-<profile>}  or export credentials"
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

cleanup_tagged_instances_once() {
  if [[ "${CLEANUP_RAN}" == "true" ]]; then
    return 0
  fi
  CLEANUP_RAN="true"
  if ! command -v aws >/dev/null 2>&1; then
    return 0
  fi
  terminate_tagged_instances || true
}

on_exit_cleanup() {
  local rc=$?
  if [[ "${CLEANUP_RAN}" != "true" ]]; then
    echo "[safe-e2e] ensuring post-run EC2 cleanup..."
    cleanup_tagged_instances_once
  fi
  if [[ ${rc} -ne 0 ]]; then
    echo "[safe-e2e] failed (exit ${rc}). Cleanup attempted; rerun after fixing above error."
  fi
}

reset_tidb_database() {
  python3 - "${CONFIG_PATH}" <<'PY'
import sys, yaml
import mysql.connector
sys.path.insert(0, ".")
from lib.tidb_cloud import validate_tidb_cloud_username

cfg = yaml.safe_load(open(sys.argv[1])) or {}
tidb = cfg.get("tidb") or {}
tier = ((cfg.get("tier") or {}).get("selected"))
required = ["host", "user", "password"]
missing = [k for k in required if not str(tidb.get(k, "")).strip()]
if missing:
    raise SystemExit(f"[safe-e2e] missing tidb fields in config: {', '.join(missing)}")

msg = validate_tidb_cloud_username(tidb, tier=tier)
if msg:
    raise SystemExit(f"[safe-e2e] invalid TiDB username format: {msg}")

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

s3_upload_preflight() {
  local bucket="${POV_S3_BUCKET:-${S3_BUCKET:-${S3_ARTIFACTS_BUCKET:-}}}"
  local prefix="${POV_S3_PREFIX:-${S3_PREFIX:-tidb-pov}}"
  local project="${POV_S3_PROJECT:-${S3_ARTIFACTS_PROJECT:-default}}"
  local region="${POV_S3_REGION:-${S3_REGION:-${AWS_REGION}}}"
  if [[ -z "${bucket}" ]]; then
    return 0
  fi
  if [[ ! -f "scripts/upload_results_s3.py" ]]; then
    return 0
  fi
  if ! python3 scripts/upload_results_s3.py \
      --bucket "${bucket}" \
      --prefix "${prefix}" \
      --project "${project}" \
      --region "${region}" \
      --check-only >/dev/null; then
    if aws_reauth_if_sso; then
      python3 scripts/upload_results_s3.py \
        --bucket "${bucket}" \
        --prefix "${prefix}" \
        --project "${project}" \
        --region "${region}" \
        --check-only >/dev/null
      return 0
    fi
    echo "[safe-e2e] S3 preflight failed for s3://${bucket}/${prefix}/${project}/"
    echo "           fix AWS credentials/bucket policy before running."
    exit 2
  fi
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
test["point_get_phase_enabled"] = True
test["point_get_duration_seconds"] = int(test.get("point_get_duration_seconds", 90) or 90)
test["point_get_concurrency"] = int(test.get("point_get_concurrency", 16) or 16)
test["import_rows"] = int(test.get("import_rows", 250000) or 250000)
test["import_batch_size"] = int(test.get("import_batch_size", 2500) or 2500)
test["import_into_threads"] = int(test.get("import_into_threads", 1) or 1)

runner["instance_size"] = "small"
runner["auto_shutdown_minutes"] = int(runner.get("auto_shutdown_minutes", 10) or 10)
industry["selected"] = "general_auto"

bootstrap["enabled"] = True
bootstrap["required"] = True
bootstrap["profile_key"] = "general_auto"
bootstrap["s3_prefix"] = str(bootstrap.get("s3_prefix") or "tidb-pov/datasets")
bootstrap["aws_region"] = str(bootstrap.get("aws_region") or "us-east-1")
bootstrap["import_threads"] = int(bootstrap.get("import_threads", 1) or 1)
bootstrap["parallel_import_jobs"] = int(bootstrap.get("parallel_import_jobs", 1) or 1)
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

ensure_dataset_import_auth() {
  # If role-based auth is already configured, prefer it.
  if [[ -n "${POV_DATASET_S3_ROLE_ARN:-}" ]]; then
    echo "[safe-e2e] using role-based dataset import auth from env."
    return 0
  fi

  # If explicit dataset keys already exist, use them.
  if [[ -n "${POV_DATASET_S3_ACCESS_KEY_ID:-}" && -n "${POV_DATASET_S3_SECRET_ACCESS_KEY:-}" ]]; then
    echo "[safe-e2e] using explicit dataset access key auth from env."
    return 0
  fi

  # Reuse ambient AWS env credentials if present.
  if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    export POV_DATASET_S3_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
    export POV_DATASET_S3_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
    export POV_DATASET_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
    echo "[safe-e2e] using ambient AWS env credentials for TiDB S3 import auth."
    return 0
  fi

  # Last fallback: export temp credentials from active AWS profile/session.
  if command -v aws >/dev/null 2>&1; then
    local profile
    profile="${AWS_PROFILE:-default}"
    if eval "$(aws configure export-credentials --profile "${profile}" --format env-no-export 2>/dev/null)"; then
      if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
        export POV_DATASET_S3_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"
        export POV_DATASET_S3_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"
        export POV_DATASET_S3_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"
        echo "[safe-e2e] exported temp credentials from profile ${profile} for TiDB S3 import auth."
        return 0
      fi
    fi
  fi

  echo "[safe-e2e] unable to resolve dataset import auth."
  echo "           set one of:"
  echo "           - POV_DATASET_S3_ROLE_ARN (+ POV_DATASET_S3_EXTERNAL_ID if needed), or"
  echo "           - POV_DATASET_S3_ACCESS_KEY_ID / POV_DATASET_S3_SECRET_ACCESS_KEY (/ POV_DATASET_S3_SESSION_TOKEN)"
  exit 2
}

reset_local_run_artifacts() {
  mkdir -p results
  rm -f \
    results/results.db \
    results/metrics_summary.json \
    results/tidb_pov_report.pdf \
    results/run_all.log \
    results/data_manifest.json \
    results/dataset_bootstrap.json
  echo "[safe-e2e] reset local run artifacts for a clean E2E signal."
}

echo "[safe-e2e] config: ${CONFIG_PATH}"
echo "[safe-e2e] region: ${AWS_REGION}"
trap on_exit_cleanup EXIT
aws_identity_check
s3_upload_preflight
terminate_tagged_instances
reset_tidb_database
reset_local_run_artifacts
enforce_small_defaults
publish_general_dataset_pack
ensure_dataset_import_auth

export POV_REQUIRE_COMPLETE_REPORT_DATA="${POV_REQUIRE_COMPLETE_REPORT_DATA:-true}"

echo "[safe-e2e] starting PoV run..."
./run_all.sh "${CONFIG_PATH}" --no-menu --no-wizard

echo "[safe-e2e] post-run EC2 cleanup..."
cleanup_tagged_instances_once

if is_true "${OPEN_REPORT}"; then
  if command -v open >/dev/null 2>&1; then
    open results/tidb_pov_report.pdf || true
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open results/tidb_pov_report.pdf || true
  fi
fi

echo "[safe-e2e] done"
