#!/usr/bin/env bash
set -euo pipefail

# Safe small end-to-end PoV runner:
# 1) terminate any existing EC2 with tag tidb-pov-managed=true
# 2) drop + recreate TiDB database from config
# 3) enforce small defaults for run profile
# 4) run PoV
# 5) terminate leftover tagged EC2 again

CONFIG_PATH="${1:-config.small.yaml}"
TIER="${POV_TIER:-serverless}"
STRICT_AWS_CHECK="${POV_STRICT_AWS_CHECK:-true}"
OPEN_REPORT="${POV_OPEN_REPORT:-false}"

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

test["data_scale"] = "small"
runner["instance_size"] = "small"
runner["auto_shutdown_minutes"] = int(runner.get("auto_shutdown_minutes", 10) or 10)
industry["selected"] = str(industry.get("selected") or "general_auto")

with open(p, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)
print("[safe-e2e] enforced small defaults (data_scale, runner size, auto-shutdown).")
PY
}

echo "[safe-e2e] config: ${CONFIG_PATH}"
echo "[safe-e2e] region: ${AWS_REGION}"
aws_identity_check
terminate_tagged_instances
reset_tidb_database
enforce_small_defaults

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
