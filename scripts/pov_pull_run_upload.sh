#!/usr/bin/env bash
set -euo pipefail

POV_ENV_FILE="${POV_ENV_FILE:-}"

load_env_file_safe() {
  local env_path="$1"
  python3 - "${env_path}" <<'PY'
import re
import shlex
import sys

env_path = sys.argv[1]
key_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

with open(env_path, "r", encoding="utf-8") as fh:
    for line in fh:
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("export "):
            raw = raw[len("export "):].strip()
        if "=" not in raw:
            # Ignore non env-assignment lines to avoid shell execution on source.
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key_re.match(key):
            continue
        value = value.strip()
        if (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        ):
            value = value[1:-1]
        print(f"export {key}={shlex.quote(value)}")
PY
}

if [[ -n "${POV_ENV_FILE}" ]]; then
  if [[ ! -f "${POV_ENV_FILE}" ]]; then
    echo "[runner] env file not found: ${POV_ENV_FILE}"
    exit 2
  fi
  # Load only valid KEY=VALUE assignments from env file.
  # This prevents accidental command execution from malformed lines.
  # shellcheck disable=SC2046
  eval "$(load_env_file_safe "${POV_ENV_FILE}")"
fi

POV_REPO_URL="${POV_REPO_URL:-https://github.com/stephenlthorn/tidb-pov-kit-self-service.git}"
POV_REPO_REF="${POV_REPO_REF:-main}"
POV_WORK_DIR="${POV_WORK_DIR:-$HOME/tidb-pov-kit-self-service-runner}"
POV_CONFIG_SOURCE="${POV_CONFIG_SOURCE:-}"
POV_RUN_ARGS="${POV_RUN_ARGS:---no-menu --no-wizard}"
POV_RESULTS_DIR="${POV_RESULTS_DIR:-results}"
POV_RUN_TAG="${POV_RUN_TAG:-}"
POV_S3_BUCKET="${POV_S3_BUCKET:-${S3_BUCKET:-}}"
POV_S3_PREFIX="${POV_S3_PREFIX:-${S3_PREFIX:-tidb-pov}}"
POV_S3_PROJECT="${POV_S3_PROJECT:-${S3_ARTIFACTS_PROJECT:-default}}"
POV_S3_REGION="${POV_S3_REGION:-${S3_REGION:-${AWS_REGION:-}}}"
POV_S3_ROLE_ARN="${POV_S3_ROLE_ARN:-${S3_ROLE_ARN:-}}"
POV_S3_EXTERNAL_ID="${POV_S3_EXTERNAL_ID:-${S3_EXTERNAL_ID:-}}"
POV_S3_SESSION_NAME="${POV_S3_SESSION_NAME:-tidb-pov-runner}"

# Ensure required S3 vars are visible to child commands.
export POV_S3_BUCKET POV_S3_PREFIX POV_S3_PROJECT POV_S3_REGION POV_RUN_TAG
export POV_ENFORCE_S3_UPLOAD POV_S3_EXPECTED_BUCKET_OWNER POV_S3_KMS_KEY_ID

if [[ -n "${POV_S3_ROLE_ARN}" ]]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "[runner] aws CLI is required when POV_S3_ROLE_ARN is set"
    exit 2
  fi
fi

if [[ -z "${POV_S3_BUCKET}" ]]; then
  echo "[runner] missing POV_S3_BUCKET (or S3_BUCKET)"
  exit 2
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[runner] git is required"
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "[runner] python3 is required"
  exit 2
fi

mkdir -p "${POV_WORK_DIR}"

if [[ ! -d "${POV_WORK_DIR}/.git" ]]; then
  echo "[runner] cloning ${POV_REPO_URL} -> ${POV_WORK_DIR}"
  git clone "${POV_REPO_URL}" "${POV_WORK_DIR}"
fi

cd "${POV_WORK_DIR}"
echo "[runner] updating repo"
git fetch --all --tags
git checkout "${POV_REPO_REF}"
git pull --ff-only origin "${POV_REPO_REF}"

if [[ -n "${POV_CONFIG_SOURCE}" ]]; then
  if [[ ! -f "${POV_CONFIG_SOURCE}" ]]; then
    echo "[runner] config source not found: ${POV_CONFIG_SOURCE}"
    exit 2
  fi
  cp "${POV_CONFIG_SOURCE}" "config.yaml"
  echo "[runner] copied config from ${POV_CONFIG_SOURCE}"
fi

if [[ ! -f "config.yaml" ]]; then
  if [[ -f "config.yaml.example" ]]; then
    cp "config.yaml.example" "config.yaml"
    echo "[runner] generated config.yaml from config.yaml.example (edit TiDB values before first run)"
  else
    echo "[runner] config.yaml missing"
    exit 2
  fi
fi

chmod +x setup/01_install_deps.sh run_all.sh scripts/upload_results_s3.py
if [[ -f "scripts/aws_assume_role_env.sh" ]]; then
  chmod +x scripts/aws_assume_role_env.sh
fi

if [[ -n "${POV_S3_ROLE_ARN}" ]]; then
  echo "[runner] assuming role for S3 upload: ${POV_S3_ROLE_ARN}"
  # shellcheck disable=SC2046
  eval "$(
    POV_S3_SESSION_NAME="${POV_S3_SESSION_NAME}" \
    scripts/aws_assume_role_env.sh "${POV_S3_ROLE_ARN}" "${POV_S3_EXTERNAL_ID}" "${POV_S3_REGION:-us-east-1}"
  )"
fi

echo "[runner] installing dependencies"
bash setup/01_install_deps.sh

echo "[runner] running PoV"
./run_all.sh config.yaml ${POV_RUN_ARGS}

echo "[runner] uploading artifacts to S3"
python3 scripts/upload_results_s3.py \
  --results-dir "${POV_RESULTS_DIR}" \
  --runs-dir "runs" \
  --bucket "${POV_S3_BUCKET}" \
  --prefix "${POV_S3_PREFIX}" \
  --project "${POV_S3_PROJECT}" \
  --run-tag "${POV_RUN_TAG}" \
  --region "${POV_S3_REGION}" \
  --expected-bucket-owner "${POV_S3_EXPECTED_BUCKET_OWNER:-}" \
  --kms-key-id "${POV_S3_KMS_KEY_ID:-}"

echo "[runner] done"
