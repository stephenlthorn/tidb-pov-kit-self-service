#!/usr/bin/env bash
set -euo pipefail

POV_ENV_FILE="${POV_ENV_FILE:-}"
if [[ -n "${POV_ENV_FILE}" ]]; then
  if [[ ! -f "${POV_ENV_FILE}" ]]; then
    echo "[runner] env file not found: ${POV_ENV_FILE}"
    exit 2
  fi
  # shellcheck disable=SC1090
  source "${POV_ENV_FILE}"
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
  --region "${POV_S3_REGION}"

echo "[runner] done"
