#!/usr/bin/env bash
set -euo pipefail

ROLE_ARN="${1:-${POV_S3_ROLE_ARN:-}}"
EXTERNAL_ID="${2:-${POV_S3_EXTERNAL_ID:-}}"
REGION="${3:-${AWS_REGION:-us-east-1}}"
SESSION_NAME="${POV_S3_SESSION_NAME:-tidb-pov-$(date +%s)}"
DURATION_SECONDS="${POV_S3_SESSION_DURATION:-3600}"

if [[ -z "${ROLE_ARN}" ]]; then
  echo "missing role arn (arg1 or POV_S3_ROLE_ARN)" >&2
  exit 2
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required" >&2
  exit 2
fi

CMD=(
  aws sts assume-role
  --role-arn "${ROLE_ARN}"
  --role-session-name "${SESSION_NAME}"
  --duration-seconds "${DURATION_SECONDS}"
  --region "${REGION}"
  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken,Expiration]'
  --output text
)

if [[ -n "${EXTERNAL_ID}" ]]; then
  CMD+=(--external-id "${EXTERNAL_ID}")
fi

read -r ACCESS_KEY SECRET_KEY SESSION_TOKEN EXPIRATION <<<"$(${CMD[@]})"

cat <<VARS
export AWS_ACCESS_KEY_ID='${ACCESS_KEY}'
export AWS_SECRET_ACCESS_KEY='${SECRET_KEY}'
export AWS_SESSION_TOKEN='${SESSION_TOKEN}'
export AWS_REGION='${REGION}'
# assumed_role_expires='${EXPIRATION}'
VARS
