#!/usr/bin/env bash
# =============================================================================
#  TiDB Cloud PoV Kit — Full Orchestrator
#  run_all.sh
#
#  Usage:
#    ./run_all.sh [config.yaml] [--menu|--web-ui|--no-menu]
#                 [--regen] [--report-only|--report-json-only]
#                 [--wizard|--no-wizard] [--tier TIER]
#
#  What it does:
#    1. Optional pre-PoC intake (tier decision + security checklist)
#    2. Checks Python / config / TiDB connectivity
#    3. Installs Python dependencies
#    4. Creates the PoV database and generates synthetic data
#    5. Runs test modules (M0–M8) sequentially
#    6. Aggregates metrics and generates the PDF report
#
#  All output goes to results/  (PDF, SQLite, JSON manifests, checklist).
# =============================================================================

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./run_all.sh [config.yaml] [options]

Options:
  --menu             Open interactive PoC control panel
  --web-ui           Launch dark-themed web UI
  --no-menu          Skip control panel and run directly
  --report-only       Build PDF from existing results/* artifacts only
  --report-json-only  Build results/metrics_summary.json only (no PDF, no tests)
  --regen             Regenerate synthetic data even if manifest exists
  --wizard            Force interactive pre-PoC intake wizard
  --no-wizard         Skip intake wizard
  --tier <tier>       Force tier (serverless|essential|premium|dedicated|byoc)
  --allow-blocked     Continue even if checklist reports blocking failures
  -h, --help          Show this help message
EOF
}

CONFIG="config.yaml"
REGEN=false
REPORT_ONLY=false
REPORT_JSON_ONLY=false
RUN_INTAKE="auto"          # auto | yes | no
FORCE_TIER=""
ALLOW_BLOCKED=false
SHOW_MENU="auto"           # auto | yes | no
SHOW_WEB_UI="no"
HAS_EXEC_FLAGS=false
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --regen)
      REGEN=true
      HAS_EXEC_FLAGS=true
      shift
      ;;
    --report-only)
      REPORT_ONLY=true
      HAS_EXEC_FLAGS=true
      shift
      ;;
    --report-json-only)
      REPORT_JSON_ONLY=true
      HAS_EXEC_FLAGS=true
      shift
      ;;
    --menu)
      SHOW_MENU="yes"
      shift
      ;;
    --web-ui)
      SHOW_WEB_UI="yes"
      shift
      ;;
    --no-menu)
      SHOW_MENU="no"
      shift
      ;;
    --wizard|--intake)
      RUN_INTAKE="yes"
      HAS_EXEC_FLAGS=true
      shift
      ;;
    --no-wizard|--no-intake)
      RUN_INTAKE="no"
      HAS_EXEC_FLAGS=true
      shift
      ;;
    --tier)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --tier"
        exit 1
      fi
      FORCE_TIER="$2"
      HAS_EXEC_FLAGS=true
      shift 2
      ;;
    --allow-blocked)
      ALLOW_BLOCKED=true
      HAS_EXEC_FLAGS=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ ${#POSITIONAL[@]} -gt 0 ]]; then
  CONFIG="${POSITIONAL[0]}"
fi

if [[ "${REPORT_ONLY}" == "true" && "${REPORT_JSON_ONLY}" == "true" ]]; then
  echo "Options --report-only and --report-json-only are mutually exclusive."
  exit 1
fi

PYTHON="${PYTHON:-python3}"
PIP="${PIP:-pip3}"
RESULTS_DIR="results"
LOG_FILE="${RESULTS_DIR}/web_ui_run.log"
INTAKE_JSON="${RESULTS_DIR}/pre_poc_intake.json"
INTAKE_MD="${RESULTS_DIR}/pre_poc_checklist.md"
RESOLVED_CONFIG="${RESULTS_DIR}/config.resolved.yaml"
S3_ENFORCE_RAW="${POV_ENFORCE_S3_UPLOAD:-${S3_ARTIFACTS_REQUIRED:-true}}"
S3_BUCKET="${POV_S3_BUCKET:-${S3_BUCKET:-${S3_ARTIFACTS_BUCKET:-}}}"
S3_PREFIX="${POV_S3_PREFIX:-${S3_PREFIX:-${S3_ARTIFACTS_PREFIX:-tidb-pov}}}"
S3_PROJECT="${POV_S3_PROJECT:-${S3_ARTIFACTS_PROJECT:-default}}"
S3_REGION="${POV_S3_REGION:-${S3_REGION:-${AWS_REGION:-}}}"
# Canonical PingCAP results bucket — used when no env var or config.yaml override is present
POV_S3_CANONICAL_BUCKET="pingcap-tidb-pov-results-219248915861"
POV_S3_CANONICAL_REGION="us-east-1"
S3_RUN_TAG_DEFAULT="$(date -u +%Y%m%d_%H%M%S)_$(hostname | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9_-' '-')"
S3_RUN_TAG="${POV_RUN_TAG:-${S3_RUN_TAG_DEFAULT}}"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'  # No Colour

banner() { echo -e "\n${BOLD}${BLUE}══ $1 ══${NC}"; }
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
warn()   { echo -e "  ${YELLOW}⚠${NC}  $1"; }
err()    { echo -e "  ${RED}✗${NC} $1"; }
step()   { echo -e "\n${BOLD}[$1]${NC} $2"; }

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

is_true() {
  local v
  v="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  [[ "${v}" == "1" || "${v}" == "true" || "${v}" == "yes" || "${v}" == "y" || "${v}" == "on" ]]
}

if is_true "${S3_ENFORCE_RAW}"; then
  S3_ENFORCED="true"
else
  S3_ENFORCED="false"
fi

s3_preflight_required() {
  if [[ "${S3_ENFORCED}" != "true" ]]; then
    warn "S3 enforcement disabled (POV_ENFORCE_S3_UPLOAD=false)."
    return 0
  fi

  if [[ -z "${S3_BUCKET}" ]]; then
    err "S3 enforcement is enabled but bucket is not configured."
    echo "  Set POV_S3_BUCKET (or S3_BUCKET / S3_ARTIFACTS_BUCKET)."
    exit 1
  fi

  if [[ ! -f "scripts/upload_results_s3.py" ]]; then
    err "Missing scripts/upload_results_s3.py; cannot enforce S3 archival."
    exit 1
  fi

  set +e
  "${PYTHON}" scripts/upload_results_s3.py \
    --bucket "${S3_BUCKET}" \
    --prefix "${S3_PREFIX}" \
    --project "${S3_PROJECT}" \
    --run-tag "${S3_RUN_TAG}" \
    --region "${S3_REGION}" \
    --check-only
  local rc=$?
  set -e

  if [[ ${rc} -ne 0 ]]; then
    err "S3 preflight failed. Run blocked to prevent unarchived execution."
    echo "  Verify AWS credentials and bucket write access for s3://${S3_BUCKET}/${S3_PREFIX}/${S3_PROJECT}/"
    exit 1
  fi
  ok "S3 archival preflight passed: s3://${S3_BUCKET}/${S3_PREFIX}/${S3_PROJECT}/"
}

s3_upload_required() {
  local step_label="$1"
  if [[ "${S3_ENFORCED}" != "true" ]]; then
    return 0
  fi

  step "${step_label}" "Uploading artifacts to S3 (required)"
  set +e
  "${PYTHON}" scripts/upload_results_s3.py \
    --results-dir "${RESULTS_DIR}" \
    --runs-dir "runs" \
    --bucket "${S3_BUCKET}" \
    --prefix "${S3_PREFIX}" \
    --project "${S3_PROJECT}" \
    --run-tag "${S3_RUN_TAG}" \
    --region "${S3_REGION}"
  local rc=$?
  set -e

  if [[ ${rc} -ne 0 ]]; then
    err "Required S3 upload failed. Marking run as failed."
    echo "  No successful archival confirmation for run tag: ${S3_RUN_TAG}"
    exit 1
  fi

  ok "Artifacts archived to s3://${S3_BUCKET}/${S3_PREFIX}/${S3_PROJECT}/runs/${S3_RUN_TAG}/"
}

print_s3_download_links() {
  local latest_manifest
  local links_file
  latest_manifest="$(ls -1t "${RESULTS_DIR}"/s3_upload_manifest_*.json 2>/dev/null | head -n 1 || true)"
  if [[ -z "${latest_manifest}" || ! -f "${latest_manifest}" ]]; then
    return 0
  fi
  links_file="${RESULTS_DIR}/s3_download_links.txt"

  echo ""
  echo "  S3 downloads (copy/paste friendly):"
  "${PYTHON}" - "${latest_manifest}" "${links_file}" <<'PY'
import json
from pathlib import Path
import sys
from pathlib import Path

path = sys.argv[1]
links_path = Path(sys.argv[2])
try:
    payload = json.load(open(path))
except Exception:
    raise SystemExit(0)

uploaded = payload.get("uploaded") or []
def pick(suffix: str):
    for row in uploaded:
        key = str(row.get("key") or "")
        if key.endswith(suffix):
            return row
    return {}

report = pick("/results/tidb_pov_report.pdf")
metrics = pick("/results/metrics_summary.json")
manifest_url = str(payload.get("manifest_download_url") or "").strip()
entries = []

if report:
    entries.append(("REPORT_URL", report.get("download_url") or report.get("s3_uri") or ""))
if metrics:
    entries.append(("METRICS_URL", metrics.get("download_url") or metrics.get("s3_uri") or ""))
if manifest_url:
    entries.append(("MANIFEST_URL", manifest_url))

if entries:
    links_path.parent.mkdir(parents=True, exist_ok=True)
    with links_path.open("w", encoding="utf-8") as fh:
        for k, v in entries:
            fh.write(f"{k}={v}\n")
    for k, v in entries:
        print(f"    {k}={v}")
    print(f"    LINKS_FILE={links_path}")
PY
}

if ! command -v "${PYTHON}" &>/dev/null; then
  echo "Python 3 not found. Install Python 3.9+ and retry."
  exit 1
fi

if [[ "${SHOW_WEB_UI}" == "yes" ]]; then
  if [[ ! -f "${CONFIG}" ]]; then
    echo "Config file not found: ${CONFIG}"
    echo "Copy config.yaml.example to config.yaml and fill in your TiDB credentials."
    exit 1
  fi
  "${PYTHON}" setup/poc_web_ui.py --config "${CONFIG}"
  exit $?
fi

if [[ "${SHOW_MENU}" == "yes" || ( "${SHOW_MENU}" == "auto" && -t 0 && "${HAS_EXEC_FLAGS}" == "false" ) ]]; then
  if [[ ! -f "${CONFIG}" ]]; then
    echo "Config file not found: ${CONFIG}"
    echo "Copy config.yaml.example to config.yaml and fill in your TiDB credentials."
    exit 1
  fi
  "${PYTHON}" setup/poc_control_panel.py --config "${CONFIG}" --runner "${SCRIPT_PATH}"
  exit $?
fi

# ── Logging ───────────────────────────────────────────────────────────────────
mkdir -p "${RESULTS_DIR}"
exec > >(tee -a "${LOG_FILE}") 2>&1

START_TS=$(date +%s)

banner "TiDB Cloud PoV Kit"
echo "  Config requested : ${CONFIG}"
if [[ "${REPORT_ONLY}" == "true" ]]; then
  MODE_LABEL="report-only"
elif [[ "${REPORT_JSON_ONLY}" == "true" ]]; then
  MODE_LABEL="report-json-only"
else
  MODE_LABEL="full-run"
fi
echo "  Mode             : ${MODE_LABEL}"
echo "  Started          : $(date)"
echo "  Log              : ${LOG_FILE}"
if [[ "${S3_ENFORCED}" == "true" ]]; then
  echo "  S3 archive       : required"
  if [[ -n "${S3_BUCKET}" ]]; then
    echo "  S3 target        : s3://${S3_BUCKET}/${S3_PREFIX}/${S3_PROJECT}/runs/${S3_RUN_TAG}/"
  else
    echo "  S3 target        : (resolving from config.yaml / canonical default)"
  fi
else
  echo "  S3 archive       : optional"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 0. Basic checks
# ─────────────────────────────────────────────────────────────────────────────
step "0/10" "Basic checks"

if ! command -v "${PYTHON}" &>/dev/null; then
  err "Python 3 not found. Install Python 3.9+ and retry."
  exit 1
fi
ok "Python: $(${PYTHON} --version)"

if [[ ! -f "${CONFIG}" ]]; then
  err "Config file not found: ${CONFIG}"
  echo "  Copy config.yaml.example to config.yaml and fill in your TiDB credentials."
  exit 1
fi
ok "Config: ${CONFIG}"

if ! "${PYTHON}" -c "import yaml" &>/dev/null; then
  warn "PyYAML not found. Installing bootstrap dependency..."
  "${PYTHON}" -m pip install -q pyyaml
fi
ok "PyYAML available"
if ! "${PYTHON}" -c "import boto3" &>/dev/null; then
  warn "boto3 not found. Installing bootstrap dependency for S3 archival checks..."
  "${PYTHON}" -m pip install -q boto3
fi
ok "boto3 available"

# Resolve S3 results settings: env vars > config.yaml pov_results > canonical defaults
if [[ -f "${CONFIG}" ]]; then
  _pov_s3="$("${PYTHON}" -c "
import yaml, sys
try:
    cfg = yaml.safe_load(open('${CONFIG}')) or {}
    pr = cfg.get('pov_results') or {}
    print('|'.join([
        str(pr.get('s3_bucket') or ''),
        str(pr.get('s3_region') or ''),
        str(pr.get('s3_prefix') or ''),
        str(pr.get('s3_project') or ''),
    ]))
except Exception:
    print('|||')
" 2>/dev/null || echo "|||")"
  _pcfg_bucket="${_pov_s3%%|*}";  _pov_rest="${_pov_s3#*|}"
  _pcfg_region="${_pov_rest%%|*}"; _pov_rest="${_pov_rest#*|}"
  _pcfg_prefix="${_pov_rest%%|*}"
  _pcfg_project="${_pov_rest#*|}"
  [[ -z "${S3_BUCKET}"  && -n "${_pcfg_bucket}"  ]] && S3_BUCKET="${_pcfg_bucket}"
  [[ -z "${S3_REGION}"  && -n "${_pcfg_region}"  ]] && S3_REGION="${_pcfg_region}"
  [[ "${S3_PREFIX}" == "tidb-pov" && -n "${_pcfg_prefix}"  ]] && S3_PREFIX="${_pcfg_prefix}"
  [[ "${S3_PROJECT}" == "default"  && -n "${_pcfg_project}" ]] && S3_PROJECT="${_pcfg_project}"
fi
# Apply canonical defaults so S3 upload always has a destination
S3_BUCKET="${S3_BUCKET:-${POV_S3_CANONICAL_BUCKET}}"
S3_REGION="${S3_REGION:-${POV_S3_CANONICAL_REGION}}"
if [[ "${S3_ENFORCED}" == "true" ]]; then
  ok "S3 results bucket : s3://${S3_BUCKET}/${S3_PREFIX}/${S3_PROJECT}/"
fi
s3_preflight_required

if [[ "${REPORT_ONLY}" == "true" || "${REPORT_JSON_ONLY}" == "true" ]]; then
  CONFIG_EFFECTIVE="${CONFIG}"
  if [[ -f "${RESOLVED_CONFIG}" ]]; then
    CONFIG_EFFECTIVE="${RESOLVED_CONFIG}"
    ok "Using resolved config from previous run: ${CONFIG_EFFECTIVE}"
  fi

  step "R1/3" "Validating report artifacts"
  if [[ ! -f "${RESULTS_DIR}/results.db" ]]; then
    err "Missing ${RESULTS_DIR}/results.db"
    echo "  Run at least one full PoV execution first to populate results."
    exit 1
  fi
  ok "Found ${RESULTS_DIR}/results.db"

  step "R2/3" "Checking report dependencies"
  if ! "${PYTHON}" - <<'PY' &>/dev/null
import yaml, numpy, matplotlib, fpdf
PY
  then
    warn "Report dependencies missing; installing requirements..."
    if [[ -f "setup/01_install_deps.sh" ]]; then
      bash setup/01_install_deps.sh
    else
      "${PIP}" install -q -r requirements.txt
    fi
  fi
  ok "Report dependencies available"

  if [[ "${REPORT_JSON_ONLY}" == "true" ]]; then
    step "R3/3" "Generating metrics JSON from existing results"
    "${PYTHON}" report/collect_metrics.py --quiet
    ok "Metrics JSON written to ${RESULTS_DIR}/metrics_summary.json"
    echo ""
    echo "  To view the JSON, open: ${RESULTS_DIR}/metrics_summary.json"
    s3_upload_required "R4/4"
  else
    step "R3/3" "Generating PDF report from existing results"
    "${PYTHON}" report/generate_report.py "${CONFIG_EFFECTIVE}"
    ok "Report written to ${RESULTS_DIR}/tidb_pov_report.pdf"
    echo ""
    echo "  To view the report, open: ${RESULTS_DIR}/tidb_pov_report.pdf"
    s3_upload_required "R4/4"
  fi
  exit 0
fi

if ! "${PYTHON}" -c "import mysql.connector" &>/dev/null; then
  warn "mysql-connector-python not found. Installing bootstrap dependency..."
  "${PYTHON}" -m pip install -q mysql-connector-python
fi
ok "MySQL connector available"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Optional intake wizard (tier decision + security checklist)
# ─────────────────────────────────────────────────────────────────────────────
SHOULD_RUN_INTAKE="false"
INTAKE_NON_INTERACTIVE="false"

if [[ "${RUN_INTAKE}" == "yes" ]]; then
  SHOULD_RUN_INTAKE="true"
elif [[ "${RUN_INTAKE}" == "auto" && -t 0 ]]; then
  SHOULD_RUN_INTAKE="true"
elif [[ -n "${FORCE_TIER}" ]]; then
  # If tier is forced, still run intake in non-interactive mode to apply tier profile.
  SHOULD_RUN_INTAKE="true"
  INTAKE_NON_INTERACTIVE="true"
fi

CONFIG_EFFECTIVE="${CONFIG}"

if [[ "${SHOULD_RUN_INTAKE}" == "true" ]]; then
  step "1/10" "Pre-PoC intake (tier decision tree + security checklist)"

  INTAKE_ARGS=(
    --config "${CONFIG}"
    --output-config "${RESOLVED_CONFIG}"
    --output-json "${INTAKE_JSON}"
    --output-md "${INTAKE_MD}"
  )

  if [[ -n "${FORCE_TIER}" ]]; then
    INTAKE_ARGS+=(--tier "${FORCE_TIER}")
  fi

  if [[ "${ALLOW_BLOCKED}" == "true" ]]; then
    INTAKE_ARGS+=(--allow-blocked)
  fi

  if [[ "${INTAKE_NON_INTERACTIVE}" == "true" || ! -t 0 ]]; then
    INTAKE_ARGS+=(--non-interactive)
  fi

  set +e
  "${PYTHON}" setup/pre_poc_intake.py "${INTAKE_ARGS[@]}"
  INTAKE_RC=$?
  set -e

  if [[ ${INTAKE_RC} -eq 0 ]]; then
    CONFIG_EFFECTIVE="${RESOLVED_CONFIG}"
    ok "Intake complete"
    ok "Using resolved config: ${CONFIG_EFFECTIVE}"
  else
    err "Pre-PoC checklist returned HOLD (exit ${INTAKE_RC})."
    echo "  Review ${INTAKE_MD} and ${INTAKE_JSON}."
    echo "  Re-run with --allow-blocked only if this is a dry-run and risks are accepted."
    exit ${INTAKE_RC}
  fi
else
  step "1/10" "Pre-PoC intake"
  warn "Skipped (non-interactive mode or --no-wizard)."
  if [[ -n "${FORCE_TIER}" ]]; then
    warn "--tier ignored because intake is disabled."
  fi
fi

read_run_profile() {
  "${PYTHON}" - <<PY
import yaml
with open("${CONFIG_EFFECTIVE}") as f:
    cfg = yaml.safe_load(f) or {}
test = cfg.get("test") or {}
run_mode = str(test.get("run_mode", "validation")).strip().lower() or "validation"
if run_mode not in {"validation", "performance"}:
    run_mode = "validation"
schema_mode = str(test.get("schema_mode", "tidb_optimized")).strip().lower() or "tidb_optimized"
if schema_mode not in {"tidb_optimized", "mysql_compatible"}:
    schema_mode = "tidb_optimized"
print(f"{run_mode}|{schema_mode}")
PY
}

read_workload_generator_profile() {
  "${PYTHON}" - <<PY
import yaml
with open("${CONFIG_EFFECTIVE}") as f:
    cfg = yaml.safe_load(f) or {}
blaster = (((cfg.get("workload_lab") or {}).get("blaster")) or {})
mode = str(blaster.get("mode", "rawsql")).strip().lower() or "rawsql"
if mode not in {"rawsql", "tpcc", "ycsb"}:
    mode = "rawsql"
tag = str(blaster.get("tag", "")).strip() or "performance"
print(f"{mode}|{tag}")
PY
}

read_dataset_bootstrap_profile() {
  "${PYTHON}" - <<PY
import yaml
with open("${CONFIG_EFFECTIVE}") as f:
    cfg = yaml.safe_load(f) or {}
ds = cfg.get("dataset_bootstrap") or {}
enabled = bool(ds.get("enabled", False))
required = bool(ds.get("required", False))
skip_synth = bool(ds.get("skip_synthetic_generation", False))
print(f"{str(enabled).lower()}|{str(required).lower()}|{str(skip_synth).lower()}")
PY
}

sync_workload_generator_summary() {
  "${PYTHON}" - <<'PY'
import json
from pathlib import Path

root = Path(".").resolve()
results_dir = root / "results"
runs_dir = root / "runs"
results_dir.mkdir(parents=True, exist_ok=True)

summary = None
run_dir = None

last_run_file = results_dir / "blaster_last_run.txt"
if last_run_file.exists():
    raw = last_run_file.read_text(encoding="utf-8").strip()
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = (root / raw).resolve()
        s = p / "summary.json"
        if s.exists():
            run_dir = p
            summary = json.loads(s.read_text(encoding="utf-8"))

if summary is None and runs_dir.exists():
    for cand in sorted([p for p in runs_dir.iterdir() if p.is_dir()], reverse=True):
        s = cand / "summary.json"
        if not s.exists():
            continue
        try:
            loaded = json.loads(s.read_text(encoding="utf-8"))
        except Exception:
            continue
        mode = str(loaded.get("mode", "")).strip().lower()
        if mode in {"rawsql", "tpcc", "ycsb"}:
            run_dir = cand
            summary = loaded
            break

if summary is not None and run_dir is not None:
    (results_dir / "workload_generator_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    (results_dir / "workload_generator_last_run.txt").write_text(
        str(run_dir), encoding="utf-8"
    )
    print(str(run_dir))
PY
}

RUN_PROFILE="$(read_run_profile)"
POV_RUN_MODE="${RUN_PROFILE%%|*}"
POV_SCHEMA_MODE="${RUN_PROFILE#*|}"
ok "Run profile: mode=${POV_RUN_MODE}, schema=${POV_SCHEMA_MODE}"
if [[ "${POV_RUN_MODE}" == "performance" ]]; then
  warn "Performance mode selected."
  warn "For peak-throughput claims, use Workload Generator (tidb_blaster) and multi-loadgen orchestration."
fi

# Helpers for interactive connectivity recovery
cfg_get_tidb_field() {
  local field="$1"
  "${PYTHON}" - <<PY
import yaml
with open("${CONFIG_EFFECTIVE}") as f:
    cfg = yaml.safe_load(f) or {}
v = (cfg.get("tidb") or {}).get("${field}", "")
if isinstance(v, bool):
    print("true" if v else "false")
else:
    print("" if v is None else v)
PY
}

prompt_with_default() {
  local label="$1"
  local default_val="${2:-}"
  local input_val=""
  if [[ -n "${default_val}" ]]; then
    read -r -p "${label} [${default_val}]: " input_val
  else
    read -r -p "${label}: " input_val
  fi
  echo "${input_val:-${default_val}}"
}

prompt_tidb_connection_update() {
  local cur_host cur_port cur_user cur_password cur_database cur_ssl
  cur_host="$(cfg_get_tidb_field "host")"
  cur_port="$(cfg_get_tidb_field "port")"
  cur_user="$(cfg_get_tidb_field "user")"
  cur_password="$(cfg_get_tidb_field "password")"
  cur_database="$(cfg_get_tidb_field "database")"
  cur_ssl="$(cfg_get_tidb_field "ssl")"
  [[ -z "${cur_ssl}" ]] && cur_ssl="true"

  echo ""
  banner "Update TiDB Connection Settings"
  echo "  Enter values from TiDB Cloud -> Connect dialog (MySQL connector format)."
  echo "  Tip: TiDB Cloud username usually includes a prefix, for example: <prefix>.root"
  echo ""

  local new_host new_port new_user new_database new_ssl new_password pwd_input
  new_host="$(prompt_with_default "  Host" "${cur_host}")"
  new_user="$(prompt_with_default "  User" "${cur_user}")"
  new_database="$(prompt_with_default "  Database" "${cur_database}")"

  while true; do
    new_port="$(prompt_with_default "  Port" "${cur_port}")"
    if [[ "${new_port}" =~ ^[0-9]+$ ]]; then
      break
    fi
    warn "Port must be numeric (example: 4000)."
  done

  if [[ -n "${cur_password}" ]]; then
    read -r -s -p "  Password [hidden, press Enter to keep current]: " pwd_input
  else
    read -r -s -p "  Password [hidden]: " pwd_input
  fi
  echo ""
  if [[ -n "${pwd_input}" ]]; then
    new_password="${pwd_input}"
  else
    new_password="${cur_password}"
  fi

  while true; do
    new_ssl="$(prompt_with_default "  SSL true/false" "${cur_ssl}")"
    local new_ssl_lc
    new_ssl_lc="$(printf '%s' "${new_ssl}" | tr '[:upper:]' '[:lower:]')"
    case "${new_ssl_lc}" in
      true|t|yes|y|1) new_ssl="true"; break ;;
      false|f|no|n|0) new_ssl="false"; break ;;
      *) warn "Enter true or false." ;;
    esac
  done

  CFG_PATH="${CONFIG_EFFECTIVE}" \
  NEW_HOST="${new_host}" \
  NEW_PORT="${new_port}" \
  NEW_USER="${new_user}" \
  NEW_PASSWORD="${new_password}" \
  NEW_DATABASE="${new_database}" \
  NEW_SSL="${new_ssl}" \
  "${PYTHON}" - <<'PY'
import os
import yaml

path = os.environ["CFG_PATH"]
with open(path) as f:
    cfg = yaml.safe_load(f) or {}

cfg.setdefault("tidb", {})
cfg["tidb"]["host"] = os.environ["NEW_HOST"]
cfg["tidb"]["port"] = int(os.environ["NEW_PORT"])
cfg["tidb"]["user"] = os.environ["NEW_USER"]
cfg["tidb"]["password"] = os.environ["NEW_PASSWORD"]
cfg["tidb"]["database"] = os.environ["NEW_DATABASE"]
cfg["tidb"]["ssl"] = os.environ["NEW_SSL"].lower() == "true"

with open(path, "w") as f:
    yaml.safe_dump(cfg, f, sort_keys=False)
PY

  ok "Updated TiDB connection settings in ${CONFIG_EFFECTIVE}"
}

check_tidb_connection() {
  "${PYTHON}" - <<PY
import yaml, sys
sys.path.insert(0, '.')
with open('${CONFIG_EFFECTIVE}') as f:
    cfg = yaml.safe_load(f) or {}
from lib.db_utils import ping
ok, msg = ping(cfg.get('tidb', {}))
print(msg if ok else 'FAIL: ' + msg)
sys.exit(0 if ok else 1)
PY
}

check_tidb_username_format() {
  "${PYTHON}" - <<PY
import yaml, sys
sys.path.insert(0, '.')
with open('${CONFIG_EFFECTIVE}') as f:
    cfg = yaml.safe_load(f) or {}
from lib.tidb_cloud import validate_tidb_cloud_username
tier = (cfg.get('tier') or {}).get('selected')
msg = validate_tidb_cloud_username(cfg.get('tidb', {}), tier=tier)
if msg:
    print(msg)
    sys.exit(1)
print("ok")
PY
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. DB connectivity check
# ─────────────────────────────────────────────────────────────────────────────
step "2/10" "TiDB connectivity check"

while true; do
  set +e
  USER_FMT_CHECK="$(check_tidb_username_format)"
  USER_FMT_RC=$?
  set -e
  if [[ ${USER_FMT_RC} -ne 0 ]]; then
    err "Invalid TiDB username format: ${USER_FMT_CHECK}"
    echo "  Use the exact TiDB Cloud username from Connect, for example: <prefix>.root"
    if [[ ! -t 0 ]]; then
      echo "  Non-interactive mode: cannot prompt for connection details."
      exit 1
    fi
    echo ""
    echo "  Choose next action:"
    echo "    1) Update connection settings and retry"
    echo "    2) Abort"
    read -r -p "  Selection [1/2] (default: 1): " conn_action
    case "${conn_action:-1}" in
      1)
        prompt_tidb_connection_update
        ;;
      2)
        err "Aborted by user."
        exit 1
        ;;
      *)
        warn "Unknown selection; retrying username check."
        ;;
    esac
    continue
  fi

  set +e
  CONN_CHECK="$(check_tidb_connection)"
  CONN_RC=$?
  set -e

  if [[ ${CONN_RC} -eq 0 ]] && ! echo "${CONN_CHECK}" | grep -q "FAIL"; then
    ok "TiDB connection: ${CONN_CHECK}"
    break
  fi

  err "Cannot connect to TiDB: ${CONN_CHECK}"
  echo "  Check host/port/credentials and network access in ${CONFIG_EFFECTIVE}."

  if echo "${CONN_CHECK}" | grep -qi "Missing user name prefix"; then
    warn "Username format is likely wrong."
    echo "  Use the exact TiDB Cloud username, for example: <prefix>.root"
  fi

  if [[ ! -t 0 ]]; then
    echo "  Non-interactive mode: cannot prompt for connection details."
    exit 1
  fi

  echo ""
  echo "  Choose next action:"
  echo "    1) Update connection settings and retry"
  echo "    2) Retry without changes"
  echo "    3) Abort"
  read -r -p "  Selection [1/2/3] (default: 1): " conn_action

  case "${conn_action:-1}" in
    1)
      prompt_tidb_connection_update
      ;;
    2)
      ;;
    3)
      err "Aborted by user."
      exit 1
      ;;
    *)
      warn "Unknown selection; retrying connection check."
      ;;
  esac
done

# ─────────────────────────────────────────────────────────────────────────────
# 3. Install dependencies
# ─────────────────────────────────────────────────────────────────────────────
step "3/10" "Installing Python dependencies"
if [[ -f "setup/01_install_deps.sh" ]]; then
  bash setup/01_install_deps.sh
else
  "${PIP}" install -q -r requirements.txt
fi
ok "Dependencies installed"

# ─────────────────────────────────────────────────────────────────────────────
# 4. Dataset bootstrap and synthetic data
# ─────────────────────────────────────────────────────────────────────────────
step "4/10" "Dataset bootstrap and synthetic data"

DATASET_PROFILE="$(read_dataset_bootstrap_profile)"
DATASET_BOOTSTRAP_ENABLED="${DATASET_PROFILE%%|*}"
DATASET_PROFILE_REST="${DATASET_PROFILE#*|}"
DATASET_BOOTSTRAP_REQUIRED="${DATASET_PROFILE_REST%%|*}"
DATASET_SKIP_SYNTH="${DATASET_PROFILE_REST##*|}"
DATASET_BOOTSTRAP_STATUS="skipped"

db_has_nonempty_data() {
  "${PYTHON}" - "${CONFIG_EFFECTIVE}" <<'PY'
import json
import os
import sys
import yaml
import mysql.connector

cfg_path = sys.argv[1]
cfg = yaml.safe_load(open(cfg_path)) or {}
tidb = cfg.get("tidb") or {}
db = str(tidb.get("database") or "").strip()
if not db:
    print("false")
    raise SystemExit(0)

try:
    conn = mysql.connector.connect(
        host=tidb.get("host"),
        port=int(tidb.get("port", 4000) or 4000),
        user=tidb.get("user"),
        password=tidb.get("password"),
        database=db,
        ssl_disabled=not bool(tidb.get("ssl", True)),
        connection_timeout=10,
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema=%s ORDER BY table_name LIMIT 20",
        (db,),
    )
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema=%s ORDER BY table_name",
        (db,),
    )
    tables = [r[0] for r in cur.fetchall() if r and r[0]]
    table_set = set(tables)
    has_data = False
    for table in tables[:50]:
        try:
            cur.execute(f"SELECT 1 FROM `{table}` LIMIT 1")
            if cur.fetchone():
                has_data = True
                break
        except Exception:
            continue

    # If a data manifest exists, require its table set to be present before
    # skipping synthetic generation. This avoids false "data ready" positives
    # when bootstrap-only seed tables exist but core workload schema is missing.
    manifest_path = os.path.join("results", "data_manifest.json")
    if has_data and os.path.exists(manifest_path):
        try:
            manifest = json.load(open(manifest_path, "r", encoding="utf-8")) or {}
            counts = manifest.get("counts") or {}
            if isinstance(counts, dict) and counts:
                required_tables = [
                    str(t) for t, v in counts.items()
                    if isinstance(v, (int, float))
                    and int(v) > 0
                    and str(t).strip()
                    and str(t) not in {"poc_seed_oltp", "poc_seed_olap"}
                ]
                if required_tables:
                    missing = [t for t in required_tables if t not in table_set]
                    if missing:
                        has_data = False
        except Exception:
            # Keep legacy behavior if manifest is malformed.
            pass
    conn.close()
    print("true" if has_data else "false")
except Exception:
    print("false")
PY
}

if [[ "${DATASET_BOOTSTRAP_ENABLED}" == "true" ]]; then
  echo "  Dataset bootstrap: enabled"
  set +e
  "${PYTHON}" setup/bootstrap_dataset.py --config "${CONFIG_EFFECTIVE}"
  DATASET_BOOTSTRAP_RC=$?
  set -e
  if [[ ${DATASET_BOOTSTRAP_RC} -eq 0 ]]; then
    DATASET_BOOTSTRAP_STATUS="passed"
    ok "Dataset bootstrap complete (S3 -> TiDB IMPORT INTO)"
  else
    DATASET_BOOTSTRAP_STATUS="failed"
    if [[ "${DATASET_BOOTSTRAP_REQUIRED}" == "true" ]]; then
      err "Dataset bootstrap failed and is required."
      exit ${DATASET_BOOTSTRAP_RC}
    fi
    warn "Dataset bootstrap failed; continuing with synthetic data path."
  fi
else
  warn "Dataset bootstrap disabled."
fi

if [[ "${DATASET_BOOTSTRAP_STATUS}" == "passed" && "${DATASET_SKIP_SYNTH}" == "true" ]]; then
  warn "dataset_bootstrap.skip_synthetic_generation=true — skipping synthetic data generation."
elif [[ -f "${RESULTS_DIR}/data_manifest.json" && "${REGEN}" == "false" ]]; then
  if [[ "$(db_has_nonempty_data)" == "true" ]]; then
    warn "data_manifest.json exists — skipping synthetic data generation"
    warn "  Use --regen to force regeneration"
  else
    warn "data_manifest.json exists but target database appears empty; regenerating data."
    "${PYTHON}" setup/generate_data.py --config "${CONFIG_EFFECTIVE}"
  fi
else
  if [[ "${REGEN}" == "true" ]]; then
    warn "--regen flag detected, regenerating data..."
    "${PYTHON}" setup/generate_data.py --config "${CONFIG_EFFECTIVE}"
  else
    "${PYTHON}" setup/generate_data.py --config "${CONFIG_EFFECTIVE}" --skip-if-exists
  fi
fi
ok "Data ready"

# ─────────────────────────────────────────────────────────────────────────────
# Helper: run a test module, capture exit code, continue on failure
# ─────────────────────────────────────────────────────────────────────────────
MODULE_PASS=0
MODULE_FAIL=0
MODULE_SKIP=0
RUN_STANDARD_MODULES="true"

module_enabled() {
  local keys_csv="$1"
  "${PYTHON}" - <<PY
import yaml
keys = [k.strip() for k in "${keys_csv}".split(",") if k.strip()]
with open("${CONFIG_EFFECTIVE}") as f:
    cfg = yaml.safe_load(f) or {}
mods = cfg.get("modules", {}) or {}
for key in keys:
    if key in mods:
        print(str(bool(mods.get(key))).lower())
        raise SystemExit(0)
print("true")
PY
}

run_module() {
  local num="$1"
  local label="$2"
  local script="$3"
  local enabled_keys="$4"

  local enabled
  enabled=$(module_enabled "${enabled_keys}" 2>/dev/null || echo "true")

  if [[ "${enabled}" == "false" ]]; then
    step "${num}/10" "${label} — SKIPPED (disabled in config)"
    MODULE_SKIP=$((MODULE_SKIP + 1))
    return 0
  fi

  step "${num}/10" "${label}"
  if "${PYTHON}" "${script}" "${CONFIG_EFFECTIVE}"; then
    ok "${label} complete"
    MODULE_PASS=$((MODULE_PASS + 1))
  else
    warn "${label} returned non-zero (results may be partial)"
    MODULE_FAIL=$((MODULE_FAIL + 1))
  fi
}

validate_report_data_completeness() {
  local strict_raw strict_mode
  # Strict completeness by default so reports cannot ship with blank/missing sections.
  strict_raw="${POV_REQUIRE_COMPLETE_REPORT_DATA:-true}"
  strict_mode="false"
  if is_true "${strict_raw}"; then
    strict_mode="true"
  fi

  set +e
  local validation_output
  validation_output="$("${PYTHON}" - "${CONFIG_EFFECTIVE}" "${RESULTS_DIR}/metrics_summary.json" <<'PY'
import json
import sys
import yaml
from pathlib import Path

cfg_path = sys.argv[1]
metrics_path = sys.argv[2]
results_dir = str(Path(metrics_path).resolve().parent)

cfg = yaml.safe_load(open(cfg_path)) or {}
metrics = {}
try:
    metrics = json.load(open(metrics_path))
except Exception:
    print("report-data-check: metrics_summary.json missing or unreadable")
    sys.exit(2)

mods_cfg = cfg.get("modules") or {}
module_rows = [
    ("00_customer_queries", ["customer_queries", "customer_query_validation"], "customer_queries"),
    ("01_baseline_perf", ["baseline_perf"], "latency"),
    ("01b_user_growth", ["user_growth"], "latency"),
    ("02_elastic_scale", ["elastic_scale"], "latency"),
    ("03_high_availability", ["high_availability"], "latency"),
    ("03b_write_contention", ["write_contention"], "latency"),
    ("04_htap_concurrent", ["htap"], "latency"),
    ("05_online_ddl", ["online_ddl"], "latency"),
    ("06_mysql_compat", ["mysql_compat"], "compat"),
    ("07_data_import", ["data_import"], "import"),
    ("08_vector_search", ["vector_search"], "latency"),
    ("09_tidb_features", ["tidb_features"], "latency"),
]

def enabled(keys):
    for key in keys:
        if key in mods_cfg:
            return bool(mods_cfg.get(key))
    return True

def point_count(entry):
    total = 0
    tidb = entry.get("tidb", {}) if isinstance(entry, dict) else {}
    if isinstance(tidb, dict):
        for stats in tidb.values():
            if isinstance(stats, dict):
                try:
                    total += int(stats.get("count", 0) or 0)
                except Exception:
                    pass
    return total

issues = []
mods_metrics = metrics.get("modules", {}) or {}
compat = metrics.get("compat_checks", {}) or {}
imports = metrics.get("import_stats", []) or []

for module_key, config_keys, mode in module_rows:
    if not enabled(config_keys):
        continue
    entry = mods_metrics.get(module_key, {}) or {}
    status = str(entry.get("status") or "not_run").strip().lower()
    notes = str(entry.get("notes") or "").strip()
    points = point_count(entry)

    if status == "not_run":
        issues.append(f"{module_key}: status not_run")
        continue

    if mode == "compat":
        total_checks = int(compat.get("total", 0) or 0)
        if total_checks <= 0:
            issues.append(f"{module_key}: no compatibility checks captured")
        continue

    if mode == "customer_queries":
        cq_path = Path(results_dir) / "customer_query_validation.json"
        if not cq_path.exists():
            issues.append(f"{module_key}: customer_query_validation.json missing")
            continue
        try:
            cq_rows = json.load(open(cq_path))
        except Exception:
            cq_rows = []
        if not isinstance(cq_rows, list) or len(cq_rows) == 0:
            issues.append(f"{module_key}: customer query validation artifact empty")
        continue

    if mode == "import":
        if len(imports) == 0:
            issues.append(f"{module_key}: no import stats captured")
        continue

    if module_key == "08_vector_search" and status == "skipped":
        if not notes:
            issues.append(f"{module_key}: skipped without reason")
        continue

    if status == "passed" and points <= 0:
        issues.append(f"{module_key}: passed but recorded 0 data points")
    elif status in {"failed", "warning", "skipped"} and points <= 0 and not notes:
        issues.append(f"{module_key}: {status} with no data points and no notes")

if issues:
    print("report-data-check: FAILED")
    for item in issues:
        print(f"  - {item}")
    sys.exit(2)

print("report-data-check: OK")
PY
)"
  local validation_rc=$?
  set -e

  if [[ ${validation_rc} -ne 0 ]]; then
    if [[ "${strict_mode}" == "true" ]]; then
      err "Data completeness validation failed."
      echo "${validation_output}"
      exit ${validation_rc}
    fi
    warn "Data completeness validation reported gaps (continuing)."
    echo "${validation_output}"
  else
    ok "Data completeness validation passed."
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# 5-9. Test Modules
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${POV_RUN_MODE}" == "performance" ]]; then
  BLASTER_PROFILE="$(read_workload_generator_profile)"
  BLASTER_MODE="${BLASTER_PROFILE%%|*}"
  BLASTER_TAG="${BLASTER_PROFILE#*|}"
  RUN_STANDARD_MODULES="false"

  step "5/10" "Workload Generator validation (${BLASTER_MODE})"
  if [[ ! -f "load/tidb_blaster_runner.py" ]]; then
    warn "Workload Generator runner script not found."
    warn "Falling back to standard validation module suite."
    RUN_STANDARD_MODULES="true"
  else
    set +e
    "${PYTHON}" load/tidb_blaster_runner.py \
      --config "${CONFIG_EFFECTIVE}" \
      --action validate \
      --mode "${BLASTER_MODE}" \
      --tag "${BLASTER_TAG}"
    BLASTER_VALIDATE_RC=$?
    set -e

    if [[ ${BLASTER_VALIDATE_RC} -ne 0 ]]; then
      warn "Workload Generator validation failed (exit ${BLASTER_VALIDATE_RC})."
      warn "Falling back to standard validation module suite."
      RUN_STANDARD_MODULES="true"
    else
      ok "Workload Generator validation passed"
      step "6/10" "Executing Workload Generator (${BLASTER_MODE})"
      set +e
      "${PYTHON}" load/tidb_blaster_runner.py \
        --config "${CONFIG_EFFECTIVE}" \
        --action run \
        --mode "${BLASTER_MODE}" \
        --tag "${BLASTER_TAG}"
      BLASTER_RUN_RC=$?
      set -e

      if [[ ${BLASTER_RUN_RC} -ne 0 ]]; then
        warn "Workload Generator execution returned non-zero (exit ${BLASTER_RUN_RC})."
        warn "Falling back to standard validation module suite."
        RUN_STANDARD_MODULES="true"
      else
        MODULE_PASS=$((MODULE_PASS + 1))
        WG_RUN_DIR="$(sync_workload_generator_summary || true)"
        if [[ -n "${WG_RUN_DIR}" ]]; then
          ok "Workload Generator summary synced from ${WG_RUN_DIR}"
        else
          warn "Workload Generator completed but summary artifact sync was not found."
        fi
      fi
    fi
  fi
fi

if [[ "${RUN_STANDARD_MODULES}" == "true" ]]; then
  run_module "5" "M0 — Customer Query Validation" "tests/00_customer_queries/validate_queries.py" "customer_queries,customer_query_validation"
  run_module "5" "M1 — Baseline OLTP Performance" "tests/01_baseline_perf/run.py" "baseline_perf"
  run_module "5" "M1b— User Growth Ramp" "tests/01b_user_growth/run.py" "user_growth"
  run_module "6" "M2 — Elastic Auto-Scaling" "tests/02_elastic_scale/run.py" "elastic_scale"
  run_module "6" "M3 — High Availability" "tests/03_high_availability/run.py" "high_availability"
  run_module "7" "M3b— Write Contention" "tests/03b_write_contention/run.py" "write_contention"
  run_module "7" "M4 — HTAP Concurrent" "tests/04_htap_concurrent/run.py" "htap"
  run_module "8" "M5 — Online DDL" "tests/05_online_ddl/run.py" "online_ddl"
  run_module "8" "M6 — SQL Compatibility" "tests/06_mysql_compat/run.py" "mysql_compat"
  run_module "9" "M7 — Data Import Speed" "tests/07_data_import/run.py" "data_import"
  run_module "9" "M8 — Vector Search (AI Track)" "tests/08_vector_search/run.py" "vector_search"
  run_module "9" "M9 — TiDB Feature Showcase" "tests/09_tidb_features/run.py" "tidb_features"
else
  step "7/10" "Scenario module suite"
  warn "Skipped M0-M8 module suite; performance mode executed via Workload Generator."
  MODULE_SKIP=$((MODULE_SKIP + 10))
fi

# ─────────────────────────────────────────────────────────────────────────────
# 10. Generate report
# ─────────────────────────────────────────────────────────────────────────────
step "10/10" "Generating PDF report"
"${PYTHON}" report/collect_metrics.py --quiet
ok "Metrics JSON written to ${RESULTS_DIR}/metrics_summary.json"
validate_report_data_completeness
"${PYTHON}" report/generate_report.py "${CONFIG_EFFECTIVE}"
ok "Report written to ${RESULTS_DIR}/tidb_pov_report.pdf"
s3_upload_required "10b/10"

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))
MINS=$(( ELAPSED / 60 ))
SECS=$(( ELAPSED % 60 ))

banner "PoV Complete"
echo -e "  ${GREEN}Modules passed : ${MODULE_PASS}${NC}"
echo -e "  ${YELLOW}Modules warning: ${MODULE_FAIL}${NC}"
echo -e "  ${BLUE}Modules skipped: ${MODULE_SKIP}${NC}"
echo -e "  Total runtime  : ${MINS}m ${SECS}s"
echo -e "  Config used    : ${CONFIG_EFFECTIVE}"
echo -e "  Report         : ${RESULTS_DIR}/tidb_pov_report.pdf"

if [[ -f "${INTAKE_JSON}" || -f "${INTAKE_MD}" ]]; then
  echo ""
  echo "  Intake artifacts:"
  [[ -f "${INTAKE_JSON}" ]] && echo "    - ${INTAKE_JSON}"
  [[ -f "${INTAKE_MD}" ]] && echo "    - ${INTAKE_MD}"
fi

if [[ "${S3_ENFORCED}" == "true" ]]; then
  print_s3_download_links
fi

echo ""
echo "  To view the report, open: ${RESULTS_DIR}/tidb_pov_report.pdf"
