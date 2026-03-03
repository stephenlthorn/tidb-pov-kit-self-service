#!/usr/bin/env bash
# =============================================================================
#  TiDB Cloud PoV Kit — Full Orchestrator
#  run_all.sh
#
#  Usage:
#    ./run_all.sh [config.yaml] [--regen] [--wizard|--no-wizard] [--tier TIER]
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
RUN_INTAKE="auto"          # auto | yes | no
FORCE_TIER=""
ALLOW_BLOCKED=false
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --regen)
      REGEN=true
      shift
      ;;
    --wizard|--intake)
      RUN_INTAKE="yes"
      shift
      ;;
    --no-wizard|--no-intake)
      RUN_INTAKE="no"
      shift
      ;;
    --tier)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --tier"
        exit 1
      fi
      FORCE_TIER="$2"
      shift 2
      ;;
    --allow-blocked)
      ALLOW_BLOCKED=true
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

PYTHON="${PYTHON:-python3}"
PIP="${PIP:-pip3}"
RESULTS_DIR="results"
LOG_FILE="${RESULTS_DIR}/run_all.log"
INTAKE_JSON="${RESULTS_DIR}/pre_poc_intake.json"
INTAKE_MD="${RESULTS_DIR}/pre_poc_checklist.md"
RESOLVED_CONFIG="${RESULTS_DIR}/config.resolved.yaml"

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

# ── Logging ───────────────────────────────────────────────────────────────────
mkdir -p "${RESULTS_DIR}"
exec > >(tee -a "${LOG_FILE}") 2>&1

START_TS=$(date +%s)

banner "TiDB Cloud PoV Kit"
echo "  Config requested : ${CONFIG}"
echo "  Started          : $(date)"
echo "  Log              : ${LOG_FILE}"

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

# ─────────────────────────────────────────────────────────────────────────────
# 2. DB connectivity check
# ─────────────────────────────────────────────────────────────────────────────
step "2/10" "TiDB connectivity check"

set +e
CONN_CHECK=$(
  "${PYTHON}" - <<PY
import yaml, sys
sys.path.insert(0, '.')
with open('${CONFIG_EFFECTIVE}') as f:
    cfg = yaml.safe_load(f)
from lib.db_utils import ping
ok, msg = ping(cfg['tidb'])
print(msg if ok else 'FAIL: ' + msg)
sys.exit(0 if ok else 1)
PY
)
CONN_RC=$?
set -e

if [[ ${CONN_RC} -ne 0 ]] || echo "${CONN_CHECK}" | grep -q "FAIL"; then
  err "Cannot connect to TiDB: ${CONN_CHECK}"
  echo "  Check host/port/credentials and network access in ${CONFIG_EFFECTIVE}."
  exit 1
fi
ok "TiDB connection: ${CONN_CHECK}"

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
# 4. Generate synthetic data
# ─────────────────────────────────────────────────────────────────────────────
step "4/10" "Generating synthetic data"

if [[ -f "${RESULTS_DIR}/data_manifest.json" && "${REGEN}" == "false" ]]; then
  warn "data_manifest.json exists — skipping data generation"
  warn "  Use --regen to force regeneration"
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

# ─────────────────────────────────────────────────────────────────────────────
# 5-9. Test Modules
# ─────────────────────────────────────────────────────────────────────────────
run_module "5" "M0 — Customer Query Validation" "tests/00_customer_queries/validate_queries.py" "customer_queries,customer_query_validation"
run_module "5" "M1 — Baseline OLTP Performance" "tests/01_baseline_perf/run.py" "baseline_perf"
run_module "6" "M2 — Elastic Auto-Scaling" "tests/02_elastic_scale/run.py" "elastic_scale"
run_module "6" "M3 — High Availability" "tests/03_high_availability/run.py" "high_availability"
run_module "7" "M3b— Write Contention" "tests/03b_write_contention/run.py" "write_contention"
run_module "7" "M4 — HTAP Concurrent" "tests/04_htap_concurrent/run.py" "htap"
run_module "8" "M5 — Online DDL" "tests/05_online_ddl/run.py" "online_ddl"
run_module "8" "M6 — MySQL Compatibility" "tests/06_mysql_compat/run.py" "mysql_compat"
run_module "9" "M7 — Data Import Speed" "tests/07_data_import/run.py" "data_import"
run_module "9" "M8 — Vector Search (AI Track)" "tests/08_vector_search/run.py" "vector_search"

# ─────────────────────────────────────────────────────────────────────────────
# 10. Generate report
# ─────────────────────────────────────────────────────────────────────────────
step "10/10" "Generating PDF report"
"${PYTHON}" report/generate_report.py "${CONFIG_EFFECTIVE}"
ok "Report written to ${RESULTS_DIR}/tidb_pov_report.pdf"

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

echo ""
echo "  To view the report, open: ${RESULTS_DIR}/tidb_pov_report.pdf"
