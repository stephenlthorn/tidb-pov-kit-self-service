#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${ROOT_DIR}"

CONFIG_DEFAULT="config.yaml"

if [[ ! -t 0 ]]; then
  echo "Interactive menu requires a TTY."
  echo "Use one of these instead:"
  echo "  ./run_all.sh"
  echo "  ./run_all.sh --report-only"
  echo "  ./run_all.sh --report-json-only"
  exit 1
fi

while true; do
  echo ""
  echo "=============================================="
  echo " TiDB PoV Kit - Main Menu"
  echo "=============================================="
  echo "  1) Run full PoV workflow"
  echo "  2) Generate PDF report only (reuse existing results)"
  echo "  3) Generate metrics JSON only (reuse existing results)"
  echo "  4) Generate PDF report only (choose config file)"
  echo "  5) Exit"
  echo ""
  read -r -p "Select option [1-5]: " choice

  case "${choice}" in
    1)
      ./run_all.sh "${CONFIG_DEFAULT}"
      ;;
    2)
      ./run_all.sh "${CONFIG_DEFAULT}" --report-only
      ;;
    3)
      ./run_all.sh "${CONFIG_DEFAULT}" --report-json-only
      ;;
    4)
      read -r -p "Config path (default: ${CONFIG_DEFAULT}): " cfg
      cfg="${cfg:-${CONFIG_DEFAULT}}"
      ./run_all.sh "${cfg}" --report-only
      ;;
    5)
      echo "Exiting."
      exit 0
      ;;
    *)
      echo "Invalid choice. Enter 1, 2, 3, 4, or 5."
      ;;
  esac

done
