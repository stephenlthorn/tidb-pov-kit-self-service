#!/usr/bin/env bash
# setup/01_install_deps.sh
# Idempotent dependency installer for the TiDB Cloud PoV Kit.
# Safe to run multiple times.

set -euo pipefail

PYTHON="${PYTHON:-python3}"
PIP="${PIP:-pip3}"

echo "[deps] Installing Python requirements..."
# Upgrade pip if possible; ignore failure on rpm-managed pip (RECORD file missing)
${PIP} install --quiet --upgrade pip --ignore-installed 2>/dev/null || \
  ${PIP} install --quiet --upgrade pip 2>/dev/null || true
${PIP} install --quiet -r requirements.txt

echo "[deps] Verifying key imports..."
${PYTHON} -c "
import mysql.connector, faker, yaml, matplotlib, fpdf, pandas, flask, psycopg, boto3
print('  All packages OK')
"
echo "[deps] Done."
