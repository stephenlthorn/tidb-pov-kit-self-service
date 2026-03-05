#!/usr/bin/env bash
# setup/01_install_deps.sh
# Idempotent dependency installer for the TiDB Cloud PoV Kit.
# Safe to run multiple times.

set -euo pipefail

PYTHON="${PYTHON:-python3}"
PIP="${PIP:-pip3}"

echo "[deps] Installing Python requirements..."
${PIP} install --quiet --upgrade pip
${PIP} install --quiet -r requirements.txt

echo "[deps] Verifying key imports..."
${PYTHON} -c "
import mysql.connector, faker, yaml, matplotlib, fpdf, pandas, flask, psycopg
print('  All packages OK')
"
echo "[deps] Done."
