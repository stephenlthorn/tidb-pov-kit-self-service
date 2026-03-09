#!/usr/bin/env bash
set -euo pipefail

# One-shot bootstrap for local CLI usage (macOS/Linux).
# Installs base tooling + Python deps so run_all.sh and helper scripts work.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "[bootstrap] repo: ${ROOT_DIR}"

has_cmd() { command -v "$1" >/dev/null 2>&1; }

install_macos() {
  if ! has_cmd brew; then
    echo "[bootstrap] Homebrew not found. Install from https://brew.sh and re-run."
    exit 2
  fi
  echo "[bootstrap] installing macOS packages via brew..."
  brew update
  brew install git python awscli jq || true
}

install_debian() {
  echo "[bootstrap] installing Debian/Ubuntu packages..."
  sudo apt-get update
  sudo apt-get install -y git python3 python3-pip python3-venv awscli jq curl
}

install_rhel() {
  echo "[bootstrap] installing RHEL/Amazon Linux packages..."
  if has_cmd dnf; then
    sudo dnf install -y git python3 python3-pip awscli jq curl
  else
    sudo yum install -y git python3 python3-pip awscli jq curl
  fi
}

OS="$(uname -s)"
case "${OS}" in
  Darwin)
    install_macos
    ;;
  Linux)
    if has_cmd apt-get; then
      install_debian
    elif has_cmd yum || has_cmd dnf; then
      install_rhel
    else
      echo "[bootstrap] unsupported Linux distro: install git/python3/pip/awscli/jq/curl manually."
      exit 2
    fi
    ;;
  *)
    echo "[bootstrap] unsupported OS: ${OS}"
    exit 2
    ;;
esac

PYTHON_BIN="${PYTHON:-python3}"
if ! has_cmd "${PYTHON_BIN}"; then
  echo "[bootstrap] python3 not found after package install."
  exit 2
fi

echo "[bootstrap] installing Python dependencies..."
chmod +x setup/01_install_deps.sh
bash setup/01_install_deps.sh

echo
echo "[bootstrap] done."
echo "[bootstrap] next:"
echo "  1) cp config.yaml.example config.small.yaml"
echo "  2) edit config.small.yaml (tidb.host/user/password/database)"
echo "  3) run local (no AWS checks):"
echo "     POV_ENFORCE_S3_UPLOAD=false POV_STRICT_AWS_CHECK=false bash scripts/pov_safe_small_e2e.sh config.small.yaml"
