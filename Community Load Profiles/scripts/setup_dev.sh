#!/usr/bin/env bash
set -euo pipefail

# Dev environment bootstrap for Microgrid backend.
# Requires: uv, docker

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_DIR="$ROOT_DIR/api"

cd "$API_DIR"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install from https://docs.astral.sh/uv/" >&2
  exit 1
fi

uv venv
uv sync --all-extras

echo "Dev environment ready. Next: ../scripts/setup_db.sh"