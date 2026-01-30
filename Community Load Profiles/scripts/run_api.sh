#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/api"

uv run uvicorn microgrid_api.main:app --reload --host 0.0.0.0 --port 8000
