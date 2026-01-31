#!/usr/bin/env bash
set -euo pipefail

# Download all inputs from NREL OEDI S3.
#
# Configuration:
# - Fill in bucket/prefixes in: Community Load Profiles/data/sources/oedi_manifest.json
# - Optionally set DATA_DIR to choose destination (default: ../data)
#
# Usage:
#   ./scripts/download_inputs.sh --dry-run
#   ./scripts/download_inputs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/data/sources/oedi_manifest.json"

cd "$ROOT_DIR/scripts"

python3 ./download_oedi.py --manifest "$MANIFEST" "${@}" 
