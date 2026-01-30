#!/usr/bin/env bash
set -euo pipefail

# Placeholder OEDI downloader entrypoint.
# In later PRs this will read data/sources/oedi_manifest.yaml and sync from S3.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/data/sources/oedi_manifest.yaml"

if [[ ! -f "$MANIFEST" ]]; then
  echo "Manifest not found: $MANIFEST" >&2
  exit 1
fi

echo "OEDI downloader scaffold is in place." >&2
echo "Fill in S3 bucket/prefixes in: $MANIFEST" >&2
echo "Then re-run this script once OEDI URLs are known." >&2
exit 0
