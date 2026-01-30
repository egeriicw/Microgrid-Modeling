#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "This will delete the local docker volume 'microgrid_db' and ALL local DB data." >&2
read -r -p "Type 'delete' to continue: " CONFIRM
if [[ "$CONFIRM" != "delete" ]]; then
  echo "Aborted." >&2
  exit 1
fi

docker compose down -v

echo "DB volume removed."