#!/usr/bin/env bash
set -euo pipefail

# Bring up the DB and run migrations.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_DIR="$ROOT_DIR/api"

cd "$ROOT_DIR"
docker compose up -d

echo "Waiting for DB to become healthy..."
for i in {1..60}; do
  if docker compose ps --status running | grep -q "db" && docker compose ps | grep -q "healthy"; then
    break
  fi
  sleep 2
done

echo "Running migrations..."
cd "$API_DIR"
uv run alembic upgrade head

echo "DB ready."