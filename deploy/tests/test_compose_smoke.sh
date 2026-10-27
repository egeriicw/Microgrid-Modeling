#!/usr/bin/env bash
# FR-D1 — compose smoke test (CI job "compose-smoke"). RED placeholder.
# Assumes: docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.ci.yml up -d
# already ran. See docs/infrastructure.md §6.
set -euo pipefail

API=${API:-http://localhost:8000}
WEB=${WEB:-http://localhost:3000}
DEADLINE=$(( $(date +%s) + 90 ))

echo "waiting for ${API}/ready ..."
until curl -fsS "${API}/ready" >/dev/null 2>&1; do
  [ "$(date +%s)" -lt "${DEADLINE}" ] || { echo "api not ready in 90s"; exit 1; }
  sleep 2
done
echo "api ready"

curl -fsS "${API}/health"  | grep -q '"status": *"ok"'
curl -fsS "${API}/openapi.json" >/dev/null
curl -fsS "${WEB}" >/dev/null && echo "web reachable"

echo "launching a mock run via seed script ..."
python scripts/seed.py --smoke   # inserts template, ensures bucket, launches max_runs=6 mock run

# poll the newest run to terminal state
for _ in $(seq 1 60); do
  STATUS=$(curl -fsS "${API}/runs?limit=1" | python -c 'import sys,json; print(json.load(sys.stdin)["items"][0]["status"])')
  echo "run status: ${STATUS}"
  case "${STATUS}" in
    succeeded) echo "SMOKE OK"; exit 0 ;;
    failed|cancelled|delete_failed) echo "SMOKE FAILED (${STATUS})"; exit 1 ;;
  esac
  sleep 2
done
echo "run did not finish in time"; exit 1
