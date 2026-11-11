#!/usr/bin/env bash
# Owner-run smoke test for the local stack (story 1.7). Run from anywhere on your Mac:
#   ./deploy/tests/test_compose_smoke.sh
# Starts the stack, waits for health, curls the API, then stops it. Volumes are NEVER deleted.
# Env: SMOKE_REQUIRE_API=1 (default) require the api; SMOKE_REQUIRE_WORKER=0 (default until story 1.10);
#      SMOKE_TIMEOUT=300 seconds; SMOKE_KEEP_UP=0 set 1 to leave the stack running.
set -euo pipefail
cd "$(dirname "$0")/../.."

ENV_FILE=deploy/.env
[ -f "$ENV_FILE" ] || { cp deploy/.env.example "$ENV_FILE"; echo "created $ENV_FILE from template (dev-only defaults)"; }
set -a; . "$ENV_FILE"; set +a
API="http://127.0.0.1:${API_PORT:-8000}"
WEB="http://127.0.0.1:${WEB_PORT:-3000}"
TIMEOUT="${SMOKE_TIMEOUT:-300}"
COMPOSE=(docker compose --project-directory . --env-file "$ENV_FILE" -f deploy/docker-compose.yml)

cleanup() {
  if [ "${SMOKE_KEEP_UP:-0}" = "1" ]; then echo "leaving stack up (SMOKE_KEEP_UP=1)"; return; fi
  "${COMPOSE[@]}" stop   # stop only: containers and named volumes are kept
}
trap cleanup EXIT

"${COMPOSE[@]}" config -q
"${COMPOSE[@]}" up -d --build db redis web
if ! "${COMPOSE[@]}" up -d --build api; then
  echo "api failed to start"; "${COMPOSE[@]}" logs --tail 50 api
  [ "${SMOKE_REQUIRE_API:-1}" = "0" ] || exit 1
fi

wait_healthy() {  # service
  local deadline=$(( $(date +%s) + TIMEOUT )) state=""
  until state=$("${COMPOSE[@]}" ps --format '{{.Health}}' "$1" 2>/dev/null) && [ "$state" = "healthy" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "$1 not healthy after ${TIMEOUT}s (last: ${state:-none})"; "${COMPOSE[@]}" logs --tail 50 "$1"; return 1
    fi
    sleep 3
  done
  echo "$1 healthy"
}
for svc in db redis web; do wait_healthy "$svc"; done
curl -fsS "$WEB/" >/dev/null && echo "web placeholder reachable"

if [ "${SMOKE_REQUIRE_API:-1}" = "1" ]; then
  wait_healthy api
  curl -fsS "$API/api/v1/health"; echo
  curl -fsS "$API/api/v1/ready"; echo
fi

if [ "${SMOKE_REQUIRE_WORKER:-0}" = "1" ]; then
  "${COMPOSE[@]}" up -d worker
  wait_healthy worker
else
  echo "worker skipped (SMOKE_REQUIRE_WORKER=0): placeholder until story 1.10"
fi
echo "SMOKE OK"
