#!/usr/bin/env bash
# Smoke test for the full local stack (FR-PLAT-2, FR-PLAT-4; carry-over A3). Two modes:
#
#   Owner mode (default; `make smoke`):  ./deploy/tests/test_compose_smoke.sh
#     Runs the free-disk preflight first and aborts if it refuses. Uses deploy/.env (created from the template) and
#     your data folder. Starts all six services, waits for health, checks /api/v1/health, /api/v1/ready (db, schema,
#     redis), the web page, the SPA fallback, a security header and the /api/ proxy, then STOPS the stack (volumes are
#     never deleted). SMOKE_RUN=1 also submits a tiny run (needs your data in the layout the API expects).
#
#   CI mode:  SMOKE_CI=1 ./deploy/tests/test_compose_smoke.sh
#     Uses deploy/.env.example and the CI overlay (generated synthetic fixture data, see deploy/ci/make_fixture_data.py)
#     under its own project name, so it never touches the owner's database volume. Always submits the run.
#
# The run check: POST /api/v1/runs THROUGH THE WEB PROXY with a bearer key (proves nginx forwards Authorization), poll
# GET /api/v1/runs/{id} until `succeeded` (fails on failed/cancelled/timeout, printing container logs), fetch
# /api/v1/runs/{id}/results (the document the worker wrote to the outputs volume), and read the SSE stream
# /api/v1/runs/{id}/events through the proxy.
#
# Env: SMOKE_TIMEOUT=300 (seconds per service health wait), SMOKE_RUN_TIMEOUT=300 (seconds for the run),
#      SMOKE_KEEP_UP=0 (1 leaves the stack running), SMOKE_API_KEY (default: the first key in API_KEYS).
set -euo pipefail
cd "$(dirname "$0")/../.."

# Free-disk guard first: a refusal aborts here, before deploy/.env is created and before any docker call.
./deploy/preflight.sh full

if [ "${SMOKE_CI:-0}" = "1" ]; then
  ENV_FILE=deploy/.env.example
  if [ ! -f tests/fixtures/data/background/DC_upgrade0.xlsx ]; then
    echo "smoke: CI mode needs the generated fixture data. Run first:" >&2
    echo "  python deploy/ci/make_fixture_data.py tests/fixtures/data" >&2
    exit 1
  fi
  COMPOSE=(docker compose --project-directory . --env-file "$ENV_FILE"
    -f deploy/docker-compose.yml -f deploy/docker-compose.ci.yml --project-name microgrid-smoke)
  RUN_CHECK=1
else
  ENV_FILE=deploy/.env
  [ -f "$ENV_FILE" ] || { cp deploy/.env.example "$ENV_FILE"; echo "created $ENV_FILE from template (dev-only defaults)"; }
  COMPOSE=(docker compose --project-directory . --env-file "$ENV_FILE" -f deploy/docker-compose.yml)
  RUN_CHECK="${SMOKE_RUN:-0}"
fi
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
API="http://127.0.0.1:${API_PORT:-8000}"
WEB="http://127.0.0.1:${WEB_PORT:-3000}"
TIMEOUT="${SMOKE_TIMEOUT:-300}"
RUN_TIMEOUT="${SMOKE_RUN_TIMEOUT:-300}"
SERVICES=(db redis migrate api worker web)
LONG_RUNNING=(db redis api worker web)

dump_logs() {  # [service...]
  "${COMPOSE[@]}" ps -a || :
  "${COMPOSE[@]}" logs --no-color --timestamps --tail 200 "$@" || :
}
fail() {  # message [service...]
  echo "SMOKE FAILED: $1" >&2
  shift
  dump_logs "$@"
  exit 1
}
cleanup() {
  if [ "${SMOKE_KEEP_UP:-0}" = "1" ]; then echo "leaving stack up (SMOKE_KEEP_UP=1)"; return; fi
  "${COMPOSE[@]}" stop || :  # stop only: containers and named volumes are kept
}
trap cleanup EXIT

"${COMPOSE[@]}" config -q
# `api` and `worker` wait for the one-shot `migrate` (alembic upgrade head) to exit 0; compose enforces the order.
"${COMPOSE[@]}" up -d --build "${SERVICES[@]}" || fail "docker compose up failed"

wait_healthy() {  # service
  local deadline=$(( $(date +%s) + TIMEOUT )) state=""
  while :; do
    state=$("${COMPOSE[@]}" ps -a --format '{{.State}}/{{.Health}}' "$1" 2>/dev/null || :)
    case "$state" in
      */healthy) echo "$1 healthy"; return 0 ;;
      exited/*|dead/*) fail "$1 stopped (state ${state})" "$1" ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || fail "$1 not healthy after ${TIMEOUT}s (last: ${state:-none})" "$1"
    sleep 3
  done
}
for svc in "${LONG_RUNNING[@]}"; do wait_healthy "$svc"; done

# --- api ---
curl -fsS "$API/api/v1/health" >/dev/null || fail "api /health unreachable" api
ready=$(curl -sS "$API/api/v1/ready") || fail "api /ready unreachable" api
echo "ready: $ready"
for want in '"ready":true' '"db":"ok"' '"schema":"ok"' '"redis":"ok"'; do
  case "$ready" in *"$want"*) ;; *) fail "/api/v1/ready lacks $want (database, migrations or Redis)" api ;; esac
done

# --- web: the built app, SPA fallback, security header, and the /api/ proxy ---
page=$(curl -fsS "$WEB/") || fail "web root unreachable" web
case "$page" in *'id="root"'*) ;; *) fail "web root is not the app page" web ;; esac
deep=$(curl -fsS "$WEB/runs/smoke-deep-link") || fail "SPA fallback failed for a client-side route" web
case "$deep" in *'id="root"'*) ;; *) fail "SPA fallback did not return the app page" web ;; esac
headers=$(curl -fsS -D - -o /dev/null "$WEB/") || fail "web headers unreadable" web
printf '%s' "$headers" | grep -qi '^x-content-type-options: *nosniff' || fail "security headers missing" web
proxied=$(curl -fsS "$WEB/api/v1/health") || fail "web proxy to the api failed" web api
case "$proxied" in *'"status":"ok"'*) ;; *) fail "proxied /api/v1/health returned: $proxied" web api ;; esac
echo "web app, SPA fallback, headers and /api/ proxy OK"

# --- a real run: API -> Redis -> worker -> stored result ---
if [ "$RUN_CHECK" = "1" ]; then
  KEY="${SMOKE_API_KEY:-$(printf '%s' "${API_KEYS:-}" | sed -n 's/^[^}]*"key"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')}"
  [ -n "$KEY" ] || fail "no API key: set API_KEYS in $ENV_FILE or SMOKE_API_KEY"
  AUTH="Authorization: Bearer $KEY"

  code=$(curl -sS -o /dev/null -w '%{http_code}' "$WEB/api/v1/runs") || fail "web proxy unreachable" web
  [ "$code" = "401" ] || fail "an unauthenticated request got HTTP $code, expected 401" api

  resp=$(curl -sS -w '\n%{http_code}' -X POST "$WEB/api/v1/runs" -H "$AUTH" -H 'Content-Type: application/json' \
    --data-binary @deploy/tests/smoke-run.json) || fail "POST /api/v1/runs through the web proxy failed" web api
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  [ "$code" = "202" ] || fail "POST /api/v1/runs returned HTTP $code: $body" api web
  # RunOut serialises `id` then `status` first (field order of the response model).
  run_id=$(printf '%s' "$body" | sed -n 's/^{"id":"\([0-9a-f-]*\)".*/\1/p')
  [ -n "$run_id" ] || fail "no run id in: $body" api
  echo "run $run_id queued"

  started=$(date +%s)
  deadline=$(( started + RUN_TIMEOUT ))
  while :; do
    detail=$(curl -fsS -H "$AUTH" "$API/api/v1/runs/$run_id") || fail "GET /api/v1/runs/$run_id failed" api
    status=$(printf '%s' "$detail" | sed -n 's/^{"id":"[0-9a-f-]*","status":"\([a-z]*\)".*/\1/p')
    case "$status" in
      succeeded) echo "run succeeded in $(( $(date +%s) - started ))s"; break ;;
      failed|cancelled) fail "run ended '$status': $detail" worker api ;;
      queued|running) ;;
      *) fail "unexpected run detail: $detail" api ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || fail "run still '$status' after ${RUN_TIMEOUT}s" worker api redis
    sleep 2
  done

  results=$(curl -fsS -H "$AUTH" "$API/api/v1/runs/$run_id/results") || fail "results document not served" api worker
  for want in '"schema_version"' '"results"'; do
    case "$results" in *"$want"*) ;; *) fail "results document lacks $want" api worker ;; esac
  done
  echo "results document: ${#results} bytes"

  events=$(curl -sS -N --max-time 60 -H "$AUTH" -H 'Accept: text/event-stream' "$WEB/api/v1/runs/$run_id/events") \
    || fail "SSE stream through the web proxy failed or did not end" web api
  case "$events" in *"event: status"*succeeded*) ;; *) fail "SSE stream lacks the final status: $events" web api ;; esac
  echo "SSE progress stream through the proxy OK"
else
  echo "run check skipped (SMOKE_RUN=1 to submit a tiny run against your data)"
fi
echo "SMOKE OK"
