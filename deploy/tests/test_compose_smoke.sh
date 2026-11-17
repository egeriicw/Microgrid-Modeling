#!/usr/bin/env bash
# Smoke test for the full local stack (FR-PLAT-2, FR-PLAT-4; carry-over A3). Two modes:
#
#   Owner mode (default; `make smoke`):  ./deploy/tests/test_compose_smoke.sh
#     Runs the free-disk preflight first and aborts if it refuses. Uses deploy/.env (created from the template) and
#     your data folder. Starts all six services, waits for health, checks /api/v1/health, /api/v1/ready (db, schema,
#     redis), the web page, the SPA fallback, security headers and the /api/ proxy, then STOPS the stack (volumes are
#     never deleted). SMOKE_RUN=1 also submits a small run (needs your data in the layout the API expects).
#
#   CI mode:  SMOKE_CI=1 ./deploy/tests/test_compose_smoke.sh
#     Uses deploy/.env.example and the CI overlay (generated synthetic fixture data, see deploy/ci/make_fixture_data.py)
#     under its own project name, so it never touches the owner's database volume. Always submits the run.
#
# The run check: POST /api/v1/runs THROUGH THE WEB PROXY with a bearer key (proves nginx forwards Authorization); open
# the SSE stream /api/v1/runs/{id}/events through the proxy straight away and require a progress event to arrive while
# the run is still queued/running (a buffering proxy would hold it back until the end); poll GET /api/v1/runs/{id}
# until `succeeded` (fails on failed/cancelled/timeout, printing container logs); fetch /api/v1/runs/{id}/results (the
# document the worker wrote to the outputs volume) and require a non-empty hourly time axis and total envelope; and
# require the finished stream to hold progress events followed by a final `succeeded` status.
# Every JSON reply is parsed with python3 (part of the Xcode command line tools on a Mac, preinstalled on CI runners).
#
# Env: SMOKE_TIMEOUT=300 (seconds per service health wait), SMOKE_RUN_TIMEOUT=300 (seconds for the run),
#      SMOKE_KEEP_UP=0 (1 leaves the stack running), SMOKE_API_KEY (default: the first key in API_KEYS).
set -euo pipefail
cd "$(dirname "$0")/../.."

# Free-disk guard first: a refusal aborts here, before deploy/.env is created and before any docker call.
./deploy/preflight.sh full

command -v python3 >/dev/null 2>&1 || {
  echo "smoke: python3 is needed to read the API's JSON replies (on a Mac: xcode-select --install)." >&2
  exit 1
}

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
STREAM_PID=""
STREAM_FILE=""

# check MODE [ARG]: reads a reply on stdin; exits non-zero with a reason on stderr unless it is what MODE requires.
# Modes: health, ready, field NAME (prints a non-empty string field), first-key (from API_KEYS), results,
# sse-progress (a complete progress event is present), sse-final (progress events, then a final `succeeded`).
# shellcheck disable=SC2016
CHECK_PY='
import json, re, sys
mode = sys.argv[1]
raw = sys.stdin.read()

def need(cond, why):
    if not cond:
        sys.exit("check " + mode + ": " + why)

def sse_events(text, complete_only):
    blocks = re.split(r"\n\n", text.replace("\r\n", "\n").replace("\r", "\n"))
    if complete_only:
        blocks = blocks[:-1]  # the last block may still be arriving
    events = []
    for block in blocks:
        name, data = "message", []
        for line in block.split("\n"):
            if line.startswith("event:"):
                name = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
        if data:
            events.append((name, "\n".join(data)))
    return events

if mode == "sse-progress":
    need(any(n == "progress" for n, _ in sse_events(raw, True)), "no progress event yet")
    sys.exit(0)
if mode == "sse-final":
    events = sse_events(raw, False)
    names = [n for n, _ in events]
    need("status" in names, "no final status event")
    first_status = names.index("status")
    need("progress" in names[:first_status], "no progress event before the status event")
    need(names[-1] == "status" and first_status == len(names) - 1, "events after the status event")
    need(json.loads(events[-1][1]).get("status") == "succeeded", "final status is not succeeded")
    sys.exit(0)
try:
    d = json.loads(raw)
except ValueError:
    sys.exit("check " + mode + ": reply is not JSON")
if mode == "health":
    need(isinstance(d, dict) and d.get("status") == "ok", "status is not ok")
elif mode == "ready":
    need(isinstance(d, dict) and d.get("ready") is True, "ready is not true")
    checks = d.get("checks")
    for key in ("db", "schema", "redis"):
        need(isinstance(checks, dict) and checks.get(key) == "ok", key + " is not ok")
elif mode == "field":
    need(isinstance(d, dict) and isinstance(d.get(sys.argv[2]), str) and d[sys.argv[2]], "no " + sys.argv[2])
    print(d[sys.argv[2]])
elif mode == "first-key":
    need(isinstance(d, list) and d and isinstance(d[0], dict) and isinstance(d[0].get("key"), str), "no key")
    print(d[0]["key"])
elif mode == "results":
    need(isinstance(d, dict) and d.get("schema_version"), "no schema_version")
    r = d.get("results")
    need(isinstance(r, dict) and r, "the results block is empty or missing")
    need(isinstance(r.get("n_runs"), int) and r["n_runs"] >= 1, "results.n_runs is below 1")
    hourly = (r.get("time") or {}).get("hourly") or {}
    count = hourly.get("count") if isinstance(hourly, dict) else None
    need(isinstance(count, int) and count > 0, "results.time.hourly.count is not positive (empty time axis)")
    env = (((r.get("envelopes") or {}).get("hourly") or {}).get("total") or {}).get("mean_kw")
    need(isinstance(env, list) and len(env) == count, "hourly total mean_kw envelope is empty or the wrong length")
    need(any(isinstance(v, (int, float)) and v > 0 for v in env), "hourly total mean_kw has no positive value")
else:
    sys.exit("check: unknown mode " + mode)
'
check() { python3 -c "$CHECK_PY" "$@"; }

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
  if [ -n "$STREAM_PID" ]; then kill "$STREAM_PID" 2>/dev/null || :; fi
  if [ -n "$STREAM_FILE" ] && [ -f "$STREAM_FILE" ]; then rm -- "$STREAM_FILE" || :; fi  # our own mktemp file only
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
health=$(curl -fsS "$API/api/v1/health") || fail "api /health unreachable" api
printf '%s' "$health" | check health || fail "api /health returned: $health" api
ready=$(curl -sS "$API/api/v1/ready") || fail "api /ready unreachable" api
echo "ready: $ready"
printf '%s' "$ready" | check ready || fail "/api/v1/ready is not ready (database, migrations or Redis)" api

# --- web: the built app, SPA fallback, security headers, and the /api/ proxy ---
page=$(curl -fsS "$WEB/") || fail "web root unreachable" web
case "$page" in *'id="root"'*) ;; *) fail "web root is not the app page" web ;; esac
deep=$(curl -fsS "$WEB/runs/smoke-deep-link") || fail "SPA fallback failed for a client-side route" web
case "$deep" in *'id="root"'*) ;; *) fail "SPA fallback did not return the app page" web ;; esac
headers=$(curl -fsS -D - -o /dev/null "$WEB/") || fail "web headers unreadable" web
printf '%s' "$headers" | grep -qi '^x-content-type-options: *nosniff' || fail "X-Content-Type-Options missing" web
printf '%s' "$headers" | grep -qi "^content-security-policy: *default-src 'self'" \
  || fail "Content-Security-Policy missing on the web root" web
proxied=$(curl -fsS "$WEB/api/v1/health") || fail "web proxy to the api failed" web api
printf '%s' "$proxied" | check health || fail "proxied /api/v1/health returned: $proxied" web api
echo "web app, SPA fallback, headers and /api/ proxy OK"

# --- a real run: API -> Redis -> worker -> stored result, progress streamed live through the proxy ---
if [ "$RUN_CHECK" = "1" ]; then
  KEY="${SMOKE_API_KEY:-}"
  if [ -z "$KEY" ]; then
    KEY=$(printf '%s' "${API_KEYS:-}" | check first-key) || fail "no API key: set API_KEYS in $ENV_FILE or SMOKE_API_KEY"
  fi
  AUTH="Authorization: Bearer $KEY"

  code=$(curl -sS -o /dev/null -w '%{http_code}' "$WEB/api/v1/runs") || fail "web proxy unreachable" web
  [ "$code" = "401" ] || fail "an unauthenticated request got HTTP $code, expected 401" api

  resp=$(curl -sS -w '\n%{http_code}' -X POST "$WEB/api/v1/runs" -H "$AUTH" -H 'Content-Type: application/json' \
    --data-binary @deploy/tests/smoke-run.json) || fail "POST /api/v1/runs through the web proxy failed" web api
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  [ "$code" = "202" ] || fail "POST /api/v1/runs returned HTTP $code: $body" api web
  run_id=$(printf '%s' "$body" | check field id) || fail "no run id in: $body" api
  started=$(date +%s)
  deadline=$(( started + RUN_TIMEOUT ))
  echo "run $run_id queued"

  # Open the progress stream through the proxy right away, in the background.
  STREAM_FILE=$(mktemp "${TMPDIR:-/tmp}/microgrid-smoke-sse.XXXXXX")
  curl -sS -N -f --max-time $(( RUN_TIMEOUT + 60 )) -H "$AUTH" -H 'Accept: text/event-stream' \
    "$WEB/api/v1/runs/$run_id/events" >"$STREAM_FILE" &
  STREAM_PID=$!
  rc=0  # the stream's curl exit code, once it has ended

  # A progress event must come through while the run is still in progress. A proxy that buffers the stream would
  # deliver it only when the run ends, and the status read AFTER seeing the event would already be terminal.
  until check sse-progress <"$STREAM_FILE" 2>/dev/null; do
    if ! kill -0 "$STREAM_PID" 2>/dev/null; then
      rc=0; wait "$STREAM_PID" || rc=$?; STREAM_PID=""
      check sse-progress <"$STREAM_FILE" 2>/dev/null && break  # it finished writing between the two checks
      fail "SSE stream through the proxy ended (curl exit $rc) without a progress event: $(head -c 2000 "$STREAM_FILE")" \
        web api worker
    fi
    [ "$(date +%s)" -lt "$deadline" ] || fail "no progress event through the proxy within ${RUN_TIMEOUT}s" web api worker
    sleep 1
  done
  detail=$(curl -fsS -H "$AUTH" "$API/api/v1/runs/$run_id") || fail "GET /api/v1/runs/$run_id failed" api
  status=$(printf '%s' "$detail" | check field status) || fail "unreadable run detail: $detail" api
  case "$status" in
    queued|running) echo "progress event streamed through the proxy while the run was $status" ;;
    *) fail "the first progress event reached the stream only after the run was '$status': the proxy is buffering \
the stream (or the run is too short to tell; it should take several seconds)" web api ;;
  esac

  while :; do
    detail=$(curl -fsS -H "$AUTH" "$API/api/v1/runs/$run_id") || fail "GET /api/v1/runs/$run_id failed" api
    status=$(printf '%s' "$detail" | check field status) || fail "unreadable run detail: $detail" api
    case "$status" in
      succeeded) echo "run succeeded in $(( $(date +%s) - started ))s"; break ;;
      failed|cancelled) fail "run ended '$status': $detail" worker api ;;
      queued|running) ;;
      *) fail "unexpected run status '$status': $detail" api ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || fail "run still '$status' after ${RUN_TIMEOUT}s" worker api redis
    sleep 2
  done

  results=$(curl -fsS -H "$AUTH" "$API/api/v1/runs/$run_id/results") || fail "results document not served" api worker
  printf '%s' "$results" | check results || fail "results document is not a usable result" api worker
  echo "results document: ${#results} bytes"

  if [ -n "$STREAM_PID" ]; then rc=0; wait "$STREAM_PID" || rc=$?; STREAM_PID=""; fi
  [ "$rc" = "0" ] || fail "SSE stream through the proxy failed or did not end (curl exit $rc)" web api
  check sse-final <"$STREAM_FILE" \
    || fail "SSE stream is not progress events then a final succeeded: $(tail -c 2000 "$STREAM_FILE")" web api
  echo "SSE progress stream through the proxy OK"
else
  echo "run check skipped (SMOKE_RUN=1 to submit a small run against your data)"
fi
echo "SMOKE OK"
