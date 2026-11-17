#!/bin/sh
# Worker entrypoint: one Celery worker that runs engine jobs (microgrid_api.worker, FR-PLAT-2) and, with --beat, also
# schedules the stuck-run reaper (microgrid.reap_stale_runs). Run exactly ONE worker with --beat: a second beat would
# schedule the reaper twice (harmless, the reaper's updates are guarded, but wasteful).
# Settings come from the environment (MICROGRID_REDIS_URL, MICROGRID_DATABASE_URL, MICROGRID_API_KEYS, ...);
# a missing one makes the module import fail loudly with a ConfigurationError naming the variable.
# /app is owned by root, so the beat schedule file lives in /tmp.
set -eu

concurrency="${WORKER_CONCURRENCY:-2}"
case "$concurrency" in
  ''|*[!0-9]*|0*)
    echo "worker-entrypoint: WORKER_CONCURRENCY must be a whole number from 1 upwards (got '${concurrency}')." >&2
    exit 2
    ;;
esac

exec celery -A microgrid_api.worker worker \
  --loglevel="${LOG_LEVEL:-INFO}" \
  --concurrency="${concurrency}" \
  --beat \
  --schedule=/tmp/celerybeat-schedule
