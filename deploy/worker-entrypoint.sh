#!/bin/sh
# Worker entrypoint. PLACEHOLDER until Sprint 1 story 1.10 creates microgrid_api.worker.
# Fails loudly (non-zero exit, clear message) rather than pretending to work.
PY="${PYTHON:-python}"
if ! "$PY" -c "import microgrid_api.worker" 2>/dev/null; then
  echo "ERROR: module microgrid_api.worker does not exist yet (Celery worker arrives in story 1.10)." >&2
  echo "The worker service is expected to exit until then; api, db, redis and web are unaffected." >&2
  exit 1
fi
exec celery -A microgrid_api.worker worker --loglevel="${LOG_LEVEL:-INFO}" --concurrency="${WORKER_CONCURRENCY:-2}"
