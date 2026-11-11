# Runbook: local Docker stack

The stack has six services: `db` (PostgreSQL with PostGIS and TimescaleDB), `redis` (job queue broker), `migrate` (a
one-shot job that applies database migrations, then exits), `api` (FastAPI, waits for `migrate` to finish), `worker`
(Celery; a placeholder that exits with a message until story 1.10) and `web` (placeholder page).
Everything listens on `127.0.0.1` only, so nothing is reachable from other machines.

**Settings names.** The API reads environment variables with the `MICROGRID_` prefix (`MICROGRID_DATABASE_URL`,
`MICROGRID_REDIS_URL`, `MICROGRID_JWT_SECRET`, `MICROGRID_DATA_DIR`, `MICROGRID_OUTPUT_DIR`, `MICROGRID_ENVIRONMENT`).
Compose builds them from the values in `deploy/.env` and sets `MICROGRID_ENVIRONMENT=production`, so the API refuses to
start without real settings. You normally never edit these names.

**Floating database image tag.** `db` uses `timescale/timescaledb-ha:pg17`, a floating tag: it can change under you
when you pull. Its arm64 build and size are not yet verified. Run `make image-check` (it only prints commands) and
run what it prints on your Mac. Once confirmed, pin the full tag in `deploy/docker-compose.yml`.

## Prerequisites
- A Mac with Apple silicon (M-series) and Docker Desktop for Mac (Apple silicon build). In Docker Desktop settings,
  Resources: 6 or more CPUs and 8 GB or more memory is a reasonable start (the worker is capped at 8 GB by default).
- About 8 to 10 GB of free disk for images and volumes **[estimate; measure with `make disk`]**. The owner's `data/`
  folder (about 11 GB) is separate: it is mounted read-only and never copied.
- `make` (ships with Xcode command line tools).

## First start
```bash
cd Microgrid-Modeling
make up        # creates deploy/.env from the template on first run, builds, starts, waits for health
make ps        # db, redis, web, api say "healthy"; migrate says "exited (0)"; no worker is started
```
`make up` starts `db redis web migrate api` only. The worker is a placeholder that exits until story 1.10, and
`--wait` would report failure because of it, so it is left out of `make up` until then (the Makefile has a comment
marking where to add it back). `migrate` runs `python -m microgrid_api.db.migrate` (alembic upgrade head) once per
start; an `exited (0)` status is correct. If it fails, `api` does not start: see `make logs s=migrate`.
URLs: web http://127.0.0.1:3000, API http://127.0.0.1:8000/api/v1/health and `/api/v1/ready`.
To point at data elsewhere, edit `MICROGRID_DATA_DIR` in `deploy/.env` (default `./data`). It is mounted read-only.
The database password and JWT secret in the template are development-only values. Never commit `deploy/.env`
(it is git-ignored).

Automated check: `make smoke` (or `./deploy/tests/test_compose_smoke.sh`). It builds and starts db, redis and web,
then `migrate` and `api`; waits for each to be healthy; fetches `/api/v1/health`; and FAILS unless `/api/v1/ready`
reports `"ready":true` (database reachable and migrations applied). The worker is skipped unless
`SMOKE_REQUIRE_WORKER=1`. It stops the stack at the end but keeps volumes.

## What only you can verify on your Mac
Only you can verify these; the cloud environment has no Docker daemon or Apple silicon:
- `make up` completes and `make ps` looks as described above on an M2 (first build takes several minutes).
- `make image-check` output: arm64 manifest for every image, plus real image sizes (`make disk`).
- `make smoke` ends with `SMOKE OK`.
- Memory and disk use under load (Docker Desktop settings, `docker stats`).

## Check disk use
```bash
make disk              # docker system df
docker system df -v    # per image and per volume
du -sh data            # the data folder (not counted by Docker)
```
Docker Desktop also shows total usage under Settings, Resources, Disk image size. The VM disk file only shrinks when
Docker Desktop reclaims it; deleting images may not immediately free space on macOS.

## Stop
```bash
make down              # stops and removes containers; the database and outputs volumes are KEPT
```

## Reclaim space safely
Run `make prune-help` to print options (it never runs them). Safe ones keep volumes:
```bash
docker builder prune   # build cache
docker image prune     # dangling images
```
**Never run `docker volume prune`, `docker system prune --volumes`, or `docker compose down -v`.** They delete named
volumes, and `pgdata` holds the database (configs, runs, results, calibration uploads) while `outputs` holds run
outputs. To wipe them on purpose, use `make nuke`, which asks you to type DELETE first.

## Troubleshooting
- **Port already in use** (for example a local Postgres on 5432): change `DB_PORT` (or `API_PORT`, `REDIS_PORT`,
  `WEB_PORT`) in `deploy/.env`, then `make up`.
- **`worker` exited** (only if you started it by hand): expected until story 1.10; `make logs s=worker` shows the explanatory message.
- **`api` unhealthy or exited**: `make logs s=api`. If it never starts, check that `migrate` succeeded.
- **`db` slow on first start**: it initialises the data volume and extensions once (up to a minute).
- **Image arch warning** (`amd64` on `arm64`): the image lacks an arm64 build; report it, do not ignore it. Check with
  `docker buildx imagetools inspect <image>`.
- **Changed Postgres password has no effect**: the password is fixed when the volume is first created. Either reset it
  inside the database or (deliberately, with data loss) `make nuke`.
- **Out of memory kills**: lower the run size or raise Docker Desktop memory; adjust `WORKER_MEM_LIMIT`.
