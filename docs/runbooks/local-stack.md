# Runbook: local Docker stack

The stack is five containers: `db` (PostgreSQL with PostGIS and TimescaleDB), `redis` (job queue broker), `api`
(FastAPI), `worker` (Celery; a placeholder that exits with a message until story 1.10) and `web` (placeholder page).
Everything listens on `127.0.0.1` only, so nothing is reachable from other machines.

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
make ps        # every service should say "healthy" (worker is expected to be "exited" until story 1.10)
```
URLs: web http://127.0.0.1:3000, API http://127.0.0.1:8000/api/v1/health and `/api/v1/ready`.
To point at data elsewhere, edit `MICROGRID_DATA_DIR` in `deploy/.env` (default `./data`). It is mounted read-only.
The database password and JWT secret in the template are development-only values. Never commit `deploy/.env`
(it is git-ignored).

Automated check: `make smoke` (or `./deploy/tests/test_compose_smoke.sh`). It stops the stack at the end but keeps volumes.

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
- **`worker` exited**: expected until story 1.10; `make logs s=worker` shows the explanatory message.
- **`api` unhealthy or exited**: `make logs s=api`. Before story 1.8 lands the module `microgrid_api.main` may not exist.
- **`db` slow on first start**: it initialises the data volume and extensions once (up to a minute).
- **Image arch warning** (`amd64` on `arm64`): the image lacks an arm64 build; report it, do not ignore it. Check with
  `docker buildx imagetools inspect <image>`.
- **Changed Postgres password has no effect**: the password is fixed when the volume is first created. Either reset it
  inside the database or (deliberately, with data loss) `make nuke`.
- **Out of memory kills**: lower the run size or raise Docker Desktop memory; adjust `WORKER_MEM_LIMIT`.
