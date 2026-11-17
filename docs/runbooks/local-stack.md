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
- About 12 GB of free disk for the full stack (images, build cache, volumes; the preflight check enforces it) **[estimate; measure with `make disk`]**. Less than that: use the lite stack, see "Small disk" below. The owner's `data/`
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

Automated check: `make smoke` (or `./deploy/tests/test_compose_smoke.sh`). It first runs the free-disk preflight (full
mode) and stops if that refuses, before creating `deploy/.env` or calling Docker. Then it builds and starts db, redis and web,
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
outputs. To wipe them on purpose, use `make nuke` (full stack) or `make lite-nuke` (lite stack only), which ask you to type DELETE
first. `make lite-nuke` removes only the `microgrid-lite_*` volumes and never touches the full stack's `microgrid_*` volumes.

## Small disk

A full disk can freeze macOS, so check before you start. With about 6.7 GB free (as `df` reports), do NOT run the full
stack. Use the lite stack.

**First commands to run on your Mac (in this order).** Run from the repo root. Nothing here deletes anything.
```bash
df -h /System/Volumes/Data                 # 1. real free space
make disk-plan                             # 2. prints the size plan (runs nothing)
docker system df                           # 3. what Docker already uses (BEFORE)
./deploy/preflight.sh lite                 # 4. expect: "preflight: OK (about 6.7 GB free, 5 GB needed for lite)." and exit 0
./deploy/preflight.sh full                 # 5. expect: "preflight: REFUSING to start the full stack: ... 12 GB needed" and exit 1
make -n lite-up                            # 6. prints the commands lite-up would run (runs nothing)
make lite-up && curl -fsS http://127.0.0.1:8000/api/v1/health   # 7. expect a small JSON health reply
docker system df                           # 8. Docker use AFTER
make lite-down                             # 9. stops the containers; KEEPS the volumes
```
Step 5 refusing is the correct, expected result at about 6.7 GB free. What to paste back (plain language): the output of
steps 1, 3 and 8 (so we can compare before and after); the two preflight messages from steps 4 and 5; the health reply
from step 7; how long `make lite-up` took the first time; and, if anything failed, the last 30 lines of its output.

**Preflight guard details.** It fails closed: any value it cannot read with certainty makes it refuse (exit non-zero).
`PREFLIGHT_MIN_FREE_GB` must be a whole number from 1 to 99999 with no leading zeros, spaces or signs, otherwise the
script refuses. `PREFLIGHT_MIN_FREE_GB=0` is the one explicit way to switch the guard off: it prints a loud
"DISABLED" warning and checks nothing. `make smoke` and `./deploy/tests/test_compose_smoke.sh` run the full-mode
preflight first, and `make up` runs it before creating `deploy/.env`, so a refused run leaves no new file behind.

**Where the thresholds come from.** The 12 GB (full) and 5 GB (lite) limits are headroom over the estimates below, not
measurements: roughly the estimate's upper end plus a few GB so macOS keeps free space. Measure with `docker system df`.

**Step 1: look, change nothing.**
```bash
df -h /System/Volumes/Data   # the real free space on the Mac's data volume
make disk-plan               # prints the expected need for full and lite (never runs anything)
docker system df             # what Docker already uses (images, containers, volumes, build cache)
```
`make up` and `make lite-up` first run `deploy/preflight.sh`, which only reads free space and refuses (non-zero exit,
clear message) below 12 GB for full and 5 GB for lite. It never deletes anything. Override the threshold on purpose with
`PREFLIGHT_MIN_FREE_GB=8 make up`; the script announces the override. If Docker's disk image lives on another drive,
point the check at it: `PREFLIGHT_PATH=/Volumes/External make up`.

**Step 2: run the lite stack.**
```bash
make lite-up     # api + migrate only, one image, SQLite file in a named volume; API at http://127.0.0.1:8000
make lite-down   # stops and removes containers; the lite volumes (SQLite file, outputs) are KEPT
make lite-nuke   # DESTRUCTIVE, asks you to type DELETE: removes ONLY the lite volumes (microgrid-lite_*); like `make nuke` for the full stack
```
What lite gives you: the same API image as the full stack, the data folder mounted read-only, loopback-only port,
healthcheck, non-root user and dropped capabilities. What you lose: PostGIS (spatial SQL), TimescaleDB (time-series
SQL), Redis, the web placeholder, the worker, and production mode. Lite sets `MICROGRID_ENVIRONMENT=development`
because the API only allows SQLite without a JWT secret in development; no secrets file is needed, and for that reason
lite is for this laptop on 127.0.0.1 only. Nothing in the engine or API uses PostGIS or TimescaleDB yet (the
migration runs on SQLite, checked in the cloud). Run the full stack when you have the space, or on a bigger disk, to
check the production-like setup. Lite and full both default to port 8000: run one at a time.

**Step 3: free space safely (only what you recognise).**
```bash
docker builder prune      # build cache only; volumes untouched
docker image prune        # dangling (untagged) images only
```
Never `docker volume prune`, `docker system prune --volumes`, or `docker compose down -v`: they delete the database and
outputs volumes. Ordinary macOS clean-up is fine: empty the Trash, Apple menu > About This Mac > Storage > Manage.
Do not delete anything inside `data/`.

**Step 4: move Docker's disk image to an external drive (Docker Desktop).** Docker Desktop > Settings > Resources >
Advanced > "Disk image location" > Browse > choose a folder on the external drive > Apply & restart. Docker Desktop
moves the file itself. Use a fast SSD (USB 3 or Thunderbolt), keep it plugged in while Docker runs, and do not pick a
network drive. Existing images and volumes move with it [verify the option name in your Docker Desktop version]. Then
set `PREFLIGHT_PATH` to that drive so the free-space check looks at the right place. Also note: the Docker disk file
grows but does not shrink by itself after you delete images (Settings > Resources > Disk image size shows usage).

### Measured and estimated sizes (checked 2026-11-11, linux/arm64)
"Compressed" is the sum of the layer sizes the registry reports for the arm64 image (this is the download size), read
through the registry API without a Docker daemon. "On disk" is the expanded size Docker Desktop stores; the expansion
ratio was MEASURED for the two images marked (m) by downloading and unpacking their layers, and is an ESTIMATE
(ratio 2.4 to 3.4) for the others. Docker Desktop with the containerd image store may also keep the compressed copy,
so allow for both [verify on your Mac with `docker system df`].

| Image (used by) | Compressed | On disk | Index digest (first 12) | Notes |
|---|---|---|---|---|
| timescale/timescaledb-ha:pg17 (full `db`) | 891.8 MB | about 2.1 to 3.0 GB ESTIMATE | 3f0b3f6bdf49 | PostgreSQL + PostGIS + TimescaleDB; floating tag |
| redis:7.4-alpine (full `redis`) | 16.9 MB | 40.7 MB (m) | 7b1753b4c5ad | |
| nginxinc/nginx-unprivileged:1.27-alpine (full `web`) | 21.8 MB | about 55 MB ESTIMATE | 65e3e85dbaed | |
| python:3.12-slim-bookworm (api base) | 44.9 MB | 154 MB (m, layer tar bytes) | 935c3c5b1235 | |
| microgrid-api:dev (api, migrate, worker; shared layers) | about 170 MB ESTIMATE | about 0.6 to 0.7 GB ESTIMATE | n/a (built locally) | base + Python packages below |
| python:3.12-alpine | 18.5 MB | n/a | e064671ca452 | NOT usable: pyarrow publishes no musl wheels [verify] |
| postgres:17-alpine | 115.0 MB | about 280 MB ESTIMATE | 3540c0362c80 | no PostGIS, no TimescaleDB |
| postgres:17-bookworm | 154.2 MB | about 400 MB ESTIMATE | 665dc7b37ac5 | no PostGIS, no TimescaleDB |
| timescale/timescaledb:latest-pg17 | 549.5 MB | about 1.4 GB ESTIMATE | b56a3f51991d | TimescaleDB, no PostGIS |
| timescale/timescaledb:2.31.0-pg17-oss | 383 MB (Docker Hub tag API) | n/a | n/a | open-source subset, no PostGIS |
| postgis/postgis:17-3.5-alpine (official) | 189 MB (amd64 only) | n/a | n/a | NO arm64 build: would run emulated on an M2 |
| imresamu/postgis:17-3.5-alpine (community) | 192 MB arm64 (Docker Hub tag API) | n/a | n/a | PostGIS only, unofficial publisher |

Python packages in `microgrid-api` (arm64 wheels from PyPI, resolved with `uv pip compile`, 53 packages, 121 MB of
wheels in total; there is no scipy). Largest: pyarrow 51.8 MB, botocore 16.3 MB (from boto3, an API dependency), numpy
15.7 MB, pandas 11.5 MB, psycopg-binary 6.8 MB, sqlalchemy 4.7 MB, uvloop 4.4 MB. Unpacked, these are roughly 400 to 550 MB
ESTIMATE. The root package needs only numpy, pandas, pyarrow, pyyaml and openpyxl.

| Scenario | Estimated disk | Basis |
|---|---|---|
| Full stack: images + build cache + empty volumes | about 5 to 8 GB of new use, ESTIMATE | db 2.1 to 3.0 GB + api 0.7 + small images 0.1 + build cache 0.5 to 1 + volumes 0.1 |
| Lite stack | about 1.5 to 3 GB of new use, ESTIMATE | api image 0.7 + build cache 0.5 to 1 + base image layers and build overhead (QA's more cautious range) |
| Your `data/` folder | about 11 GB, already used | read-only mount, never copied |

Lite saves roughly 3 GB, almost all of it the `db` image. A smaller real Postgres is possible (postgres:17-alpine, about
0.3 GB) but loses PostGIS and TimescaleDB, so it is not offered until the owner decides that trade.

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
