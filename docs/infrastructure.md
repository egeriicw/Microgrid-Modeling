# Infrastructure & Orchestration

Reflects D2 (Redis + RQ), D3 (Postgres + MinIO), D4 (full app, Compose for
local/dev/CI). Kubernetes is a **non-goal** (PRD §2.2); §9 has advisory
production notes only.

The concrete `deploy/docker-compose.yml` in this repo is the runnable
companion to this document.

---

## 1. Services

| Service | Image / build | Ports (host:container) | Depends on | Notes |
|---|---|---|---|---|
| `web` | build `web/Dockerfile` | `3000:3000` | `api` | Next.js; `NEXT_PUBLIC_API_BASE_URL=http://localhost:8000` for the browser |
| `api` | build `api/Dockerfile` | `8000:8000` | `db` (healthy), `redis` (healthy), `minio` (healthy) | entrypoint runs `alembic upgrade head` then `uvicorn microgrid_api.main:app` |
| `worker` | same image as `api`, `command: python -m microgrid_api.worker` | — | `db`, `redis`, `minio` | mounts `./data:/data:ro`; scale with `--scale worker=N` |
| `db` | `postgres:16` | `5432:5432` | — | volume `pgdata`; healthcheck `pg_isready` |
| `redis` | `redis:7` | `6379:6379` | — | RQ queue + SSE pub/sub; `--save "" --appendonly no` (ephemeral) |
| `minio` | `minio/minio` | `9000:9000`, `9001:9001` | — | volume `miniodata`; `server /data --console-address :9001` |
| `minio-init` | `minio/mc` | — | `minio` (healthy) | one-shot: `mc mb --ignore-existing`, sets bucket, exits 0 |

`data/` is a **host bind mount** into `worker` (read-only) holding the large
ResStock/ComStock parquet inputs (`infrastructure.md` §7). Everything a run
*produces* goes to MinIO, not the bind mount.

---

## 2. Networking & env

- One user-defined bridge network (Compose default). Only `web`, `api`,
  `minio` console are published to the host.
- `api` CORS allows exactly `http://localhost:3000`.
- All config via `deploy/.env` (from `.env.example`, FR-D6):

```dotenv
# --- api / worker ---
DATABASE_URL=postgresql+psycopg://microgrid:microgrid@db:5432/microgrid
REDIS_URL=redis://redis:6379/0
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=microgrid
S3_SECRET_KEY=microgrid-secret
S3_BUCKET=snlg-artifacts
API_TOKEN=dev-token-change-me
MAX_RUNS_CEILING=500
PRESIGN_TTL_SECONDS=900
DATA_DIR=/data
LOG_LEVEL=INFO
# --- web ---
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
# --- db (compose only) ---
POSTGRES_USER=microgrid
POSTGRES_PASSWORD=microgrid
POSTGRES_DB=microgrid
```

Settings classes (`pydantic-settings`) fail fast on missing required vars
(`api/tests/test_settings.py`).

---

## 3. Startup ordering

```
db, redis, minio  ── healthchecks ──▶ minio-init (create bucket) ──▶ api (migrate, serve) ──▶ worker ──▶ web
```

`api` and `worker` use `depends_on: { <dep>: { condition: service_healthy } }`.
`api` entrypoint:
```sh
alembic upgrade head
exec uvicorn microgrid_api.main:app --host 0.0.0.0 --port 8000
```

---

## 4. Dockerfiles (outline)

### `api/Dockerfile`
```dockerfile
FROM python:3.11-slim AS base
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY snlg/ /app/snlg/
COPY api/pyproject.toml api/uv.lock /app/api/
RUN cd /app/api && uv pip install --system -e . -e /app/snlg
COPY api/ /app/api/
WORKDIR /app/api
EXPOSE 8000
ENTRYPOINT ["./entrypoint.sh"]
```
`worker` reuses this image, overriding `command`.

### `web/Dockerfile` (multi-stage)
```dockerfile
FROM node:20-slim AS deps
WORKDIR /app
COPY web/package*.json ./
RUN npm ci
FROM node:20-slim AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY web/ ./
RUN npm run build
FROM node:20-slim AS run
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/.next ./.next
COPY --from=build /app/public ./public
COPY --from=build /app/package*.json ./
COPY --from=build /app/node_modules ./node_modules
EXPOSE 3000
CMD ["npm","start"]
```

---

## 5. Makefile targets (FR-D2)

| Target | Action |
|---|---|
| `make up` / `make down` | `docker compose -f deploy/docker-compose.yml up -d` / `down -v` |
| `make logs s=api` | tail one service |
| `make test` | engine + api + web suites (spins ephemeral db/redis/minio) |
| `make test-engine` | `pytest snlg/tests -m "not integration"` + integration on fixtures |
| `make test-api` | `pytest api/tests` (Postgres service + fakeredis + LocalStorage) |
| `make test-web` | `npm --prefix web test` |
| `make e2e` | `up` (mock runner) + `npm --prefix web run test:e2e` |
| `make db-upgrade` | `alembic upgrade head` (in `api` container) |
| `make db-revision m="msg"` | `alembic revision --autogenerate -m "msg"` |
| `make seed` | `python scripts/seed.py` (template config + bucket + smoke run) |
| `make smoke` | `up` + wait for `/ready` + one mock run to `succeeded` |
| `make fmt` / `make lint` | ruff+black / eslint+prettier across `snlg api web` |

---

## 6. CI (`.github/workflows/ci.yml`, FR-D4)

Jobs (fail-fast off so all report):

1. **lint** — `ruff check`, `black --check`, `eslint`, `tsc --noEmit`.
2. **engine** — `pytest snlg/tests` incl. `integration` on
   `tests/fixtures/data/`; coverage ≥ 90%.
3. **api** — services: `postgres:16`; `pytest api/tests` with `REDIS` via
   `fakeredis` and `Storage` = `LocalStorage`; `alembic upgrade head` +
   `downgrade base` check; coverage ≥ 85%.
4. **web** — `npm ci && npm test`; critical-component coverage ≥ 80%.
5. **compose-smoke** — `docker compose -f deploy/docker-compose.yml -f
   deploy/docker-compose.ci.yml up -d`; poll `/ready`; `scripts/seed.py`
   (mock runner) → assert run `succeeded`; run Playwright
   `run-lifecycle.spec.ts`; `docker compose logs` on failure.

`deploy/docker-compose.ci.yml` overrides: bind-mount
`tests/fixtures/data` as `/data`, set `MICROGRID_RUNNER_MODE=mock`, smaller
healthcheck intervals.

---

## 7. Input data provisioning — OEDI (FR-D3)

Real runs need ResStock/ComStock data in:

```
data/
  input/resstock/timeseries_individual_buildings/upgrade=<u>/state=<s>/<bldg_id>-0.parquet
  input/comstock/timeseries_individual_buildings/upgrade=<u>/state=<s>/<bldg_id>-0.parquet
  background/<resstock_chars>.xlsx      # sheet: building_characteristics
  background/<comstock_chars_agg>.xlsx  # sheet: building_characteristics
```

`scripts/download_oedi.py --manifest data/sources/oedi_manifest.yaml
[--dry-run]` (harvest the scaffold from `feat/oedi-downloader`):

```yaml
# data/sources/oedi_manifest.yaml
datasets:
  - name: resstock
    release: "2024.2/resstock_amy2018_release_2"
    upgrade: "0"
    state: "DC"
    transport: s3                       # aws s3 sync (no-sign-request) | https
    s3_uri: "s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/..."
    building_ids: [204, 296, 300, 483]  # or: {count: 50, seed: 1}
    target: "data/input/resstock/timeseries_individual_buildings/upgrade=0/state=DC"
  - name: comstock
    release: "2024.1/comstock_amy2018_release_1"
    upgrade: "0"
    state: "DC"
    transport: s3
    building_ids: [1905, 20785]
    target: "data/input/comstock/timeseries_individual_buildings/upgrade=0/state=DC"
characteristics:
  - source: resstock
    target: "data/background/DC_upgrade0.xlsx"
  - source: comstock
    target: "data/background/DC_upgrade0_agg.xlsx"
```

Behaviour: `--dry-run` prints the plan; existing files skipped
(idempotent); transport mocked in tests
(`scripts/tests/test_download_oedi.py`). `boto3` (already a dependency) or
`aws s3 sync --no-sign-request` for the S3 path.

A tiny **committed** subset lives at `tests/fixtures/data/` for CI /
offline dev (FR-E11) — the current `data/input/**` already holds ~140 DC
ComStock parquet files; trim/downsample a handful into fixtures.

---

## 8. Volumes & persistence

| Volume | Holds | Lifecycle |
|---|---|---|
| `pgdata` | Postgres | survives `down`, wiped by `down -v` |
| `miniodata` | artifacts | survives `down`, wiped by `down -v` |
| bind `./data` | large inputs | host-managed, gitignored (except fixtures) |

`redis` is intentionally non-persistent (queue + pub/sub only; a lost
queue on restart means re-enqueue, acceptable for v1).

---

## 9. Production notes (advisory, not v1 scope)

- Swap `minio` for real S3; `db` for RDS/Cloud SQL; `redis` for a managed
  Redis. Only env vars change.
- Put `web` + `api` behind a reverse proxy (TLS, real auth). Replace the
  static `API_TOKEN` with OIDC in `auth.py` (kept deliberately thin, D7).
- Run `worker` as its own autoscaling deployment; set memory limits per
  NFR-4 (start 2Gi/worker) and `--max-jobs` for recycling.
- Move `alembic upgrade` out of the `api` entrypoint into a one-shot
  migration job.
- Ship images from CI; pin digests. None of this blocks v1.
