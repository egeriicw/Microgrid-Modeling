# Architecture and Technology Stack (proposal for owner approval)

Companion to [`PRD.md`](./PRD.md). Choices marked **REQUIRED** come from the owner. Choices marked **RECOMMENDED** are
proposals; each becomes a decision record (ADR) in `docs/adr/` when approved. Items marked **[verify]** must be confirmed
by an agent against current documentation before use.

## 1. Guiding principles
1. **Notebook-first, library-centered.** All science lives in an installable Python library; notebooks, API, and worker
   are thin callers of it.
2. **Modular boundaries with enforced dependency direction** (already tested in `tests/test_architecture.py`).
3. **Boring, widely-used technology** where possible; few moving parts for a one-person + agents team.
4. **Local-first**: everything runs on the owner's laptop with Docker Compose; cloud later.
5. **Data stays where it belongs**: raw Parquet on disk, derived series in TimescaleDB, geometry in PostGIS.

## 2. System context

```
 Browser (React SPA)
      │ HTTPS / JSON (REST /api/v1) + Server-Sent Events (progress)
      ▼
 FastAPI service ───► PostgreSQL + PostGIS + TimescaleDB   (configs, runs, users, results, geometry, calibration)
      │  enqueue                    ▲
      ▼                             │ write results/progress
 Redis (broker) ──► Celery worker ──┴──► microgrid library (engine) ──► data/input Parquet (disk)
                                                   ▲
                                      IPython notebooks import the same library
```

## 3. Repository layout (target)

```
src/microgrid/            library: core, geo, community_load/{synthetic,sources,categorize}, calibration, registry,
                          meters, (der, dispatch, economics, weather = interfaces/placeholders)
api/                      FastAPI app: routers/ (v1), services/, models/, schemas/, auth/, worker/, alembic/
web/                      React + TypeScript app: features/ (composition, run, results, calibration, map, admin)
notebooks/                maintained, tested example notebooks (import the library; no logic of their own)
configs/                  scenario & composition YAML; demo compositions
data/                     input/ background/ output/ (git-ignored contents; manifest tracked)
deploy/                   docker-compose.yml, Dockerfiles, env templates
docs/                     PRD, architecture, ADRs, sprint plan, agent team, user guide
tests/                    engine tests; api/tests; web tests; e2e/
```

## 4. Stack

| Layer | Choice | Status | Rationale / notes |
|---|---|---|---|
| Language (back end) | Python 3.12+ (project currently pins 3.13) | REQUIRED | Match ResStock/ComStock/REopt ecosystem. Fix the `requires-python` mismatch. |
| API | **FastAPI** + Pydantic v2, `/api/v1` | REQUIRED | Auto OpenAPI; versioned routers. |
| ORM / migrations | **SQLAlchemy 2 + Alembic** | REQUIRED | Owner asked to keep; also the local-testing path (SQLite/ephemeral Postgres for fast tests; Postgres for integration). |
| Database | **PostgreSQL + PostGIS + TimescaleDB** in Docker | REQUIRED | A Timescale image that bundles PostGIS exists **[verify]**; otherwise build one image with both extensions. |
| Queue / worker | **Celery + Redis** | APPROVED by owner 2026-11-06 | Mature, supports retries, cancellation (revoke), and progress via task state plus DB rows. Alternative considered: RQ (simpler, weaker cancel/progress), Dramatiq/Arq. Requires owner OK as a decision record. |
| Progress to browser | Server-Sent Events from FastAPI reading run-progress rows | RECOMMENDED | One-way, simple; polling fallback. |
| Analytics | pandas, numpy, scipy, scikit-learn, pyarrow | REQUIRED | Already used; add statsmodels if change-point regression is built later. |
| Notebooks | JupyterLab + ipykernel; `pip install -e .` | REQUIRED | Notebook-first working style. |
| Front end | **React + TypeScript + Vite** | REQUIRED (React) / RECOMMENDED (rest) | Vite for speed; SPA is enough (no SSR needed). The old plan's Next.js is dropped unless needed for SEO of a public landing page. |
| UI components | **MUI** (agent default, owner unsure, easily reversed; spike dropped) | DEFAULT chosen 2026-11-06 | MUI: fastest for forms/tables/admin, one consistent kit. shadcn/Tailwind: more design control. Agent proposes, owner picks. |
| Server state | TanStack Query | RECOMMENDED | Caching, polling, SSE integration. |
| Charts | **Apache ECharts** | RECOMMENDED | Handles 35k-point series, zoom, linked tooltips (load + temperature), large-data sampling. Alternatives: Plotly (heavier), Recharts (weak at this scale). |
| Map | **MapLibre GL JS** (+ vector tiles via PostGIS) | RECOMMENDED | Open source, no vendor key required; basemap tiles source to confirm **[verify]** terms. |
| Auth | FastAPI JWT (access + refresh), argon2/bcrypt hashing, role claims | RECOMMENDED | Three fixed roles in MVP; a library such as fastapi-users is an option **[verify]**. |
| Packaging | uv for Python; npm/pnpm for web | RECOMMENDED | Repo already uses uv. |
| Containers | Docker Compose: `db`, `redis`, `api`, `worker`, `web` (+ optional `jupyter`) | REQUIRED | Watch disk: pin slim base images; prune. |
| CI | GitHub Actions: lint, mypy, tests, coverage, build images, e2e smoke | RECOMMENDED | Existing workflow has most jobs disabled / non-blocking; fix. |
| Observability | structlog JSON logs, `/health` `/ready`; Sentry optional | RECOMMENDED | |
| Hosting (demo) | **Local first.** Public hosting later within a **$100/month total ceiling**; agent produces a costed recommendation | OPEN, deferred by owner | No spending without approval. |
| Object storage | Local filesystem for MVP; S3-compatible interface | RECOMMENDED | The old plan's MinIO is dropped for MVP to save moving parts and disk. |
| SEED integration | Adapter interface only | REQUIRED placeholder | Real coupling later **[verify]** SEED API. |
| Energy tools | REopt, SAM/PySAM, dGen, PyPSA/pandapower | LATER | Adapters behind interfaces. |

## 5. Key design decisions to record as ADRs

| ADR | Decision | Notes |
|---|---|---|
| 001 | Library-centered architecture with a single `run_scenario` facade | Replaces old "snlg" naming; package is `microgrid`. |
| 002 | Building-type registry as data | Enables FR-ENG-1. |
| 003 | Meter hierarchy placeholder | Building→class→total now; room for submeters and VPP. |
| 004 | Timescale hypertables for interval results; Parquet for raw inputs | Protects disk and query speed. |
| 005 | Celery + Redis for jobs | See §4. |
| 006 | Convergence criteria | **Owner approval required** (methodology). |
| 007 | UI kit and chart library | Sprint 1 spike. |
| 008 | Hosting and demo isolation | Sprint 2. |
| 009 | Versioning scheme (engine semver, API `/v1`, web semver, Conventional Commits) | |
| 010 | Footprint data source and licensing | Owner approval. |

## 6. Data model (summary)

- `users(id, email, password_hash, role, created_at)` — roles: admin, client, demo.
- `building_types(key, label, source, filters_json, active)` — the registry.
- `compositions(id, owner_id, name, body_json, version, is_demo)`.
- `runs(id, composition_snapshot_json, seeds_json, engine_version, data_manifest_hash, status, progress_json,
  created_at, started_at, finished_at)`; `run_events(run_id, ts, stage, message, payload_json)`.
- `run_metrics(run_id, scope, metric, value)` — scalar results (peak, energy, load factor, diversity).
- `meters(id, run_id, parent_meter_id, kind, building_ref, class)` — placeholder hierarchy.
- `load_series(run_id, meter_id, ts, resolution, kw_mean, kw_p10, kw_p50, kw_p90)` — **hypertable**.
- `weather_series(run_id, ts, temp_c, …)` — hypertable.
- `calibration_uploads(id, owner_id, name, meta_json)`, `calibration_series(upload_id, ts, kw)` — hypertable;
  `calibration_results(run_id, upload_id, metrics_json)`.
- `footprints(id, geom, use_class, stories, units, year_built, source)` — PostGIS; LATER: `networks`, `assets`,
  `policy_constraints`.

## 7. API surface (v1, summary)

`POST /auth/login` · `GET/POST /compositions`, `POST /compositions/validate` · `GET /building-types` ·
`POST /runs`, `GET /runs`, `GET /runs/{id}`, `GET /runs/{id}/events` (SSE), `POST /runs/{id}/cancel` ·
`GET /runs/{id}/results/{summary|series|duration-curve|seasonal|composition}` · `GET /runs/{id}/download.csv` ·
`POST /calibration/uploads`, `POST /runs/{id}/calibration` · `GET /footprints` (SHOULD) · `GET /health`, `/ready`.
Every endpoint checks role; Demo is limited to read endpoints on predefined runs.

## 8. Versioning and change management
- Engine: SemVer with CHANGELOG; breaking changes need an ADR.
- API: URL-versioned (`/v1`); additive changes only inside a version; contract tests from the OpenAPI schema.
- Web: SemVer; feature folders; shared typed API client generated from OpenAPI.
- Git: trunk-based with short-lived branches, Conventional Commits, PR template, required CI, tagged releases (`v0.1.0` = MVP).
- Reproducibility record: each run stores engine version + data manifest hash + config snapshot.

## 9. What changes from the old plan
Dropped for MVP: Next.js Pages Router, MinIO, TimescaleDB-as-optional (now required), RQ (replaced by recommended
Celery), the `snlg` package name. Kept: layered library, determinism contract, test-first discipline, Docker Compose,
Alembic. The old specs in `docs/` (api-spec, data-model, frontend-spec, infrastructure, implementation-plan, test-plan,
requirements) are **superseded by this document and the PRD** where they disagree; agents should update or retire them
in Sprint 1 rather than follow them blindly.
