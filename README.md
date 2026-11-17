# Community Energy Profiler (working title: Microgrid Planner)

Estimates the hourly and 15-minute electricity load of a community, with uncertainty, before any meters exist. It builds
many plausible neighborhoods by sampling real buildings from NREL **ResStock** (residential) and **ComStock**
(commercial), repeats until the average load converges, and reports the result as percentile envelopes (for example,
"the load was below this line in 90% of the simulated neighborhoods"). The first region is Washington, DC. The goal is a
web app for microgrid planning, RFP support and policy analysis.

## Status (live picture: `docs/handoff/session-handoff.md`)
- **Built and tested (last run 2026-11-17):** the load engine and library (944 tests pass), the API in `api/` (231
  tests pass) and the web app in `web/` (97 tests pass). `api/` and `web/` are real packages, not scaffolds.
- **Stack:** FastAPI + Celery + Redis + Postgres (PostGIS/Timescale image) on the back end, and a Vite / React / MUI /
  ECharts web app, run together with Docker Compose.
- **Not yet verified:** the web app currently runs against a mock API client, not the real API. The Docker images, a
  live background worker, and runs on real DC data have not yet been verified.
- **Goal:** a complete demo on the owner's machine in 30 days; a public live link is a stretch.

## Repo map
```
src/microgrid/   the library: core (LoadResult contract), geo, community_load (registry, categorize, sources,
                 synthetic = the Monte-Carlo engine), der / dispatch / economics (interfaces only), study.py
api/             FastAPI service (own tests and settings)
web/             Vite + React web app (own tests)
deploy/          Docker Compose files and helper scripts
tests/           library tests (fixtures only)
configs/scenarios/  example scenario files
notebooks/  docs/  scripts/
```
Dependencies in `src/microgrid/` point downward only (enforced by `tests/test_architecture.py`).

## Develop
From the repo root:
```bash
uv run --extra dev pytest -q                       # library tests
uv run --extra dev ruff check src tests            # lint
uv run --extra dev ruff format --check src tests   # format check
uv run --extra dev mypy                            # types
```
`make test` and `make lint` run the same commands. API tests: `uv run --project api --extra dev pytest api/tests -q`.
Web tests: `npm test` inside `web/` (runs `vitest run`).

Docker (see `docs/runbooks/local-stack.md` before first use; it checks free disk first):
`make up` starts the full stack except the background worker, `make down` stops it (data volumes are kept), and
`make lite-up` starts a smaller API-only stack.

The ResStock/ComStock data lives under `data/` and is git-ignored (it is large and stays on the owner's machine). Tests
use synthetic fixtures only.

## Documentation
Start with [`docs/README.md`](docs/README.md), which lists every document in reading order. The key ones:
- [`docs/PRD.md`](docs/PRD.md): the requirements (the contract).
- [`docs/architecture-and-stack.md`](docs/architecture-and-stack.md): technology and structure.
- [`docs/sprint-plan.md`](docs/sprint-plan.md): the 30-day plan.
- [`docs/handoff/session-handoff.md`](docs/handoff/session-handoff.md): where the project is right now.
- [`docs/archive/`](docs/archive/README.md): superseded v1 documents, history only.
