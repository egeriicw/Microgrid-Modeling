# Microgrid Planner

Planning software for community microgrids. Its first component, the Community
Load Profiler, builds a neighborhood from GIS footprints and produces
uncertainty-aware hourly load using SNLG (Synthetic Neighborhood Load
Generation): Monte-Carlo profiles built from NREL ResStock / ComStock stock.

## Structure

```
microgrid                     Microgrid Planner (this repository)
|-- core                      shared contracts: Structure, Neighborhood, LoadResult
|-- community_load            Community Load Profiler: what load does a community have?
|   |-- geo                   GIS footprints -> classified Neighborhood
|   |-- synthetic             SNLG engine: Monte-Carlo ResStock/ComStock ensembles
|   `-- sources               LoadSource adapters (synthetic, measured)
|-- der / dispatch / economics   interfaces; implementations on the roadmap
`-- study                     pipeline chaining the layers
```

Dependencies point downward only (`tests/test_architecture.py`). The engine's
`snlg` name survives as prose ("SNLG") and in the `snlg-ensemble` source label.

## Status

- **`src/microgrid/community_load/synthetic/`** — the load-generation engine (methodology-driven, typed,
  **50 passing tests**). Runs today via
  `notebooks/community_microgrid_load_profiles.ipynb`.
- **Platform (planned)** — REST API + async workers + Next.js UI +
  Postgres/MinIO + Docker Compose. Specified in [`docs/`](./docs/README.md).

## Documentation

- [`docs/prd-suite-4d.md`](./docs/prd-suite-4d.md) — **suite PRD v2** (4D template structure) and [`docs/prd-gap-analysis.md`](./docs/prd-gap-analysis.md) — gaps in `PRD.md`.

Everything starts at **[`docs/README.md`](./docs/README.md)**:

- [`docs/PRD.md`](./docs/PRD.md) — product requirements, the 4 locked design
  decisions, scope, success criteria.
- [`docs/engine-reference.md`](./docs/engine-reference.md) — reference for
  the existing SNLG engine + known gaps.
- [`docs/architecture.md`](./docs/architecture.md),
  [`docs/requirements.md`](./docs/requirements.md),
  [`docs/api-spec.md`](./docs/api-spec.md),
  [`docs/data-model.md`](./docs/data-model.md),
  [`docs/frontend-spec.md`](./docs/frontend-spec.md),
  [`docs/infrastructure.md`](./docs/infrastructure.md).
- [`docs/test-plan.md`](./docs/test-plan.md) — **binding** TDD methodology +
  full test-file index.
- [`docs/implementation-plan.md`](./docs/implementation-plan.md) — phased
  build plan.

## Run the engine tests today

```bash
PYTHONPATH=src uv run pytest -q          # 50 pass (+ RED placeholders skip/xfail)
```

`pytest` now runs from the repo root with no `PYTHONPATH`; packaging for release is Phase 1.1 in
`docs/implementation-plan.md` (Phase 1.1).

## Repo map

| Path | What |
|---|---|
| `src/microgrid/community_load/synthetic/` | the engine (see `docs/engine-reference.md`) |
| `src/microgrid/` | the planner: `core`, `community_load/{geo,sources}`, `study`; `der`/`dispatch`/`economics` are interfaces (see `docs/prd-suite-4d.md`) |
| `tests/` | engine tests + new RED placeholders (`test_packaging.py`, `test_run_facade.py`, `test_determinism.py`) |
| `configs/scenarios/` | scenario YAMLs (also import/parity fixtures) |
| `notebooks/` | the orchestration notebook |
| `api/` | **(scaffold)** FastAPI service + RQ worker + test skeletons |
| `web/` | **(scaffold)** Next.js app + test skeletons |
| `deploy/` | `docker-compose.yml` (+ CI override), `.env.example`, smoke test |
| `scripts/` | OEDI downloader + seed (planned) + test skeletons |
| `docs/` | PRD + all specs |
| `Makefile` | `up`, `test`, `db-upgrade`, `seed`, ... |
