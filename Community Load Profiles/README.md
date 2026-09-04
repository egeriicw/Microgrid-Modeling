# Community Microgrid Load Profiles — SNLG

Synthetic Neighborhood Load Generation: Monte-Carlo hourly electricity load
profiles for hypothetical neighborhoods, built from NREL ResStock / ComStock
building stock.

## Status

- **`src/snlg/`** — the load-generation engine (methodology-driven, typed,
  **50 passing tests**). Runs today via
  `notebooks/community_microgrid_load_profiles.ipynb`.
- **Platform (planned)** — REST API + async workers + Next.js UI +
  Postgres/MinIO + Docker Compose. Specified in [`docs/`](./docs/README.md).

## Documentation

Everything starts at **[`docs/README.md`](./docs/README.md)**:

- [`docs/PRD.md`](./docs/PRD.md) — product requirements, the 4 locked design
  decisions, scope, success criteria.
- [`docs/engine-reference.md`](./docs/engine-reference.md) — reference for
  the existing `snlg` engine + known gaps.
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

Making `snlg` importable without `PYTHONPATH` is the first task in
`docs/implementation-plan.md` (Phase 1.1).

## Repo map

| Path | What |
|---|---|
| `src/snlg/` | the engine (see `docs/engine-reference.md`) |
| `tests/` | engine tests + new RED placeholders (`test_packaging.py`, `test_run_facade.py`, `test_determinism.py`) |
| `configs/scenarios/` | scenario YAMLs (also import/parity fixtures) |
| `notebooks/` | the orchestration notebook |
| `api/` | **(scaffold)** FastAPI service + RQ worker + test skeletons |
| `web/` | **(scaffold)** Next.js app + test skeletons |
| `deploy/` | `docker-compose.yml` (+ CI override), `.env.example`, smoke test |
| `scripts/` | OEDI downloader + seed (planned) + test skeletons |
| `docs/` | PRD + all specs |
| `Makefile` | `up`, `test`, `db-upgrade`, `seed`, ... |
