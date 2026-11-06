# Sprint 1 Backlog — Foundation and the thin slice (Days 1–7)

**Goal:** one DC composition runs from a notebook and from an API call through the worker and lands in the database,
on `docker compose up`, with the engine's known defects fixed.
Every story: write the failing test first (TDD), keep commits small, link FR IDs, follow the Definition of Done
(`../agent-team.md` §4). Cloud agents use fixtures only; stories marked 🖥 need the owner's real data and are verified on
the owner's machine.

## Order of work (riskiest and most-blocking first)
1. 1.1 Packaging and 1.2 CI (unblocks everyone)
2. 1.4 RNG, 1.3 strict config, 1.5 registry (engine hardening, parallel)
3. 1.6 Data layer and 1.11b dataset catalog (🖥 partly)
4. 1.9 facade and progress, 1.7 Compose, 1.8 API skeleton
5. 1.6b filters, 1.10 worker, 1.11 baseline timing, 1.12 spikes

## Stories

### 1.1 Packaging and dependency cleanup — FR-PLAT-3
- Fix `pyproject.toml`: remove runtime deps that are only for dev (`pytest`, `ipykernel`), the stdlib backport
  `dataclasses`, and `boto3`/`pvlib` unless used; align `requires-python` with `.python-version`; add a license
  placeholder note; add dev extras.
- **AC:** `pip install -e .` in a clean venv works; `python -c "import microgrid"` works; `pytest` runs from the repo root
  without `PYTHONPATH`; the two currently-xpassing packaging placeholders are converted to real tests.
- **Test first:** update `tests/test_packaging.py` expectations, then change config.

### 1.2 CI that blocks — FR-PLAT-5
- Make ruff/black/mypy failures fail the build (remove `|| true`); add coverage reporting; keep api/web jobs disabled
  until those folders have code, with a documented trigger.
- **AC:** a deliberate lint error fails the workflow in a throwaway branch; engine job runs on the fixtures.

### 1.3 Strict configuration — FR-ENG-3
- Unknown top-level or nested config keys raise an error listing them. Decide per ignored key (`arealimit`, `garage`,
  `heatpump`, `electric_vehicle`, `solar_pv`, `require_data_complete`): implement as an attribute filter (see 1.6b) or
  reject with a clear message. Normalize `scenario.name`/`description` list to string. Enum-validate `ensemble.mode`
  and `composition.method`.
- **AC:** tests for each key; the two shipped YAMLs either load cleanly or are fixed; a config with a typo fails loudly.
- **Owner approval:** none unless a key's meaning is a methodology choice (then ask).

### 1.4 RNG and replayability — FR-ENG-4
- Replace seed arithmetic with `SeedSequence.spawn`; fresh random seeds by default, always recorded; fixed seeds on request.
- **AC:** golden determinism test; 10,000 streams distinct; seed-robustness test on a converged fixture ensemble.
- **Note:** changing RNG changes numbers; record in CHANGELOG. This is an engine-internals change, not a methodology
  change, but log it.

### 1.5 Building-type registry — FR-ENG-1
- Types become data (registry entries with default filters and ResStock/ComStock mapping). Add single-family
  detached/attached, institutional, civic, warehouse (only if ComStock defines it). Rewire `categorize.py` to it. Split
  GIS `INDUSTRIAL` into warehouse (modeled) vs factory/manufacture (unmodeled).
- **AC:** adding a fake type needs no engine code change; GIS categorization tests extended; skipped/unmodeled
  structures still reported.

### 1.6 Data ingestion, fixtures, characteristics store — FR-DATA-1, DATA-2, DATA-7 🖥
- Scripted, resumable ingestion for the chosen datasets with a manifest (file, size, checksum), configurable data root,
  progress output, and a disk-use report. Build a small fixture set (≤5 buildings per type) for tests. Load characteristics
  tables into Postgres with a generated data dictionary.
- **AC:** clean-machine run reproduces the layout; fixtures let the engine suite run with no real data; the
  characteristics table is queryable.
- **Blocked on owner action A1–A3** for the real-data part; fixtures can proceed without it.

### 1.6b Attribute filters (Admin-only) — FR-ENG-11
- Filter language, attribute discovery, admin-configured exposed-attribute list with the starter set, pool preview and
  small-pool warning, permission flag `filters.use`.
- **AC:** as in PRD FR-ENG-11 (a)–(f), tested on fixtures.

### 1.7 Docker Compose — FR-PLAT-4, DATA-4
- Services: `db` (PostgreSQL + PostGIS + TimescaleDB; confirm an image bundling PostGIS and Timescale), `redis`, `api`,
  `worker`, `web` placeholder. One command up; named volumes; image size and disk budget reported (30 GB limit).
- **AC:** `docker compose up` brings services healthy; `/ready` green; disk use documented.

### 1.8 API skeleton and migrations — FR-PLAT-1
- FastAPI `/api/v1`, health/ready, SQLAlchemy models, first Alembic migrations (upgrade and downgrade tested), SQLite or
  ephemeral Postgres for fast local tests.
- **AC:** migrations round-trip on an empty DB; OpenAPI served; contract test for health.

### 1.9 Engine facade and progress events — FR-ENG-8
- `run_scenario(config, progress=callback)` returns a typed result; progress events carry stage, run index, feasible
  count, convergence status, message. Notebook example calls it.
- **AC:** test collects events from a fixture run; notebook example executes in CI on fixtures.

### 1.10 Worker task — FR-PLAT-2
- Celery task calls the facade, writes progress rows and summary metrics; cancel supported.
- **AC:** integration test with fakeredis/ephemeral services on fixtures; cancel leaves no partial results marked complete.

### 1.11 Performance baseline 🖥
- Measure run time and memory versus buildings × variations on the owner's machine; record disk and image sizes.
- **AC:** a table in `docs/` used to set caps for the Demo role and to size hosting.

### 1.11b Dataset catalog — FR-DATA-8
- Catalog table, data dictionary per release, internal scenario key plus on-disk partition id; confirm what `upgrade=0`
  and the other ids mean in the chosen releases; engine takes dataset ids as inputs; gross-vs-net flag recorded.
- **AC:** catalog describes two scenarios for the same state in fixtures; pools built per dataset id.

### 1.12 Spikes
- ECharts with a 35,040-point series (hover with two series, zoom); footprint source comparison (coverage, attributes,
  license) → recommendation for owner approval; (hosting costed recommendation can wait for Day 14).
- **AC:** short written findings in `docs/` with a recommendation and evidence.

## Out of scope this sprint
Web UI beyond placeholder, charts, calibration, auth, map, DER, weather normalization (interface only later).

## Questions that may come up (agent should ask the owner)
Any change to numbers produced by the methodology · any new service that costs money · deleting or overwriting data ·
publishing anything.
