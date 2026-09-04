# Implementation Plan — SNLG Platform

Phased, test-first. Each phase ends green in CI. Within a phase, work in
**tiny commits**: one behaviour, test + code together, message
`"<area>: <change> (FR-Exx)"`, co-authored trailer as configured.

Legend: ☐ task · `→ test` primary test file · **[harvest]** = copy/adapt
from a `feat/*` branch (allowlist in §7).

---

## Phase 0 — Groundwork (no behaviour change)

- ☐ Add `docs/` (this set) to the repo; link from a top-level `README.md`.
- ☐ Create the skeleton test tree with RED placeholders (already scaffolded
  in this branch): `snlg/tests/`, `api/tests/`, `web/src/**`, `web/e2e/`,
  `deploy/`, `scripts/tests/`.
- ☐ `deploy/docker-compose.yml` + `deploy/.env.example` + `Makefile`
  committed (services may not build yet). **[harvest]** compose from
  `feat/run-launcher`.
- ☐ CI workflow file with all jobs present but `continue-on-error` on the
  not-yet-real ones; flip off per phase.
- **Exit:** `make lint` green; existing 50 engine tests still pass (now via
  pytest config, see Phase 1.1).

---

## Phase 1 — Engine: package & harden (`snlg`)  — PRD G1, G2; FR-E1..E11

### 1.1 Make it installable (FR-E1, FR-E2) — Gap G-13
- ☐ Add `snlg/pyproject.toml` (`name="snlg"`, `version="0.1.0"`,
  `[tool.setuptools.packages.find] where=["src"]`), `snlg/CHANGELOG.md`.
  Decide layout: keep `src/snlg/` in place **or** move to `snlg/src/snlg/`
  (recommended). Update notebook `sys.path` hack → `pip install -e ./snlg`.
- ☐ Root `pyproject.toml` `[tool.pytest.ini_options] pythonpath = ["src"]`
  (or `snlg/src`) so `pytest` needs no `PYTHONPATH`.  `→ test_packaging.py`
- ☐ `snlg/__init__.py`: add `__version__`, finalize `__all__`
  (add `run`, `perturbation`, `feasibility`, `validation`, `_types` as
  intended surface).  `→ test_packaging.py::test_public_surface`
- **Exit:** `pip install -e ./snlg && pytest` green from repo root, A1.

### 1.2 RNG hardening (FR-E5) — Gap G-8
- ☐ `rng.derive_run_rng` → `SeedSequence(base_seed).spawn`.  `→ test_rng.py`
  (extend: 10k-index distinctness; keep the 5 existing tests green).

### 1.3 Config parser completeness (FR-E6) — Gaps G-1,G-2,G-3,G-5,G-6
- ☐ Normalize `scenario.name`/`description` list→str.  `→ test_config_scenarios.py`
- ☐ Type or reject `arealimit`(→`max_sqft`), `garage`(→`require_no_garage`),
  `heatpump`/`electric_vehicle`/`solar_pv` (new adoption-probability fields
  on `ResidentialFilterConfig`, applied in `archetype._filter_resstock_category`
  as a probabilistic gate seeded by L2 — or reject if not implementing).
- ☐ Enum-guard `ensemble.mode`, `composition.method`.
- ☐ Unknown top-level section / unknown key → `ValueError` with the list.
- ☐ Apply `require_data_complete` in the resstock filter (drop rows with
  NA in required columns).  `→ test_archetype.py`

### 1.4 Result enrichment (FR-E7) — Gaps G-7,G-10
- ☐ Add `coincidence_factor`, `diversity_factor` to `NeighborhoodResult`
  (defaults for back-compat); populate in `ensemble._run_single` via
  `aggregation.compute_coincidence_factor`.  `→ test_aggregation.py`, `test_ensemble.py`
- ☐ Remove dead `NeighborhoodResult` import in `feasibility.py`.
- ☐ Update `test_validation.py` positional `NeighborhoodResult(...)`
  constructors for the new fields.

### 1.5 `run_scenario` façade + progress (FR-E3, FR-E8) — Gaps G-4,G-9,G-11
- ☐ `snlg/run.py`: `RunProgress` (frozen dc), `ArtifactSink` protocol,
  `RunResult`, `run_scenario(config, *, data_dirs, progress=None,
  artifacts=None, should_cancel=None)`.
  - accepts `dict | ScenarioConfig`; validates internally.
  - `data_dirs` explicit — no relative-path assumptions (G-4).
  - decide end-use perturbation wiring (G-9): thread `df_15min` from
    `loader`→`ensemble`→`perturbation`, **or** document as CLI/notebook-only
    and assert temporal-shift-only in the ensemble path.
  - artifact sink invoked **incrementally** (NFR-4); disk fallback ==
    `outputs.save_all_outputs` when `artifacts is None`.
  - `should_cancel` checked between Monte Carlo runs; sets
    `RunResult.cancelled`.
  `→ test_run_facade.py`
- ☐ `ensemble.run_ensemble(..., progress=None)` param, no-op when omitted.
- ☐ Replace `str(time.time())` timestamp with
  `datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")` (G-11).  `→ test_outputs.py`
- ☐ Bound the parquet `lru_cache` (`maxsize` from env/param) (G-12).  `→ test_loader.py`

### 1.6 Determinism contract (FR-E4)
- ☐ `tests/fixtures/data/` — trim ~5 ResStock + 2 ComStock parquet from the
  existing `data/input/**`; build minimal `chars_*.xlsx`.
- ☐ `snlg/tests/golden/dc_multifamily_tiny.json` + `--update-golden` flag
  in `conftest.py`.
- ☐ `test_determinism.py`: exact reproduction, seed-sensitivity,
  version-guard.  `→ test_determinism.py`

### 1.7 CLI + integration + docs coverage (FR-E9, E11, E10)
- ☐ `snlg/cli.py` + `python -m snlg`.  `→ test_cli.py`
- ☐ `test_integration_tiny.py` (`@pytest.mark.integration`).
- ☐ `test_docs_coverage.py` vs `docs/engine-reference.md`.
- ☐ Fill any remaining §7-A2 coverage gaps in `docs/engine-reference.md`.

**Phase 1 exit:** engine coverage ≥ 90%; A1, A2, A5 pass; `CHANGELOG.md`
0.1.0 entry; CI `engine` job required.

---

## Phase 2 — API foundation (`api`)  — FR-A1..A5, A14..A17; FR-D6

- ☐ `api/pyproject.toml` (fastapi, uvicorn, sqlalchemy2, alembic,
  psycopg, pydantic-settings, rq, redis, boto3, pytest, httpx, fakeredis).
  Depends on `snlg` (path dep).  **[harvest]** structure from
  `feat/config-crud`.
- ☐ `settings.py` (FR-D6).  `→ test_settings.py`
- ☐ `db.py`, `models.py` (Config, ConfigVersion, Run, RunMetric, Artifact —
  `data-model.md` §2), Alembic `0001_init`, `0002_runs`.
  `→ test_migrations.py`  **[harvest]** alembic wiring from `feat/config-crud`.
- ☐ `storage.py`: `Storage` protocol + `S3Storage` + `LocalStorage`.
  `→ test_storage.py`
- ☐ `schemas/scenario.py`: Pydantic mirror of `ScenarioConfig` + `validate()`
  delegating to `snlg.config`.  `→ test_scenario_validation_api.py`, `test_schema_parity.py`
- ☐ `auth.py` (`require_token`).  `→ test_auth.py`
- ☐ `main.py` app factory + lifespan (redis/minio/engine-version); CORS.
- ☐ `routers/health.py`.  `→ test_health.py`
- ☐ `routers/configs.py`: CRUD + `/validate` + `/clone` + versions +
  import/export.  `→ test_configs_api.py`
- ☐ `scripts/seed.py` (template from `dc_multifamily_baseline.yaml`).
  `→ test_seed.py` (run-launch part deferred to Phase 3)
- **Exit:** `api` job green; `docker compose up db redis minio api` serves
  `/docs`; migrations up/down clean (A8).

---

## Phase 3 — Runs, worker, results  — FR-A6..A13; NFR-1,3,8

- ☐ `queue.py` (RQ `Queue("runs")`, `enqueue_run`).
- ☐ `routers/runs.py`: `POST /runs` (freeze snapshot, seeds,
  engine_version, enqueue), list, detail, cancel.  `→ test_runs_api.py`
- ☐ `progress.py`: Redis pub/sub publish/subscribe; `routers/runs.py`
  `GET /runs/{id}/events` SSE relay.  `→ test_runs_api.py::test_sse_*`
- ☐ `worker/__main__.py`, `worker/tasks.py`, `worker/runner.py`,
  `worker/artifacts.py`, `worker/cancellation.py`:
  - `run_ensemble_task(run_id)` → `snlg.run.run_scenario` with progress
    callback (insert `run_metrics` + publish), `ArtifactSink` → MinIO +
    `artifacts` rows, `should_cancel` → Redis flag.
  - `MICROGRID_RUNNER_MODE=mock` fast path.  **[harvest]** mock-mode +
    subprocess-streaming ideas from `feat/run-launcher/run_worker.py`.
  `→ test_worker.py`
- ☐ `worker/artifacts.py`: serialize `EnsembleResult` → legacy CSVs +
  `series/*.parquet` + figures; upload incrementally; keep legacy names
  (`data-model.md` §3, NFR-9).
- ☐ `routers/results.py`: `/metrics`, `/summary`, `/series/envelope`,
  `/series/load-duration`, `/artifacts` (presigned), `DELETE /runs/{id}`
  (purge).  `→ test_results_api.py`
- ☐ `test_reproduce.py` (NFR-1); wire `test_seed.py` full path (D5).
- ☐ `test_openapi_contract.py`, `test_logging.py`, `test_cors.py`.
- **Exit:** A4 (fixture ensemble via API < 3 min), A5, A7 pass;
  `api` coverage ≥ 85%; worker scales (`--scale worker=3`) in a manual check.

---

## Phase 4 — Frontend (`web`)  — FR-F1..F9

- ☐ **[harvest]** the whole Next.js foundation from
  `feat/frontend-foundation-nextjs` (`web/` package.json, tsconfig, jest,
  `_app`/`_document`, `AppShell`, theme) and the config pages from
  `feat/config-editor-ui` (`configs/index|new|[id]`).
- ☐ `lib/apiClient.ts` + `lib/sse.ts` + `lib/scenarioSchema.ts` + hooks.
  `→ apiClient.test.ts`, `sse.test.ts`
- ☐ `AppShell` + `HealthDot` + `ApiTokenDialog`.  `→ AppShell.test.tsx`, `HealthDot.test.tsx`
- ☐ Config list + `new` + editor (Form ⇄ YAML, debounced validate).
  `→ configs/index.test.tsx`, `configs/editor.test.tsx`, `ValidationErrors.test.tsx`
- ☐ `LaunchRunDialog`.  `→ LaunchRunDialog.test.tsx`
- ☐ `RunsTable` + `RunStatusChip` (live poll).  `→ RunsTable.test.tsx`
- ☐ `runs/[id]` Progress tab: `RunProgress` (SSE) + `LogPane` + cancel.
  `→ RunProgress.test.tsx`, `runs/detail.test.tsx`
- **Exit:** `web` job green; UJ-1 up to "run launched" works manually.

---

## Phase 5 — Results dashboard  — FR-F7, F8, F10

- ☐ `results/` components: `EnvelopeChart`, `PeakHistogram`,
  `EnergyHistogram`, `LoadDurationChart`, `ConvergenceChart`,
  `RepresentativeCases`, `ValidationPanel`, `ArtifactList`. Load `dataviz`
  skill before writing chart code.  `→ results.test.tsx` + per-chart tests
- ☐ `runs/[id]` Results tab wiring; empty states.
- ☐ `compare.tsx` (P2).  `→ compare.test.tsx`
- ☐ `web/e2e/run-lifecycle.spec.ts` (Playwright, mock runner).  `→` FR-F10
- **Exit:** A3, A4 (full UJ-1 e2e in CI < 3 min) pass.

---

## Phase 6 — Orchestration, data pipeline, hardening  — FR-D1..D5; NFR-*

- ☐ Finalize `deploy/docker-compose.yml` + `docker-compose.ci.yml`;
  Dockerfiles for `api` and `web`; `minio-init.sh`.  `→ deploy/tests/test_compose_smoke.sh`
- ☐ `Makefile` all targets working (FR-D2).
- ☐ `scripts/download_oedi.py` + `data/sources/oedi_manifest.yaml`.
  **[harvest]** from `feat/oedi-downloader`.  `→ scripts/tests/test_download_oedi.py`
- ☐ CI: remove all `continue-on-error`; enforce coverage gates; add
  `compose-smoke` + `e2e` jobs (FR-D4).
- ☐ NFR sweep: structured logging (NFR-5), CORS lockdown (NFR-7),
  memory check (NFR-4), delete-purge transactionality (NFR-8),
  macOS-arm64 + linux-amd64 smoke (NFR-6).
- ☐ Owner walkthrough of UJ-1 against **real** data (post-OEDI fetch).
- **Exit:** all of PRD §7 A1–A8 green in CI; §10 Definition of Done met.

---

## 7. `feat/*` harvest allowlist

Copy/adapt **only** these paths; do **not** import the `microgrid_profiles`
engine (D1 — `snlg` is the engine).

| From branch | Paths permitted |
|---|---|
| `feat/backend-foundation` / `feat/config-crud` | `api/alembic*`, `api/microgrid_api/{db,settings,models,schemas}.py`, `api/pyproject.toml`, `scripts/{setup_db,reset_db,run_api,setup_dev}.sh`, `docker-compose.yml` |
| `feat/run-launcher` | `api/microgrid_api/{run_worker,run_schemas}.py` (mock-mode + progress-streaming patterns), `api/tests/test_runs.py` (as a starting point) |
| `feat/frontend-foundation-nextjs` | all of `web/` (package.json, jest, tsconfig, `_app`, `_document`, `AppShell`, `theme`, `JobsWidget` → becomes `RunsTable`) |
| `feat/config-editor-ui` | `web/src/pages/configs/{index,new,[id]}.tsx` |
| `feat/oedi-downloader` | `scripts/download_oedi.py`, `scripts/test_download_oedi.py`, `data/sources/oedi_manifest.*` |
| `feat/load-duration-curves` | `viz.py` load-duration logic → port into `worker/artifacts.py` + `LoadDurationChart` |
| `docs/improve-python-docstrings` | docstring wording only, **not** the `microgrid_profiles` code it documents |

Everything referencing `microgrid_profiles` (`src/microgrid_profiles/**`,
`scripts/run_pipeline.py`, `config/config.yaml`) is **reference only** —
re-implement against `snlg` if a capability is needed.

---

## 8. Sequencing & parallelism

```
Phase 0
  └─ Phase 1 (engine)  ───────────────┐
        └─ Phase 2 (api foundation)   │  (Phase 4 web foundation can start
              └─ Phase 3 (runs+worker)│   in parallel after Phase 0 using
                    └─ Phase 5 (dash) │   mock API responses; wire to real
Phase 6 (orchestration) ──────────────┘   API at end of Phase 3)
```

Critical path: 1 → 2 → 3 → 5 → 6. Phase 4 foundation is parallelizable;
Phase 4 config editor needs Phase 2 `/configs*`; Phase 5 needs Phase 3
results endpoints.

## 9. Definition of Done per phase

A phase is done when: every ☐ has a green test named in `requirements.md`
traceability; the phase's CI job is **required** (no `continue-on-error`);
coverage gate for touched suites holds; `docs/` updated for any contract
change; `CHANGELOG.md` (engine) / PR description (platform) explains
behaviour changes.
