# Requirements — SNLG Platform

Companion to `PRD.md` / `architecture.md`. Each requirement has a stable ID,
a statement, acceptance criteria, and the verifying test file(s) from
`test-plan.md`. Priority: **P0** = v1 blocker, **P1** = v1 expected,
**P2** = v1 if time / v1.1.

---

## A. Engine requirements (FR-E)

### FR-E1 — Installable package (P0)
`snlg` installs as a distribution and imports with no `sys.path` / `PYTHONPATH`
manipulation.
- **AC1** `pip install -e ./snlg` (or repo root) succeeds on py3.10–3.12.
- **AC2** From a clean shell at repo root, `python -c "import snlg; print(snlg.__version__)"` prints a semver string.
- **AC3** `pytest` at repo root collects and passes the engine suite with no `PYTHONPATH` set (pytest config supplies `pythonpath`/package).
- **Tests:** `snlg/tests/test_packaging.py`

### FR-E2 — Public API surface is explicit and stable (P0)
- **AC1** `snlg.__all__` enumerates the supported modules/callables; a test pins it.
- **AC2** `snlg.run.run_scenario`, `snlg.config.load_config`,
  `snlg.__version__` are importable top-level.
- **AC3** Breaking a name in `__all__` fails a test.
- **Tests:** `snlg/tests/test_packaging.py::test_public_surface`

### FR-E3 — `run_scenario` façade (P0)
A single function the platform calls:
```python
run_scenario(
    config: dict | ScenarioConfig,
    *,
    data_dirs: Mapping[str, Path],
    progress: Callable[[RunProgress], None] | None = None,
    artifacts: ArtifactSink | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> RunResult
```
- **AC1** Given a valid config dict + fixture `data_dirs`, returns a
  `RunResult` exposing `ensemble` (`EnsembleResult`), `summary` (dict),
  `metrics` (list of per-run scalar dicts), `engine_version`, `seeds`.
- **AC2** `progress` is called at least once per attempted Monte Carlo run
  and once at completion, each with a `RunProgress` (fields: `stage`,
  `run_index`, `n_attempted`, `n_feasible`, `converged`, `message`).
- **AC3** If `should_cancel()` returns `True`, the loop stops before the next
  run; `RunResult.cancelled is True`; no exception raised.
- **AC4** `artifacts.put(name, kind, obj)` (or a generator handoff) is
  invoked for each artifact instead of writing to disk when an
  `ArtifactSink` is supplied; when it is `None`, behaviour matches the
  current `outputs.save_all_outputs` on disk.
- **AC5** Accepts a plain `dict` (validates internally) or a pre-built
  `ScenarioConfig`.
- **Tests:** `snlg/tests/test_run_facade.py`

### FR-E4 — Determinism contract (P0)
Same config + same three seeds + same `engine_version` ⇒ identical scalar
metrics.
- **AC1** Two `run_scenario` calls with identical inputs produce
  `metrics` lists that are equal element-wise (exact for ints/bools,
  `== ` for floats — no tolerance).
- **AC2** A committed golden file
  (`snlg/tests/golden/dc_multifamily_tiny.json`) records peak_kw,
  annual_energy_kwh, load_factor, soft_score, feasible per run for the
  fixture scenario; the test asserts equality and **fails loudly** with a
  diff on drift.
- **AC3** Changing any RNG seed changes ≥1 metric.
- **AC4** The golden file header stores `engine_version`; a mismatch is a
  test failure that instructs the dev to regenerate intentionally.
- **Tests:** `snlg/tests/test_determinism.py`

### FR-E5 — RNG layering hardened (P0)
- **AC1** `derive_run_rng` uses `numpy.random.SeedSequence.spawn` (or
  equivalent) so per-run streams cannot collide for adjacent `run_index`
  (current impl adds `run_index` to a base int).
- **AC2** Existing `test_rng.py` assertions still pass (independence,
  reproducibility, per-index distinctness).
- **AC3** New test: 10 000 consecutive `run_index` values yield 10 000
  distinct first draws per layer.
- **Tests:** `snlg/tests/test_rng.py` (extended)

### FR-E6 — Config parser honours all documented keys (P1)
The YAML keys present in `configs/scenarios/*.yaml` but currently dropped by
`config.py` are either parsed into typed fields **or** rejected with a clear
error — never silently ignored.
- **AC1** Keys `arealimit`, `garage`, `heatpump`, `electric_vehicle`,
  `solar_pv` under `archetype_filters.resstock.<cat>` map to
  `ResidentialFilterConfig` fields (`max_sqft` alias, `require_no_garage`,
  and probabilistic-adoption fields) OR raise
  `ValueError("unknown archetype filter key: ...")`.
- **AC2** `scenario.name` / `scenario.description` given as a YAML list
  (as in `dc_multifamily_baseline_2.yaml`) is normalised to a string, not
  `"['DC Multifamily Baseline 2']"`.
- **AC3** `ensemble.mode` accepts only `fixed`/`adaptive`; anything else
  raises.
- **AC4** Unknown **top-level** sections raise, listing the offending keys.
- **Tests:** `snlg/tests/test_config.py` (extended),
  `snlg/tests/test_config_scenarios.py`

### FR-E7 — Coincidence & diversity attached to results (P1)
`compute_coincidence_factor` / diversity metrics are surfaced on
`NeighborhoodResult` (currently computed only in `aggregation`/`ensemble`
and partly discarded).
- **AC1** `NeighborhoodResult` gains `coincidence_factor: float` and
  `diversity_factor: float`, populated in `ensemble._run_single`.
- **AC2** `outputs` and the API summary expose them.
- **AC3** Backward-compat: dataclass fields have defaults so old pickles /
  test constructors don't break (existing `test_validation.py` builds
  `NeighborhoodResult` positionally — update fixtures accordingly).
- **Tests:** `snlg/tests/test_aggregation.py` (extended),
  `snlg/tests/test_ensemble.py`

### FR-E8 — Structured progress events (P0)
- **AC1** `snlg.run.RunProgress` is a frozen dataclass with the fields in
  FR-E3 AC2 plus `timestamp: float`.
- **AC2** `ensemble.run_ensemble` accepts an optional `progress` callback
  and invokes it without changing results when omitted.
- **Tests:** `snlg/tests/test_run_facade.py::test_progress_events`

### FR-E9 — CLI preserved (P1)
- **AC1** `python -m snlg run --config PATH --out DIR [--max-runs N]`
  writes the same legacy files `outputs.save_all_outputs` produces today.
- **AC2** Exit code 0 on success, non-zero + stderr message on config error.
- **Tests:** `snlg/tests/test_cli.py`

### FR-E10 — Engine reference doc (P0)
`docs/engine-reference.md` documents every public symbol, every config key,
the three RNG layers, and each methodology section, with at least one
diagram.
- **AC1** A doc-lint test (or manual review checklist in `test-plan.md` §9)
  confirms 100% public-symbol coverage.
- **Tests:** `snlg/tests/test_docs_coverage.py` (introspects `snlg.__all__`
  vs headings in the markdown)

### FR-E11 — Integration path with fixture data (P0)
- **AC1** `tests/fixtures/data/` holds a committed dataset: ≤5 ResStock +
  ≤2 ComStock parquet files (trimmed to a few weeks or downsampled) + a
  minimal `*.xlsx` characteristics pair.
- **AC2** `snlg/tests/test_integration_tiny.py` (marked `integration`) runs
  a full `max_runs=6` ensemble against it in <30s.
- **AC3** `pytest -m "not integration"` skips it; default CI runs it.
- **Tests:** `snlg/tests/test_integration_tiny.py`

---

## B. Backend / API requirements (FR-A)

### FR-A1 — Health & readiness (P0)
- **AC1** `GET /health` → `200 {"status":"ok"}` always (liveness).
- **AC2** `GET /ready` → `200` only when DB, Redis, and object store are
  reachable; `503` with a per-dependency breakdown otherwise.
- **Tests:** `api/tests/test_health.py`

### FR-A2 — Config CRUD (P0)
- **AC1** `POST /configs {name, body}` (body = structured JSON) → `201`
  with `id`, `version=1`, timestamps; `body` rejected with `422` +
  field-level errors if it fails scenario validation.
- **AC2** `GET /configs` lists (id, name, version, updated_at), newest
  first, paginated (`?limit`&`?cursor`).
- **AC3** `GET /configs/{id}` returns full body + current version.
- **AC4** `PUT /configs/{id}` creates a **new `config_versions` row**
  (immutable history) and bumps `configs.version`; returns the new state.
- **AC5** `DELETE /configs/{id}` → `409` if runs reference it, else `204`.
- **AC6** All mutations require the bearer token (`401` without).
- **Tests:** `api/tests/test_configs_api.py`

### FR-A3 — Config validation endpoint (P0)
- **AC1** `POST /configs/validate {body}` → `200 {valid: true}` or
  `200 {valid: false, errors: [{loc, msg}]}` (never `422` — this endpoint
  *reports* validity).
- **AC2** Errors match `snlg.config.validate_config` semantics (simplex
  bounds feasibility, mode enum, etc.).
- **Tests:** `api/tests/test_scenario_validation_api.py`

### FR-A4 — YAML import / export (P1)
- **AC1** `POST /configs/import` (text/yaml body) parses via the same path
  as `snlg.config.load_config`, stores the structured JSON, returns the new
  config.
- **AC2** `GET /configs/{id}/export` returns `text/yaml` that round-trips
  back through import to an equivalent body.
- **AC3** The four `configs/scenarios/*.yaml` files import without error.
- **Tests:** `api/tests/test_configs_api.py::test_yaml_roundtrip`

### FR-A5 — Config clone & templates (P1)
- **AC1** `POST /configs/{id}/clone {name}` → new config, `version=1`,
  body copied.
- **AC2** Seed data marks the DC-Multifamily baseline as
  `is_template=true`; templates are listable via `GET /configs?template=true`
  and cannot be deleted or edited (clone to modify).
- **Tests:** `api/tests/test_configs_api.py::test_clone`,
  `api/tests/test_seed.py`

### FR-A6 — Run creation & enqueue (P0)
- **AC1** `POST /runs {config_id, overrides?}` where `overrides` may set
  `seeds`, `ensemble.max_runs`, `ensemble.mode` → `202`
  `{run_id, status:"queued"}`.
- **AC2** The run row stores a **frozen JSON snapshot** of the effective
  config (base + overrides), the three seeds, and `engine_version`.
- **AC3** Unknown `config_id` → `404`.
- **AC4** An RQ job is enqueued on the `runs` queue (assert via fakeredis).
- **AC5** `overrides.ensemble.max_runs` above a configurable ceiling
  (`MAX_RUNS_CEILING`, default 500) → `422`.
- **Tests:** `api/tests/test_runs_api.py`

### FR-A7 — Run listing & detail (P0)
- **AC1** `GET /runs?status=&config_id=&limit=&cursor=` returns rows:
  `id, status, config_id, config_name, created_at, started_at, finished_at,
  progress {current,total,message,n_feasible}`.
- **AC2** `GET /runs/{id}` adds `summary` (once succeeded),
  `error_message`, `log_tail` (last 200 lines), `engine_version`, `seeds`,
  effective-config snapshot.
- **AC3** `404` for unknown id.
- **Tests:** `api/tests/test_runs_api.py`

### FR-A8 — Run progress stream (P0)
- **AC1** `GET /runs/{id}/events` is an SSE stream; each event is a JSON
  `RunProgressEvent`. On connect it replays the last known state, then
  streams live events from the Redis channel `progress:{run_id}`.
- **AC2** A terminal event (`stage in {done,failed,cancelled}`) is sent and
  the stream closes.
- **AC3** Connecting to a finished run yields one terminal event then close.
- **Tests:** `api/tests/test_runs_api.py::test_sse_*` (using a fake pub/sub)

### FR-A9 — Cancellation (P1)
- **AC1** `POST /runs/{id}/cancel` on a `queued` run removes it from the
  queue and sets `cancelled`.
- **AC2** On a `running` run, sets a Redis cancel flag; the worker stops
  within 5s (A7) and finalizes `cancelled`.
- **AC3** On a terminal run → `409`.
- **Tests:** `api/tests/test_runs_api.py::test_cancel_*`,
  `api/tests/test_worker.py::test_cooperative_cancel`

### FR-A10 — Results: metrics & summary (P0)
- **AC1** `GET /runs/{id}/metrics` → per-Monte-Carlo-run scalar rows
  (`run_index, peak_kw, annual_energy_kwh, load_factor, soft_score,
  coincidence_factor, feasible`).
- **AC2** `GET /runs/{id}/summary` → `diversity_metrics`,
  `convergence_history`, `validation_report`, counts, `converged`,
  representative-case pointers.
- **AC3** `404` if the run isn't `succeeded`/`cancelled` with partial data;
  `409` if still `running` (with `Retry-After`).
- **Tests:** `api/tests/test_results_api.py`

### FR-A11 — Results: series for charts (P1)
- **AC1** `GET /runs/{id}/series/envelope` → `{index: [...8760 iso ts],
  p10:[...], p50:[...], p90:[...]}` (percentiles per config).
- **AC2** `GET /runs/{id}/series/load-duration?case=median|high_peak|p50`
  → sorted descending load vs exceedance fraction.
- **AC3** Series are read from the MinIO Parquet artifacts, not recomputed
  from CSVs; response is gzip-eligible and cached (`ETag`).
- **Tests:** `api/tests/test_results_api.py::test_series_*`

### FR-A12 — Artifacts index & download (P0)
- **AC1** `GET /runs/{id}/artifacts` lists `{kind, filename, bytes,
  content_type, sha256, download_url}` where `download_url` is a
  time-limited presigned MinIO GET.
- **AC2** Kinds cover: `per_run_csv`, `compiled_runs_csv`,
  `load_profile_total_csv`, `percentile_envelopes_csv`,
  `ensemble_statistics_csv`, `convergence_history_csv`,
  `representative_case_csv`, `summary_figure_png`,
  `load_duration_png`, `overview_txt`, `full_log`.
- **AC3** Presign TTL is configurable (`PRESIGN_TTL_SECONDS`, default 900).
- **Tests:** `api/tests/test_results_api.py::test_artifacts_*`,
  `api/tests/test_storage.py`

### FR-A13 — Worker executes ensembles (P0)
- **AC1** `run_ensemble_task(run_id)` sets `running`, calls
  `snlg.run.run_scenario` with a progress callback that inserts
  `run_metrics` rows and publishes to Redis, an artifact sink that uploads
  to storage, and a `should_cancel` bound to the Redis flag.
- **AC2** On success: all artifacts uploaded, `artifacts` rows written,
  `runs.summary` populated, `status=succeeded`, `finished_at` set.
- **AC3** On engine exception: `status=failed`, `error_message` = str(exc)
  + truncated traceback in `full_log` artifact; already-uploaded artifacts
  tagged `partial=true`.
- **AC4** Runs with `MICROGRID_RUNNER_MODE=mock` complete deterministically
  fast for tests (no engine, synthetic metrics) — harvested from
  `feat/run-launcher` `run_worker.py`.
- **Tests:** `api/tests/test_worker.py`

### FR-A14 — Storage abstraction (P0)
- **AC1** `Storage` protocol: `put(key, data, content_type) -> ObjectRef`,
  `get(key) -> bytes`, `presign_get(key, ttl) -> str`, `delete(key)`,
  `list(prefix) -> list[ObjectRef]`.
- **AC2** `S3Storage` (boto3, MinIO endpoint) and `LocalStorage` (temp dir,
  fake presign returning a `file://`+token) both pass the same test class.
- **Tests:** `api/tests/test_storage.py` (parametrized over both impls)

### FR-A15 — Migrations (P0)
- **AC1** Every model change ships an Alembic revision.
- **AC2** `alembic upgrade head` then `alembic downgrade base` run clean on
  an empty Postgres (A8).
- **AC3** `make db-revision m="..."` autogenerates; CI fails if models and
  migrations diverge (`alembic check`).
- **Tests:** `api/tests/test_migrations.py`

### FR-A16 — Config/engine schema parity (P0)
- **AC1** For every YAML in `configs/scenarios/`, the API's Pydantic
  validator and `snlg.config.load_config` agree on validity.
- **AC2** A structurally valid body accepted by the API and passed to the
  worker is accepted by the engine (no "valid here, rejected there").
- **Tests:** `api/tests/test_schema_parity.py`

### FR-A17 — AuthN (P1)
- **AC1** `require_token` dependency: `Authorization: Bearer <API_TOKEN>`;
  missing/wrong → `401`.
- **AC2** Applied to all `POST/PUT/DELETE`; `GET` open.
- **AC3** When `API_TOKEN` is unset, mutations are allowed but a startup
  warning is logged (dev convenience).
- **Tests:** `api/tests/test_auth.py`

---

## C. Frontend requirements (FR-F)

### FR-F1 — App shell (P0)
- **AC1** Persistent nav: Configs, Runs, (Compare). Health dot polls
  `/health` + `/ready`.
- **AC2** API token entry stored in `localStorage`; attached by the API
  client; a 401 surfaces a "set your token" prompt.
- **Tests:** `web/src/components/AppShell.test.tsx`

### FR-F2 — Config list (P0)
- **AC1** Table of configs (name, version, updated); "New from template"
  and per-row Edit / Clone / Launch / Delete.
- **AC2** Delete disabled for templates and configs with runs (tooltip).
- **Tests:** `web/src/pages/configs/index.test.tsx`

### FR-F3 — Config editor (P0)
- **AC1** Two tabs: **Form** (structured fields for `composition`,
  `archetype_filters`, `feasibility`, `perturbation`, `ensemble`,
  `convergence`, `validation`, `output`) and **YAML** (raw textarea).
- **AC2** On change (debounced 400ms) → `POST /configs/validate`; errors
  render inline next to the offending field (Form) or as a list (YAML).
- **AC3** Save disabled while invalid; Save on an existing config warns it
  creates a new version.
- **AC4** YAML tab import/export uses `/configs/import` + `/export`.
- **Tests:** `web/src/pages/configs/editor.test.tsx`,
  `web/src/components/ValidationErrors.test.tsx`

### FR-F4 — Run launcher (P1)
- **AC1** From a config: modal to optionally override `max_runs`, `mode`,
  and the three seeds; Submit → `POST /runs` → redirect to run detail.
- **Tests:** `web/src/components/LaunchRunDialog.test.tsx`

### FR-F5 — Runs table (P0)
- **AC1** Live-updating list (poll `GET /runs?active=true` every 3s, or a
  shared SSE); columns: id (short), config, status chip, progress,
  started, duration.
- **AC2** Filter by status and config.
- **Tests:** `web/src/components/RunsTable.test.tsx`

### FR-F6 — Run detail + live progress (P0)
- **AC1** Subscribes to `/runs/{id}/events` (SSE); renders a progress bar
  (`current/total`), `n_feasible`, current stage, and a live-tailing log
  pane.
- **AC2** Cancel button for non-terminal runs; disabled + reason otherwise.
- **AC3** On terminal event, shows a "View results" CTA (success) or the
  error message (failure).
- **Tests:** `web/src/components/RunProgress.test.tsx`,
  `web/src/pages/runs/detail.test.tsx`

### FR-F7 — Results dashboard (P1)
- **AC1** Panels: (a) P10–P90 hourly envelope area chart with P50 line;
  (b) peak-demand histogram + median line; (c) annual-energy histogram;
  (d) load-duration curve (selectable case); (e) convergence history
  (normalized lines); (f) representative-cases table (median / high_peak /
  low_load_factor with peak, energy, LF); (g) validation panel
  (pass/fail + warnings); (h) artifact download list.
- **AC2** Charts hydrate from `/runs/{id}/summary` + `/series/*`; empty
  states when no feasible runs.
- **AC3** Every panel matches `dataviz` skill guidance (theme-aware,
  accessible, consistent palette).
- **Tests:** `web/src/pages/runs/results.test.tsx`,
  `web/src/components/EnvelopeChart.test.tsx`,
  `web/src/components/LoadDurationChart.test.tsx`

### FR-F8 — Compare view (P2)
- **AC1** Pick two succeeded runs → overlaid envelopes + a scalar-delta
  table (Δpeak, Δenergy, ΔLF, Δload-factor CV).
- **Tests:** `web/src/pages/compare.test.tsx`

### FR-F9 — API client (P0)
- **AC1** Typed client (`web/src/lib/apiClient.ts`) wrapping every
  endpoint; injects the token; normalizes errors to
  `{status, message, fieldErrors?}`.
- **AC2** SSE helper with reconnect/backoff.
- **Tests:** `web/src/lib/apiClient.test.ts`

### FR-F10 — E2E happy path (P1)
- **AC1** Playwright: create-from-template → edit → validate passes →
  launch (mock runner) → progress completes → results dashboard renders
  all panels → download one artifact.
- **Tests:** `web/e2e/run-lifecycle.spec.ts`

---

## D. Data & infrastructure requirements (FR-D)

### FR-D1 — Compose stack (P0)
- **AC1** `docker compose -f deploy/docker-compose.yml up` starts web, api,
  worker, db, redis, minio, minio-init; `minio-init` creates the bucket and
  exits 0.
- **AC2** api waits for db+redis+minio (healthchecks / `depends_on:
  condition: service_healthy`); `alembic upgrade head` runs on api start.
- **AC3** Web reachable at `:3000`, API docs at `:8000/docs`, MinIO console
  at `:9001`, within 90s on a clean machine (A3).
- **Tests:** `deploy/tests/test_compose_smoke.sh` (CI job),
  `api/tests/test_health.py` (against the live stack in the smoke job)

### FR-D2 — Makefile (P0)
- **AC1** Targets: `up`, `down`, `logs`, `test` (all suites),
  `test-engine`, `test-api`, `test-web`, `e2e`, `db-upgrade`,
  `db-revision`, `seed`, `fmt`, `lint`, `smoke`.
- **AC2** `make test` runs green on a clean checkout with Docker available.
- **Tests:** exercised by CI (FD-4)

### FR-D3 — OEDI input fetch (P1)
- **AC1** `scripts/download_oedi.py --manifest data/sources/oedi_manifest.yaml`
  populates `data/input/{resstock,comstock}/timeseries_individual_buildings/
  upgrade=<u>/state=<s>/<bldg_id>-0.parquet` and
  `data/background/<chars>.xlsx`.
- **AC2** Manifest declares source (dataset, release, upgrade, state,
  building id list or count), transport (`s3` via `aws s3 sync` or
  `https`), and target paths.
- **AC3** `--dry-run` prints the plan; idempotent re-runs skip existing
  files.
- **AC4** Unit tests mock the transport (harvest
  `feat/oedi-downloader/scripts/test_download_oedi.py`).
- **Tests:** `scripts/tests/test_download_oedi.py`

### FR-D4 — CI pipeline (P0)
- **AC1** `.github/workflows/ci.yml` runs on push + PR: `ruff`/`black
  --check`, `eslint`, engine `pytest -m "not integration"` + integration on
  the fixture data, api `pytest` (Postgres + fakeredis services), web
  `jest`, then a compose smoke job (`up` + health + one mock run + e2e).
- **AC2** Coverage uploaded; gates enforced (A6).
- **AC3** A red test fails the pipeline.
- **Tests:** self (the workflow) + `test-plan.md` §7

### FR-D5 — Seed / smoke script (P1)
- **AC1** `scripts/seed.py` (or `make seed`) inserts the baseline template
  config, ensures the bucket, and launches one `max_runs=6` mock/real run
  against fixture data, asserting it reaches `succeeded` with a non-empty
  summary and ≥1 artifact.
- **Tests:** `api/tests/test_seed.py`

### FR-D6 — `.env` contract (P0)
- **AC1** `deploy/.env.example` lists every variable with a safe default or
  a `CHANGE_ME`: `DATABASE_URL`, `REDIS_URL`, `S3_ENDPOINT`,
  `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `API_TOKEN`,
  `MAX_RUNS_CEILING`, `PRESIGN_TTL_SECONDS`, `NEXT_PUBLIC_API_BASE_URL`,
  `DATA_DIR`.
- **AC2** Settings classes fail fast with a clear message on a missing
  required var.
- **Tests:** `api/tests/test_settings.py`

---

## E. Non-functional requirements (NFR)

| ID | Requirement | Measure / test |
|---|---|---|
| **NFR-1 Reproducibility** | A stored run re-executed from its snapshot reproduces `run_metrics` exactly | `snlg/tests/test_determinism.py`, `api/tests/test_reproduce.py` |
| **NFR-2 Performance (dev loop)** | Fixture ensemble (`max_runs=6`, tiny data) completes < 30s on CI; UJ-1 e2e < 3 min | CI timing assertion in `web/e2e` + integration test |
| **NFR-3 Scalability** | `docker compose up --scale worker=3` drains 3 queued runs concurrently; API p95 latency for reads < 300ms with 10k `run_metrics` rows | `api/tests/test_worker.py::test_concurrent_pickup`, load note in `infrastructure.md` |
| **NFR-4 Memory** | Worker RSS stays < 2GB for a 50-run ensemble on the fixture data via incremental artifact flush + capped parquet cache | `snlg/tests/test_run_facade.py::test_incremental_artifacts`, doc guidance |
| **NFR-5 Observability** | api + worker emit structured JSON logs; `/ready` reports per-dependency status | `api/tests/test_health.py`, `test_logging.py` |
| **NFR-6 Portability** | Whole stack runs on Docker Desktop (macOS/arm64) and Linux/amd64 with no code change | CI matrix note; `platform` unset in compose |
| **NFR-7 Security** | No secrets in images or git; mutations token-gated; CORS restricted to the web origin; MinIO not published beyond the Compose net (console excepted) | `api/tests/test_auth.py`, `test_cors.py`, compose review |
| **NFR-8 Data safety** | `DELETE /runs/{id}` purges MinIO objects and DB rows transactionally; a failed purge leaves the run marked `delete_failed`, not half-gone | `api/tests/test_results_api.py::test_delete_run_purges` |
| **NFR-9 Backward compat** | The headless CLI / notebook path still produces the legacy `data/output/scenario_runs/<ts>/` file set unchanged | `snlg/tests/test_cli.py`, `test_outputs.py` |
| **NFR-10 Migration integrity** | `upgrade head` → `downgrade base` clean; `alembic check` green | `api/tests/test_migrations.py` |
| **NFR-11 Accessibility** | Charts and forms meet WCAG AA contrast in light/dark; keyboard-navigable | RTL a11y assertions; `dataviz` review |
| **NFR-12 Test isolation** | No test depends on network, real OEDI, or a developer's local data dir | CI runs with network egress restricted for unit jobs |

---

## F. Traceability index (requirement → primary test files)

| Req | Test file(s) |
|---|---|
| FR-E1..E2 | `snlg/tests/test_packaging.py` |
| FR-E3, E8 | `snlg/tests/test_run_facade.py` |
| FR-E4 | `snlg/tests/test_determinism.py` |
| FR-E5 | `snlg/tests/test_rng.py` |
| FR-E6 | `snlg/tests/test_config.py`, `test_config_scenarios.py` |
| FR-E7 | `snlg/tests/test_aggregation.py`, `test_ensemble.py` |
| FR-E9 | `snlg/tests/test_cli.py` |
| FR-E10 | `snlg/tests/test_docs_coverage.py` |
| FR-E11 | `snlg/tests/test_integration_tiny.py` |
| FR-A1 | `api/tests/test_health.py` |
| FR-A2, A4, A5 | `api/tests/test_configs_api.py` |
| FR-A3, A16 | `api/tests/test_scenario_validation_api.py`, `test_schema_parity.py` |
| FR-A6..A9 | `api/tests/test_runs_api.py` |
| FR-A10..A12 | `api/tests/test_results_api.py` |
| FR-A13 | `api/tests/test_worker.py` |
| FR-A14 | `api/tests/test_storage.py` |
| FR-A15 | `api/tests/test_migrations.py` |
| FR-A17 | `api/tests/test_auth.py` |
| FR-F1..F9 | `web/src/**/**.test.tsx`, `web/src/lib/apiClient.test.ts` |
| FR-F10 | `web/e2e/run-lifecycle.spec.ts` |
| FR-D1 | `deploy/tests/test_compose_smoke.sh` |
| FR-D3 | `scripts/tests/test_download_oedi.py` |
| FR-D5 | `api/tests/test_seed.py` |
| FR-D6 | `api/tests/test_settings.py` |
| NFR-1 | `api/tests/test_reproduce.py` |
