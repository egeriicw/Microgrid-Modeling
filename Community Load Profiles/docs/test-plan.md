# Test Plan — TDD Methodology & Suite Specification

This plan is **binding** for the implementing agent. Follow it in order.

---

## 1. TDD loop (non-negotiable)

For every requirement (FR/NFR) and every bug fix:

1. **RED** — write the smallest test that fails for the right reason. Run
   it; confirm it fails with a message that names the missing behaviour.
2. **GREEN** — write the minimum production code to pass. No extra scope.
3. **REFACTOR** — clean up code *and* test while green. Re-run the file,
   then the suite.
4. **Commit** — one logical change, test + code together, message
   references the FR-ID. (`implementation-plan.md` §"tiny commits".)

Rules:
- No production module is created without a failing test that required it.
- A PR that lowers coverage below the gate (§6) fails CI.
- "Can't test it" → the design is wrong; add a seam (`codebase-design`
  skill vocabulary: deepen the module, inject the dependency).
- Skeletons already committed in this repo (`snlg/tests/`, `api/tests/`,
  `web/src/**`, `web/e2e/`, `deploy/tests/`) are **RED placeholders**:
  each has real assertions but `pytest.importorskip` / `test.skip` /
  `xfail` guards or import errors until the code exists. Removing the guard
  is step RED for that unit.

---

## 2. Test pyramid & where each layer lives

```
        ┌─────────────────────────┐
        │  e2e (Playwright)        │  web/e2e/                1 happy path (FR-F10)
        ├─────────────────────────┤
        │  integration            │  snlg/tests/*integration*  fixture-data ensemble
        │                         │  api/tests (ASGI+PG)       full request→worker→storage
        │                         │  deploy/tests              compose smoke (CI)
        ├─────────────────────────┤
        │  unit (bulk)            │  snlg/tests, api/tests, web/src/**  pure funcs, components
        └─────────────────────────┘
```

- **Engine unit**: pure, in-memory, synthetic Series/DataFrames (extend the
  existing `conftest.py` fixtures). No disk, no parquet.
- **Engine integration** (`-m integration`): reads
  `tests/fixtures/data/`; one per major path (`archetype`, `loader`,
  `run_scenario`, `cli`, `determinism`).
- **API unit**: FastAPI `TestClient` / `httpx.ASGITransport`; DB =
  Postgres test container or a per-test transactional SQLite-incompatible?
  → **use Postgres** (JSONB, enum); `fakeredis`; `LocalStorage`.
- **API integration**: real `worker` function invoked in-process (RQ
  `SimpleWorker` / `is_async=False`), `MICROGRID_RUNNER_MODE=mock` for
  speed, plus one real-engine path on fixture data.
- **Web unit**: Jest + RTL; `apiClient` mocked with `msw` or hand fakes;
  SSE faked.
- **Web e2e**: Playwright against `docker compose ... -f
  docker-compose.ci.yml` with the mock runner.

---

## 3. Fixtures & test data

| Fixture | Location | Contents |
|---|---|---|
| Engine in-memory | `snlg/tests/conftest.py` (extend existing) | `rng_bundle`, `minimal_cfg`, `synthetic_8760`, `synthetic_15min_df`, **+ new**: `tiny_cfg` (fixture-data paths), `archetype_pool_fixture` |
| Tiny dataset | `tests/fixtures/data/` | ≤5 ResStock + ≤2 ComStock parquet (trimmed), `chars_resstock.xlsx`, `chars_comstock.xlsx` — enough for `total_buildings≤5`, `max_runs≤6` |
| Golden metrics | `snlg/tests/golden/dc_multifamily_tiny.json` | `{engine_version, runs:[{run_index,peak_kw,annual_energy_kwh,load_factor,soft_score,feasible}]}` — regenerated only by an explicit `--update-golden` flag |
| Scenario YAMLs | `configs/scenarios/*.yaml` (existing) | import/parity fixtures for FR-E6 / FR-A4 / FR-A16 |
| API DB | `api/tests/conftest.py` | `db` fixture: create schema via `alembic upgrade head` on a throwaway database, wrap each test in a rollback |
| API queue | `api/tests/conftest.py` | `fake_queue` (fakeredis + RQ `SimpleWorker`) |
| API storage | `api/tests/conftest.py` | `LocalStorage(tmp_path)` |
| Web | `web/src/test/` | `renderWithProviders`, `mockApi`, `mockSse` |

---

## 4. Test-file index (create in this order)

### Phase 1 — engine hardening (`snlg/tests/`)

| File | FR | Red-phase assertions |
|---|---|---|
| `test_packaging.py` | E1, E2 | `import snlg` w/o `PYTHONPATH`; `snlg.__version__` is semver; `set(snlg.__all__)` == expected & every name importable; `snlg.run.run_scenario` callable |
| `test_rng.py` *(extend existing)* | E5 | 10k consecutive `derive_run_rng` indices → 10k distinct first draws per layer; existing 5 tests still pass; impl uses `SeedSequence` (assert no `+ run_index` int arithmetic via a behavioural collision test) |
| `test_config.py` *(extend)* | E6 | unknown top-level section raises `ValueError` listing keys; `mode="fixd"` raises; `method` typo raises |
| `test_config_scenarios.py` | E6 | every `configs/scenarios/*.yaml` loads; `cfg.name` is `str` not `list`; `arealimit`/`heatpump`/... either land on typed fields or raise `unknown archetype filter key` |
| `test_run_facade.py` | E3, E8 | `run_scenario(dict, data_dirs=fixture)` → `RunResult` with `.ensemble/.summary/.metrics/.engine_version/.seeds`; `progress` called ≥ `n_attempted+1` times with `RunProgress(stage,run_index,n_attempted,n_feasible,converged,message,timestamp)`; `should_cancel→True` ⇒ `.cancelled` & loop stops; `ArtifactSink.put` invoked per artifact & memory released between (assert sink called incrementally, not once at end) |
| `test_determinism.py` | E4, NFR-1 | two `run_scenario` calls, identical inputs ⇒ `metrics` equal (exact); compare to `golden/dc_multifamily_tiny.json` with a readable diff on mismatch; changing any one seed changes ≥1 metric; golden `engine_version` mismatch → explicit failure message |
| `test_aggregation.py` *(extend)* | E7 | `NeighborhoodResult` carries `coincidence_factor`/`diversity_factor`; staggered vs coincident peaks give expected CF (existing) + now surfaced on the result |
| `test_ensemble.py` | E7, E8 | `run_ensemble` accepts `progress=` without changing results; fixed vs adaptive stop behaviour; `n_rejected == n_attempted - n_feasible`; convergence triggers only after `lookback+min_stable` |
| `test_archetype.py` | (coverage) | filters apply each gate (`building_type_heights`, `max_sqft`, `require_no_garage`, comstock public exclusion); empty pool recorded in `metadata`, not raised |
| `test_loader.py` | (coverage), G12 | path resolution; 15-min→hourly sum; MF multiplier only for MF types & `unit>1`; `lru_cache` bounded (`maxsize` set) & `clear_cache` works |
| `test_outputs.py` | E-legacy, NFR-9 | `save_all_outputs` writes exactly the legacy filename set with legacy column names; `ArtifactSink` path yields the same logical files |
| `test_cli.py` | E9 | `python -m snlg run --config <tiny> --out <tmp>` exit 0 + legacy files; bad config → non-zero + stderr |
| `test_integration_tiny.py` `@pytest.mark.integration` | E11 | full `max_runs=6` ensemble on `tests/fixtures/data/` < 30s; ≥1 feasible run; envelope has 8760 rows |
| `test_docs_coverage.py` | E10 | every symbol in `snlg.__all__` appears as a heading/anchor in `docs/engine-reference.md` |

### Phase 2 — API + worker (`api/tests/`)

| File | FR | Red-phase assertions |
|---|---|---|
| `test_settings.py` | D6 | missing required env → clear `ValidationError`; defaults present for optional |
| `test_health.py` | A1 | `/health`→200 always; `/ready`→200 when deps up, 503 + per-dep breakdown when one is down (monkeypatch) |
| `test_migrations.py` | A15, NFR-10 | `upgrade head` then `downgrade base` clean on empty PG; `alembic check` reports no diff vs models |
| `test_storage.py` | A14 | one parametrized class over `S3Storage`(minio test container or moto) + `LocalStorage`: put/get round-trip, `sha256`, `presign_get` returns a URL that GETs the bytes, `delete`, `list(prefix)` |
| `test_scenario_validation_api.py` | A3 | `/configs/validate` always 200; valid body → `{valid:true}`; simplex-infeasible → `{valid:false, errors:[{loc,msg}]}` matching engine semantics |
| `test_schema_parity.py` | A16, NFR-1 | for each `configs/scenarios/*.yaml`: Pydantic validator verdict == `snlg.config.load_config` verdict; a body the API accepts is accepted by `snlg.run.run_scenario` (smoke, mock data) |
| `test_configs_api.py` | A2, A4, A5 | CRUD happy paths + codes; `PUT` creates a `config_versions` row & bumps `version`; `DELETE` 409 with runs / on template; `clone`; YAML import/export round-trip for all shipped configs; token required on mutations |
| `test_runs_api.py` | A6–A9 | `POST /runs` → 202 + row w/ frozen `config_snapshot`,`seeds`,`engine_version` + job enqueued (assert via `fake_queue`); `max_runs>ceiling`→422; unknown config→404; list filters; detail shape; SSE stream replays state then streams then closes on terminal; cancel: queued→cancelled (dequeued), running→flag set, terminal→409 |
| `test_worker.py` | A13, NFR-3 | `run_ensemble_task` (mock mode) → `succeeded`, progress rows + published events, artifacts uploaded, `summary` populated; engine raises → `failed` + `error_message` + `partial` artifacts; cooperative cancel stops < 5 iterations; `--scale`-style concurrent pickup of 3 queued jobs by 3 `SimpleWorker`s (no double-run: `UNIQUE(run_id,run_index)` holds) |
| `test_results_api.py` | A10–A12, NFR-8 | `/metrics`, `/summary` shapes; 409 while running; `/series/envelope` from parquet artifact (not recomputed) w/ `ETag`; `/series/load-duration` sorted desc; `/artifacts` lists all `kind`s w/ presigned URLs; `DELETE /runs/{id}` purges MinIO + rows, partial failure → `delete_failed` |
| `test_auth.py` | A17, NFR-7 | no token on mutation → 401; correct token → allowed; `GET` open; unset `API_TOKEN` → allowed + startup warning |
| `test_cors.py` | NFR-7 | preflight from `http://localhost:3000` allowed; other origin blocked |
| `test_reproduce.py` | NFR-1 | launch a seeded run on fixture data → succeeded; re-run the stored `config_snapshot`+`seeds` → `run_metrics` byte-equal |
| `test_seed.py` | D5 | `scripts/seed.py` inserts the template (`is_template=true`), ensures the bucket, launches a `max_runs=6` run reaching `succeeded` with non-empty `summary` + ≥1 artifact |
| `test_openapi_contract.py` | api-spec §7 | snapshot `/openapi.json`; fail on undocumented drift |
| `test_logging.py` | NFR-5 | api + worker emit JSON lines with `run_id` where applicable |

### Phase 3 — frontend (`web/src/**`, `web/e2e/`)

| File | FR |
|---|---|
| `src/lib/apiClient.test.ts` | F9 |
| `src/lib/sse.test.ts` | F9 |
| `src/components/AppShell.test.tsx` | F1 |
| `src/components/HealthDot.test.tsx` | F1 |
| `src/pages/configs/index.test.tsx` | F2 |
| `src/pages/configs/editor.test.tsx` | F3 |
| `src/components/ValidationErrors.test.tsx` | F3 |
| `src/components/LaunchRunDialog.test.tsx` | F4 |
| `src/components/RunsTable.test.tsx` | F5 |
| `src/components/RunProgress.test.tsx` | F6 |
| `src/pages/runs/detail.test.tsx` | F6 |
| `src/components/results/EnvelopeChart.test.tsx` | F7 |
| `src/components/results/LoadDurationChart.test.tsx` | F7 |
| `src/components/results/ConvergenceChart.test.tsx` | F7 |
| `src/pages/runs/results.test.tsx` | F7 |
| `src/pages/compare.test.tsx` | F8 (P2) |
| `e2e/run-lifecycle.spec.ts` | F10 |

### Phase 4 — infra / scripts

| File | FR |
|---|---|
| `scripts/tests/test_download_oedi.py` | D3 (transport mocked) |
| `deploy/tests/test_compose_smoke.sh` | D1 (CI job) |

---

## 5. Test doubles & isolation (NFR-12)

- **No network** in unit jobs (CI blocks egress). OEDI transport is always
  mocked. MinIO in API tests = `LocalStorage` or `moto`/testcontainer, not
  a live endpoint.
- **No developer local data**: engine integration reads only
  `tests/fixtures/data/`. A test that touches `data/input/**` fails review.
- **Time**: freeze with `freezegun` where timestamps are asserted.
- **RNG**: always seed explicitly; never rely on default entropy.
- **Redis**: `fakeredis`; RQ `SimpleWorker`/`is_async=False`.

---

## 6. Coverage gates (CI-enforced, A6)

| Suite | Line coverage | Notes |
|---|---|---|
| `snlg` | **≥ 90%** | `outputs.py` legacy writers + `cli.py` count; exclude `__main__` guard lines |
| `api` | **≥ 85%** | routers, worker, storage, schemas |
| `web` | **≥ 80%** on `src/lib` + `src/components/results` + `RunProgress` + `apiClient`; overall reported not gated |

Drop below gate ⇒ CI fail. New code should raise, not lower, the number.

---

## 7. CI wiring (see `infrastructure.md` §6)

`lint → {engine, api, web} in parallel → compose-smoke (needs engine+api
images) → e2e`. All green required to merge. Coverage XML uploaded per
suite.

---

## 8. Determinism regeneration protocol (FR-E4)

The golden file changes **only** on an intentional methodology change:
1. Make the change + its tests.
2. `pytest snlg/tests/test_determinism.py --update-golden` (custom flag in
   `conftest.py`).
3. Commit the new golden **in the same commit** as the code change, with a
   `CHANGELOG.md` entry and a bumped `snlg.__version__`.
4. PR description must state *why* results moved.
An un-bumped version with a changed golden fails the version-guard test.

## 9. Engine-reference doc-coverage checklist (FR-E10)

`test_docs_coverage.py` enforces symbol coverage automatically. A human
review additionally confirms: every config key in §4.1 is described; the
three RNG layers; all 12 known gaps carried until resolved or explicitly
accepted; at least the two mermaid diagrams render.
