# Product Requirements Document — Synthetic Neighborhood Load Generation (SNLG) Platform

**Status:** Draft v1.0
**Date:** 2026-09-03
**Owner:** Bill Eger (egeriicw@gmail.com)
**Repository:** `Microgrid-Modeling/Community Load Profiles`
**Working branch context:** `claude/software-methodology-plan-*`

---

## 0. How to use this document

This PRD is the entry point for an implementing agent. It has two jobs:

1. **Document the existing system.** The `src/snlg/` package and
   `notebooks/community_microgrid_load_profiles.ipynb` already implement a
   working Monte-Carlo load-generation methodology. The agent must produce
   faithful reference documentation for it (see `docs/engine-reference.md`,
   which this PRD seeds) and lock its public contract with tests before any
   new feature work.
2. **Build the platform around it.** Wrap the engine in a service layer
   (REST API + async workers), a Postgres/object-store persistence tier, and
   a Next.js web application, all orchestrated with Docker Compose for local
   and CI use.

Companion specs (all under `docs/`):

| File | Purpose |
|---|---|
| `architecture.md` | System context, containers, components, data flow, repo layout |
| `requirements.md` | Numbered functional / non-functional requirements + acceptance criteria |
| `engine-reference.md` | Reference documentation for the **existing** `snlg` engine + known gaps |
| `domain-glossary.md` | Ubiquitous language for the load-generation methodology |
| `api-spec.md` | REST contract: resources, endpoints, schemas, status codes |
| `data-model.md` | Postgres schema (DDL) + MinIO object layout + retention |
| `frontend-spec.md` | Next.js pages, components, flows, state, API client |
| `infrastructure.md` | Docker Compose services, Dockerfiles, env, Make targets, CI |
| `test-plan.md` | TDD methodology, test pyramid, coverage gates, full test-file index |
| `implementation-plan.md` | Phased milestones, tiny-commit task list, sequencing, DoD |

**The agent MUST follow `test-plan.md`: write the failing test first, then the
code that makes it pass, then refactor. No production code lands without a
test that fails without it.**

---

## 1. Problem & background

Distribution planners, community-microgrid developers, and researchers need
**realistic, uncertainty-aware hourly electricity load profiles** for a
*hypothetical* neighborhood before any meters exist. A single deterministic
profile understates diversity and peak risk. The methodology in this repo
generates an **ensemble** of plausible neighborhoods by:

- filtering NREL **ResStock** (residential) and **ComStock** (commercial)
  building stock to archetypes that match a scenario,
- sampling a **building mix** (category proportions → integer counts),
- drawing **building instances** from the filtered pools,
- optionally **perturbing** each instance (temporal shift, end-use scaling),
- **aggregating** to a neighborhood 8760-hour profile,
- screening each realization against **feasibility** constraints,
- repeating until ensemble statistics **converge**,
- emitting **percentile envelopes**, diversity metrics, representative cases,
  and a **validation** report.

Today this runs only as a notebook driven by hand-edited YAML, executed on a
laptop, with outputs as loose CSV/PNG files. There is no way for a
non-Python user to configure a scenario, launch a run, watch progress, or
browse results; no run history; no reproducible environment.

### Prior art already in the repository

Two parallel engine implementations exist:

- **`src/snlg/`** (branch `main`, and this branch) — the methodology-driven,
  typed, unit-tested package. **50 tests currently pass.** This is the
  **engine of record.**
- **`microgrid_profiles`** on the `feat/*` branches — an earlier
  ChatGPT-assisted scaffold that *also* ships a FastAPI + SQLAlchemy/Alembic
  skeleton, a Next.js (Pages Router + MUI) frontend, `docker-compose.yml`
  (timescaledb image), an OEDI downloader, and an in-process run worker.
  Useful as **reference material to harvest**, not as the engine.

**Decision (see §4):** keep both, cleanly separated — `snlg` becomes a
standalone installable library; a new service layer depends on it. The
`feat/*` API/DB/frontend/compose scaffolds are mined for code and patterns.

---

## 2. Goals & non-goals

### 2.1 Goals

- **G1 — Package the engine.** `snlg` is `pip install`-able, importable
  without `PYTHONPATH` hacks, with a documented, versioned public API and a
  frozen determinism contract.
- **G2 — Reference-document the existing system.** Every `snlg` module,
  dataclass, config key, RNG layer, and methodology section has accurate
  prose + diagrams in `docs/engine-reference.md`.
- **G3 — Scenario management.** Users create, validate, edit, clone, and
  version scenario configurations through an API and a web UI, without
  writing YAML by hand (YAML import/export still supported).
- **G4 — Run orchestration.** Users launch ensemble runs from a config;
  runs execute asynchronously on a worker; progress, logs, and status are
  observable in real time; runs are cancellable.
- **G5 — Results & persistence.** Scalar metrics, lineage, and convergence
  history live in Postgres; bulk artifacts (per-run CSVs, envelopes,
  figures, compiled runs) live in an S3-compatible object store; every run
  is reproducible from its stored config + seeds + engine version.
- **G6 — Visualization.** The web app renders load envelopes, peak/energy
  distributions, load-duration curves, convergence history, and
  representative cases without downloading files.
- **G7 — Reproducible environment.** `docker compose up` brings up web +
  api + worker + db + redis + object store; one command runs the full test
  suite; CI runs it on every push.
- **G8 — Input data pipeline.** A documented, scriptable path to fetch the
  required ResStock/ComStock parquet + characteristics files from NREL OEDI
  into the expected `data/input/**` layout.

### 2.2 Non-goals (v1)

- Multi-tenant auth/RBAC, billing, or public hosting (single-team, trusted
  network; a single API token is acceptable).
- Kubernetes / Helm / cloud IaC (Compose only; production notes are
  advisory — see `infrastructure.md` §9).
- Power-flow / optimal-dispatch / DER-sizing modeling (load *generation*
  only; downstream tools consume the profiles).
- Replacing matplotlib exports for publication figures (the UI complements,
  not replaces, notebook analysis).
- Real-time streaming of meter data; weather-normalization beyond what the
  engine already does.
- Editing methodology math through the UI (config only).

---

## 3. Personas & primary use cases

| Persona | Needs |
|---|---|
| **Planner Priya** (utility DER planning) | Point-and-click scenario, launch 200-run ensemble, read P90 peak and load-duration curve, export CSV for her power-flow tool. |
| **Researcher Ravi** (methodology dev) | Import the engine as a library, tweak perturbation math, run parity tests, compare two configs' envelopes side by side. |
| **Developer Dana** (platform maintainer) | Stand the whole stack up locally, run tests, add an endpoint, ship a migration, keep CI green. |

**Core user journey (UJ-1):** open web app → create config from the
"DC Multifamily" template → adjust `total_buildings` and category bounds →
UI shows validation passes → "Launch run" → watch progress bar and streamed
log → run succeeds → open results dashboard → inspect P10/P50/P90 envelope,
peak histogram, load-duration curve, representative "high_peak" case →
download the compiled-runs CSV.

---

## 4. Key design decisions (locked)

| # | Decision | Rationale |
|---|---|---|
| **D1 — Engine boundary** | Keep both engines, cleanly separated. `snlg` becomes a standalone, `pip`-installable library (its own `pyproject.toml`, semver, changelog). The platform (`api/`, `web/`) is a **separate service layer that depends on `snlg`**. `microgrid_profiles` and the `feat/*` scaffolds are reference-only; code is harvested into `api/`. Monorepo, two Python packages. | Preserves the tested methodology as the source of truth; lets researchers use the engine headless; avoids a risky big-bang merge; keeps API concerns out of the science. |
| **D2 — Run execution** | **Redis + RQ worker queue.** FastAPI enqueues a job; one or more `worker` containers execute ensembles; progress/log/status stream to Postgres via the engine's progress callback. Runs survive API restarts; workers scale horizontally; jobs are cancellable. | Ensembles take minutes–hours and must not block the API or die with it. RQ is lighter than Celery and adequate for this scale. |
| **D3 — Results storage** | **Postgres for metadata + MinIO (S3-compatible) for artifacts.** Postgres holds configs, runs, per-run scalars, convergence history, validation reports, artifact index rows. MinIO holds per-run/ensemble CSVs, Parquet, and PNGs, keyed and referenced from Postgres. | Keeps the DB lean and fast to query; artifacts are large and write-once; S3 API makes a later cloud move trivial; MinIO runs in Compose. |
| **D4 — Frontend & deployment** | **Full Next.js app** (Pages Router + MUI, matching the `feat/*` foundation): config list/editor with live schema validation, run launcher + live progress, results dashboard (envelopes, distributions, load-duration curves, convergence, representative cases). **Single `docker-compose.yml`**: `web`, `api`, `worker`, `db` (Postgres 16), `redis`, `minio` (+ `minio-init`). Production guidance is documentation only. | Delivers the non-Python UX the project lacks; Compose is enough for a single team and CI; k8s is deferred (non-goal). |

Supporting decisions:

- **D5** — Database engine is **plain PostgreSQL 16** (not TimescaleDB).
  Hourly 8760 arrays are **not** row-per-hour in Postgres; they live as
  Parquet artifacts in MinIO. Postgres stores only scalars + JSON. Revisit
  Timescale only if SQL-side envelope queries become a hard requirement.
- **D6** — Config canonical form is **structured JSON** in Postgres,
  validated by a **Pydantic v2** schema generated to mirror
  `snlg.config.ScenarioConfig`. YAML is an import/export format only.
- **D7** — API auth v1: a single static bearer token (`API_TOKEN` env),
  required on all mutating endpoints; read endpoints open on the local
  network. Auth is a thin dependency so it can be swapped later.
- **D8** — Engine ↔ platform contract is a **`snlg.run` façade** (new,
  thin) exposing `run_scenario(config_dict, *, progress=cb, artifacts=sink)
  -> RunResult`. The worker calls only this façade, never internal modules.

---

## 5. Scope — feature list

Features are specified in full in `requirements.md` (FR-IDs) and sequenced in
`implementation-plan.md` (phases). Summary:

### 5.1 Engine (existing → hardened)

- **FE-1** Package `snlg` as an installable distribution; add `snlg.run`
  façade with progress + artifact-sink hooks.
- **FE-2** Lock determinism: same config + seeds + engine version ⇒
  byte-identical scalar metrics (golden test).
- **FE-3** Fix known gaps (see `engine-reference.md` §7): config parser
  ignores several documented YAML keys (`arealimit`, `garage`, `heatpump`,
  `electric_vehicle`, `solar_pv`, `require_data_complete`); `scenario.name`
  as a YAML list leaks a `["..."]` string; `derive_run_rng` adds `run_index`
  to a seed (collision-prone); coincidence/diversity factors are computed
  but never attached to `NeighborhoodResult`; `feasibility.py` imports an
  unused symbol; timestamped output dir uses raw `str(time.time())`.
- **FE-4** Structured logging + a typed `RunProgress` event
  (`stage`, `run_index`, `n_feasible`, `n_attempted`, `converged`, `message`).
- **FE-5** `snlg` CLI (`python -m snlg run --config path.yaml --out dir/`)
  preserved for headless/parity use.

### 5.2 Backend API + worker

- **FB-1** Health/readiness endpoints.
- **FB-2** Config CRUD + validate + clone + YAML import/export + versioning.
- **FB-3** Scenario templates (seed the "DC Multifamily" baseline as a
  read-only template).
- **FB-4** Run lifecycle: create (enqueue) → `queued` → `running` →
  `succeeded` / `failed` / `cancelled`; cancel endpoint; list with filters;
  detail with progress + tail-of-log; SSE/websocket progress stream.
- **FB-5** Results API: per-run scalars, ensemble summary (diversity
  metrics, convergence history, validation report), artifact index +
  presigned download URLs, envelope/series JSON for charts.
- **FB-6** RQ worker: consumes jobs, runs `snlg.run`, streams progress rows,
  uploads artifacts to MinIO, writes metadata, honors cancellation.
- **FB-7** Alembic migrations for every schema change; `make db-upgrade`.
- **FB-8** Object-storage client abstraction (`Storage` protocol) with a
  MinIO/S3 impl and a local-fs impl for unit tests.

### 5.3 Frontend

- **FF-1** App shell (nav, health indicator, API-token entry).
- **FF-2** Config list + create-from-template + editor (form + raw-YAML
  tab) with debounced server-side validation and inline error display.
- **FF-3** Run launcher (pick config, override seeds/`max_runs`, submit) and
  runs table with live status.
- **FF-4** Run detail: progress bar, streamed log, cancel button.
- **FF-5** Results dashboard: P10/P50/P90 hourly envelope, peak-demand
  histogram, annual-energy histogram, load-duration curve(s), convergence
  history, representative-cases table, validation panel, artifact downloads.
- **FF-6** Compare view: two runs' envelopes + scalar deltas.

### 5.4 Data & infrastructure

- **FD-1** `deploy/docker-compose.yml` (web, api, worker, db, redis, minio,
  minio-init) + per-service Dockerfiles + `.env.example`.
- **FD-2** `Makefile` targets: `up`, `down`, `test`, `test-api`,
  `test-web`, `db-upgrade`, `db-revision`, `seed`, `fmt`, `lint`.
- **FD-3** OEDI input-fetch: `scripts/download_oedi.py` (manifest-driven
  `aws s3 sync` / HTTPS) producing `data/input/{resstock,comstock}/...` and
  `data/background/*.xlsx`; documented in `infrastructure.md` §7.
- **FD-4** CI workflow: lint + engine tests + api tests + web tests +
  compose smoke test on every push/PR.
- **FD-5** Seed script: create the baseline template config + a MinIO
  bucket + run one tiny ensemble end-to-end as a smoke test.

---

## 6. Requirements traceability

Every FR/NFR in `requirements.md` carries a stable ID and lists the test
file(s) that verify it. `test-plan.md` inverts the mapping (test → FRs).
The implementing agent keeps both current; a requirement with no green test
is "not done."

---

## 7. Success metrics / acceptance for v1

- **A1** `pip install ./snlg && python -c "import snlg"` works; `pytest`
  (engine) green from repo root with **no `PYTHONPATH` override**.
- **A2** `docs/engine-reference.md` covers 100% of public `snlg` symbols;
  reviewed by the owner.
- **A3** `docker compose up` yields a browsable app at `http://localhost:3000`
  and API docs at `http://localhost:8000/docs` within 90s on a clean
  checkout.
- **A4** UJ-1 (the core user journey, §3) completes end-to-end against a
  **small fixture dataset** (≤5 buildings, `max_runs=6`) in CI in under
  3 minutes.
- **A5** Determinism golden test (FE-2) passes; re-running the same config
  reproduces stored scalar metrics exactly.
- **A6** Coverage gates met: engine ≥ 90% line, api ≥ 85%, web critical
  components ≥ 80% (see `test-plan.md` §6).
- **A7** A cancelled run reaches `cancelled` within 5s and uploads no
  partial artifacts (or clearly marks them partial).
- **A8** All Alembic migrations run forward and backward cleanly on an empty
  DB.

---

## 8. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Real ResStock/ComStock data is large (GBs) and gated behind OEDI | Devs/CI can't run the full pipeline | Ship a tiny committed fixture dataset (`tests/fixtures/data/`) exercised by an `integration`-marked path; OEDI fetch is opt-in for real runs |
| Engine determinism is fragile (`derive_run_rng` seed arithmetic, float ordering) | Results not reproducible → G5/A5 fail | FE-2 golden test first; if it fails, fix `rng.py` to use `SeedSequence.spawn` before building anything on top |
| `snlg` not currently installable (tests need `PYTHONPATH=src`) | Blocks FE-1 and CI | First task in Phase 1; add `snlg/pyproject.toml`, `pip install -e`, `pythonpath` in pytest config |
| Long ensembles exhaust worker memory (300 runs × 8760 × many buildings, `lru_cache` on parquet) | Worker OOM-killed mid-run | Stream per-run artifacts and release; cap `lru_cache`; document worker memory sizing; expose `max_runs` guardrail |
| Two config schemas drift (Pydantic vs `snlg.config` dataclasses) | Validation says OK, engine rejects | Single source: generate/derive the Pydantic model from the dataclasses; contract test asserts round-trip parity |
| Harvesting `feat/*` code pulls in the `microgrid_profiles` engine by accident | Re-forks the source of truth | Explicit allowlist in `implementation-plan.md` of which `feat/*` files may be copied (API/DB/compose/frontend only) |
| Scope creep into auth/k8s | v1 slips | §2.2 non-goals are binding; defer with a note, don't build |

---

## 9. Open questions (non-blocking; default in brackets)

- OQ-1 Retention policy for old run artifacts? [keep all; add a manual
  `DELETE /runs/{id}` that also purges MinIO objects]
- OQ-2 Should the UI allow uploading a custom parquet dataset? [no in v1;
  data is provisioned by `scripts/download_oedi.py`]
- OQ-3 Progress transport: SSE vs WebSocket? [SSE — simpler, one-way is
  enough; `frontend-spec.md` assumes SSE]
- OQ-4 Multi-config batch runs (sweep)? [deferred to v1.1; data model
  leaves room via nullable `runs.batch_id`]

---

## 10. Definition of done (v1)

All of §7 A1–A8 pass in CI; every FR in `requirements.md` maps to ≥1 green
test; `docs/engine-reference.md`, `architecture.md`, `api-spec.md`,
`data-model.md`, `frontend-spec.md`, `infrastructure.md` reviewed and merged;
`implementation-plan.md` Phases 1–6 complete; owner walks UJ-1 successfully.
