# 30-Day Sprint Plan — MVP

Companion to [`PRD.md`](./PRD.md), [`architecture-and-stack.md`](./architecture-and-stack.md), [`agent-team.md`](./agent-team.md).
Start date: **Day 1 = the day the owner approves this plan.** Four weekly sprints. Riskiest work first so we learn by
Day 14 whether the date holds. Estimates are rough; the PM re-forecasts at every review.

## Sequencing logic
Nothing is demonstrable until **data → engine → API → UI** works end to end on one tiny path. So Sprint 1 builds a thin,
real vertical slice (even if ugly), and later sprints widen it. Items most likely to slip, in order: map (FR-WEB-7),
tuning (FR-CAL-4, already LATER), admin console (FR-WEB-9), public repo (FR-PLAT-7), public hosting polish.

## Sprint 0 — Pre-flight (Days 1–2, owner + PM)
- Owner answers/approves: O1 (run `du -sh data/` and confirm DC files exist), O3 proposed convergence default (can be
  provisional), O4 license intent, O5 hosting/budget ceiling, product name (O8).
- Owner approves ADR-005 (Celery/Redis) and the UI-kit spike plan.
- PM creates board, labels, PR template, `docs/decision-log.md`, traceability matrix skeleton.
- Make the DC data available to agents: either a small fixture subset (preferred) or a documented local-only path.

## Sprint 1 — Foundation and the thin slice (Days 1–7)
**Goal:** one DC composition runs from a notebook *and* from an API call through the worker and lands in the database.

| # | Story | FR | Owner role |
|---|---|---|---|
| 1.1 | Fix packaging: dependency cleanup (`pytest`, `ipykernel`, `dataclasses`, unused `boto3`/`pvlib` moved or removed), Python version alignment, `pip install -e .` | PLAT-3 | Architect/Backend |
| 1.2 | CI that blocks on lint, types, tests; coverage gate | PLAT-5 | DevOps/QA |
| 1.3 | Strict config: reject unknown keys; implement or remove ignored keys; fix `scenario.name` list bug | ENG-3 | Modeling |
| 1.4 | RNG with `SeedSequence.spawn`; determinism golden test | ENG-4 | Modeling/QA |
| 1.5 | Building-type registry incl. single-family detached/attached, institutional, civic; categorize rewired to registry | ENG-1 | Modeling |
| 1.6 | Data ingestion script + manifest + fixtures (≤5 buildings per type); load characteristics tables to Postgres with data dictionary | DATA-1/2/7 | Data |
| 1.6b | Attribute-filter engine: filter language, attribute discovery, admin-configured exposed-attribute list (starter set), pool preview with small-pool warning (tests first on fixtures) | ENG-11 | Modeling/Backend |
| 1.7 | Docker Compose: Postgres+PostGIS+TimescaleDB, Redis, api, worker; one-command up; disk-budget check | PLAT-4, DATA-4 | DevOps |
| 1.8 | FastAPI skeleton `/api/v1`, health/ready, SQLAlchemy models, first Alembic migrations (up/down tested) | PLAT-1 | Backend |
| 1.9 | Engine facade `run_scenario(config, progress=cb)` + typed progress events | ENG-8 | Modeling |
| 1.10 | Worker task runs the facade, writes progress and summary metrics (no UI yet) | PLAT-2 | Backend |
| 1.11 | Performance baseline on owner's data/hardware (run time vs buildings × variations; memory) | NFR | Modeling/QA |
| 1.11b | Dataset catalog + data dictionary per release; confirm what `upgrade=0` and other upgrade ids mean in the release(s) the owner holds; engine takes dataset ids | DATA-8 | Data/Modeling |
| 1.12 | Spikes: ECharts with a 35k-point series (UI kit decided: MUI); footprint source evaluation; free static-hosting options | ADR-007/008/010 | Frontend/Data/DevOps |

**Exit criteria:** `make up` works; a fixture composition runs through API→worker→DB with progress rows; notebook example
calls the same facade; determinism test green; baseline timing recorded; decision log has Sprint-1 entries.
**Owner review:** demo via curl/notebook, decision log, baseline timings.

## Sprint 2 — Real runs, results, and the first UI (Days 8–14)
**Goal:** a user can create a composition in the browser, run it, and watch progress.

| # | Story | FR |
|---|---|---|
| 2.1 | Adaptive convergence + `max_runs` + configurable criteria (provisional defaults, tests on synthetic ensembles) | ENG-6 |
| 2.2 | Results model: residential/commercial/type splits, P10/P50/P90, 15-min and hourly, diversity factors attached | ENG-5/7 |
| 2.3 | Meter hierarchy placeholder (building→class→total) persisted | DATA-3 |
| 2.4 | Temperature series attached to results; weather series ingested/catalogued per dataset; empty `weather` module with interface | ENG-10, ENG-13 |
| 2.5 | Load results in Timescale hypertable; results endpoints | DATA-4 |
| 2.6 | Cancel run; SSE progress; run list/detail | WEB-2 / PLAT-2 |
| 2.7 | React app shell, typed API client from OpenAPI, login placeholder | WEB |
| 2.8 | Composition form with live validation, save/clone, YAML import/export, per-category filter builder with pool preview | WEB-1, ENG-11 |
| 2.9 | Run screen with progress bar, stage text, convergence status, cancel | WEB-2 |
| 2.10 | Auth: users table, JWT, three roles enforced in API | WEB-5 |
| 2.11 | Hosting evaluation for the $0 live demo (resource needs vs free options); recommendation at the Day-14 checkpoint; static fallback design | PLAT-6 |
| 2.12 | Compact building-profile library build step + parity test against raw files | DATA-9 |

**Week-2 checkpoint (Day 14):** PM reports status against the success criteria and gives a go / cut / slip
recommendation. Likely cut candidates: map, admin console, repo publishing. **The owner decides.**
**Exit criteria:** from the browser on the owner's machine, define composition → run with progress → completed run stored.

## Sprint 3 — Charts, downloads, calibration (Days 15–21)
**Goal:** the core demo is complete and trustworthy.

| # | Story | FR |
|---|---|---|
| 3.1 | Time-series chart residential vs commercial with envelope; hourly↔15-min toggle | WEB-3 |
| 3.2 | Load-duration curves | WEB-3 |
| 3.3 | Seasonal average daily profiles by load type | WEB-3 |
| 3.4 | Composition chart + table | WEB-3 |
| 3.5 | Hover with load and temperature; zoom/pan | WEB-3 |
| 3.6 | CSV downloads with metadata header | WEB-4 |
| 3.7 | Calibration: upload, validation, overlay, NMBE/CV(RMSE)/peak/energy errors, band coverage | CAL-1/2 |
| 3.8 | Benchmark set (small curated), annual EUI/shape checks per type | CAL-3 |
| 3.9 | Accuracy caveat component everywhere results appear | WEB-8 |
| 3.10 | Scientific test suite: conservation, known-answer, convergence, benchmark-range | §8 |
| 3.11 | Validation report v0: what was compared, results, honest limitations | QC |
| 3.12 | e2e happy-path test (compose → run → charts → CSV → calibration) | QA |

**Exit criteria:** the PRD §10 criteria 1–2 hold on the owner's real DC data. **Owner review:** hands-on session.

## Sprint 4 — Demo account, public deployment, hardening (Days 22–30)
| # | Story | FR |
|---|---|---|
| 4.1 | Demo role: predefined compositions + precomputed runs; locked-down endpoints | WEB-5 |
| 4.2 | Admin: manage demo compositions (minimal) | WEB-9 |
| 4.3 | Minimal map viewer: DC footprints, select area → fixed counts (**first to cut**) | WEB-7 |
| 4.4 | Public demo deployment with live capped computation (or static fallback); caps, rate limits, kill switch; owner approves publishing | PLAT-6 |
| 4.5 | Security review and fixes (auth, uploads, CORS, headers) | NFR |
| 4.6 | Performance and resource pass (disk, memory, query speed) | NFR |
| 4.7 | Data licenses/attribution displayed in-app; per-dataset review | NFR |
| 4.8 | Documentation pass: README from clean clone, user guide, architecture, ADR index, example notebooks tested | PLAT-7 |
| 4.9 | Public repo readiness (license decision, history scan for secrets/large files) — **publish only on owner's command** | PLAT-7 |
| 4.10 | Release `v0.1.0`, changelog, tag; final validation report; demo script for the owner's post | — |

**Exit criteria:** PRD §10 met (items 5–6 as achievable). **Owner review:** go/no-go on publishing.

## Metrics tracked each sprint
Stories done vs planned · test count and coverage · CI pass rate · open risks · decisions awaiting owner · run-time
baseline · disk used.

## Risk watch list (PM maintains)
Data availability (O1) · convergence definition (O3) · hosting cost/limits (O5) · Docker disk use · ResStock/ComStock
column/schema surprises on real data · ingestion size · front-end chart performance on 35k points · single-owner review
bandwidth.

## After the MVP (backlog, unordered)
Weather normalization (Kissock change-point) · scenarios (EV, heat pump, efficiency, solar via upgrades) · full GIS
workspace and network (one-line diagram) · capacity checks, power flow · DER sizing and dispatch, REopt/SAM adapters ·
economics (PJM markets) · policy-constraint checker · submeters/VPP/aggregate meter · campus multi-meter · SEED
coupling · calibration tuning and PJM node data · multi-region data · Parquet export · admin/billing · AWS scale-out.
