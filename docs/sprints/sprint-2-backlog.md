# Sprint 2 Backlog (draft, 2026-11-11)

Carries the Day-14 checkpoint (about 2026-11-20). Stories reference `docs/PRD.md` FR IDs. Each follows the process in
`CLAUDE.md` ("Working with sub-agents"): parallel build -> independent QA with mutation checks -> fix pass -> lead integration.

## A. Carry-overs from Sprint 1 (found by QA, deliberately deferred)
| # | Story | Source |
|---|---|---|
| A1 | HTTP run endpoints (`POST /runs`, `GET /runs`, `GET /runs/{id}`, SSE events, `POST /runs/{id}/cancel`) that build the `data` section SERVER-SIDE (`server_side_data_section`), never from the client, with containment checks; roles enforced server-side | QA 1.10 trust boundary |
| A2 | Stuck-run reaper: mark `running` rows older than the hard limit plus margin as `failed` (building block `find_stale_running` exists) | QA 1.10 |
| A3 | Real Redis + live `celery worker` test (compose worker service, flip `SMOKE_REQUIRE_WORKER`, `/ready` Redis check against a real broker) | Unverified in 1.10 |
| A4 | API for filters: DB-stored, versioned exposed-attribute configuration; admin edit endpoints; saves must call `validate_filter_edit` with the stored composition; selections only through `ExposedAttributes.to_filters`, never raw columns | QA 1.6b |
| A5 | Split `upgrade`/`state` per product in `DataConfig` if real partition ids differ (owner decision pending) | QA 1.6b |
| A6 | Notebook example on the synthetic fixture (public demo-data helper + headless runner) | Story 1.9 AC |
| A7 | Catalog: fill `manifest_sha256`/`size_bytes` and `verified` from the owner's real data (inspector output) | 1.11b |
| A8 | Run `check_against_data` / `discover_candidates` on the real DC characteristics file; fix exposed-attribute choices that match nothing | QA 1.6b |

## B. Planned Sprint 2 stories (from `docs/sprint-plan.md`)
Adaptive convergence + `max_runs` + configurable criteria (ENG-6, needs owner-approved rule), results model with splits and
percentiles at hourly and 15-minute resolution (ENG-5/7), meter hierarchy placeholder (DATA-3), temperature series attached
to results (ENG-10) and weather module interface (ENG-13), compact profile library + parity test (DATA-9), React shell with
typed API client and composition form (WEB-1/2), run screen with progress and cancel (WEB-2), auth with three roles (WEB-5),
characteristics store in Postgres with generated data dictionary (DATA-7), hosting decision using measured resource use
(PLAT-6), release checklist (`docs/release-checklist.md`).

## C. Owner inputs that unblock work
1. Run the data inspector on the Mac and paste the report (answers real column names, `completed_status` values, building-type
   names, release evidence, partitions).
2. Lite Docker check on the Mac (see `docs/runbooks/local-stack.md`, 'Small disk').
3. Decisions: gross/net mixing policy; require verified datasets for non-demo runs; shared vs per-product `upgrade`/`state`;
   civic building mapping; footprint source and CAMA acceptable; sell data/exports or only results; TimescaleDB needed on a
   public server; who holds the cloud account; whether to keep the `PREFLIGHT_MIN_FREE_GB=0` escape hatch; perturbations (ENG-9).
