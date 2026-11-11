# Session Handoff

**Written:** 2026-11-07, end of session 1 · **Branch:** `claude/loving-pascal-x8khbu` · **Next role:** you are the Project
Manager and Project Lead (see `CLAUDE.md`), with the sub-agent team in `.claude/agents/`.

## 1. One-paragraph situation
The owner (Bill Eger, non-expert builder) is turning a notebook-era prototype into a production web app for community
load profiling (Monte-Carlo sampling of NREL ResStock/ComStock buildings, DC first). In session 1 we interviewed the
owner, wrote the PRD v2, stack, sprint plan and agent charter, set up Sprint 0 (templates, ADRs, traceability), and began
Sprint 1 with engine hardening. The goal is a **30-day MVP demo running on the owner's machine under Docker Compose**
(Day 1 ≈ 2026-11-06, so Day 14 ≈ 2026-11-20 and Day 30 ≈ 2026-12-06; confirm the dates with the owner); a public live
link is a stretch.

## 2. Read in this order (do not re-read everything)
1. `CLAUDE.md` (rules, commands, constraints). 2. This file. 3. `docs/sprints/sprint-1-progress.md`.
4. `docs/decision-log.md`. 5. `docs/sprints/sprint-1-backlog.md` (story detail and acceptance criteria).
6. `docs/PRD.md` only for the FR IDs of the stories you run. 7. `docs/architecture-and-stack.md` when touching structure.
Everything in `docs/archive/` and the older specs (`api-spec`, `data-model`, `frontend-spec`, `infrastructure`,
`implementation-plan`, `test-plan`, `requirements`, `engine-reference`) is **stale or superseded**; use `engine-reference.md`
only as a description of the original engine, and plan to update or retire the rest (tech-writer + architect).

## 3. Baseline (measured 2026-11-11, end of Wave A)
- Root: `uv run --extra dev pytest -q` -> **360 passed, 1 skipped**. Engine coverage **90%** (weakest: `outputs.py` 16%,
  `snlg_source.py` 66%, `loader.py` 73%). Gate of 90% is **not enforced** (margin too thin).
- API: `uv run --project api --extra dev pytest api/tests -q` -> **49 passed, 6 skipped, 27 expected failures** (old placeholders
  for later stories: runs, worker, storage, configs, scenario validation). API ruff/format/mypy run from `api/`.
- ruff check, ruff format --check, mypy: clean. `docker compose config -q` passes for the base file and the CI overlay.
- NOT verified anywhere: Docker images (no daemon in the cloud), GitHub CI (never observed running), anything on the owner's real data.
- All work is on the branch `claude/loving-pascal-x8khbu`. Nothing is merged to `main`; no pull request exists.

## 4. Sprint 1 status
| Story | Status |
|---|---|
| 1.1 Packaging (FR-PLAT-3) | Done |
| 1.2 CI that blocks (FR-PLAT-5) | Done in repo; GitHub run unconfirmed. A `compose-config` job was added; api/web/compose-smoke jobs still `if: false` |
| 1.3 Strict configuration (FR-ENG-3) | Done (`require_data_complete` implemented per owner) |
| 1.4 RNG and replayable seeds (FR-ENG-4) | Done (golden + replay + seed-robustness tests run on the generated fixture) |
| 1.5 Building-type registry (FR-ENG-1) | Done (`civic` inactive pending the owner) |
| 1.6 Fixtures, catalog, manifest (DATA-1/2/8) | Done for the cloud part. Open: Postgres characteristics store (DATA-7), engine takes dataset ids, download/provisioning step, real-data timing |
| 1.7 Docker Compose (PLAT-4) | Code-complete, statically verified only. Needs the owner's Mac (section 6) |
| 1.8 API skeleton (PLAT-1) | Done; verified on SQLite and Postgres 16 |
| 1.9 Run facade + progress (ENG-8) | Done; notebook-on-fixtures acceptance item open |
| 1.6b Attribute filters, Admin-only (ENG-11) | **Not started** (dictionary groundwork exists: `describe_characteristics`) |
| 1.10 Celery worker task + cancel (PLAT-2) | **Not started** (unblocked: facade, `RunCancelled`, API, migrate step exist; add redis/storage to `/ready`, flip `SMOKE_REQUIRE_WORKER`) |
| 1.11 Performance baseline (owner machine) | Not started |
| 1.11b Dataset ids as engine inputs; catalog hash/size filled; confirm `upgrade=0` meaning | Not started (partly owner machine) |
| 1.12 Spikes (ECharts 35k points, footprint source, free hosting options) | Not started |

### Suggested next steps (Sprint 1 remainder, then Sprint 2)
1. Run the process that worked (see `CLAUDE.md` "Working with sub-agents"): parallel builds, independent QA, fix pass, lead integration.
2. **Parallel batch:** `backend-engineer` -> 1.10 (Celery worker, cancel, progress rows; add redis check to `/ready`);
   `modeling-engineer` + `backend-engineer` -> 1.6b (admin-only filters, permission flag `filters.use`);
   `data-engineer` -> 1.11b (dataset ids into the config schema, using the catalog); `frontend-engineer` -> 1.12 spikes.
3. After the owner's Mac check, record real figures (image sizes, timing) and fix what it finds.
4. Then Sprint 2 per `docs/sprint-plan.md`: convergence (ENG-6), results model (ENG-5/7), meter placeholder (DATA-3), React shell
   and composition form, auth roles, Day-14 checkpoint (about 2026-11-20).

## 5. Decisions already made (details in `docs/decision-log.md`)
Convergence rule (provisional yes) · data root and dataset choice configurable · hosting: local first, $100/month ceiling,
no spending without approval · Celery + Redis · MUI + ECharts · Demo role: predefined compositions, live capped runs,
fixed stored seeds by default · agents: lead session + role sub-agents · Day-30 minimum = local Docker demo, public link
stretch · datasets ResStock 2025 R1 + ComStock 2025 R3, AMY2018, DC · synthetic fixtures in CI · heavy industrial out of scope,
community-scale warehouses in scope (if ComStock defines them) · attribute filters are Admin-only for now (permission flag
`filters.use`) · "typical year" is really "2018 weather year".

## 6. Open items for the owner (ask in one batch, plain language)
1. **Civic buildings:** which ComStock type(s) represent government/fire/religious/public buildings?
2. **Local data release:** which ResStock/ComStock releases are in `data/`? (Command in `docs/sprints/sprint-0.md`.)
   Also run the `completed_status` check from the earlier discussion if not done.
3. **Footprint source** for DC (agent recommends after spike 1.12; owner approves).
4. **Convergence rule** was approved provisionally; revisit after first real runs.
5. **Perturbations** (FR-ENG-9): end-use scaling never runs in the engine; keep, fix, or remove? (methodology)
6. Repo license and product name (not urgent); confirm "CBEX" means CBECS.
7. Whether the owner wants the review cadence (sprint review at each sprint end + Day-14 checkpoint) unchanged.

### Owner tasks on the Mac (batch these into one request; exact commands are in `docs/runbooks/local-stack.md`)
Prerequisite: Docker Desktop for Apple silicon; copy `deploy/.env.example` to `deploy/.env` first.
`make image-check` (arm64 manifests and sizes; then pin the full db image tag) -> `make config-check` -> `make up`
(expect migrate to exit 0 and db, redis, web, api healthy; the worker is intentionally not started) -> `make smoke`
(expects `/api/v1/ready` to report ready with db ok and schema ok) -> `make disk` -> `make down`. Also still open from earlier:
`du -sh data/` details and the release folder names, and the `completed_status` check on your characteristics file.

## 7. Known issues and cautions
- Wave A findings worth remembering: components built in isolation disagreed on env-var names (caught by QA); cloud tests cannot start Docker; the golden file was generated on Linux and may need a tolerance tweak on the first Mac run.
- An earlier global rename left the phrase `microgrid.community_load.synthetic` inside ordinary prose in ~30 comments,
  docstrings and several docs. Cosmetic; `tech-writer` cleans it in a docs pass.
- Engine coverage is low where data is needed; do not "fix" it with mocks that hide behavior. Use synthetic fixtures.
- ComStock scenarios `gen_0001/0002`, `pkg_0008`, `pkg_0012` include PV/battery, so those loads are *net*; gross and net
  must never be mixed silently (see PRD FR-DATA-8).
- The web build tooling (Node) is not yet set up; `web/` and `api/` hold placeholder tests only. `api/` tests are skipped
  until the package exists.
- Free-form claims about NREL datasets in the docs are marked `[verify]`; do not remove the marker until checked.

## 8. How to run a story (the pattern)
1. Pick the story, restate its acceptance criteria. 2. Delegate to the named sub-agent with a self-contained brief (goal,
FR IDs, files, constraints, "tests first", "report format"). 3. Verify yourself: run the four checks and read the diff.
4. Have `qa-engineer` or another role review. 5. Commit, update records, push. Keep your own context small: ask sub-agents
for conclusions, not file dumps.

## 9. First message to send in the new session
See `docs/handoff/start-prompt.md`.
