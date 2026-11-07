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

## 3. Baseline (measured at hand-off)
- `uv run --extra dev pytest -q`: **192 passed, 2 skipped, 1 expected failure** (run facade, story 1.9). Coverage **77%**
  (gate of 90% not enforced yet; weakest: `ensemble.py`, `outputs.py`, `loader.py`, `archetype.py`: they need fixtures).
- ruff check, ruff format --check, mypy: clean. CI workflow is updated to block on these but **has not been observed
  running on GitHub**; confirm on the first push/PR (the owner can check the Actions tab).
- Last commits on the branch are the Sprint 1 stories below. Nothing is merged to `main`; no pull request exists.

## 4. Sprint 1 status
| Story | Status |
|---|---|
| 1.1 Packaging and dependency cleanup (FR-PLAT-3) | Done |
| 1.2 CI that blocks (FR-PLAT-5) | Done in repo; GitHub run unconfirmed |
| 1.3 Strict configuration (FR-ENG-3) | Done. `require_data_complete` implemented per owner ("include"): keeps `completed_status == "Success"` and `applicability == true`; column meaning to verify in the data dictionary |
| 1.4 RNG and replayable seeds (FR-ENG-4) | Done at engine level; **[numbers change]** vs before. Golden determinism + full-ensemble seed-robustness tests still pending (need 1.6 and 1.9) |
| 1.5 Building-type registry (FR-ENG-1) | Done. `civic` inactive (owner to decide ComStock mapping); ComStock type names to verify |
| 1.6 Data ingestion, synthetic fixtures, characteristics store (DATA-1/2/7) | **Not started** |
| 1.6b Attribute filters, Admin-only, admin-configured exposed list (ENG-11) | Not started |
| 1.7 Docker Compose (db with PostGIS+Timescale, redis, api, worker, web placeholder) | Not started |
| 1.8 API skeleton + Alembic migrations (PLAT-1) | Not started |
| 1.9 Engine facade `run_scenario` + typed progress events (ENG-8) | Not started (placeholder tests in `tests/test_run_facade.py`, `tests/test_determinism.py` are skipped) |
| 1.10 Celery worker task + cancel (PLAT-2) | Not started (needs 1.8, 1.9) |
| 1.11 Performance baseline on owner's machine | Not started (owner machine) |
| 1.11b Dataset catalog, scenario key, gross/net flag, confirm `upgrade=0` meaning (DATA-8) | Not started (partly owner machine) |
| 1.12 Spikes: ECharts on 35k points, footprint source comparison, free hosting options | Not started |

### Suggested order and parallelism for the next session
1. **In parallel:** `modeling-engineer` → 1.9 (facade + progress events, then the golden determinism and robustness tests);
   `data-engineer` → 1.6 (synthetic fixtures first, then the ingestion script and catalog 1.11b design).
2. **In parallel after 1:** `modeling-engineer` + `backend-engineer` → 1.6b (filters); `devops-engineer` → 1.7;
   `backend-engineer` → 1.8.
3. `backend-engineer` → 1.10 once 1.8 and 1.9 land. `qa-engineer` checks traceability and raises engine coverage toward 90%
   using the new fixtures. Spikes (1.12) by `frontend-engineer` / `data-engineer` any time there is slack.
4. **Owner-machine tasks** (give the owner exact commands, batch them into one request): 1.11, real-data parts of 1.6 and
   1.11b, confirming the release of the local data.
5. End with `/sprint-review`. Day-14 checkpoint: go / cut / slip recommendation to the owner.

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

## 7. Known issues and cautions
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
