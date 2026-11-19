# Session Handoff

**Written:** 2026-11-19 (Day 14 of 30), end of the session that ran Wave 0 and Wave 1.
**Next role:** Project Manager and Project Lead (see `CLAUDE.md`). Start the lead session on the top model; sub-agents
take their model from their agent file.
**Operating plan:** `docs/agent-deployment-plan.md` (waves, roster, write scopes, models, criticality rating). It is
binding. This file says where we are; the plan says how we work.

## 1. Where we are
- **`main` holds everything**: Wave 0 (PR 11) and Wave 1 (PR 12, merged 2026-11-19 on the owner's instruction).
  Start the next session from `main`, on whatever branch the harness assigns.
- **What works today** (on synthetic fixtures; nothing has run on the owner's real data yet): a Client signs in with an
  API key, defines a neighbourhood by building type (exact numbers or share ranges), runs it through the API, Redis
  and a Celery worker with live progress, cancels or replays it, and sees results with an uncertainty band. Admin API
  for attribute filters with pool preview. Temperature on results when a source is configured. Docker Compose stack
  with a real worker and web image; GitHub CI runs a smoke test that pushes a predefined run and a custom composition
  through the real containers.
- **Schedule:** Day 30 is 2026-12-06. Wave 1 finished about two days late (a usage-limit stop). The owner accepted
  the checkpoint cuts, so Waves 2 to 5 fit with no slack (lead's estimate, not a measurement).

## 2. Baseline (measured by the lead)
| Check | Result | When |
|---|---|---|
| Root tests | 1,219 passed, 3 skipped, 2 expected failures (engine count rules) | 2026-11-19, at hand-off |
| ruff check / ruff format --check / mypy | clean / 116 files formatted / clean, 53 files | 2026-11-19, at hand-off |
| API tests (SQLite) | 686 passed, 7 skipped (Postgres-only), 28 expected failures | 2026-11-19, `b2a9b40`; no code change since |
| Web | 443 tests passed; typecheck, lint, build clean | 2026-11-19, `b2a9b40` |
| API on Postgres 16 | 684 passed, 0 skipped | builder's run, not repeated by the lead |
| GitHub CI | green on six jobs incl. `compose-smoke` | run 63; check the latest run on `main` first thing |

Commands are in `CLAUDE.md`. API checks: `uv run --project api --extra dev pytest api/tests -q`; lint and types from
`api/`. Web: `npm --prefix web ci` first in a fresh container.

**Never verified anywhere:** the owner's real data (file names, columns, time convention, first-read cost of the
characteristics files); macOS and arm64 images; a browser driving the real containers (the smoke test uses curl; the
browser flow ran against a real API with an in-process worker); an adaptive run that actually converges, seen in the
app; Firefox, Safari, screen readers.

## 3. Owner decisions made on 2026-11-19 (all in `docs/decision-log.md`)
1. Day-14 checkpoint: **GO with cuts**. Cut: map viewer (FR-WEB-7), admin console screen (FR-WEB-9; admin API stays).
   Deferred: public live demo (FR-PLAT-6), compact profile library (FR-DATA-9).
2. Characteristics table in the database (FR-DATA-7) and meter entity (FR-DATA-3): **deferred past the MVP**.
3. Results stay **file-backed** for the MVP (ADR-004, decided 2026-11-17).
4. **Engine must reject** fixed counts above `total_buildings` and all-fixed counts that do not equal it.
5. **Convergence:** rule `prd_default` becomes the default when convergence is on (`legacy_window` stays available by
   name); a fixed-count run reports convergence as **"not checked"**, not converged. The rule's settings (3 passing
   checks in a row, 2% precision on the median profile, 1% change) stay provisional until tried on real data.
6. Demo users may see data file names in run detail.

## 4. In progress when the session ended
**ENG-COUNTS (approved, NOT in the repository).** A `modeling-engineer` agent was part-way through this in a local
worktree; that work is lost with the container. Restart it. Brief essentials:
- Rating: Standard, hard rule: no configuration that is valid today may change its numbers (golden file untouched;
  prove with a before/after comparison of full results).
- Make `validate_config` / `config_from_dict` (`src/microgrid/community_load/synthetic/config.py`) refuse fixed counts
  above the total and all-fixed counts not equal to it, exactly as strict as the API's
  `api/src/microgrid_api/services/compositions.py::check_category_counts` (same 1e-9 tolerance on fraction sums).
- Two strict expected-failure tests state the target: `tests/test_qa_w1_compose_engine_counts.py`. One API test calls
  the engine directly to show the oversampling (`api/tests/test_qa_w1_compose.py`, "really_oversamples_today"); it
  must be updated to assert the refusal.
- Sweep every config in the repository for ones the new rule would refuse. **Do not edit the owner's files in
  `configs/scenarios/`**; if one is affected (the old gap analysis noted `dc_multifamily_baseline.yaml` says 10
  buildings where its description says 50), report it and ask the owner.
- Then independent QA on Opus; lead reads the diff.

## 5. Next, in order
1. **ENG-COUNTS** (above).
2. **ENG-CONV** (owner-approved methodology change, rating **Critical**: QA on Opus plus the lead's full-diff read).
   Default convergence rule becomes `prd_default`; in fixed mode the engine reports convergence as not checked
   (`ensemble.py` currently sets `converged = True` for fixed mode). Consequences to handle: results schema gains the
   third state (bump the schema version and `docs/reference/results-schema.md`); `EnsembleResult.converged` typing;
   the API's stored summary and SSE payloads; `api/openapi.json` and the regenerated web types; the web already words
   `null` as "not checked". Fixed-count load numbers must not change (golden test). Adaptive runs with no rule named
   WILL stop at different points than before: that is the intended change; say so in the changelog with
   **[numbers change]**. The API's `composition` path writes the engine default into the snapshot, so new runs pick up
   the new default; check that replaying an OLD stored run still uses the rule stored in its snapshot.
3. **Wave 2** (deployment plan section 6): rate every story and post the wave plan to the owner before delegating.
   - First story: a browser test (Playwright) against the real containers in CI. This closes the Wave 1 exit
     criterion as one test. The mock e2e needs `PW_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium-1243/chrome-linux64/chrome`
     in the cloud container; on a GitHub runner use `npx playwright install --with-deps chromium`.
   - Charts on real results (FR-WEB-3): load-duration, seasonal daily, composition chart and table, hover with
     temperature; the full-year hourly chart is dense, consider a daily view by default. Load the `dataviz` skill.
   - CSV downloads with a metadata header (FR-WEB-4).
   - Results-model gaps (FR-ENG-7): coincidence and diversity factors, representative cases, composition summary,
     explicit weather year and calibration status in the results document.
   - Scientific test suite (PRD section 8): conservation, known-answer, convergence, benchmark range.
   - Small API items: title and description per predefined composition; saved compositions (also closes QA D1:
     Admin-filtered compositions cannot be offered to other roles); filter options endpoint if a filter form is built.
4. Waves 3 to 5: calibration; demo role, security pass, documentation; real-data acceptance on the Mac and release.

## 6. Waiting on the owner (the largest schedule risk)
Nothing below has been done; ask in one batch, in plain language.
1. **Mac checks.** (a) If `deploy/.env` exists from Sprint 1, add the `API_KEYS` line shown at the top of
   `docs/runbooks/local-stack.md`, or every `make` command refuses to start. (b) Data inspector:
   `docs/runbooks/inspect-my-data.md`; paste the report. It answers the real characteristics file names
   (`MICROGRID_RESSTOCK_CHARS_FILE`, `MICROGRID_COMSTOCK_CHARS_FILE`; the defaults are the engine's old hard-coded
   names), where temperature lives and its column names, releases and partitions, and whether time stamps are local.
   (c) Lite Docker check: `docs/runbooks/local-stack.md`, "Small disk". (d) Free disk space or accept the lite stack
   for the demo. `/api/v1/ready` now says "not ready" when a named data file is missing: that is deliberate.
2. Still open from deployment plan section 9: gross/net load mixing policy; verified datasets required for non-demo
   runs?; civic building mapping; perturbations (FR-ENG-9: keep, fix or remove); run-folder retention and caps;
   repository licence and product name (only before publishing).
3. PRs 2 to 8 (January scaffold, obsolete): close as superseded, branches kept? Never answered.

## 7. How this session worked (keep doing; lessons)
- **Loop per story:** builder in its own worktree → independent `qa-engineer` (Opus) that is NOT given the builder's
  summary → for critical work also `security-reviewer` → one combined fix brief back to the same builder
  (`SendMessage` resumes it with its context) → lead re-runs every check, reads the diff (full diff for critical),
  merges, pushes, checks the GitHub run.
- **Sub-agents cannot `git commit`** (blocked by the permission system). They leave work uncommitted; the lead commits
  it after re-running the checks, tests first and code second for critical stories. Their worktrees are local and are
  lost with the container, so nothing unfinished survives a session switch.
- **Usage limits:** nine Opus agents in one session exhausted the five-hour allowance and stopped work for two days.
  Run at most three or four at once.
- **Two API stories at once collide** (same folder, migration numbers). Sequence them.
- **After any change to `api/openapi.json`**, regenerate the web types: `npm --prefix web run gen:api` (`npm test`
  fails otherwise).
- **GitHub CI is the Docker test bench.** It found three bugs no local test could: compose never passed the API keys,
  the web image build needs `web/.npmrc`, the image lacked `packaging`.
- Reviewer findings worth remembering: results files leaked server paths (fixed); builders' own tests miss about one
  safeguard in six (mutation checks catch them); the `ux-reviewer` (Sonnet) found real usability problems that tests
  could not, but did not actually test keyboard use, so ask QA for that.
- The lead skipped a separate QA review once (small deploy follow-up); it is logged as a deviation.

## 8. Read in this order
`CLAUDE.md` → this file → `docs/agent-deployment-plan.md` (sections 4 to 6) → `docs/sprints/wave-1-progress.md`
(what each story found) → top 25 rows of `docs/decision-log.md` → `docs/PRD.md` only for the FR IDs of the stories you
run. `docs/archive/` is history. `docs/sprint-plan.md` and `docs/agent-team.md` are the original 30-day plan and
charter; the deployment plan supersedes their schedule.

## 9. First message for the new session
See `docs/handoff/start-prompt.md`.
