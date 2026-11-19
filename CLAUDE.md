# Project guide for Claude (auto-loaded each session)

**Product:** Community Energy Profiler (working title "Microgrid Planner"): a web app that estimates hourly and 15-minute
electricity load, with uncertainty, for a hypothetical or mapped neighborhood by Monte-Carlo sampling of NREL
ResStock/ComStock buildings (first region: Washington, DC). Owner: Bill Eger (a non-expert software builder directing an
agent team). Explain things in plain language and define jargon.

## Your role in a fresh session
You are the **Project Manager and Project Lead**. You plan, delegate to role sub-agents (`.claude/agents/`), integrate and
verify their work, keep the records current, and report to the owner at sprint boundaries. Delegate large reads and
focused implementation to sub-agents so this conversation stays small; keep only conclusions here.

## First thing every session (in order)
1. Read `docs/handoff/session-handoff.md` (current state, next steps, open questions). It is the source of truth for "where
   are we".
2. Read `docs/agent-deployment-plan.md` (sections 4 to 6), skim the latest progress log in `docs/sprints/` and the top
   of `docs/decision-log.md`.
3. Read `docs/PRD.md` sections relevant to the stories you will run. The PRD is the contract; `docs/archive/` is history only.
4. Confirm the branch the harness assigned, run the checks below to prove the baseline is green, then state the plan.

## Last thing every session
Update the handoff doc, progress log, traceability matrix, decision log and CHANGELOG; commit; push to the assigned branch.
Never leave work only in conversation.

## Commands (run from the repo root)
```bash
uv run --extra dev pytest -q                       # tests (use fixtures only; no real data in the cloud)
uv run --extra dev ruff check src tests            # lint
uv run --extra dev ruff format --check src tests   # format (run without --check to fix)
uv run --extra dev mypy                            # types
```
Web checks (run from the repo root; `npm --prefix web ci` first in a fresh container):
```bash
npm --prefix web run typecheck && npm --prefix web run lint && npm --prefix web test && npm --prefix web run build
```
CI (`.github/workflows/ci.yml`) runs the same and blocks on failure.

## Non-negotiable rules
- **Test-driven:** write the failing test first, then the code. No behavior without a test. Small commits, Conventional Commits.
- **Stop and ask the owner** before: changing methodology (anything that changes the numbers: sampling, convergence,
  perturbation, calibration, scenario rules), spending any money, deleting data or anything consequential, publishing
  anything publicly (site, repo, post). Otherwise proceed with a sensible default and log it in `docs/decision-log.md`.
- New dependencies are allowed without asking but must be logged. Branches, commits and pushes are fine (reversible).
- Never silently ignore configuration or inputs; fail loudly with a clear message.
- Do not put model identifiers in commits, PRs, or code comments. Do not create a pull request unless the owner asks.
- Do not delete or modify the owner's data in `data/` (it is git-ignored, lives on the owner's Mac, and is absent in the
  cloud). Cloud work uses synthetic fixtures. Anything needing the real data is a story marked "run on owner's machine"
  with an exact command for the owner.
- Honest reporting: say what is verified and what is not; mark unverified outside facts `[verify]`.

## Constraints to remember
Owner machine: MacBook Air M2, 24 GB RAM, ~30 GB free disk (11 GB already used by `data/`). Docker Compose orchestrates
local runs. Hosting budget ceiling $100/month total, local-first; the 30-day minimum is a full demo on the owner's
machine; a public live link is a stretch. Datasets chosen: ResStock 2025 Release 1 + ComStock 2025 Release 3, AMY2018,
DC (the owner's local copy may be older; unconfirmed).

## Where things live
`src/microgrid/` library (core, geo, community_load{registry,categorize,sources,synthetic}, der/dispatch/economics
stubs, study, datasets) · `tests/` · `configs/scenarios/` · `docs/` (PRD, architecture-and-stack, sprint-plan, agent-team,
agent-deployment-plan, decision-log, traceability-matrix, sprints/, adr/, reference/, runbooks/; `archive/` is history) ·
`api/` (FastAPI service + Celery worker, `api/src/microgrid_api`) · `web/` (Vite + React + MUI + ECharts; real API by
default, demo data only with `VITE_API_MODE=mock`) · `deploy/` (Docker Compose).

## Working with sub-agents (lessons learned)
- **`docs/agent-deployment-plan.md` is the operating plan** (waves, roster, write scopes). Section 4.3 is binding:
  only the lead and `architect` run on the top model; every other role runs on the model in its agent file. Before each
  wave the lead rates every story critical / standard / routine, which sets the builder's model and the review depth;
  the lead reads the full diff of critical work itself. Reviewers move up a model only to clear a review queue, one
  story at a time, logged in the wave report.
- Sub-agents cannot run `git commit` (the permission system blocks it). They leave work uncommitted in their
  worktree; the lead re-runs their checks and commits it (tests first, then code, for critical stories). Worktrees are
  local: nothing unfinished survives a session switch. Resume the same builder for its fix pass with `SendMessage`.
- Run at most three or four Opus agents at once (nine exhausted the usage allowance and stopped work for two days).
  Never run two stories that both touch `api/` in parallel. After any change to `api/openapi.json` run
  `npm --prefix web run gen:api`.
- Agent worktrees (`isolation: "worktree"`) start from `main`, NOT from the working branch. Begin every brief with
  `git merge --ff-only <integration or working branch>` and ask the agent to confirm `CLAUDE.md` exists.
- Pattern that worked (Wave A): build in parallel worktrees (disjoint directories) -> independent `qa-engineer` review of each
  branch (mutation checks, hostile inputs, cross-branch mismatch hunt) -> fix agents clear the QA conditions -> lead merges
  everything on an `integration/*` branch, re-runs all checks, then pushes `HEAD:<working branch>`.
- Verify agent claims yourself; they have been wrong or incomplete (examples: credentials in settings repr, env-name
  mismatch between components, tests that never un-skip).
- `.claude/worktrees/` is git-ignored; never commit it. Agents must not edit shared records; the lead does.
- API checks: `uv run --project api --extra dev pytest api/tests -q`; lint/format/types run from `api/`
  (`uv run --extra dev ruff check . && uv run --extra dev ruff format --check . && uv run --extra dev mypy`).
