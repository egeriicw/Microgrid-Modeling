# Community Energy Profiler (working title: Microgrid Planner)

Estimates the hourly and 15-minute electricity load of a community, with uncertainty, before any meters exist. It builds
many plausible neighborhoods by sampling real buildings from NREL **ResStock** (residential) and **ComStock**
(commercial), repeats until the average load converges, and reports the result as percentile envelopes. The first
region is Washington, DC. The goal is a web app (FastAPI + React) for microgrid planning, RFP support and policy
analysis; today the engine exists and the web platform is being built.

## Status (see `docs/handoff/session-handoff.md` for the live picture)
- **Built and tested:** the load engine (`src/microgrid/community_load/synthetic/`), a building-type registry, the
  GIS-to-engine bridge, strict configuration, and replayable random seeding. Tests: 192 passing.
- **Not built yet:** API, background worker, database, web UI, calibration module, Docker stack (specified in `docs/`,
  scheduled in `docs/sprint-plan.md`).
- **Goal:** a complete demo on the owner's machine in 30 days; a public live link is a stretch.

## Structure
```
src/microgrid/
  core/            LoadResult contract
  geo/             structures, neighborhoods, GeoJSON reading, use classification
  community_load/  registry (building types), categorize, sources, synthetic (the Monte-Carlo engine)
  der/ dispatch/ economics/   interfaces only
  study.py         chains neighborhood -> load -> DER
tests/  configs/scenarios/  notebooks/  docs/  api/ (scaffold)  web/ (scaffold)  deploy/
```
Dependencies point downward only (enforced by `tests/test_architecture.py`).

## Develop
```bash
uv run --extra dev pytest -q
uv run --extra dev ruff check src tests
uv run --extra dev ruff format --check src tests
uv run --extra dev mypy
```
The ResStock/ComStock data lives under `data/` and is git-ignored (it is large and stays on the owner's machine). Tests
use synthetic fixtures only.

## Documentation map
| Read | For |
|---|---|
| `docs/PRD.md` | What we are building and why (requirements with acceptance criteria) |
| `docs/architecture-and-stack.md` | Technology choices and structure |
| `docs/sprint-plan.md`, `docs/sprints/` | The 30-day plan and sprint records |
| `docs/agent-team.md`, `.claude/agents/` | How the agent team works and its roles |
| `docs/decision-log.md`, `docs/adr/` | Decisions made and why |
| `docs/traceability-matrix.md` | Requirement -> tests -> code |
| `docs/codebase-review.md` | Honest review of where the project started |
| `docs/handoff/` | Resume instructions for a new session |
| `docs/archive/` | Superseded documents kept for history |

Older specs in `docs/` (api-spec, data-model, frontend-spec, infrastructure, implementation-plan, test-plan,
requirements, engine-reference) predate the new PRD and are being reconciled; where they disagree, the PRD wins.
