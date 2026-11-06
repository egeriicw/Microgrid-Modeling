# Decision Log

Newest first. Agents add an entry for every autonomous decision (see `agent-team.md` §5). The owner reviews at each sprint review.

| Date | Decision | Alternatives | Reason | Reversible? | Status |
|---|---|---|---|---|---|
| 2026-11-03 | PRD v1 archived (not deleted) to `docs/archive/`; new PRD v2 written from owner interview | Overwrite | Owner asked to keep the old PRD as a historical artifact | Yes | Done |
| 2026-11-03 | Map demoted to SHOULD; load forecasting core is the success-or-failure item | Map as MUST | Owner priority statement | Yes | Owner-stated |
| 2026-11-03 | Start from scratch for web/DB layers (no recoverable `feat/*` scaffold); SQLAlchemy + Alembic kept | Reuse old scaffold | Not in any git ref; owner could not locate | Yes | Owner-stated |
| 2026-11-03 | Week-2 slip/cut rule: owner decides at checkpoint | Auto-cut | Owner did not specify; default chosen | Yes | Default |
| 2026-11-03 | New dependencies allowed without approval but logged | Require approval | Interpretation of owner statement; confirm | Yes | Confirm |
| 2026-11-03 | Celery + Redis recommended for jobs | RQ, Dramatiq, Arq | Maturity, cancel/progress support | Yes | Proposed (ADR-005) |
| 2026-11-06 | Dataset = (product, release, weather, upgrade, state); MVP baseline only; scenario switching designed (FR-ENG-12) but LATER | Hard-code upgrade=0 | Owner: scenarios like high EV/heat-pump penetration require switching datasets | Yes | Owner-stated; thresholds/method need owner approval |
| 2026-11-06 | MVP datasets: ResStock 2025 Release 1 AMY2018 + ComStock 2025 Release 3 AMY2018, DC only; "typical year" relabeled "2018 weather year" | Older releases; TMY3 sets (only older releases) | Newest, shared weather year, full measure lists; owner said use recommendations | Yes (re-ingest) | Owner-approved; agent verifies names/DC coverage/known issues |
| 2026-11-06 | Convergence default (FR-ENG-6) approved provisionally | Other rules | Owner: "agree" | Yes | Owner-approved |
| 2026-11-06 | Data root and dataset choice are configurable | Hard-coded paths | Owner request | Yes | Owner-stated |
| 2026-11-06 | Hosting budget $0; public demo proposed as static precomputed site | Always-on server | Owner budget; avoids cost/abuse | Yes | Proposed, needs owner OK |
| 2026-11-06 | Celery + Redis approved | RQ etc. | Owner: "right" | Yes | Owner-approved |
| 2026-11-06 | UI kit: MUI + ECharts (spike dropped) | shadcn/Tailwind | Owner unsure; agent default, fastest for forms/admin | Yes | Agent default under autonomy rule |
| 2026-11-06 | Demo account shows precomputed runs with fixed stored seeds | Fresh random per view | Owner: yes | Yes | Owner-approved |
| 2026-11-06 | Agent execution: lead session + role sub-agents on separate branches | One session per role | Cost, coordination | Yes | Recommended; owner asked what is recommended |
