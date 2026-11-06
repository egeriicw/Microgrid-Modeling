# Sprint 0 — Pre-flight (Day 1–2)

**Goal:** everything needed so Sprint 1 starts without open questions: working structure, approved decisions, and a known
state of the owner's data.

## Done (by the agent team, 2026-11-06)
- [x] PRD v2, architecture and stack, sprint plan, agent charter, decision log committed.
- [x] Old PRD archived at `docs/archive/` (not deleted).
- [x] Pull request template and story template in `.github/`.
- [x] ADR folder and index (`docs/adr/README.md`).
- [x] Requirements traceability matrix generated (`docs/traceability-matrix.md`, 43 requirements).
- [x] Sprint 1 backlog refined with acceptance criteria and test-first notes (`docs/sprints/sprint-1-backlog.md`).
- [x] Owner decisions recorded: convergence default, configurable data, Celery + Redis, MUI + ECharts, demo behavior,
      agent execution model, local-first, $100/month ceiling, Day-30 minimum vs stretch, datasets (ResStock 2025 R1 +
      ComStock 2025 R3, AMY2018).

## Owner actions still open (these gate Sprint 1 story 1.6 and 1.11b, not the whole sprint)
| # | Action | Why |
|---|---|---|
| A1 | On your Mac, in the repo: record the folder names under `data/` and total size (commands below) | Tells us which releases you hold, and whether DC data must be re-downloaded within your ~30 GB |
| A2 | Tell us whether the characteristics spreadsheets (`DC_upgrade0.xlsx`, `DC_upgrade0_agg.xlsx`) exist and which release they came from | The characteristics store and filters are built from them |
| A3 | Decide whether a small fixture subset of real DC buildings may be committed to the repo (public later), or kept private | Cloud agents can only test on fixtures; licensing to verify |
| A4 | Confirm the footprint-source choice later (agent will propose) | FR-DATA-6, not blocking |

```bash
du -sh data/
```
```bash
find data -maxdepth 4 -type d | head -40
```
```bash
ls data/background
```

## Risks carried into Sprint 1
Local data may be an older release than the chosen datasets · 30 GB disk budget · convergence rule untested on real data
· Docker image sizes · single-owner review bandwidth.

## Exit criteria
Backlog approved by the owner; A1–A3 answered or explicitly deferred with a fixture-only fallback. Sprint 1 then starts.
