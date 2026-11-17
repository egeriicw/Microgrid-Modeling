# Architecture Decision Records

One short file per consequential decision, numbered. Template: context, decision, alternatives, consequences, status
(proposed / accepted / superseded), who approved. Autonomous low-consequence choices go in `../decision-log.md` instead.

| ADR | Title | Status |
|---|---|---|
| 001 | Library-centered architecture with a single `run_scenario` facade | Proposed |
| 002 | Building-type registry as data | Proposed |
| 003 | Meter hierarchy placeholder | Proposed |
| 004 | Timescale hypertables for interval results; Parquet for raw inputs | Proposed |
| 005 | Celery + Redis for background jobs | **Accepted** (owner, 2026-11-06) |
| 006 | Convergence criteria | Accepted provisionally (owner, 2026-11-06) |
| 007 | UI kit (MUI) and charts (ECharts) | Accepted as agent default, reversible |
| 008 | Hosting and demo isolation | Open: local first, $100/month ceiling |
| 009 | Versioning scheme | Proposed |
| 010 | Footprint data source and licensing | Open: owner approval needed |
| 011 | Dataset catalog and scenario identity | Proposed |
| 012 | Compact building-profile library | Proposed |
