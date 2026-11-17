# Documentation index

The current contract is [`PRD.md`](./PRD.md) (v2). Where any other document disagrees with it, the PRD wins.
Superseded v1 documents are in [`archive/`](./archive/README.md) and are history only.

## Read these first, in order

| # | Doc | Read it for |
|---|---|---|
| 1 | [`PRD.md`](./PRD.md) | What we are building and why: requirements (FR IDs) with acceptance criteria, and a glossary. |
| 2 | [`domain-glossary.md`](./domain-glossary.md) | The project vocabulary. Use these exact terms in code, API and UI. |
| 3 | [`architecture-and-stack.md`](./architecture-and-stack.md) | Technology choices and system structure. |
| 4 | [`sprint-plan.md`](./sprint-plan.md) | The 30-day MVP plan. |
| 5 | [`engine-reference.md`](./engine-reference.md) | How the load engine (`src/microgrid/community_load/synthetic/`) works, module by module. Written early; check the code for current details. |

## Running and checking things

| Doc | Read it for |
|---|---|
| [`runbooks/local-stack.md`](./runbooks/local-stack.md) | Starting the Docker stack on your machine. |
| [`runbooks/inspect-my-data.md`](./runbooks/inspect-my-data.md) | One read-only command to inspect your local ResStock/ComStock data. |
| [`reference/results-schema.md`](./reference/results-schema.md) | The results JSON format. |
| [`reference/data-licenses.md`](./reference/data-licenses.md) | Data licenses and attribution (draft, mostly unverified). |
| [`reference/resstock-upgrade-measures.md`](./reference/resstock-upgrade-measures.md), [`reference/comstock-upgrade-measures.md`](./reference/comstock-upgrade-measures.md) | Owner-supplied lists of upgrade measures. |

## Project records

| Doc | Read it for |
|---|---|
| [`decision-log.md`](./decision-log.md), [`adr/`](./adr/README.md) | Decisions made and why. |
| [`traceability-matrix.md`](./traceability-matrix.md) | Requirement, tests, code. |
| [`sprints/`](./sprints/) | Sprint backlogs and progress logs. |
| [`handoff/session-handoff.md`](./handoff/session-handoff.md) | Where the project is right now and what comes next (source of truth for status). |
| [`agent-team.md`](./agent-team.md), [`agent-deployment-plan.md`](./agent-deployment-plan.md) | How the agent team works and how it is deployed. |

## Research and background

| Doc | Read it for |
|---|---|
| [`codebase-review.md`](./codebase-review.md) | Review of the codebase as of 2026-11-02. |
| [`research/footprint-sources-dc.md`](./research/footprint-sources-dc.md) | Building footprint sources for Washington, DC. |
| [`research/hosting-options.md`](./research/hosting-options.md) | Hosting options for a public demo. |
| [`research/echarts-spike.md`](./research/echarts-spike.md) | Whether ECharts can plot a year of 15-minute data. |
| [`prd-suite-4d.md`](./prd-suite-4d.md), [`prd-gap-analysis.md`](./prd-gap-analysis.md) | Pre-v2 PRD drafts (2026-10-27), kept as background; see the note at the top of each. |

## Archive

[`archive/`](./archive/README.md): the v1 PRD and eight v1 specs, superseded on 2026-11-03, with a table of what replaced each.
