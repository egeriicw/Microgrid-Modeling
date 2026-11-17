# Wave 0 Progress: one trunk, real CI (2026-11-17)

Plan: `docs/agent-deployment-plan.md` section 6. Lead session plus three delegated stories.

| Story | Role | Model | Rating | Result |
|---|---|---|---|---|
| Enable `api` and `web` CI jobs; blocking image build; permission allowlist | `devops-engineer` | Opus | Standard | Done. Lead read the workflow diff and re-ran the root suite. |
| README, docs index, archive banners after the v1 move | `tech-writer` | Sonnet | Routine | Done. Lead checked every link and the README claims. |
| Traceability matrix from evidence | records-keeper brief | Sonnet | Routine | Done. Lead spot-checked; FR-ENG-2 lowered to Partial. |
| Agent files (model, write scope), two new roles, `CLAUDE.md`, plan correction | Lead | n/a | Routine | Done. |

No critical stories; no reviewer escalation.

## Measured
- Local: root 950 passed, 3 skipped; ruff, format, mypy clean. API 231 passed, 3 skipped, 27 expected failures. Web 97 passed; typecheck, lint, build clean.
- GitHub run 46 (commit `d72fb01`): lint, engine, api, web, compose-config all green. In the `api` job the three Postgres-only tests run (234 passed per the builder's local simulation; not read from the GitHub log). The API image builds on GitHub with the step blocking.
- API coverage 88% (builder's measurement, not re-measured by the lead).

## Correction
The hand-off said GitHub CI had never been observed. It had run 42 times and passed, but only lint, engine and the compose file check; the API and web suites first ran on GitHub in this wave.

## Still unverified
Containers started and healthy (image builds, never run) · real Redis with a live worker · anything on the owner's real data · macOS · web app against the real API · the new cloud environment's setup script in a fresh session.

## Open for the owner
Merge of the Wave 0 pull request · PRs 2 to 8 (close as superseded?) · section 3.1 and section 9 of the deployment plan.
