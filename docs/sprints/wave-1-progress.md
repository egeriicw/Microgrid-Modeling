# Wave 1 Progress: close the thin slice with real parts (started 2026-11-17)

Plan: `docs/agent-deployment-plan.md` section 6. Owner approved the ratings and "files for results" on 2026-11-17.
Builders work in separate worktrees from `main` (PR 11 merged) and commit on their own branches; the lead integrates
on `integration/wave-1`.

## Batch 1 (running in parallel)
| Story | Role | Model | Rating | Reason for rating | Review | Status |
|---|---|---|---|---|---|---|
| W1-WEB: generated typed client, real API wiring, API-key login, replay with same seeds | `frontend-engineer` | Opus | Standard | Typed contract, tests and CI catch errors | QA on Opus | Building |
| W1-DEPLOY: real worker and web services in compose; smoke test in CI | `devops-engineer` | Opus | Standard | The CI run is the proof | QA on Opus | Building |
| W1-FILTERS: stored, versioned exposed-attribute config; admin endpoints; pool preview | `backend-engineer` | Opus | **Critical** | Authorization boundary and a migration | QA and security review on Opus, then lead full-diff read | Building |
| W1-TEMP: temperature on results; empty `weather` module | `modeling-engineer` | Opus | Standard (must not change existing numbers; stop if it would) | Adds data only | QA on Opus | Building |

## Batch 2 (after W1-FILTERS lands, because both add database migrations)
| Story | Role | Model | Rating |
|---|---|---|---|
| W1-DATA: characteristics store (FR-DATA-7) and meter placeholder (FR-DATA-3) | `data-engineer` | Opus | **Critical** (migrations) |

## Decided, not delegated
Results storage (ADR-004): file-backed for the MVP, owner decision. Recorded by the lead; no architect run needed.

## Contract fixed between W1-WEB and W1-DEPLOY
Real client calls same-origin `/api/v1` (`VITE_API_BASE_URL` overrides); `VITE_API_MODE=mock` selects the mock; the web
container serves the Vite build and proxies `/api/` to `api:8000` including Server-Sent Events.
