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

---

## Batch 1 result (2026-11-19)
All four stories built in separate worktrees on Opus, independently reviewed on Opus, fixed, re-verified by the lead and
merged on the working branch. No reviewer escalation. Work stopped for two days (2026-11-17 to 19) when the Opus agents
hit a usage limit during the filters fix pass; the builder was resumed from its saved state.

| Story | Rating | Review verdict | Lead check | State |
|---|---|---|---|---|
| W1-TEMP temperature on results (schema 1.1), `weather` interface, per-building profiles | Standard | QA: pass with conditions; numbers confirmed unchanged across 12 scenarios; 5 defects fixed | Re-ran all checks; golden file untouched | Merged |
| W1-DEPLOY real worker and web in compose; smoke test in CI | Standard | QA: pass with conditions; 6 defects fixed; smoke now parses JSON, needs a live progress event and the CSP header | Read the container files; GitHub runs 55, 57, 58 green | Merged |
| W1-WEB typed client from OpenAPI, real API wiring, API-key sign-in, seeds and replay | Standard | QA: pass with conditions; exercised against a real API in a browser; 6 defects fixed | Re-ran web checks; accepted one corrected QA expectation | Merged |
| W1-FILTERS stored, versioned exposed-attribute configuration; admin endpoints; pool preview | **Critical** | Security: no critical, one HIGH (results file leaked server paths, older than this story). QA: pass with conditions, 54 mutations caught, migration verified on a populated database on SQLite and Postgres 16. 11 items fixed | **Lead read the full production diff (12 files, about 1,400 lines)** and made three small changes: filter permission is refused before selections are validated; log formatting; an out-of-range guard on preview | Merged |

### Measured (lead, 2026-11-19, working branch `b06c0b8`)
Root 1,167 passed, 3 skipped; API 378 passed, 7 skipped (Postgres-only), 29 expected failures; web 226 passed; ruff, format,
mypy, web typecheck, lint and build clean; `openapi_export --check` passes. API on Postgres 16: 384 passed, 0 skipped
(builder's run, not repeated by the lead). GitHub run 58 green on all six jobs including `compose-smoke`.

### Bugs only the GitHub smoke test found
1. The full stack could never start: compose did not pass `MICROGRID_API_KEYS`, which production settings require.
2. The web image build needs `web/.npmrc` (legacy-peer-deps) or `npm ci` rejects the lockfile.
3. The image lacked `packaging`, which Celery's Redis transport imports; now declared in `api/pyproject.toml`.

### Now verified that was not before
Docker images start; a real Redis broker and a live Celery worker run a fixture job end to end; live progress reaches
the browser port through nginx while a run is in flight; the web app works against a real API (QA, in Chromium, with
the production Content-Security-Policy).

### Still unverified
Anything on the owner's real data · macOS and arm64 images · the web app in a browser against the real containers
(the smoke test uses curl) · Firefox and Safari · real DC column names for filters and temperature.

### Gaps found, carried to batch 2
| Gap | Story |
|---|---|
| Custom compositions cannot run against the real API: no building-type catalogue endpoint, no way to turn the form into a scenario config, nowhere to save a composition (also QA D1: Admin-filtered compositions cannot be offered to others) | W1-COMPOSE |
| No endpoint reporting the caller's subject and role | W1-COMPOSE |
| Server never names the characteristics files or a temperature source, so the engine uses hard-coded default file names and every API result says temperature is not available | W1-DATASET |
| Characteristics store in the database and meter placeholder (FR-DATA-7, FR-DATA-3) | W1-DATA |

### Recorded requirements for later waves
Rate limiting on preview and run creation before any public exposure (security review) · template development keys must
be refused or rotated before any tunnel or public deployment (QA) · server-supplied text is always rendered as text in
the web app (now a lint rule) · database-level immutability for configuration versions if audit matters.

### Waiting on the owner
How a fixed-count run should report convergence (lead recommends a "not checked" state; today it says converged) ·
data inspector and Docker checks on the Mac · the decision batch in the deployment plan section 9.

---

## Batch 2 result (2026-11-19)
At most three Opus agents at a time. No reviewer escalation.

| Story | Rating | Review | Lead check | State |
|---|---|---|---|---|
| W1-COMPOSE: `GET /me`, `GET /building-types`, `composition` runs built from the registry, server names the characteristics files and a temperature source, readiness and worker verify data files | **Critical** | Security: no critical, high or medium. QA: pass with conditions; results byte-identical to before for existing inputs; 50 mutations, one equivalent survivor; Postgres 16 green. 10 items fixed | **Lead read the full production diff (13 files, about 800 lines)**; no changes needed | Merged |
| W1-DEPLOY2: compose passes the new settings only when set; env typo guard; smoke test checks `data_files` and runs a custom composition | Standard | No separate QA (deviation from the plan, small follow-up to a reviewed story); builder mutation checks | Lead read the scripts; GitHub run 63 green | Merged |
| W1-WEB2: composition form on the real API, role from `/me`, catalogue, data-used panel, temperature overlay | Standard | QA: pass with conditions, 6 defects fixed, real-API browser run 9/9, keyboard operation tested. UX review (first use of `ux-reviewer`, Sonnet): 5 major problems and about 25 wording changes, all must-fix items fixed | Lead viewed the screens at 1280 and 390 px and re-ran the web checks | Merged |

### Measured (lead, 2026-11-19, `b2a9b40`)
Root 1,219 passed, 3 skipped, 2 expected failures (engine count rules, pending the owner); API 686 passed, 7 skipped
(Postgres-only), 28 expected failures; web 443 passed; lint, format, types, web typecheck and build clean. API on
Postgres 16: 684 passed (builder, before the deploy follow-up). GitHub run 63 green on all six jobs, including a custom
composition run on real containers; run 64 (web merge) not yet seen.

### Wave 1 exit criterion
"In a browser against the compose stack, on fixtures: define a composition, run it with progress, see the stored run."
Proven in parts, not yet as one test: the compose stack runs a custom composition through nginx, Redis and the worker
(CI smoke, curl); the browser flow works against a real API with an in-process worker (builder and QA, Chromium).
A browser test against the real containers is the first story of Wave 2 (it is also FR-PLAT's end-to-end test).

### Not built in Wave 1 (owner decision pending)
W1-DATA: characteristics store in the database (FR-DATA-7) and meter placeholder (FR-DATA-3). The API now keeps the
characteristics files in an in-process cache, so pool previews are fast after the first read; the lead recommends
deferring the database table. Risk: first-read time and memory on the real DC files is unmeasured.

### Engine defect found, not fixed (owner decision pending)
`config_from_dict` accepts fixed counts above `total_buildings` and all-fixed counts that do not equal it; the engine
then samples the wrong number of buildings. The API refuses such input for every run; direct library use is
unprotected. Two strict expected-failure tests in `tests/test_qa_w1_compose_engine_counts.py`.

### API gaps for later stories
Title and description per predefined composition · explicit weather year and calibration status in results · filter
options for a filter form · saved compositions (also closes QA D1 from batch 1).

### Still unverified
The owner's real data (file names, columns, time convention, first-read cost) · macOS and arm64 · a browser against
the real containers · an adaptive run that actually converges, seen in the app · Firefox, Safari, screen readers.

---

## Close of session (2026-11-19)
- Owner decisions today: Day-14 checkpoint GO with cuts; characteristics table and meter entity deferred; engine count
  rule approved; convergence default `prd_default` and "not checked" for fixed-count runs approved; PR 12 opened and
  merged into `main` on the owner's instruction.
- Four checks at hand-off (lead, working branch `588c2b9` plus records): root 1,219 passed, 3 skipped, 2 expected
  failures; ruff check clean; ruff format clean (116 files); mypy clean (53 files). API 686 passed and web 443 passed
  were measured earlier today on `b2a9b40` and not re-run at hand-off (no code changed since).
- In progress when the session ended: ENG-COUNTS (engine rejects impossible fixed counts). The builder's work was in a
  local worktree and is NOT in the repository; the next session restarts it from the brief in the hand-off document.
- Not started: ENG-CONV (convergence defaults), all of Wave 2.
