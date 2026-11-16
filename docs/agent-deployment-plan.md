# Agent Team Deployment Plan

**Written:** 2026-11-16 · **Status:** proposal for the owner; nothing in it has been started.
**Purpose:** a hand-off plan for running a team of agents on this repository until the product meets its goals.
**Builds on, does not replace:** [`CLAUDE.md`](../CLAUDE.md), [`agent-team.md`](./agent-team.md),
[`sprint-plan.md`](./sprint-plan.md), [`handoff/session-handoff.md`](./handoff/session-handoff.md). Where this plan and
those files disagree, this plan lists the change explicitly in section 4.4 so the lead can apply it.

---

## 1. Where the project stands

### 1.1 What was reviewed
All 10 pull requests, every remote branch, the git history, the PRD (v1 and v2), the sprint plan, the Sprint 1 and 2
records, the decision log, the traceability matrix, the agent definitions and CI.

### 1.2 Two lines of work exist, and only one is current

| Line | What it holds | State |
|---|---|---|
| `main` | Notebook-era engine, PRD v1 (RQ, MinIO, Next.js), 106 tests, placeholder `api/` and `web/` | Stale. Superseded by PRD v2 on 2026-11-03. |
| `claude/loving-pascal-x8khbu` | 113 commits ahead of `main`: PRD v2, team charter, 10 role agents, hardened engine, FastAPI + Celery API, React shell, compose stack, data tooling | **Current.** Never merged, no pull request, last commit 2026-11-11. |
| PRs 2 to 8 (`feat/*` onto `dev`) | January scaffold built on the older `microgrid_profiles` engine | Open but obsolete; kept as reference only. |

### 1.3 Baseline measured today on the current line
| Check | Result |
|---|---|
| Engine and library tests | 944 passed, 3 skipped |
| API tests | 231 passed, 3 skipped, 27 expected failures (placeholders for later stories) |
| Lint (ruff) and types (mypy, 51 files) | clean |
| Web | 97 tests reported by the last session; **not re-run today** |

### 1.4 What has never been verified anywhere
1. **GitHub CI has never run on this work.** The `api`, `web` and `compose-smoke` jobs are still switched off (`if: false`).
2. **Docker images** have never been built or started (no Docker in the cloud sessions).
3. **A real Redis broker with a live Celery worker** has never run; the compose `worker` service is still a placeholder.
4. **The owner's real DC data** has never been touched by this code. Every test uses synthetic fixtures.
5. **The web app has never talked to the real API**; it runs on a mock client.

### 1.5 Requirement status against PRD v2 (MUST items only)
| Area | Done | Partly | Not started |
|---|---|---|---|
| Engine | ENG-1, 3, 4, 8, 11 (library side) | ENG-5, 6, 7 (built, convergence rule is opt-in pending owner) | ENG-2 (needs an explicit acceptance test), ENG-10 |
| Data | DATA-2 | DATA-1, DATA-8 (code done, real data unconfirmed) | DATA-3, DATA-4, DATA-7 |
| Web | none | WEB-1, 2, 3, 5 (UI on mock API; API side of 2 and 5 done) | WEB-4, 6, 8 |
| Calibration | none | none | CAL-1, 2, 3 |
| Platform | PLAT-1, 3, 5 | PLAT-2 (no real broker), PLAT-4 (static checks only) | none |

The traceability matrix still lists ENG-5/6/7 and WEB-1/2/5 as "Not started"; the truth is in an HTML comment at the
bottom of the file. Fixing that is part of Wave 0.

### 1.6 Schedule
Day 1 was 2026-11-06. Today is Day 11. The Day-14 checkpoint is **2026-11-20** and Day 30 is **2026-12-06**.
The project has been idle for five days and Sprint 2 is about half done. Section 8 gives a cut recommendation.

---

## 2. The finish line

The team stops when these gates hold. They restate PRD v2 section 10; the PRD remains the contract.

| Gate | Statement | Proven by |
|---|---|---|
| **M1 Core demo** | On the owner's Mac, `make up` starts the stack; a Client defines a composition in the browser, runs it with live progress and convergence, sees every required chart, downloads CSVs. Real DC data. | Owner walkthrough using a written demo script, plus the automated end-to-end test on fixtures |
| **M2 Calibration** | Upload a CSV, see the overlay and the error metrics (NMBE, CV(RMSE), peak and energy error, band coverage) | Tests plus owner walkthrough |
| **M3 Roles** | Admin, Client, Demo enforced by the API; Demo limited to predefined compositions | API tests plus security review sign-off |
| **M4 Quality** | CI green on `main` with every job enabled and blocking; coverage engine ≥ 90%, API ≥ 85%; determinism golden test passes on Linux and on the Mac; a newcomer can follow the README from a clean clone | CI history, a clean-clone trial by an agent that has read nothing else |
| **S1 (should)** | Minimal map viewer; public-repository readiness | Owner decides at the checkpoint |
| **S2 (stretch)** | Public live demo within $100/month | Only with owner approval to spend and publish |

After M1 to M4 the team pauses for an owner review. Section 12 lists what comes next (weather normalization, scenarios,
DER, dispatch, economics); none of it starts without a new owner-approved scope.

---

## 3. What must be in place before the agents start

These are the things only the owner or the environment can provide. Everything else the team does itself.

### 3.1 Owner actions (one sitting, about an hour)
| # | Action | Why it matters |
|---|---|---|
| F1 | Say "open a pull request" for the current line into `main`, and say whether PRs 2 to 8 may be closed with a "superseded" note (branches kept) | Creates one trunk and makes CI run for the first time. Agents may not open or close PRs without being asked. |
| F2 | Run the data inspector on the Mac and paste the report ([`runbooks/inspect-my-data.md`](./runbooks/inspect-my-data.md)) | Answers real column names, releases, partitions. Blocks DATA-7, DATA-8 and filter defaults. |
| F3 | Run the lite Docker check on the Mac ([`runbooks/local-stack.md`](./runbooks/local-stack.md), "Small disk") | First proof that images build and start on Apple silicon. |
| F4 | Answer the decision batch in section 9 | Several stories cannot be finished without them. |
| F5 | Free disk space on the Mac, or accept the lite stack for the demo | The Mac had about 6.7 GB free; the full stack needs roughly 12 GB of headroom. This decides whether M1 runs on Postgres or SQLite. |
| F6 | Confirm Day 30 (2026-12-06) is still the target and name a daily time for a short status read | Sets the checkpoint rule. |

### 3.2 Environment setup (lead and devops, with owner approval where marked)
| # | Item | Detail |
|---|---|---|
| E1 | Cloud environment setup script | Pre-install dependencies so every session starts warm: `uv sync --extra dev`, `uv sync --project api --extra dev`, `npm --prefix web ci`. Needs owner approval in the environment prompt. |
| E2 | GitHub Actions as the Docker test bench | GitHub runners have Docker, so image builds, a real Redis, a live worker and the compose smoke test are verified there, not in the cloud session. This removes three of the five "never verified" items without the Mac. |
| E3 | Branch protection on `main` | Require the CI jobs; one PR per wave. Owner sets this in GitHub settings. |
| E4 | Real-data access | Option A (default): owner runs exact commands from runbooks and pastes results. Option B: owner attaches the Mac project folder to the lead session through Remote Control so the lead can run the real-data stories directly, each attach approved by the owner. Data in `data/` is read-only either way. |
| E5 | Permission allowlist | Add the routine read-only and test commands to `.claude/settings.json` so sub-agents are not stopped by prompts (pytest, ruff, mypy, npm test, git status/diff/log, docker compose config). |
| E6 | GitHub issues as the board | One issue per story using the existing template, labelled by wave and role. The lead creates and closes them. Optional; the sprint files remain the source of truth if the owner prefers no issues. |

---

## 4. The team

### 4.1 Structure
One **lead session** (Project Manager and Project Lead) runs the wave, and delegates to **role sub-agents** that each work
in their own git worktree. This is the model the owner approved on 2026-11-06 and the one that produced the current
line. Keep it. Do not move to one long-lived session per role; coordination cost was the reason it was rejected.

A fresh lead session is started for each wave. The hand-off document carries the state, so the lead's context stays small.

### 4.2 Roster
"Writes" is the only place that role may change files. Disjoint write areas are what make parallel work safe.

| Role (agent file) | Responsibilities | Writes | Tools | Reviewed by |
|---|---|---|---|---|
| **Lead / PM** (the main session) | Plan each wave, write briefs, integrate, re-run all checks, keep records, report to owner, raise owner decisions | `docs/` records, `CHANGELOG.md`, merge commits | All; GitHub tools (PRs, CI logs, issues); sub-agent launcher | Owner |
| `architect` | ADRs, module boundaries, API versioning, dependency-rule tests; decides storage shape for results (DATA-4) | `docs/adr/`, `tests/test_architecture.py` | Read, search, edit, shell | Lead |
| `modeling-engineer` | Engine: ENG-2 acceptance, ENG-10 temperature, results-model gaps, scientific tests | `src/microgrid/` (not `datasets/`), matching `tests/` | Read, search, edit, shell | `qa-engineer`, `validation-scientist` |
| `data-engineer` | Characteristics store (DATA-7), meter placeholder (DATA-3), catalog fill, real-data findings | `src/microgrid/datasets/`, `scripts/`, `configs/datasets/`, data migrations | Read, search, edit, shell | `qa-engineer` |
| `backend-engineer` | Filters admin API, results and download endpoints, calibration API, demo-role endpoints, worker | `api/` | Read, search, edit, shell | `qa-engineer`, `security-reviewer` |
| `frontend-engineer` | Generated API client, real-API wiring, charts, downloads, calibration screen, caveat, login | `web/` | Read, search, edit, shell, Playwright with the pre-installed Chromium; loads the `dataviz` skill before chart work | `qa-engineer`, `ux-reviewer` |
| `devops-engineer` | Compose worker and web services, Dockerfiles, CI jobs, smoke test, runbooks for the Mac | `deploy/`, `.github/`, `Makefile`, `docs/runbooks/` | Read, search, edit, shell | `qa-engineer`, `security-reviewer` |
| `qa-engineer` | Independent review of every branch before merge: acceptance tests, mutation checks, hostile inputs, cross-branch mismatch hunt; end-to-end suite | `tests/`, `api/tests/`, `web/src/qa/`, `web/e2e/` only | Read, search, edit, shell | Lead |
| `security-reviewer` | Auth, roles, uploads, path handling, CORS, headers, secrets in history | Nothing (findings only) | Read, search, shell | Lead |
| `validation-scientist` | Benchmark set, calibration maths check, validation report, caveat wording, licence checks | `docs/reference/`, `docs/validation/`, benchmark data files | Read, search, edit, shell, web search for sources | Owner (all claims) |
| `tech-writer` | README from clean clone, user guide, retire stale v1 specs, fix the leftover rename prose | `docs/` (not records), `README.md` | Read, search, edit | Lead |
| `ux-reviewer` (**new**) | Screenshot review of each screen on phone and desktop width, plain-language labels, keyboard and contrast checks | Nothing (findings only) | Read, search, shell, Playwright | Lead |
| `records-keeper` (**new**) | After each integration: traceability matrix, progress log, decision log entries from agent reports | `docs/traceability-matrix.md`, `docs/sprints/` | Read, search, edit | Lead |

### 4.3 Models by role, and the lead's criticality review
**Owner rule (2026-11-16):** only the lead and the `architect` run on Fable 5.2. Every other role runs on Opus 5.5,
Sonnet 5.5 or Haiku 5.5. The lead reviews the work, rates how critical each piece is, and that rating decides the
model and the depth of review. **Exception:** the three reviewer roles (`qa-engineer`, `security-reviewer`,
`validation-scientist`) may move up to Fable 5.2, but only to clear a major bottleneck to pace (see "Reviewer
escalation" below). Builders never move up.

#### Default model per role
| Model | Roles | Why |
|---|---|---|
| Fable 5.2 (`fable`) | Lead / PM · `architect` | Planning, integration, criticality decisions and structural design across the whole codebase. These two are also the final reviewers of critical work. |
| Opus 5.5 (`opus`) | `modeling-engineer` · `backend-engineer` · `frontend-engineer` · `data-engineer` · `devops-engineer` · `qa-engineer` · `security-reviewer` · `validation-scientist` | Building against written acceptance criteria, and independent review. The strongest model the lead may delegate to. |
| Sonnet 5.5 (`sonnet`) | `tech-writer` · `ux-reviewer` · `records-keeper` | Structured writing and checklist review from material that already exists. |
| Haiku 5.5 (`haiku`) | No standing role | Throwaway lookups and purely mechanical edits the lead hands out. |

These are defaults. The lead changes the model per brief according to the rating below, within the owner rule: a
builder is never raised to Fable, and a reviewer only under the escalation rule.

#### Criticality rating (the lead decides, at wave planning, for every story)
The lead reads each story's acceptance criteria and the code it will touch, then rates it. The question is: *if this
is wrong, will a test or CI catch it, and what does it cost if nothing does?*

| Rating | A story is this when it ... | Builder | Independent review | Final check |
|---|---|---|---|---|
| **Critical** | changes the numbers (sampling, convergence, aggregation, calibration maths); crosses a trust boundary (auth, roles, uploads, file paths); changes a database migration or the worker's cancel and terminal-state writes; states an accuracy or licence claim; or cannot be undone | Opus | `qa-engineer` on Opus, plus `security-reviewer` or `validation-scientist` on Opus where the roster says so | **The lead reads the full diff itself on Fable** and signs it off by name in the wave report (or an escalated reviewer does the read, under the rule below). Structural changes also go to the `architect`. |
| **Standard** | adds behaviour that tests, types and CI will catch: an endpoint, a screen, a chart, a compose service, a script | Opus (Sonnet allowed when the story is small and the contract is fully written) | `qa-engineer` on Opus | Lead re-runs every check and reads the QA findings and the risky parts of the diff |
| **Routine** | follows a template or changes no behaviour: documentation, records, renames, moving old specs to the archive, applying an agreed lint fix | Sonnet or Haiku | None, or a Sonnet checklist pass | Lead spot-checks the diff |

Rules that go with the rating:
1. **The rating is written down.** Each story in the wave plan carries its rating, its model and one line of reason.
   The owner sees it before the wave starts and can change it.
2. **When in doubt, rate up.** A story that is partly critical is critical.
3. **The lead may re-rate mid-wave.** A standard story is raised to critical if QA finds a silent-error or
   trust-boundary defect in it, or if the builder fails QA twice on the same finding. Raising it adds the lead's own
   full-diff review; it does not change the builder's model.
4. **The reviewer is never on a weaker model than the builder.**
5. **Critical work is never built or reviewed below Opus.** Mechanical sub-steps inside a critical story (a rename,
   a file move) may still be handed to Sonnet or Haiku as separate briefs.
6. **The lead's review time is the scarce resource.** At most two or three critical stories per wave run at once, so
   the lead can read each diff properly. This, not the number of builders, sets the pace for critical work.

Because QA, security and validation review normally run on the same model as the builders they check, the lead's own
Fable review of critical work is the layer that stands above them. It is mandatory, and it is delegated only through
the escalation below.

#### Reviewer escalation (the only way a delegated agent reaches Fable)
The lead's full-diff review is the narrowest point in the loop. When it becomes a major bottleneck, a reviewer may
take that review on the higher model so the wave keeps moving.

| Question | Answer |
|---|---|
| Who may move up | `qa-engineer`, `security-reviewer`, `validation-scientist`. No builder, no other role. |
| What counts as a major bottleneck | Any one of: (a) three or more critical stories have passed Opus review and are waiting on the lead's full-diff read; (b) a critical story has waited more than one working day for that read; (c) the wave's exit date will be missed because of the review queue, by the lead's own forecast. A slow or difficult review is not a bottleneck; a queue is. |
| Who decides | The reviewer or the lead raises it; the lead confirms the condition is met and launches the reviewer on Fable. A reviewer cannot move itself up. |
| What the escalated review covers | One named critical story per escalation. The reviewer does the full-diff read the lead would have done, in its own specialty: QA for behaviour and tests, security for trust boundaries, validation for numbers and claims. |
| What the lead still does | Re-runs every check, reads the reviewer's findings, and signs the story off. Methodology changes and anything in the "ask the owner" list still stop at the owner. |
| When it ends | With that story. The next review starts on Opus again unless the condition still holds. |
| What is recorded | Story, reviewer, which condition was met, and the outcome, in the wave report. The owner can see how often it happened. |

If escalation is needed in two waves running, the cause is planning, not review speed: the lead schedules fewer
critical stories in parallel or splits them smaller, and says so in the next wave plan.

**How it is set:** a `model:` line in each agent file's header (`fable` for the architect only; `opus` or `sonnet`
for the rest), so the default travels with the role. Per-brief changes follow the tables above; a reviewer escalation
is a per-brief override by the lead, never a change to the agent file. The lead session itself
is started on Fable 5.2. Model names stay out of commits, pull requests and code comments, as `CLAUDE.md` requires;
the agent-file header and this plan are the only places they appear.

**Cost control:** keep the lead's context small (conclusions, not file dumps), give reviewers the diff and the
acceptance criteria only, and report tokens per wave. If spend becomes a concern, the first levers are fewer parallel
builders and moving standard stories to Sonnet, not a lighter review of critical work.

### 4.4 Changes to the current setup
1. Add `ux-reviewer` and `records-keeper` agent files. The charter names a UX role but no agent exists, and the stale
   matrix shows the lead does not have time for records. `records-keeper` runs alone, after integration, so the rule
   "builders never edit shared records" still holds.
2. Give `qa-engineer` a write scope limited to test folders, stated in its agent file. It currently may edit anything.
3. Add the write-scope line from the roster to every agent file, and a required first step: fast-forward to the
   integration branch and confirm `CLAUDE.md` exists (the lesson already recorded in `CLAUDE.md`).
4. Add the web checks to `CLAUDE.md` commands: `npm --prefix web run typecheck`, `lint`, `test`, `build`.
5. Update `CLAUDE.md` "Where things live": `api/` and `web/` are no longer scaffolds.
6. Add the `model:` line from 4.3 to all twelve agent files. None of the ten existing files sets one today, so every
   sub-agent currently inherits whatever model the lead session happens to run on.

---

## 5. How the team works

### 5.1 The wave loop
Every wave follows the pattern that already worked, with a pull request added at the end.

1. **Plan.** Lead reads the hand-off, runs the baseline checks, picks stories whose owner inputs are in hand, checks
   that their write areas do not overlap, **rates each story critical, standard or routine and assigns its model
   (section 4.3)**, and posts the wave plan to the owner.
2. **Build in parallel.** At most **five** builders at once, each in a worktree branched from `integration/<wave>`.
   Tests first.
3. **Independent QA.** A `qa-engineer` instance per builder branch. It receives the acceptance criteria and the diff,
   not the builder's summary. Security or validation review is added where the roster says so.
4. **Fix pass.** The builder role clears each QA finding. A finding is closed by a test, not by an explanation.
5. **Integrate.** Lead merges into `integration/<wave>`, re-runs every check itself, and reviews each story to the
   depth its rating requires (full diff for critical work).
6. **Record.** `records-keeper` updates the matrix and logs; lead updates the hand-off.
7. **Land.** One pull request per wave into `main`. Merge only when CI is green. The owner has asked for no PRs without
   a request, so F1 must include standing permission for one PR per wave, or the lead asks each time.
8. **Report.** Short owner note: done, not done, measured numbers, what is unverified, decisions needed.

### 5.2 Brief template for a sub-agent
Goal in one sentence · criticality rating and model · FR IDs and the acceptance criteria copied in full · files it may write · files to read first ·
what is already decided (decision-log lines) · what is out of scope · "tests first" · the checks to run · the report
format from the agent file · "stop and report if this needs a methodology change, spending, deletion or publishing".

### 5.3 Gates no agent may pass without the owner
Unchanged from `CLAUDE.md`: methodology changes (anything that alters the numbers), spending money, deleting data or
branches, publishing anything. New for this plan: closing pull requests, changing branch protection, and attaching the
owner's Mac also need a yes.

### 5.4 Verification rules
- The lead never reports a builder's claim as fact. It re-runs the checks and says which it ran.
- Every owner report has an "unverified" list. An item leaves the list only when a named run proves it (CI run link,
  or pasted output from the Mac).
- A test that is skipped, expected to fail, or mocked around the behaviour under test does not count as coverage for a
  requirement. The 27 expected failures in `api/tests` are each converted to a real test or deleted with a reason by Wave 3.
- Facts about NREL datasets and outside tools stay marked `[verify]` until `validation-scientist` cites a source.

---

## 6. Work plan

Dates assume the owner actions in 3.1 happen by 2026-11-17. Each wave lists builders that can run at the same time.

### Wave 0: One trunk, real CI (Nov 16 to 17)
| Story | Role |
|---|---|
| Open PR for the current line into `main`; watch the first CI run; fix what it finds | Lead, `devops-engineer` |
| Enable the `api` and `web` CI jobs (Node 22, `npm ci`, typecheck, lint, test, build); make the image build blocking | `devops-engineer` |
| Close PRs 2 to 8 as superseded (if approved); move v1 specs (`api-spec`, `data-model`, `frontend-spec`, `infrastructure`, `implementation-plan`, `test-plan`, `requirements`) to `docs/archive/` | Lead, `tech-writer` |
| Correct the traceability matrix; add the two agent files and the roster and model changes in 4.4 | `records-keeper`, lead |
| Setup script and permission allowlist (E1, E5) | `devops-engineer` |

**Exit:** `main` holds the current line, CI is green with engine, API and web jobs blocking.

### Wave 1: Close the thin slice with real parts (Nov 17 to 20)
| Story | FR | Role |
|---|---|---|
| Typed client generated from `api/openapi.json`; web switched from mock to real API; login with API key | WEB-1, 2, 5 | `frontend-engineer` |
| Compose `worker` and `web` services; real Redis and live Celery worker; compose smoke test running in CI | PLAT-2, PLAT-4 | `devops-engineer` |
| Filters admin API, stored and versioned (carry-over A4) | ENG-11 | `backend-engineer` |
| Temperature series on results; empty `weather` module with interface | ENG-10, ENG-13 | `modeling-engineer` |
| Characteristics store and data dictionary; meter placeholder | DATA-7, DATA-3 | `data-engineer` |
| ADR: where interval results live (files plus summary rows today, or a Timescale hypertable as the PRD says) | DATA-4 | `architect`, owner confirms |

**Exit (Sprint 2 exit):** in a browser against the compose stack in CI, on fixtures: define a composition, run it with
progress, see the stored run. **Day-14 checkpoint on Nov 20:** lead presents section 8 with measured status.

### Wave 2: Results the user can trust (Nov 21 to 26)
| Story | FR | Role |
|---|---|---|
| Charts on real results: residential and commercial with band, hourly and 15-minute toggle, load-duration, seasonal daily, composition, hover with temperature | WEB-3 | `frontend-engineer` |
| CSV downloads with metadata header (seeds, versions, dataset ids, weather-year label) | WEB-4 | `backend-engineer` |
| Accuracy caveat on every results view | WEB-8 | `frontend-engineer`, wording by `validation-scientist` |
| Results-model gaps: coincidence and diversity factors, per-type 15-minute; hybrid composition acceptance test | ENG-7, ENG-2 | `modeling-engineer` |
| Scientific test suite: energy conservation, known-answer, convergence, benchmark range | PRD §8 | `qa-engineer`, `validation-scientist` |
| Screen review at phone and desktop width | none | `ux-reviewer` |

**Exit:** M1 holds on fixtures in CI through an automated browser test.

### Wave 3: Calibration (Nov 24 to 30, overlaps Wave 2 where write areas differ)
| Story | FR | Role |
|---|---|---|
| Upload with validation (gaps, duplicates, units, size cap) | CAL-1 | `backend-engineer`, `security-reviewer` |
| Compare: overlay and metrics | CAL-2, WEB-6 | `modeling-engineer` (maths), `frontend-engineer` (screen) |
| Small curated benchmark set with cited sources | CAL-3 | `validation-scientist` |
| Validation report v0 with honest limits | QC | `validation-scientist` |
| End-to-end happy path: compose, run, charts, CSV, calibration | QA | `qa-engineer` |

**Exit:** M2 holds on fixtures in CI.

### Wave 4: Demo role, hardening, documents (Dec 1 to 4)
| Story | FR | Role |
|---|---|---|
| Demo role: predefined compositions, stored seeds, caps, locked endpoints | WEB-5 | `backend-engineer`, `frontend-engineer` |
| Full security review and fix pass | NFR | `security-reviewer`, then builders |
| Performance and resource pass on the Mac (run time against buildings and runs, memory, disk) | NFR, story 1.11 | Owner runs, `modeling-engineer` analyses |
| Licence and attribution text in the app | NFR | `validation-scientist`, `frontend-engineer` |
| README from a clean clone, user guide, demo script | PLAT-7 | `tech-writer`, tested by a cold-start agent |

**Exit:** M3 and M4 hold.

### Wave 5: Real-data acceptance and release (Dec 5 to 6)
Owner walks the demo script on the Mac with real DC data. Lead fixes what breaks, tags `v0.1.0`, writes the final
validation report and the sprint review.

### Real-data track (runs alongside every wave, on the owner's Mac)
| Step | Unblocks |
|---|---|
| Inspector report (F2) | Real column names for filters and the characteristics store; release and partition ids |
| Lite or full stack start (F3) | Image and disk facts; pin the database image tag |
| Catalog fill with real hashes and sizes | "Verified dataset" policy |
| Determinism golden test on macOS | Decide whether a tolerance is needed |
| One real run through the API | Performance baseline; first sanity check of numbers |

Each step is an exact command in a runbook. The lead batches them so the owner is asked once per wave, not per story.

---

## 7. Dependencies that set the order
1. Wave 0 comes first. Without a trunk and running CI, every later "done" is unverified.
2. The generated API client must land before any chart or screen is wired to real data.
3. Compose worker in CI must land before the end-to-end test can exist.
4. The inspector report must arrive before DATA-7 and the filter starter set are finished; build them on fixtures first,
   then correct against the report.
5. The DATA-4 storage decision must be made before downloads and calibration are built on it.

---

## 8. Recommendation for the Day-14 checkpoint
About twenty days remain and roughly half the MUST items are open. Recommended cuts, for the owner to decide:

| Item | Recommendation | Reason |
|---|---|---|
| Map viewer (WEB-7) | Cut from the MVP | Already named first to cut; needs a footprint source decision that is still open |
| Admin console UI (WEB-9) | Cut; keep the admin API | Admin can edit filters and demo compositions through the API for the demo |
| Compact profile library (DATA-9) | Defer | Only needed for the public live demo |
| Public live demo (PLAT-6) | Defer | Stretch by definition; costs money and needs security sign-off |
| Timescale hypertable for results (DATA-4) | Ask the owner to accept file-backed results for the MVP | Works today; hypertable adds risk on a small disk |
| Perturbations (ENG-9) | Remove from the config surface until the owner decides | It never runs today; strict config already rejects it |

With these cuts the plan above fits. Without them, expect Day 30 to slip by about a week.

---

## 9. Owner decisions, in one batch
Carried from the hand-off and the Sprint 2 backlog, plus two new ones.

1. Make the new convergence rule the default, and confirm its settings (3 consecutive checks, standard-error target 0.02).
2. Gross and net loads: forbid mixing, or allow with a label?
3. Require a verified dataset for non-demo runs?
4. Which ComStock building types stand for civic buildings?
5. Perturbations: keep, fix, or remove?
6. Run-folder retention and the size caps (1,000 runs, 5,000 buildings).
7. Footprint source (only if the map stays in scope).
8. **New:** file-backed results for the MVP, or the Timescale hypertable?
9. **New:** full stack or lite stack for the Day-30 demo (depends on Mac disk space).
10. Repository licence and product name (needed only before publishing).

---

## 10. Risks
| Risk | Signal | Response |
|---|---|---|
| First CI run and first Docker build expose many failures at once | Wave 0 takes more than two days | Fix in Wave 0 before any feature work; this is why it is first |
| Real data differs from the synthetic fixtures (column names, partitions, statuses) | Inspector report disagrees with catalog | Real-data track starts on day one; fixtures updated to match the real layout |
| Owner review time is the bottleneck | Decisions older than two days | One batch per wave; every question has a default the team proceeds on if it is reversible |
| Parallel agents disagree on names or contracts | QA cross-branch mismatch findings | OpenAPI file is the contract; generated client; QA mismatch hunt stays mandatory |
| Agents report success that is not real | Lead re-run differs from the report | Verification rules in 5.4 |
| Mac disk fills | Preflight guard refuses to start | Lite stack; owner frees space (F5) |
| Numbers shown before they are validated | No benchmark comparison yet | Caveat component is a MUST; validation report states limits |

---

## 11. Reporting
- **Per wave:** stories done against planned · test counts and coverage, measured · CI status with link · the
  unverified list · decision-log entries · owner questions · disk and run-time numbers once the Mac track has data.
- **Daily while a wave runs:** three lines from the lead: done, next, blocked on owner.
- **Where state lives:** `docs/handoff/session-handoff.md` is the single "where are we" file and is rewritten at the
  end of every session.

---

## 12. After the MVP
Not planned in detail and not to be started without a new owner-approved scope. Order suggested by the PRD's extension
points and by the dependency between them:

1. Scenario datasets (heat pump, EV, efficiency upgrades) and weather normalization.
2. Map workspace with real footprints feeding fixed counts.
3. DER models (solar, storage, EV) behind the existing `der` interface.
4. Dispatch and economics behind their interfaces; REopt and SAM export.
5. Public hosting, billing and user administration.

The same team and wave loop apply. `validation-scientist` and `modeling-engineer` carry more of the load from step 1 on,
and every item in steps 1, 3 and 4 is a methodology decision for the owner.

---

## 13. Kick-off message for the lead session
> (Start this session on Fable 5.2.) You are the Project Manager and Project Lead. Read `CLAUDE.md`, then `docs/handoff/session-handoff.md`, then
> `docs/agent-deployment-plan.md`. Run the baseline checks and report the numbers. Then run Wave 0 from the deployment
> plan. The owner's answers to section 3.1 and section 9 are: [paste]. Do not start Wave 1 until CI is green on `main`.
