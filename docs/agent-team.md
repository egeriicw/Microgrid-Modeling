# Agent Team Charter

Companion to [`PRD.md`](./PRD.md) and [`sprint-plan.md`](./sprint-plan.md). Describes the software-development team
(modeled as agents) that builds the product, what each role owns, how work flows, and when an agent must stop and ask
the owner. Roles can be run as separate sub-agents, separate sessions, or one agent wearing different hats per task;
the responsibilities stay the same.

## 1. Roles

| Role | Owns | Typical outputs | Must not |
|---|---|---|---|
| **Project Manager / Scrum Master** | Sprint backlog, status, risks, the decision log, sprint review packs | Sprint plan updates, burndown, risk register, "decisions needing owner" list | Change scope or methodology unilaterally |
| **Product Owner liaison / Business Analyst** | Requirements traceability (FR IDs ⇄ tests), acceptance criteria, glossary | Refined stories, AC lists, traceability matrix | Invent requirements the owner did not state |
| **Software Architect** | Architecture, ADRs, module boundaries, API/versioning rules | ADRs, diagrams, dependency-rule tests | Add dependencies without logging them |
| **Scientific / Modeling Engineer** | Engine correctness, methodology implementation, convergence, calibration math | Engine code, scientific tests, method notes | Change methodology without owner approval |
| **Backend Engineer** | FastAPI, SQLAlchemy/Alembic, Celery worker, auth | API routers, migrations, worker tasks | Bypass the engine facade |
| **Frontend Engineer** | React/TypeScript UI, charts, forms, map | Feature modules, component tests | Duplicate business logic from the engine |
| **Data Engineer** | Data ingestion, manifests, fixtures, DB schema for series, footprints | Ingestion scripts, fixtures, hypertables | Delete or overwrite data; exceed disk budget |
| **DevOps / Platform Engineer** | Docker Compose, CI/CD, environments, hosting, secrets, observability | Dockerfiles, workflows, deploy runbooks | Spend money or open public ports without approval |
| **QA / Test Engineer** | Test strategy, test-first enforcement, e2e suites, regression sets | Failing tests first, e2e, load tests | Merge code lacking tests |
| **Validation / QC Scientist** | Benchmarks, calibration studies, accuracy statements, data-license checks | Validation reports, benchmark sets, caveat wording | Claim validation that was not demonstrated |
| **Security Reviewer** | Auth, roles, uploads, public demo hardening | Security checklist, findings | Weaken controls for convenience |
| **Technical Writer** | User guide, README, docstring quality, onboarding, changelog | Docs that a newcomer can follow | Document features that do not exist |
| **UX Designer** | Flows, wireframes, accessibility, chart conventions | Wireframes, UI conventions | Block delivery on polish |

## 2. Workflow (per story)
1. **Refine** (BA/PM): story linked to FR IDs with acceptance criteria and a definition of the tests.
2. **Design note** (Architect, if it touches a boundary): short ADR or note.
3. **Red** (QA/Engineer): write the failing test first, commit it.
4. **Green** (Engineer): minimal code to pass; keep commits small; Conventional Commit messages.
5. **Refactor** and document: docstrings, module README touched if behavior changed.
6. **Review**: a different role reviews (peer code review; QA verifies AC; Security if auth/upload/public; Validation if
   scientific).
7. **Integrate**: CI green (lint, types, tests, coverage); merge to trunk via PR; changelog entry.
8. **Demo-able**: each story ends in something the owner can run or see.

## 3. Definition of Ready
Linked FR IDs · acceptance criteria written · test approach known · dependencies listed · size ≤ ~1–2 days · no
unresolved owner decision.

## 4. Definition of Done
Tests written first and passing · coverage gate met · lint/type clean · docs and docstrings updated · ADR/decision logged
if consequential · no new silent config keys · migrations reversible · works from a clean clone via the documented
command · reviewed by a second role · traceability matrix updated.

## 5. Decision rules (from the owner)

**Proceed autonomously with a sensible default, and log it** in `docs/decision-log.md` (date, decision, alternatives,
reason, how to reverse) when the choice is reversible and low-consequence: naming, internal structure, library choices
inside an approved category, test design, minor UX.

**Stop and ask the owner** (record the question in the sprint pack; continue other work meanwhile) for:
- **Methodology changes** — sampling, convergence criteria, perturbations, calibration formulas, anything affecting numbers.
- **Spending money** — any paid service, hosting, API key with billing, domain.
- **Deleting data or anything consequential** — never delete ResStock/ComStock data, user data, history, or branches;
  use archive/rename instead.
- **Publishing publicly** — making a repo, site, or demo public, or posting anything outside the project.
- Anything that contradicts the PRD or reinterprets a requirement.

**Allowed without asking:** creating branches, committing, pushing to feature branches, opening PRs (reversible); adding
dependencies (log them in the decision log — owner reviews at sprint review).

## 6. Cadence and communication
- **Daily (async):** short status note per active role: done / next / blocked / needs owner.
- **Sprint planning (start of each sprint):** PM presents backlog, risks, and proposed scope; owner confirms.
- **Sprint review (end of each sprint):** live demo of working software + decision log + questions list + metrics
  (tests, coverage, open risks).
- **Retrospective:** what to change in process; PM updates this charter.
- **Week-2 checkpoint:** schedule-risk review with a go/slip/cut recommendation; the owner decides.
- Owner-facing writing uses plain language; jargon is defined in the glossary.

## 7. Working agreements
- Trunk-based, small PRs, Conventional Commits, PR template with AC checklist.
- Cloud agents run **fixtures only** (the full DC data lives on the owner's Mac). Tests needing real data are marked
  `integration_local` and documented, with a one-line command for the owner to run them.
- Every scientific claim in docs or UI text must cite a test, benchmark, or source; otherwise worded as a hypothesis.
- No behavior is built that the PRD marks LATER, except interfaces/placeholders explicitly listed.
- Keep a living **traceability matrix**: FR ID → tests → code → docs.

## 8. Escalation
Blocked > half a day, scope doubt, test failures that suggest a scientific problem, or any security concern → PM
summarizes for the owner with options and a recommendation.
