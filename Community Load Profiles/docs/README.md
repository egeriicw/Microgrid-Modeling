# SNLG Platform — Documentation Set

Start here. Read in this order.

| # | Doc | Read it for |
|---|---|---|
| 1 | [`PRD.md`](./PRD.md) | Why the project exists, goals/non-goals, the 4 locked design decisions, feature list, success criteria, risks. **Entry point for the implementing agent.** |
| 2 | [`domain-glossary.md`](./domain-glossary.md) | The ubiquitous language. Use these exact terms in code/API/UI. |
| 3 | [`engine-reference.md`](./engine-reference.md) | Faithful documentation of the **existing** `src/snlg/` engine + notebook, module by module, plus the 13 known gaps to fix. (PRD goal G2.) |
| 4 | [`architecture.md`](./architecture.md) | Target system: containers, components, sequence diagrams, repo layout, tech stack, cross-cutting concerns. |
| 5 | [`requirements.md`](./requirements.md) | Numbered FR/NFR with acceptance criteria and the test file that verifies each. Traceability table at the end. |
| 6 | [`api-spec.md`](./api-spec.md) | REST contract: every endpoint, request/response schema, status codes, the `ScenarioConfigBody` shape. |
| 7 | [`data-model.md`](./data-model.md) | Postgres DDL (Alembic-generated), MinIO object layout, retention, reproducibility invariant. |
| 8 | [`frontend-spec.md`](./frontend-spec.md) | Next.js pages, components, flows, state, component test contracts, viz rules. |
| 9 | [`infrastructure.md`](./infrastructure.md) | Docker Compose services, Dockerfiles, `.env` contract, Make targets, CI jobs, OEDI data pipeline. |
| 10 | [`test-plan.md`](./test-plan.md) | **Binding.** TDD loop, test pyramid, fixtures, the full ordered test-file index, coverage gates, determinism protocol. |
| 11 | [`implementation-plan.md`](./implementation-plan.md) | Phases 0–6, tiny-commit task lists, `feat/*` harvest allowlist, sequencing, per-phase DoD. |

## The 4 locked decisions (from `PRD.md` §4)

- **D1** — `snlg` becomes a standalone installable library (engine of
  record); the platform (`api/`, `web/`) is a separate service layer that
  depends on it. `feat/*` scaffolds are harvested, not merged.
- **D2** — Runs execute asynchronously on **Redis + RQ** workers.
- **D3** — **Postgres** for metadata/metrics/lineage; **MinIO (S3)** for
  bulk artifacts.
- **D4** — Full **Next.js** app; single **docker-compose** (web, api,
  worker, db, redis, minio) for local/dev/CI. No k8s in v1.

## For the implementing agent

1. Do the `engine-reference.md` documentation pass and lock the engine
   contract with tests **first** (Phase 1).
2. Never write production code before a failing test (`test-plan.md` §1).
3. Only copy from `feat/*` per the allowlist (`implementation-plan.md` §7);
   never pull in the `microgrid_profiles` engine.
4. Keep `requirements.md` traceability and `CHANGELOG.md` current as you go.
