# Sprint 1 Progress Log

| Date | Story | Result |
|---|---|---|
| 2026-11-07 | 1.1 Packaging and dependency cleanup (FR-PLAT-3) | **Done.** Build backend added; runtime deps reduced to numpy, pandas, pyarrow, pyyaml, openpyxl; dev and notebooks extras; Python floor 3.12 (pinned 3.13); one version source (`microgrid.__version__`); public surface pinned; clean-interpreter import test. |
| 2026-11-07 | 1.2 CI that blocks (FR-PLAT-5) | **Done in repo; not yet run on GitHub.** Lint, format check and mypy now block; engine job installs the package, no `PYTHONPATH`, reports coverage. Ruff replaces black. One-time reformat of 41 files (no behavior change; tests identical). Fixed real findings: undefined name in a type hint, loop-variable closure, `zip` strictness, two misnamed variables, awkward error strings from an earlier global rename. |
| 2026-11-07 | 1.4 RNG and replayability (FR-ENG-4) | **Done (engine level).** Stateless `SeedSequence` derivation; 10,000 distinct run streams; no root consumption; independent layers even with equal seeds; no neighbouring-seed collisions; fresh random seeds by default, always recorded in `EnsembleResult.seeds`; sampling-level seed-robustness test. **Still pending:** golden determinism test and full-ensemble seed-robustness test (need the run facade, story 1.9, and fixtures, story 1.6); UI replay (Sprint 2). **[numbers change]** noted in CHANGELOG. |
| 2026-11-07 | 1.3 Strict configuration (FR-ENG-3) | **Done.** Allow-lists for every config section; all problems reported together with paths and hints; the six legacy keys rejected with explanations; enum, count, fraction and percentile checks; list-valued scenario name/description normalised; shipped YAMLs fixed (keys commented out, behavior unchanged). 24 new tests. **Owner question raised:** see below. |
| 2026-11-07 | 1.5 Building-type registry (FR-ENG-1) | **Done.** Registry as packaged YAML with validation; 9 active types + inactive `civic`; categorisation by use, subtype and story range with most-specific-wins; extensible without engine changes (tested with a fake type); default filters fill in categories a scenario omits; GIS split of warehouse vs industrial and attached vs detached single-family; unmodeled structures still reported. 43 new tests. **Needs owner/agent confirmation:** ComStock type names (`Warehouse`, `Hospital`, `PrimarySchool`, `SecondarySchool`, `Outpatient`) against the release data; the civic mapping. |
| 2026-11-11 | 1.9 Engine facade + progress events (FR-ENG-8) | **Done.** `run_scenario(config, *, data_dirs, pool, progress, should_cancel)` with typed `ProgressEvent`, `RunCancelled` (pickle-safe), `RunResult.to_dict()`; `config_from_dict` (dicts get the same strict validation as YAML); observer hooks on `run_ensemble` (proved **no numbers change**). Golden determinism test, replay test and the full-ensemble seed-robustness test (FR-ENG-4c) now run on the generated fixture. **Open:** notebook-on-fixtures acceptance item (needs a public demo-data helper + notebook runner). |
| 2026-11-11 | 1.6 Fixtures, dataset catalog, manifest (FR-DATA-1/2/8) | **Done for the cloud-testable part.** Synthetic dataset generator in the real layout; `microgrid.datasets` (DatasetId, catalog, manifest with safe atomic writes, characteristics dictionary); `scripts/data_manifest.py`. First real end-to-end engine run happened on the fixture (QA). **Open:** Postgres load of characteristics (DATA-7), engine takes dataset ids (DATA-8 AC), no download step, real-data timing. |
| 2026-11-11 | 1.7 Docker Compose (FR-PLAT-4) | **Code-complete; verified statically only** (`docker compose config` passes; no daemon in the cloud). db (timescaledb-ha pg17 [verify]) + redis + web placeholder + migrate one-shot + api + worker placeholder. Worker excluded from `make up` until 1.10. **Needs the owner's Mac** (see handoff). |
| 2026-11-11 | 1.8 API skeleton (FR-PLAT-1) | **Done.** `api/` is a tested package: `/api/v1/health`, `/ready` (db + schema), fail-loud settings, CORS, migrations packaged with a `python -m microgrid_api.db.migrate` entry, models verified on SQLite **and a real Postgres 16**. Credentials hidden from settings output (found by lead, fixed). |

## Process notes (Wave A, 2026-11-11)
- Four sub-agents built stories in parallel worktrees; four independent `qa-engineer` reviews each returned *approve with conditions*; four fix agents cleared the conditions; the lead merged and re-verified. QA found: one high-severity data-safety bug (manifest temp-file symlink), one cross-component integration bug found independently twice (env-name prefix: API would have silently run on SQLite and reported ready), a cancel-exception pickle bug, and weak tests (mutation checks caught several). The lead's own check found one more (URL passwords in settings repr).
- Agent worktrees start from `main`, not the current branch; every brief must begin with a fast-forward onto the working branch (now in `CLAUDE.md`).

## Metrics (end of day 1)
- Tests: 192 passed, 2 skipped, 1 expected failure (run facade, story 1.9).
- Coverage baseline (engine, measured): **72%** overall. Lowest: `ensemble.py` 13%, `outputs.py` 16%, `archetype.py` 26%, `loader.py` 34%. These are the modules that need real or fixture data; synthetic fixtures (story 1.6) are the way to raise them. The 90% gate from the PRD is **not yet enforced**.
- Lint/format/types: ruff clean, mypy clean (33 files).

## Decisions logged
- Python floor 3.12, pinned 3.13 (matches the PRD and the cloud runtime).
- Ruff for lint and format; black dropped.

## Open items found
- Earlier global rename left awkward prose ("microgrid.community_load.synthetic" inside sentences) in ~30 comments/docstrings and in docs. Cosmetic; to be cleaned in a documentation pass.
- The CI workflow has not run on GitHub yet; it will on the first push to a branch with an open PR or push trigger.

## Questions for the owner (raised during Sprint 1)
1. ✅ **Decided 2026-11-07: include the filter (implemented).** Original question: **`require_data_complete`**: the shipped configs set it to true, but the engine never applied it, so *every* building in the characteristics file has been eligible. It probably refers to ResStock's `completed_status` / `applicability` columns. Should buildings with an incomplete or non-applicable status be excluded? (Methodology: it could change which buildings are sampled.) Currently rejected as unsupported; planned as an attribute filter in story 1.6b once the data dictionary confirms the column meaning.
2. **Single-family adoption rates** in `dc_multifamily_baseline_2.yaml` (heat pump, EV, solar at 10%) were never applied. They now sit as comments. Real penetration needs scenario datasets (FR-ENG-12).

## Wave A result (2026-11-11)
- Root: **360 passed, 1 skipped**; ruff, format, mypy clean (40 source files). Engine coverage **90%** (was 77%); weakest: `outputs.py` 16%, `snlg_source.py` 66%, `loader.py` 73%.
- API: **49 passed, 6 skipped, 27 expected failures** (old placeholders for later stories); ruff, format, mypy clean (14 files); Postgres 16 tests passed in QA runs.
- Not run: Docker images (no daemon), GitHub CI (not observed), anything on real data.
