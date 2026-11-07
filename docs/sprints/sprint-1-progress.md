# Sprint 1 Progress Log

| Date | Story | Result |
|---|---|---|
| 2026-11-07 | 1.1 Packaging and dependency cleanup (FR-PLAT-3) | **Done.** Build backend added; runtime deps reduced to numpy, pandas, pyarrow, pyyaml, openpyxl; dev and notebooks extras; Python floor 3.12 (pinned 3.13); one version source (`microgrid.__version__`); public surface pinned; clean-interpreter import test. |
| 2026-11-07 | 1.2 CI that blocks (FR-PLAT-5) | **Done in repo; not yet run on GitHub.** Lint, format check and mypy now block; engine job installs the package, no `PYTHONPATH`, reports coverage. Ruff replaces black. One-time reformat of 41 files (no behavior change; tests identical). Fixed real findings: undefined name in a type hint, loop-variable closure, `zip` strictness, two misnamed variables, awkward error strings from an earlier global rename. |
| 2026-11-07 | 1.4 RNG and replayability (FR-ENG-4) | **Done (engine level).** Stateless `SeedSequence` derivation; 10,000 distinct run streams; no root consumption; independent layers even with equal seeds; no neighbouring-seed collisions; fresh random seeds by default, always recorded in `EnsembleResult.seeds`; sampling-level seed-robustness test. **Still pending:** golden determinism test and full-ensemble seed-robustness test (need the run facade, story 1.9, and fixtures, story 1.6); UI replay (Sprint 2). **[numbers change]** noted in CHANGELOG. |
| 2026-11-07 | 1.3 Strict configuration (FR-ENG-3) | **Done.** Allow-lists for every config section; all problems reported together with paths and hints; the six legacy keys rejected with explanations; enum, count, fraction and percentile checks; list-valued scenario name/description normalised; shipped YAMLs fixed (keys commented out, behavior unchanged). 24 new tests. **Owner question raised:** see below. |

## Metrics (end of day 1)
- Tests: 116 passed, 2 skipped, 1 expected failure (run facade, story 1.9).
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
