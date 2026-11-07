# Sprint 1 Progress Log

| Date | Story | Result |
|---|---|---|
| 2026-11-07 | 1.1 Packaging and dependency cleanup (FR-PLAT-3) | **Done.** Build backend added; runtime deps reduced to numpy, pandas, pyarrow, pyyaml, openpyxl; dev and notebooks extras; Python floor 3.12 (pinned 3.13); one version source (`microgrid.__version__`); public surface pinned; clean-interpreter import test. |
| 2026-11-07 | 1.2 CI that blocks (FR-PLAT-5) | **Done in repo; not yet run on GitHub.** Lint, format check and mypy now block; engine job installs the package, no `PYTHONPATH`, reports coverage. Ruff replaces black. One-time reformat of 41 files (no behavior change; tests identical). Fixed real findings: undefined name in a type hint, loop-variable closure, `zip` strictness, two misnamed variables, awkward error strings from an earlier global rename. |

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
