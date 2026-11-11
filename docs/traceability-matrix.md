# Requirements Traceability Matrix

Maintained by the Business Analyst role (see `agent-team.md`). A requirement is **Done** only when it has a green
test, merged code, and updated docs. Generated from `docs/PRD.md` on 2026-11-06; update statuses each sprint.

| ID | Priority | Title | Sprint | Tests | Code | Docs | Status |
|---|---|---|---|---|---|---|---|
| FR-ENG-1 | MUST | Building-type registry | 1 | tests/test_registry.py, test_community_load_categorize.py, test_geo.py | community_load/registry.py, building_types.yaml, categorize.py | CHANGELOG | Done (ComStock names to verify on real data) |
| FR-ENG-2 | MUST | Composition modes | TBD |  |  |  | Not started |
| FR-ENG-3 | MUST | Config honesty | 1 | tests/test_config_strict.py | synthetic/config.py | CHANGELOG | Done |
| FR-ENG-4 | MUST | Seeding: random by default, replayable | 1 | tests/test_rng.py, test_determinism.py, test_seed_robustness.py | synthetic/rng.py, ensemble.py, run.py | CHANGELOG | Done (golden generated on Linux; watch first Mac run) |
| FR-ENG-5 | MUST | Resolutions | 2 |  |  |  | Not started |
| FR-ENG-6 | MUST | Adaptive convergence | 2 |  |  |  | Not started |
| FR-ENG-7 | MUST | Outputs | 2 |  |  |  | Not started |
| FR-ENG-8 | MUST | Typed progress events | 1 | tests/test_run_facade*.py, test_run_hooks_qa.py | synthetic/run.py, ensemble.py hooks | CHANGELOG | Done (notebook example open) |
| FR-ENG-9 | SHOULD | Perturbations | TBD (owner) |  |  |  | Not started |
| FR-ENG-10 | MUST | Temperature | 2 |  |  |  | Not started |
| FR-ENG-11 | MUST | Attribute filtering of source buildings, admin-configured | 1 |  |  |  | Not started |
| FR-ENG-12 | LATER | Scenario switching and mixing | later |  |  |  | Not started |
| FR-ENG-13 | LATER | Weather normalization before weather-driven use | later |  |  |  | Not started |
| FR-DATA-1 | MUST | Scripted, resumable data provisioning | 1 | tests/test_datasets_manifest.py, test_manifest_hardening.py | microgrid/datasets/manifest.py, scripts/data_manifest.py | CHANGELOG | Partly done (manifest/verify done; no download step; real data untested) |
| FR-DATA-2 | MUST | Test fixtures (synthetic) | 1 | tests/test_synthetic_fixture.py, test_qa_1_6_review.py | tests/fixtures/synthetic_data.py | fixtures README | Done |
| FR-DATA-3 | MUST | Meter hierarchy | 2 |  |  |  | Not started |
| FR-DATA-4 | MUST | Storage | 1 |  |  |  | Not started |
| FR-DATA-5 | LATER | SEED coupling | later |  |  |  | Not started |
| FR-DATA-6 | SHOULD | Building footprints, DC | 1-4 |  |  |  | Not started |
| FR-DATA-7 | MUST | Building characteristics store | 1 |  |  |  | Not started |
| FR-DATA-8 | MUST | Dataset identity and catalog | 1 | tests/test_datasets_catalog.py | microgrid/datasets/catalog.py, configs/datasets/catalog.yaml | data-licenses.md | Partly done (engine does not yet take dataset ids; manifest link optional fields added) |
| FR-DATA-9 | SHOULD | Compact building-profile library | 2 |  |  |  | Not started |
| FR-WEB-1 | MUST | Composition form | 2 |  |  |  | Not started |
| FR-WEB-2 | MUST | Run with progress | 2 |  |  |  | Not started |
| FR-WEB-3 | MUST | Charts (interactive) | 3 |  |  |  | Not started |
| FR-WEB-4 | MUST | Downloads | 3 |  |  |  | Not started |
| FR-WEB-5 | MUST | Roles | 2 |  |  |  | Not started |
| FR-WEB-6 | MUST | Calibration screen | 3 |  |  |  | Not started |
| FR-WEB-7 | SHOULD | Minimal map viewer | 4 |  |  |  | Not started |
| FR-WEB-8 | MUST | Accuracy caveat | 3 |  |  |  | Not started |
| FR-WEB-9 | SHOULD | Admin console | 4 |  |  |  | Not started |
| FR-CAL-1 | MUST | Upload | 3 |  |  |  | Not started |
| FR-CAL-2 | MUST | Compare | 3 |  |  |  | Not started |
| FR-CAL-3 | MUST | Benchmarks | 3 |  |  |  | Not started |
| FR-CAL-4 | LATER | Tuning | later |  |  |  | Not started |
| FR-CAL-5 | LATER | PJM node data | later |  |  |  | Not started |
| FR-PLAT-1 | MUST | Versioned API skeleton | 1 | api/tests/test_health.py, test_models.py, test_migrations.py, test_qa_1_8.py, test_settings_*.py | api/src/microgrid_api/ | CHANGELOG | Done |
| FR-PLAT-2 | MUST |  | 1-2 |  |  |  | Not started |
| FR-PLAT-3 | MUST | Notebook-first installable library | 1 | tests/test_packaging.py | pyproject.toml, src/microgrid/__init__.py | codebase-review, README | In progress (example notebook pending story 1.9) |
| FR-PLAT-4 | MUST | One-command local stack | 1 | tests/test_deploy_compose*.py, test_deploy_fixpass.py | deploy/, Makefile | docs/runbooks/local-stack.md | Code-complete; verified statically only; Mac check pending |
| FR-PLAT-5 | MUST | CI gates merges | 1 | tests/test_ci_config.py | .github/workflows/ci.yml, pyproject tool config | sprint-1-backlog | Done (coverage gate pending) |
| FR-PLAT-6 | SHOULD | Public demo with live capped computation | 4 (stretch) |  |  |  | Not started |
| FR-PLAT-7 | SHOULD |  | 4 |  |  |  | Not started |
