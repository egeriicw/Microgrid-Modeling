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
| FR-ENG-11 | MUST | Attribute filters (Admin-only, admin-configured) | 1-2 | tests/test_attribute_filters*.py, test_permissions.py, test_exposed_attributes.py, test_qa_filters_review.py, test_filters_fix_pass.py | synthetic/attribute_filters.py, community_load/exposed_attributes.py, permissions.py | CHANGELOG | Library done; API storage/endpoints/UI open; real column names [verify] |
| FR-ENG-12 | LATER | Scenario switching and mixing | later |  |  |  | Not started |
| FR-ENG-13 | LATER | Weather normalization before weather-driven use | later |  |  |  | Not started |
| FR-DATA-1 | MUST | Scripted data provisioning | 1 | tests/test_datasets_manifest.py, test_manifest_hardening.py, test_datasets_inspect*.py, test_qa_inspect.py | datasets/manifest.py, inspect.py, pathsafety.py, scripts/ | docs/runbooks/inspect-my-data.md | Manifest + inspector done; no download step; awaiting owner's real-data run |
| FR-DATA-2 | MUST | Test fixtures (synthetic) | 1 | tests/test_synthetic_fixture.py, test_qa_1_6_review.py | tests/fixtures/synthetic_data.py | fixtures README | Done |
| FR-DATA-3 | MUST | Meter hierarchy | 2 |  |  |  | Not started |
| FR-DATA-4 | MUST | Storage | 1 |  |  |  | Not started |
| FR-DATA-5 | LATER | SEED coupling | later |  |  |  | Not started |
| FR-DATA-6 | SHOULD | Building footprints, DC | 1-4 |  |  |  | Not started |
| FR-DATA-7 | MUST | Building characteristics store | 1 |  |  |  | Not started |
| FR-DATA-8 | MUST | Dataset identity and catalog | 1 | tests/test_datasets_catalog.py, test_dataset_resolution.py | datasets/catalog.py, community_load/dataset_resolution.py | data-licenses.md | Engine side done; real partitions/hash unfilled (owner machine) |
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
| FR-PLAT-2 | MUST | Background runs, progress, cancel | 1 | api/tests/test_worker.py, test_qa_1_10.py, test_ready_redis.py | api/src/microgrid_api/worker.py, services/runs.py | CHANGELOG | Worker done (no real Redis tested); HTTP endpoints and reaper Sprint 2 |
| FR-PLAT-3 | MUST | Notebook-first installable library | 1 | tests/test_packaging.py | pyproject.toml, src/microgrid/__init__.py | codebase-review, README | In progress (example notebook pending story 1.9) |
| FR-PLAT-4 | MUST | One-command local stack | 1 | tests/test_deploy*.py, test_qa_lite.py | deploy/, Makefile | docs/runbooks/local-stack.md | Code-complete incl. lite + preflight; statically verified only; Mac check pending |
| FR-PLAT-5 | MUST | CI gates merges | 1 | tests/test_ci_config.py | .github/workflows/ci.yml, pyproject tool config | sprint-1-backlog | Done (coverage gate pending) |
| FR-PLAT-6 | SHOULD | Public demo with live capped computation | 4 (stretch) |  |  |  | Not started |
| FR-PLAT-7 | SHOULD |  | 4 |  |  |  | Not started |

<!-- 2026-11-11: story 1.12 (chart spike) supports FR-WEB chart requirements; evidence docs/research/echarts-spike.md; QA pending -->

<!-- Sprint 2 batch A: FR-ENG-5/6/7 -> tests/test_convergence_prd.py, test_convergence_config.py, test_results_model.py, test_qa_sprint2_engine.py; FR-WEB-2/5 (API side) -> api/tests/test_http_runs.py, test_auth_roles.py, test_reaper.py, test_qa_sprint2_http.py, test_fix_pass_results.py; FR-WEB-1/2/3/8 (UI, mock API) -> web/src tests + web/e2e. -->
