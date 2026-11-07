# Requirements Traceability Matrix

Maintained by the Business Analyst role (see `agent-team.md`). A requirement is **Done** only when it has a green
test, merged code, and updated docs. Generated from `docs/PRD.md` on 2026-11-06; update statuses each sprint.

| ID | Priority | Title | Sprint | Tests | Code | Docs | Status |
|---|---|---|---|---|---|---|---|
| FR-ENG-1 | MUST | Building-type registry | 1 | tests/test_registry.py, test_community_load_categorize.py, test_geo.py | community_load/registry.py, building_types.yaml, categorize.py | CHANGELOG | Done (ComStock names to verify on real data) |
| FR-ENG-2 | MUST | Composition modes | TBD |  |  |  | Not started |
| FR-ENG-3 | MUST | Config honesty | 1 | tests/test_config_strict.py | synthetic/config.py | CHANGELOG | Done |
| FR-ENG-4 | MUST | Seeding: random by default, replayable | 1 | tests/test_rng.py | synthetic/rng.py, ensemble.py | CHANGELOG | Partly done (golden + full-ensemble robustness need 1.6/1.9) |
| FR-ENG-5 | MUST | Resolutions | 2 |  |  |  | Not started |
| FR-ENG-6 | MUST | Adaptive convergence | 2 |  |  |  | Not started |
| FR-ENG-7 | MUST | Outputs | 2 |  |  |  | Not started |
| FR-ENG-8 | MUST | Progress events | 1 |  |  |  | Not started |
| FR-ENG-9 | SHOULD | Perturbations | TBD (owner) |  |  |  | Not started |
| FR-ENG-10 | MUST | Temperature | 2 |  |  |  | Not started |
| FR-ENG-11 | MUST | Attribute filtering of source buildings, admin-configured | 1 |  |  |  | Not started |
| FR-ENG-12 | LATER | Scenario switching and mixing | later |  |  |  | Not started |
| FR-ENG-13 | LATER | Weather normalization before weather-driven use | later |  |  |  | Not started |
| FR-DATA-1 | MUST | DC ingestion | 1 |  |  |  | Not started |
| FR-DATA-2 | MUST | Test fixtures | 1 |  |  |  | Not started |
| FR-DATA-3 | MUST | Meter hierarchy | 2 |  |  |  | Not started |
| FR-DATA-4 | MUST | Storage | 1 |  |  |  | Not started |
| FR-DATA-5 | LATER | SEED coupling | later |  |  |  | Not started |
| FR-DATA-6 | SHOULD | Building footprints, DC | 1-4 |  |  |  | Not started |
| FR-DATA-7 | MUST | Building characteristics store | 1 |  |  |  | Not started |
| FR-DATA-8 | MUST | Dataset identity and catalog | 1 |  |  |  | Not started |
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
| FR-PLAT-1 | MUST |  | 1 |  |  |  | Not started |
| FR-PLAT-2 | MUST |  | 1-2 |  |  |  | Not started |
| FR-PLAT-3 | MUST | Notebook-first installable library | 1 | tests/test_packaging.py | pyproject.toml, src/microgrid/__init__.py | codebase-review, README | In progress (example notebook pending story 1.9) |
| FR-PLAT-4 | MUST |  | 1 |  |  |  | Not started |
| FR-PLAT-5 | MUST | CI gates merges | 1 | tests/test_ci_config.py | .github/workflows/ci.yml, pyproject tool config | sprint-1-backlog | Done (coverage gate pending) |
| FR-PLAT-6 | SHOULD | Public demo with live capped computation | 4 (stretch) |  |  |  | Not started |
| FR-PLAT-7 | SHOULD |  | 4 |  |  |  | Not started |
