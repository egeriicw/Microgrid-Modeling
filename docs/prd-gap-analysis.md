# PRD Gap Analysis — `docs/PRD.md` vs. the Product Faculty AI PRD template

**Date:** 2026-10-27 · **Template:** "Product Faculty — AI PRD" (4D: Discovery → Design → Develop, 34 prompts)
**Subject:** `docs/PRD.md` v1.0 (the SNLG platform PRD). The earlier HTML "SNLG Roadmap" artifact
only covers DER next steps and is superseded by `prd-suite-4d.md`.

## How to read the template for this product

The template targets generative-AI features (prompts, RAG, hallucination criteria). This product
is a simulation and planning suite, so rows are mapped by *intent*, not wording:

| Template concept | Equivalent here |
|---|---|
| Master prompt, prompt iterations | Scenario/study config schema and its versioning |
| AI model selection | Methodology selection (sampling, solver, dispatch, economic model) |
| RAG / data prep | Data pipeline: ResStock/ComStock, GIS footprints, measured loads, weather |
| Good-output checklist | Validation: physical plausibility, benchmark ranges, parity with reference tools |
| Evaluation set | Golden scenarios and known-answer test neighborhoods |

Only if natural-language scenario authoring is added would the prompt rows apply literally.

## Findings

Status: ✅ covered · 🟡 partial · ❌ missing · ➖ not applicable

| # | Template area | Status | What `docs/PRD.md` has | Gap / improvement |
|---|---|---|---|---|
| 1 | Market attractiveness | ❌ | Nothing | Name the market (community microgrids, DER planners), headwinds/tailwinds, and comparators (candidates to verify: REopt, URBANopt, HOMER, DER-CAM, Xendee, OpenDSS). Growth figures need a cited source; none is invented here. |
| 2 | Business model, stage, customers | ❌ | "Single-team, trusted network" | State whether this is internal tooling, open source, or a product; who pays; B2B vs. B2B2C. Drives auth, hosting, licensing. |
| 3 | Differentiators | ❌ | Implied (ensemble uncertainty) | Make explicit: uncertainty-aware ensembles, GIS-to-load traceability, reproducible seeds. Test each against the comparators. |
| 4 | Target personas | 🟡 | 3 personas (Priya, Ravi, Dana) | Add the community/municipal stakeholder and a developer/financier persona; the new GIS and economics scope serves them. |
| 5 | Current-state journey + pain points | ❌ | One target-state journey (UJ-1) | Add today's journey (notebook → CSV → hand-built GIS → spreadsheet) with measured effort per step. Rank pain points by frequency and severity. |
| 6 | Solution hypothesis (diverge/converge) | ❌ | Decisions D1–D8 stated, alternatives absent | Record alternatives considered (extend URBANopt vs. build; PostGIS vs. file-based GIS; RQ vs. Celery) and the ranking that led to each choice. |
| 7 | Future-state workflow | 🟡 | UJ-1 | UJ-1 stops at load results. Extend to map → neighborhood → load → DER → economics → report. |
| 8 | Wireframes / prototype | 🟡 | `frontend-spec.md` page list | No map-centric screens. The GIS builder is the new primary surface and needs wireframes first. |
| 9 | Evaluation criteria & example cases | 🟡 | Coverage gates; determinism golden test | Quality of *outputs* is undefined: validation benchmarks are placeholders. Define objective (peak within X% of metered feeder) and subjective (planner review) criteria. |
| 10 | Model/method selection & justification | 🟡 | Methodology in `engine-reference.md` | Justify against alternatives (Dirichlet vs. uniform, ensemble size, convergence ε). Add selection records for each new method. |
| 11 | Input specification table | 🟡 | YAML keys documented | No table for GIS inputs (CRS, required/optional attributes, licenses). Added in `prd-suite-4d.md`. |
| 12 | Output evaluation checklist | ❌ | Validation module exists | Document per-stage output checks and who signs off. |
| 13 | Data preparation | 🟡 | OEDI downloader | Add footprint sourcing, licensing (OSM and several footprint datasets are share-alike), CRS handling, attribute completeness reporting. |
| 14 | Evaluation set (typical + edge/negative) | ❌ | One baseline scenario | Build a golden set: known neighborhood with metered data, empty/boundary-only, missing attributes, mixed CRS, single-family-only (unmodeled). |

## Structural problems beyond the template

1. **Scope contradiction.** Non-goal §2.2 excludes "power-flow / dispatch / DER-sizing". The new
   direction makes those core. The PRD must be re-baselined, not patched.
2. **Success metrics are engineering-only** (A1–A8: installability, coverage). No user outcome
   (time to a first neighborhood study, accuracy against metered data) and no baseline.
3. **No assumptions/dependencies, decision log, or owner/date per requirement.**
4. **No data-governance section**: dataset licenses, privacy of metered load, provenance in outputs.
5. **Risks omit GIS-specific failure modes**: attribute sparsity in footprint layers, ResStock/ComStock
   coverage of local building types (single-family has no archetype category in the baseline config),
   and projection error.

## Engine defects found while reading (tracked, one fixed)

| Defect | Status |
|---|---|
| `validate_config` rejected a composition whose categories are all `fixed_count` (what a real GIS neighborhood produces). | **Fixed** with test `test_validate_config_accepts_all_fixed_count_composition`. |
| `scenario.name` loads as a list in some configs (`name=['DC Multifamily Baseline']`). | Open (already FE-3 in `PRD.md`). |
| `dc_multifamily_baseline.yaml` now says 10 buildings; its own description and notebook text say 50 (`_2.yaml` has 50). Parity tests must pin which file. | Open, needs owner decision. |

## Recommended next edits to `PRD.md`

Adopt `prd-suite-4d.md` as the v2 structure, keep `PRD.md` §4 (decisions) and §7 (acceptance) as
appendices, and move the platform specs (`api-spec`, `data-model`, ...) under a "Develop" heading.
