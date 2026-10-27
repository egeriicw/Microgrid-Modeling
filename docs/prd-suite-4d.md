# Community Microgrid Planning Suite — PRD v2 (4D structure)

**Owner:** Bill Eger · **Draft:** 2026-10-27 · **Status:** Draft. `[OPEN]` marks answers only the owner can supply;
nothing marked `[OPEN]` has been guessed. Gap rationale: `prd-gap-analysis.md`.

## 0. Product

**Name (working):** Community Microgrid Planning Suite (code package `microgrid`).
**One line:** Pick a neighborhood on a map and get an uncertainty-aware hourly load, the DER to serve it, and what it costs.
**Industry:** Energy / distributed energy planning.
**Hierarchy:** Microgrid Planner (`microgrid`) has `geo` (structures, locations, neighborhoods) as its shared reference layer. The Community Load Profiler (`microgrid.community_load`, containing the SNLG engine `.synthetic`) sits parallel to `geo` and reads it. DER, dispatch and economics also read `geo` (a battery or PV array has a place, and distribution constraints depend on layout). They receive load through `core.LoadResult`, not from the profiler. The platform and UI are specified in `PRD.md`, not built.

## 1. Discovery

### 1.1 Business value
| Topic | Response |
|---|---|
| Market and growth | `[OPEN]` Target segment and a cited growth source. Do not quote figures without one. |
| Headwinds / tailwinds | `[OPEN]` Candidate themes to validate: interconnection delays, resilience demand, DER cost declines, incentive changes. |
| Comparators to evaluate | NREL REopt, NREL URBANopt, HOMER, DER-CAM, Xendee, OpenDSS (capabilities unverified; confirm before claiming gaps). |
| Stage and model | `[OPEN]` Internal tool, open source, or product? Revenue model and buyer. |
| Differentiators (hypotheses) | Ensemble uncertainty instead of single profiles; GIS-to-load traceability per structure; reproducible seeds; open methodology. |

### 1.2 Users
| Persona | Job | Today's pain |
|---|---|---|
| Planner Priya (utility DER planning) | Size resources for a candidate community | Hand-assembling building lists and loads across tools |
| Researcher Ravi | Test methods, compare scenarios | No reproducible pipeline beyond load |
| Developer Dana | Maintain the stack | Two engines, no packaging |
| Community lead Carmen (new) | Decide whether a microgrid is worth pursuing | Cannot evaluate without engineers |
| Financier Felix (new) | Judge bankability | No uncertainty-aware economics |

**Current-state journey** `[OPEN: time each step]`: obtain footprints → classify by hand → map to archetypes → run notebook → export CSV → size DER in another tool → economics in a spreadsheet.
**Top pain points (hypothesis, rank with users):** (a) neighborhood assembly is manual; (b) uncertainty is lost between tools; (c) no single reproducible artifact.

### 1.3 Solution hypothesis
Diverge: extend URBANopt; wrap REopt for DER sizing; build native modules; notebook-only toolkit.
Converge (current lean): native Python suite that *interoperates* (exports REopt/URBANopt inputs) rather than replaces them. `[OPEN: confirm]`.

## 2. Design

### 2.1 Target workflow
Draw/choose boundary → load footprints → review classification and unmodeled structures → choose load source
(synthetic ensemble or measured) → add DER → run study → review net load, P10/P90 band, economics → export report.

### 2.2 Screens (wireframe these first; Excalidraw)
1. Map builder (boundary, structure list, class overrides, "unmodeled load" warning). 2. Study setup. 3. Results (net load, band, DER contribution). 4. Compare scenarios.

### 2.3 Evaluation criteria and cases
| Kind | Criterion |
|---|---|
| Objective | Determinism (same inputs and seeds → identical outputs); physical plausibility per run; synthetic P50 peak/energy within `[OPEN]`% of metered data for the pilot neighborhood |
| Subjective | Planner judges classification review screen understandable; report usable without the author |
| Edge/negative cases | Empty boundary; boundary-only; missing stories; unknown use; MultiPolygon; footprints in wrong CRS; single-family-only (no archetype); measured data with gaps/duplicates |

## 3. Develop

### 3.1 Method selection
| Stage | Choice | Alternatives to justify against |
|---|---|---|
| Neighborhood composition | Real structures fix counts; archetype draw stays stochastic | Fully deterministic per-structure matching (needs richer attributes) |
| Geometry | Local equirectangular, dependency-free | pyproj/shapely when extents or CRS needs grow |
| Load | `snlg` ensemble or measured CSV | Hybrid calibration of synthetic to measured |
| DER, dispatch, economics | `[OPEN]` select per module before implementing (pvlib is already a dependency for PV) | REopt, rule-based vs. optimization dispatch |

### 3.2 Input specification (GIS)
| Field | Required | Format | Notes |
|---|---|---|---|
| Footprint polygon | Yes | GeoJSON Polygon/MultiPolygon, WGS84 | Area computed from geometry, never from attributes |
| Use/building type | Recommended | string, mapped by `classify_use` | Unknown → `UNKNOWN`, never guessed |
| Stories | Recommended | int | Drives MF size category; default documented and counted |
| Dwelling units, year built, height | Optional | int / year / m | Reserved for archetype matching |
| Boundary | Yes | lon/lat ring | Membership by structure centroid |
| Measured load | Optional | CSV `timestamp,kw` | Gaps and duplicates are rejected |

### 3.3 Data preparation
Footprint sources and licenses `[OPEN]` (OSM and some open footprint datasets carry share-alike terms, which affect redistribution of outputs). Provenance (layer, version, retrieval date) must be recorded in every study output.

### 3.4 Build status and roadmap
| Phase | Scope | Status |
|---|---|---|
| 0 | `snlg` engine, notebook | Done (50 tests) |
| 1 | `microgrid.core`, `microgrid.community_load.geo`, snlg bridge, measured load, `Study` pipeline | **Started** (22 tests) |
| 2 | Real-footprint pilot and validation against metered data; resolve baseline-config ambiguity | Next |
| 3 | DER: solar (pvlib), storage, EV | Interfaces only |
| 4 | Dispatch and economics | Not started |
| 5 | Platform and map UI per `PRD.md` / `frontend-spec.md` | Specified, not built |

### 3.5 Success metrics `[OPEN: set targets after Phase 2 baseline]`
Time from boundary to first study; P50 error vs. metered; share of structures modeled vs. skipped; reproducibility pass rate.

### 3.6 Risks
Sparse footprint attributes; no archetype for single-family in the baseline config (unmodeled load must stay visible); license constraints on footprint data; scope creep into power flow.
