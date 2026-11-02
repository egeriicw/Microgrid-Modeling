# Codebase Review — Microgrid Planner (as of 2026-11-02, commit f87f29b)

Purpose: give the owner a grounded picture of what exists, what works, what is
only planned, and what is risky, so a PRD and dev plan can be built on facts.
Method: read the source, docs, config, CI and git history; ran the engine tests.
**Not done:** running the engine on real data (the ResStock/ComStock data is not in
the repo), reviewing the 1.3 MB `testing.ipynb`, or validating any numbers.

## 1. One-paragraph summary

This repo is a **working Monte-Carlo load-generation engine plus a lot of paper**.
About 2,500 lines of real, tested Python build uncertainty-aware hourly electricity
load for a hypothetical neighborhood from NREL ResStock/ComStock building data.
Around that sit a small GIS-to-engine bridge and empty interface stubs for DER,
dispatch and economics. Everything described as "platform" (API, worker, database,
web UI, Docker stack) is **a specification and RED test placeholders only**. No
implementation exists. There are 3,200 lines of docs, including a PRD, written
largely by earlier AI sessions.

## 2. Timeline (what the git history tells us)

| Date | What happened |
|---|---|
| 2026-01-16/22 | Hand/AI-built notebook pipeline: building load profiles, summaries, directory shuffling ("Basic app build out", "updated code base") |
| 2026-06-14/15 | PR #9: correctness bugs fixed, reproducibility seeds added |
| 2026-06-19 | Notebook logic extracted into a typed `snlg` package, ~50 tests |
| 2026-09-03 | uv environment, pyarrow fixes |
| 2026-10-27/28 | PR #10: package restructured to `microgrid.*`, GIS `geo` layer, `study` pipeline, DER/dispatch/economics stubs, PRD v1 + 4D PRD suite + gap analysis |

28 commits, 5 months, one author. Early commit messages are uninformative
("updates"); later ones are descriptive.

## 3. Architecture as built

```
microgrid
├─ core            LoadResult (hourly kW + optional P10/P90 + metadata)
├─ geo             Structure, Neighborhood, GeoJSON reader, point-in-polygon, use classification
├─ community_load
│  ├─ categorize   Structure -> SNLG category (mf_small/mid/large, commercial)
│  ├─ sources      LoadSource protocol: SnlgEnsembleSource, MeasuredLoadSource (CSV)
│  └─ synthetic    the engine (config, archetype, composition, loader, perturbation,
│                  aggregation, feasibility, ensemble, validation, outputs, rng)
├─ der / dispatch / economics   interfaces only (DERModel protocol; others are docstrings)
└─ study           Study(neighborhood, load_source, der_models).run() -> net load
```

`tests/test_architecture.py` enforces that dependencies point downward only. That
is a good practice.

### Engine flow (per Monte-Carlo run)
1. Sample building mix (Dirichlet or uniform, projected onto min/max fraction bounds, rounded to integer counts).
2. Draw building instances (with replacement) from filtered archetype pools.
3. Load each building's 15-min parquet, scale multifamily by unit count, resample to hourly.
4. Optionally perturb (circular time shift, lognormal end-use scaling).
5. Sum to a neighborhood profile; compute peak, annual kWh, load factor.
6. Hard/soft feasibility screen; accept or reject.
7. Repeat to `max_runs` or until adaptive convergence; output P10/P50/P90 envelopes, diversity stats, representative cases.

Three seeded RNG layers (composition, archetype, perturbation) make runs reproducible in principle.

## 4. Status by area

| Area | Real code? | Tests | Status |
|---|---|---|---|
| Engine (`synthetic/`) | Yes, ~1,900 lines | ~100 unit tests, pass in 0.5 s | Works on fixtures; **never validated against real data in CI** |
| GIS bridge (`geo`, `categorize`, `snlg_source`) | Yes, small | Yes | Works; GeoJSON-only, naive geometry; single-family and industrial are **unmodeled and skipped** |
| `study`, `LoadSource`, `DERModel` | Yes, tiny | Yes | Skeleton. No DER model exists |
| `der`, `dispatch`, `economics` | **No** (interfaces/docstrings) | No | Not started |
| API / worker (`api/`) | **No**: only `pyproject.toml` and tests | Tests are xfail/skip placeholders | Not started |
| Web (`web/`) | **No**: two test files only; no `package.json` | placeholders | Not started |
| Deploy (`deploy/`, Makefile) | Compose file written; references services that don't exist | smoke script | Untested, cannot build |
| CI | Lint (`\|\| true`, so never fails), engine tests; api/web/compose jobs `if: false` | — | Only the engine is gated |
| Notebook | The real "UI" today | none | Hand-run; hardcodes `sys.path`, hand-picks config |
| Docs | 14 files, 3,200 lines | — | Detailed but describe a system that mostly doesn't exist; some conflict (see §6) |

Test result when run: **106 passed, 2 skipped, 3 xfailed, 2 xpassed.** The xfail/skip items are
deliberate "RED" placeholders for unbuilt features. Two xpassed placeholders mean
packaging is further along than the markers say (the markers are stale).

## 5. Findings

### Correctness / scientific risk (highest importance for a modeling product)
- **Nothing proves the numbers are right.** No metered-data comparison, no benchmark ranges, no reference-tool parity. The `validation.py` module checks sanity ranges from config, not accuracy. "By eye" is the real validation.
- **Data is absent.** `data/` holds only placeholders. The parquet/xlsx files are git-ignored, so the real pipeline is unrunnable from a fresh clone or in CI. Anything touching real data has no automated check.
- **`derive_run_rng`** builds per-run seeds as `random_draw + run_index`. That is not a statistically clean seeding method; `SeedSequence.spawn` is the standard fix (already planned in the docs, not done).
- **Convergence history is O(n²)** (recomputes means from scratch each step); fine at 50-300 runs, not at scale.
- **Perturbation end-use scaling is mostly dormant.** `perturb_building` only applies end-use factors when it is given the raw 15-min frame, and `ensemble._run_single` never passes it. Only time-shifts and no-ops actually run. The config suggests more.
- **Config silently ignores keys.** `baseline_2.yaml` sets `arealimit`, `garage`, `heatpump`, `electric_vehicle`, `solar_pv`, `require_data_complete` and the parser does not use them. A user would believe they filtered or adopted something they didn't.
- **Coincidence/diversity factors** are written in the code and never attached to results.
- **Units:** the series is hourly kWh used as kW (valid for 1 h steps, but not labeled).
- **Geometry** is a local equirectangular approximation, with no CRS handling beyond assuming WGS84 lon/lat.

### Product / scope
- **Three different "products" are described:** (a) a load-generation engine, (b) a platform around it (API, web, DB), (c) a full community-microgrid planner (map, DER, dispatch, economics). The v1 PRD's non-goals exclude (c); the later suite PRD makes (c) core. The gap analysis already notes this contradiction.
- **No users, no first customer, no success metric** defined beyond engineering gates (installable, coverage).
- **`dc_multifamily_baseline.yaml` says 10 buildings; its own description (and notebook text) say 50.** It is unclear which is the parity reference.
- **Two config files differ by whole building categories** (single-family added in `_2`), and the notebook points at `_2`.

### Engineering hygiene
- Lint and format are non-blocking in CI.
- Coverage gate documented (≥90%) but not enforced.
- `pyproject.toml` lists runtime dependencies that look wrong for a library (`pytest`, `ipykernel`, the stdlib backport `dataclasses`, `boto3`, `pvlib` not imported anywhere in `src`, `tests` or `scripts`). Python floor says ≥3.10 while the pin file says 3.13.
- No logging/config/secrets strategy yet; `.env.example` exists for a stack that isn't built.
- Parquet reads are cached with an unbounded `lru_cache` (memory risk for long runs).
- 1.3 MB output-bearing notebooks are committed (`testing.ipynb`); `notebooks/backup.py` is dead legacy code containing the original random-choice approach.
- No license file.

### Strengths worth keeping
- Methodology is explicit and mapped to code sections (composition, perturbation, aggregation, feasibility, convergence).
- Typed dataclass config with validation, and a clean layered package with an enforced dependency rule.
- Determinism and architecture are tested, not just stated.
- The GIS-to-engine bridge makes "unmodeled load" visible instead of silent.
- The docs set shows a disciplined spec-first, test-first intent (traceable FR IDs).

## 6. Documentation reliability

The docs are an AI-generated plan, not a record of reality. Known inconsistencies:
- PRD v1 locks decisions (RQ, MinIO, Next.js Pages Router + MUI, Postgres 16) before any validated need.
- It names a distribution `snlg` and facade `microgrid.community_load.synthetic.run`; the code is now `microgrid.*` and the facade doesn't exist.
- README says "50 passing tests"; the suite has ~106.
- PRD refers to `feat/*` branches containing an earlier scaffold; this clone has only the current branch (confirmed: only `main` and this branch exist, so those scaffolds are not available here).

Treat `docs/` as **hypotheses to confirm with the owner**, not requirements.

## 7. What is missing for "production hosted web app"

Product definition (users, jobs-to-be-done, accuracy target) · validated model ·
data provisioning/licensing · API · persistence · job queue · web UI · auth ·
observability · deployment/IaC · security review · performance/cost envelope ·
release and versioning process. None of the last nine exist even in prototype form.

## 8. Open questions for the owner (needed before the PRD)

1. What were you originally building? (what problem, for whom)
2. Is the load profile the product, or only the first step toward sizing/dispatch/economics?
3. Who is the first real user, and what decision do they make with the output?
4. What reference data exists to check results (a metered neighborhood or feeder, a HOMER/REopt run)?
5. Which baseline config is the truth (10 or 50 buildings; with or without single-family)?
6. Can you obtain and legally redistribute (or host) ResStock/ComStock extracts for the target geographies?
7. Is the earlier `feat/*` scaffold still wanted?
