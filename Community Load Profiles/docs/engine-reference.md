# Engine Reference — `snlg` (Synthetic Neighborhood Load Generation)

Reference documentation for the **existing** engine in `src/snlg/`, as of
branch `claude/software-methodology-plan-*` (commits `bb80bbd` "Add SNLG
package" + `00f9408` "Refactor notebook to thin snlg-package orchestration
layer"). **50 unit tests pass** (`PYTHONPATH=src pytest`).

This document is both (a) the deliverable for PRD goal **G2 / FR-E10** —
the implementing agent must keep it exhaustive and current — and (b) the
authoritative description of behaviour that FR-E1..E11 must preserve.

---

## 1. What the engine does

Given a **scenario config** (YAML), the engine produces an **ensemble** of
synthetic neighborhood hourly load profiles (8760 values each) plus derived
statistics, by Monte-Carlo sampling over building mix, building instances,
and instance-level perturbations, screening each realization for feasibility
and stopping when ensemble means stabilize.

Pipeline (methodology section → module):

| § | Stage | Module | Deterministic? |
|---|---|---|---|
| 1 | Archetype filtering (`A_filtered`) | `archetype.py` | yes (pure filter) |
| 2–3 | Composition sampling → integer counts; building selection | `composition.py` | stochastic (RNG L1, L2) |
| — | Parquet load, MF unit scaling, 15-min→hourly | `loader.py` | yes (given inputs) |
| 4 | Instance perturbation (temporal shift, end-use factors) | `perturbation.py` | stochastic (RNG L3); **off by default** |
| 5 | Neighborhood aggregation + metrics | `aggregation.py` | yes within a realization |
| 6 | Feasibility (hard reject + soft score) | `feasibility.py` | yes |
| 7–8 | Ensemble loop + convergence | `ensemble.py` | orchestration |
| 9 | Output writing (legacy-compatible CSV/PNG) | `outputs.py` | yes |
| 10 | Layered RNG architecture | `rng.py` | — |
| 11 | Validation (per-sample, distributional, structural) | `validation.py` | yes |
| — | Data contracts (no logic) | `_types.py` | — |
| — | Config load + validate | `config.py` | yes |

---

## 2. Public surface (current)

`src/snlg/__init__.py`:
```python
from snlg import config, rng, archetype, loader, composition, aggregation, ensemble, outputs
__all__ = ["config", "rng", "archetype", "loader", "composition",
           "aggregation", "ensemble", "outputs"]
```
> Note: `perturbation`, `feasibility`, `validation`, `_types` are **not** in
> `__all__` today though they are used by callers/tests. FR-E2 requires
> making the surface explicit (and adding `run`, `__version__`).

### Typical caller (from the notebook, `community_microgrid_load_profiles.ipynb`)

```python
from snlg import archetype, config, ensemble, outputs, validation

cfg  = config.load_config(Path("../configs/scenarios/dc_multifamily_baseline_2.yaml"))
pool = archetype.build_archetype_pool(resstock_chars_path, comstock_chars_path, cfg)
data_dirs = {"resstock": Path(cfg.data.resstock_timeseries_dir),
             "comstock": Path(cfg.data.comstock_timeseries_dir)}
result = ensemble.run_ensemble(pool, cfg, data_dirs)          # -> EnsembleResult
out_dir = outputs.save_all_outputs(result, cfg, timestamp_str, elapsed_minutes)
report  = validation.validate_ensemble(result, cfg.validation)
```

---

## 3. Data contracts — `_types.py`

Pure dataclasses, no computation. Type aliases:
`LoadProfile = pd.Series` (DatetimeIndex → kWh/h, length 8760);
`BuildingId = int`.

| Dataclass | Fields | Produced by |
|---|---|---|
| `ArchetypePool` | `residential: dict[str, list[BuildingId]]`, `commercial: dict[str, list[BuildingId]]`, `resstock_chars: pd.DataFrame`, `comstock_chars: pd.DataFrame`, `metadata: dict` | `archetype.build_archetype_pool` |
| `NeighborhoodComposition` | `counts: dict[str,int]`, `proportions: dict[str,float]` | `composition.sample_composition` |
| `BuildingSelection` | `bldg_id`, `category`, `source` (`"resstock"`/`"comstock"`), `unit_multiplier: float` | `composition.sample_buildings` |
| `PerturbedProfile` | `bldg_id`, `category`, `source`, `hourly_kwh: LoadProfile`, `original_annual_kwh: float`, `perturbed_annual_kwh: float`, `perturbation_log: dict` | `loader.load_and_scale_profile` (identity) then `perturbation.perturb_building` |
| `NeighborhoodResult` | `run_index`, `residential_profile`, `commercial_profile`, `total_profile`, `composition`, `selections: list[BuildingSelection]`, `peak_kw`, `annual_energy_kwh`, `load_factor`, `feasible: bool`, `soft_score: float`, `perturbation_logs: list[dict]` | `ensemble._run_single` |
| `EnsembleResult` | `runs: list[NeighborhoodResult]` (feasible only), `converged: bool`, `n_attempted: int`, `n_rejected: int`, `convergence_history: dict[str,list[float]]`, `percentile_envelopes: dict[str,LoadProfile]`, `diversity_metrics: dict[str,float]`, `representative_cases: dict[str,NeighborhoodResult]` | `ensemble._build_ensemble_result` |

> **Gap G-7 (FR-E7):** `NeighborhoodResult` has **no** coincidence /
> diversity factor field even though `aggregation.compute_coincidence_factor`
> exists. `EnsembleResult.diversity_metrics` is populated but
> `percentile_envelopes` uses the string keys `p10`/`p50`/`p90` while
> `outputs`/notebook sometimes look for `p{int(p*100):02d}` — consistent, but
> note the zero-pad.

---

## 4. Configuration — `config.py`

`load_config(path) -> ScenarioConfig` reads YAML, builds nested dataclasses,
then calls `validate_config`. `_deep_merge` exists for layering (override
wins) but `load_config` currently applies section parsers directly without a
base-file merge.

### 4.1 `ScenarioConfig` tree

```
ScenarioConfig
├── name: str = "unnamed"                 # from scenario.name (see gap G-2)
├── description: str = ""
├── rng: RNGConfig
│     ├── composition_seed: int = 1001
│     ├── archetype_seed: int = 2002
│     └── perturbation_seed: int = 3003
├── data: DataConfig
│     ├── upgrade: str = "0"
│     ├── state: str = "DC"
│     ├── resstock_chars_file: str = "DC_upgrade0.xlsx"
│     ├── comstock_chars_file: str = "DC_upgrade0_agg.xlsx"
│     ├── resstock_timeseries_dir: str = "../data/input/resstock/timeseries_individual_buildings"
│     ├── comstock_timeseries_dir: str = "../data/input/comstock/timeseries_individual_buildings"
│     └── background_dir: str = "../data/background"
├── composition: CompositionConfig
│     ├── total_buildings: int = 50
│     ├── method: "dirichlet" | "uniform_renorm" = "dirichlet"
│     ├── dirichlet_concentration: float = 5.0
│     └── categories: dict[str, CategoryBounds]
│           CategoryBounds{ min_fraction=0.0, max_fraction=1.0,
│                           fixed_count: int|None=None, source="resstock" }
├── archetype_filters: ArchetypeFilterConfig
│     ├── resstock: dict[str, ResidentialFilterConfig]
│     │     ResidentialFilterConfig{ building_type_heights: list[str],
│     │        hvac_types: list|None, hvac_types_exclude: list|None,
│     │        max_sqft: float|None, require_no_garage: bool=False,
│     │        require_data_complete: bool=True }
│     └── comstock: dict[str, CommercialFilterConfig]
│           CommercialFilterConfig{ building_types: list[str], include_public: bool=False }
├── perturbation: PerturbationConfig
│     ├── enabled: bool = False
│     ├── max_temporal_shift_hours: int = 2
│     ├── energy_tolerance: float = 0.05
│     └── end_use_factors: dict[str, EndUseFactorConfig{ sigma: float=0.0 }]
├── feasibility: FeasibilityConfig
│     ├── hard: HardConstraints{ peak_kw_min=0, peak_kw_max=1e9,
│     │        annual_energy_kwh_min=0, annual_energy_kwh_max=1e12,
│     │        load_factor_min=0.0, load_factor_max=1.0 }
│     └── soft: SoftConstraints{ weights: dict[str,float],
│              bounds: dict[str, SoftConstraintBound{ min: float|None, max: float|None }] }
├── ensemble: EnsembleConfig{ mode: "fixed"|"adaptive"="fixed",
│              min_runs=20, max_runs=50, check_every=10 }
├── convergence: ConvergenceConfig{ lookback_window=10, min_stable_window=10,
│              epsilon=0.01, metrics=["peak_kw","annual_energy_kwh","load_factor"] }
├── validation: ValidationConfig{ benchmarks: dict[str,list[float]], min_cv_peak=0.01 }
└── output: OutputConfig{ base_dir="../data/output/scenario_runs",
             per_run_csvs=True, percentiles=[0.10,0.50,0.90],
             representative_cases=True, validation_report=True,
             preserve_legacy_format=True }
```

### 4.2 YAML mapping notes

- `scenario:` block → `name`, `description` (`.get("name", path.stem)`).
- Section parsers are only invoked **if the key is present** in the YAML;
  otherwise the dataclass default stands.
- `feasibility.hard` YAML uses nested `{peak_kw: {min, max}}` and is
  flattened into `HardConstraints.peak_kw_min/max` etc. by `_parse_feasibility`.
- `feasibility.soft.bounds.<metric>.{min,max}` → `SoftConstraintBound`.
- `perturbation.end_use_factors.<group>.sigma` → `EndUseFactorConfig`.
  Groups referenced by the engine: `lighting`, `plug_loads`, `hvac`,
  `hot_water` (see §7 `perturbation`).

### 4.3 `validate_config(cfg)`

Raises `ValueError` (aggregated message) when, for variable
(non-`fixed_count`) categories:
- `Σ min_fraction > 1.0` → infeasible, or
- `Σ max_fraction < 1.0` → infeasible.

Nothing else is validated today (no enum checks, no unknown-key detection,
no data-path existence). FR-E6 expands this.

### 4.4 Known config gaps (must be addressed by FR-E6)

| ID | Gap | Evidence |
|---|---|---|
| G-1 | `dc_multifamily_baseline_2.yaml` sets `archetype_filters.resstock.*` keys `arealimit`, `garage`, `heatpump`, `electric_vehicle`, `solar_pv` — **none are parsed**; `_parse_res_filter` reads only `building_type_heights, hvac_types, hvac_types_exclude, max_sqft, require_no_garage, require_data_complete`. Silent no-op. |
| G-2 | `scenario.name`/`description` are YAML **lists** (`- "DC Multifamily Baseline 2"`) in the shipped configs; `raw.get("scenario",{}).get("name")` yields a list → `cfg.name` becomes `['DC Multifamily Baseline 2']` and is later f-string-interpolated as `"['...']"`. |
| G-3 | `require_data_complete` is stored but never used by `archetype.py` filters. |
| G-4 | `data.*` paths are relative (`../data/...`) — only correct when CWD is `notebooks/`. Breaks from repo root / API worker. |
| G-5 | `ensemble.mode` / `composition.method` accept any string; a typo silently falls through to the "else" branch. |
| G-6 | No detection of unknown top-level sections or misspelled keys. |

---

## 5. Layered RNG — `rng.py` (methodology §10)

Three independent `numpy.random.Generator` streams so layers never
interfere:

| Layer | Stream | Governs |
|---|---|---|
| 1 | `RNGBundle.composition` | category mix, integer building counts |
| 2 | `RNGBundle.archetype` | building-instance selection (`rng.choice` w/ replacement) |
| 3 | `RNGBundle.perturbation` | temporal shift, end-use lognormal factors |

- `make_rng_bundle(cs, as_, ps)` → `RNGBundle(default_rng(cs), default_rng(as_), default_rng(ps))`.
- `derive_run_rng(root, run_index)` → new bundle where each stream is
  `default_rng(int(root.<stream>.integers(2**31)) + run_index)`.

> **Gap G-8 (FR-E5):** the `+ run_index` construction means run *k* and run
> *k+1* can draw seeds that differ by 1, and different `root.integers`
> outcomes plus different `run_index` can **collide** to the same seed.
> Replace with `SeedSequence(base).spawn(n)` per layer. `test_rng.py`
> currently only checks adjacent-index distinctness and reproducibility, both
> of which a spawn-based impl also satisfies.

Reproducibility guarantee **as designed**: identical `(composition_seed,
archetype_seed, perturbation_seed)` + identical call order ⇒ identical
per-run bundles ⇒ identical results. FR-E4 turns this into an enforced
golden test.

---

## 6. Archetype filtering — `archetype.py` (methodology §1)

`build_archetype_pool(resstock_chars_path, comstock_chars_path, cfg) -> ArchetypePool`

1. `load_resstock_characteristics` / `load_comstock_characteristics` read the
   Excel sheet named `building_characteristics`.
2. For each `cfg.archetype_filters.resstock[cat]`, `_filter_resstock_category`
   builds a boolean mask:
   - `in.geometry_building_type_height` ∈ `building_type_heights`
   - `in.hvac_heating_type` ∈ `hvac_types` (if set) and ∉ `hvac_types_exclude`
   - `in.sqft..ft2` ≤ `max_sqft` (if set)
   - `in.geometry_garage` is null (if `require_no_garage`)
   → returns `df.loc[mask, "bldg_id"].tolist()`.
3. For each `cfg.archetype_filters.comstock[cat]`,
   `_filter_comstock_category`:
   - `in.comstock_building_type` ∈ `building_types`
   - exclude `PrimarySchool`, `SecondarySchool` unless `include_public`.
4. `metadata` records raw counts, per-category sizes, and the source paths.

`MF_BUILDING_TYPES` (module constant, also used by `loader`):
`"Multi-Family with 2 - 4 Units"`, `"Multi-Family with 5+ Units, 1-3 Stories"`,
`"... 4-7 Stories"`, `"... 8+ Stories"`.

Empty pools are **not** an error here; they surface later in
`composition.sample_buildings` as `ValueError("Category '<cat>' has no
buildings in the filtered pool.")`.

Expected characteristics columns (contract with the input data):
`bldg_id`, `in.geometry_building_type_height`, `in.hvac_heating_type`,
`in.sqft..ft2`, `in.geometry_garage`,
`in.geometry_building_number_units_mf` (ResStock);
`bldg_id`, `in.comstock_building_type` (ComStock).

---

## 7. Composition & selection — `composition.py` (methodology §2–3)

### 7.1 `sample_composition(pool, cfg, rng) -> NeighborhoodComposition`

- Split categories into **fixed** (`fixed_count is not None`) and
  **variable**. `variable_total = total_buildings − Σ fixed_count`.
- For variable categories, build `p_min`/`p_max` arrays from
  `CategoryBounds.min_fraction/max_fraction` and dispatch on
  `cfg.composition.method`:
  - **`dirichlet`**: `alpha = max(concentration · midpoint, 1e-6)` where
    `midpoint = (p_min+p_max)/2`; `raw_p = rng.dirichlet(alpha)`; project
    onto `[p_min,p_max] ∩ simplex` via `_project_simplex_box`.
  - **`uniform_renorm`**: `raw_p = rng.uniform(p_min, p_max)`; same
    projection.
- `_project_simplex_box(p, p_min, p_max, max_iter=200)`: clip to box, then
  iteratively redistribute the deficit `1−Σp` proportionally to remaining
  headroom (`p_max−p` if deficit>0 else `p−p_min`); stop when `|deficit| <
  1e-12` or capacity exhausted. Converges when `Σp_min ≤ 1 ≤ Σp_max`.
- `_proportions_to_counts(props, total)`: `floor(props·total)`, then hand
  the remaining units to the categories with the largest fractional parts.
  **Guarantees `Σ counts == total`** (tested).
- Result `proportions` are the **realized** integer proportions
  (`count/total`), not the sampled floats.

Edge cases handled: all-fixed (`variable_total == 0` → variable cats set to
0); degenerate `min==max` (deterministic counts).

### 7.2 `sample_buildings(composition, pool, cfg, rng, resstock_chars=None) -> list[BuildingSelection]`

- For each category with `count > 0`: pick `bldg_pool` from
  `pool.residential[cat]` or `pool.commercial[cat]` by `CategoryBounds.source`.
- `rng.choice(bldg_pool, size=count, replace=True)` — **sampling with
  replacement** (the same archetype can appear multiple times).
- MF unit multiplier: if `source=="resstock"` and `resstock_chars` given,
  look up `in.geometry_building_number_units_mf` for the `bldg_id`; use it as
  `unit_multiplier` when present and non-NaN, else `1.0`.
- Raises `ValueError` if a needed pool is empty.

---

## 8. Loading & scaling — `loader.py`

- `RESSTOCK_KWH_COL = "out.electricity.total.energy_consumption..kwh"`,
  `COMSTOCK_KWH_COL = "out.electricity.total.energy_consumption"`.
- `_load_parquet_cached(path: str)` — `functools.lru_cache(maxsize=None)`;
  **unbounded** in-process cache keyed by path string (memory-growth risk
  for large ensembles — NFR-4). `clear_cache()` resets it.
- `_resolve_parquet_path` → `<data_dir>/upgrade=<u>/state=<s>/<bldg_id>-0.parquet`.
- `apply_mf_unit_multiplier(df, unit_count, building_type, elec_col)` —
  multiplies `elec_col` by `unit_count` **only** if
  `building_type ∈ MF_BUILDING_TYPES` and `unit_count > 1.0`.
- `resample_to_hourly(df, elec_col)` — `df.index = to_datetime(df["timestamp"])`
  then `df.resample("h")[elec_col].sum()` (15-min → hourly by summation).
- `load_and_scale_profile(selection, data_dirs, cfg, resstock_chars=None,
  building_type="") -> PerturbedProfile` — load → MF scale → resample →
  wrap in an **identity** `PerturbedProfile` (`original == perturbed`,
  empty log). The real perturbation is applied afterward by
  `perturbation.perturb_building`.

Input parquet contract: a `timestamp` column (15-min cadence, 35040 rows/yr)
plus the source-specific total-electricity column; end-use columns
(`out.electricity.<end_use>.energy_consumption..kwh`) are needed only when
end-use perturbations are enabled.

---

## 9. Perturbation — `perturbation.py` (methodology §4)

**Disabled by default** (`PerturbationConfig.enabled=False` → `perturb_building`
returns the profile unchanged, empty log).

When enabled, two structured perturbations:

1. **End-use scaling** (applied at 15-min resolution, needs `df_15min`):
   for each group with `sigma > 0`, draw
   `factor = rng.lognormal(mean=0.0, sigma=sigma)`; multiply matching
   end-use columns; recompute the total column from the scaled end-use
   columns; resample to hourly. Column→group map (`_END_USE_GROUP_MAP`):
   `lighting→lighting`, `plug_loads→plug_loads`, `cooling→hvac`,
   `heating→hvac`, `hot_water→hot_water`, `water_heater→hot_water`.
2. **Temporal shift** (applied to the hourly series): `shift_hours =
   rng.integers(-max, max+1)`; `np.roll` (circular ⇒ **exact** annual-energy
   conservation).

`energy_tolerance` only produces a `perturbation_log["energy_drift_warning"]`
— it never rejects.

> **Wiring gap:** `ensemble._run_single` calls
> `perturbation.perturb_building(loaded, cfg.perturbation, rng)` **without**
> `df_15min`, so **end-use factors are effectively never applied in the
> ensemble path today** — only the temporal shift runs. `dc_multifamily_
> baseline_2.yaml` sets `perturbation.enabled: true` with four end-use
> sigmas that currently have no effect through `run_ensemble`. FR-E3/FR-E7
> should decide: thread `df_15min` through `loader`→`ensemble`, or document
> end-use perturbation as CLI/notebook-only.

---

## 10. Aggregation & metrics — `aggregation.py` (methodology §5)

- `aggregate_profiles(profiles, source_filter=None) -> pd.Series` — sum
  `hourly_kwh` across (optionally source-filtered) profiles via successive
  `Series.add(..., fill_value=0.0)`. Empty input → empty Series.
- `compute_metrics(total_profile) -> {peak_kw, annual_energy_kwh,
  load_factor}` where `load_factor = annual_kwh / (peak_kw · n_hours)`.
  Zero/empty profile → all zeros (load_factor 0.0). *(The docstring mentions
  coincidence/diversity keys but the returned dict omits them.)*
- `compute_coincidence_factor(profiles, total_profile) -> float` —
  `total_peak / Σ individual_peaks`; `< 1` ⇒ diversity. **Called nowhere in
  the ensemble path** (Gap G-7).

---

## 11. Feasibility — `feasibility.py` (methodology §6)

- `passes_hard_constraints(peak_kw, annual_energy_kwh, load_factor, cfg)
  -> (bool, list[str])` — each metric checked against
  `[min, max]`; returns all violation strings.
- `compute_soft_score(...) -> float` — `S(x) = Σ wᵢ · fᵢ(x)` where `fᵢ` is a
  piecewise-linear normalized penalty: `(min−v)/min` below `min`,
  `(v−max)/max` above `max`, `0` inside. `weights` default to `1.0` per
  metric present in `soft.bounds`. Lower is better; `0` ⇒ all soft targets
  met. Only `peak_kw`, `annual_energy_kwh`, `load_factor` are recognised
  metric names.
- `feasibility.py` imports `NeighborhoodResult` and `SoftConstraintBound`;
  `NeighborhoodResult` is unused (dead import — FE-3 cleanup).

---

## 12. Ensemble loop — `ensemble.py` (methodology §7–8)

`run_ensemble(pool, cfg, data_dirs) -> EnsembleResult`

```
root = make_rng_bundle(cfg.rng.composition_seed, archetype_seed, perturbation_seed)
for i in range(cfg.ensemble.max_runs):
    run_rng = derive_run_rng(root, i)
    result  = _run_single(i, pool, cfg, run_rng, data_dirs)   # NeighborhoodResult
    all_runs.append(result); n_attempted += 1
    if result.feasible: feasible_runs.append(result)
    if mode == "adaptive" and len(feasible) >= min_runs and len(feasible) % check_every == 0:
        if _check_convergence(feasible_runs, cfg.convergence): converged = True; break
if mode == "fixed": converged = True
return _build_ensemble_result(all_runs, converged, n_attempted, cfg)
```

`_run_single`:
1. `sample_composition` (L1) → 2. `sample_buildings` (L2) →
3. per selection: resolve `building_type` from `resstock_chars`,
   `load_and_scale_profile`, `perturbation.perturb_building` (L3) →
4. aggregate residential / commercial / total →
5. `compute_metrics` → 6. `passes_hard_constraints` + `compute_soft_score` →
   build `NeighborhoodResult`.

`_check_convergence`: needs `K ≥ lookback_window + min_stable_window`
feasible runs; for each tracked metric compares
`mean(last M)` vs `mean(the M before that)`; converged when the **relative**
change `< epsilon` for **all** metrics.

`_build_ensemble_result`:
- keeps **only feasible** runs in `EnsembleResult.runs`;
  `n_rejected = n_attempted − n_feasible`.
- `convergence_history[metric][k] = mean over first k feasible runs`.
- `percentile_envelopes`: `pd.concat([r.total_profile ...], axis=1)` then
  `.quantile(p, axis=1)` for each `cfg.output.percentiles`, keyed
  `p10`/`p50`/`p90` (`f"p{int(p*100):02d}"`).
- `diversity_metrics`: `mean/std/cv` of peak, `mean` energy, `mean` load
  factor.
- `representative_cases`: `median` (closest peak to median), `high_peak`
  (max peak), `low_load_factor` (min LF).

Cost model: `_run_single` does `total_buildings` parquet loads (cached) +
resamples per run; `max_runs` up to 300 in the shipped adaptive config.

---

## 13. Outputs — `outputs.py` (methodology §9)

`save_all_outputs(ensemble, cfg, timestamp_str, elapsed_minutes=0.0) -> Path`
writes into `<cfg.output.base_dir>/<timestamp_str>/`:

| Writer | Files |
|---|---|
| `write_run_outputs` (per feasible run, if `per_run_csvs`) | `Run-<n>/<ts>_Run-<n>_{residential,comstock,total}_merged_community_load_profile.csv`, `<ts>_Run-<n>_building_selections.csv` |
| `write_compiled_runs` | `<ts>_merged_community_load_profile_total_compiled-runs.csv` (Resstock/Comstock column per run) |
| `write_load_profile_total` | `<ts>_load_profile_total.csv` (mean Residential / Commercial / Total) |
| `write_percentile_envelopes` | `<ts>_percentile_envelopes.csv` |
| `write_ensemble_statistics` | `<ts>_ensemble_statistics.csv`, `<ts>_diversity_metrics.csv` |
| `write_convergence_history` | `<ts>_convergence_history.csv` |
| `write_representative_cases` (if enabled) | `<ts>_representative_<case>.csv`, `<ts>_representative_cases_summary.csv` |
| `write_overview_txt` | `<ts>-overview.txt` |

The notebook additionally saves `<ts>_summary.png` (4-panel figure) itself.
`timestamp_str` is `str(start_time)` (a raw float like `"1712345678.9"`) in
the notebook — Gap: not filesystem-friendly / not sortable as a date
(FE-3).

The platform replaces the disk sink with an `ArtifactSink` that uploads the
same logical files to MinIO (FR-A12); `preserve_legacy_format` keeps the
CSV shapes byte-compatible.

---

## 14. Validation — `validation.py` (methodology §11)

- `validate_per_sample(result) -> list[str]`: warns on non-positive
  peak/energy, `load_factor ∉ (0,1]`, NaNs or negatives in `total_profile`.
- `validate_ensemble(ensemble, cfg) -> {passed, warnings, checks}`:
  - benchmark ranges: `benchmarks["peak_demand_kw"]` vs mean peak,
    `benchmarks["load_factor"]` vs mean LF (only these two names recognised);
  - **diversity collapse**: `cv(peak) < min_cv_peak` → fail;
  - **pathological synchronization**: `std(load_factor) ≤ 1e-6` → fail;
  - empty ensemble → `passed=False`.

---

## 15. Current test inventory (`tests/`, 50 passing)

| File | Covers |
|---|---|
| `conftest.py` | `rng_bundle`, `minimal_cfg` (2 MF cats, 10 buildings, fixed mode, 5 runs), `synthetic_8760`, `synthetic_15min_df` fixtures |
| `test_rng.py` | seed determinism, layer independence, per-run-index distinctness, reproducibility |
| `test_config.py` | `validate_config` simplex bounds, `fixed_count` exclusion, YAML round-trip |
| `test_composition.py` | `_project_simplex_box`, `_proportions_to_counts` exact sum, dirichlet/uniform sum-to-total, fixed-count, degenerate bounds |
| `test_aggregation.py` | sum correctness, source filter, metrics normal/zero, coincidence factor (coincident & staggered) |
| `test_perturbation.py` | temporal shift identity/energy-preservation/full-year, end-use factor non-negativity, disabled = identity, enabled shift preserves energy |
| `test_feasibility.py` | hard pass/reject per metric, multi-violation, soft score zero-inside / positive-outside / monotone |
| `test_validation.py` | per-sample warnings, benchmark pass/fail, diversity collapse, empty ensemble |

**Not yet covered:** `archetype.py`, `loader.py`, `ensemble.run_ensemble`
end-to-end, `outputs.py`, `config.load_config` with the real scenario files,
determinism golden, packaging. FR-E* + `test-plan.md` add these.

---

## 16. Consolidated gap list (input to FE-3 / FR-E5..E9)

| ID | Severity | Summary | Fix owner |
|---|---|---|---|
| G-1 | med | Unparsed `archetype_filters` keys silently ignored | FR-E6 |
| G-2 | med | `scenario.name`/`description` as YAML list → stringified list | FR-E6 |
| G-3 | low | `require_data_complete` stored, never applied | FR-E6 |
| G-4 | high | Relative `data.*` paths assume CWD=`notebooks/` | FR-E3 (façade takes explicit `data_dirs`) |
| G-5 | med | `mode`/`method` typos fall through silently | FR-E6 |
| G-6 | med | No unknown-key / unknown-section detection | FR-E6 |
| G-7 | med | Coincidence/diversity computed but not on `NeighborhoodResult` | FR-E7 |
| G-8 | high | `derive_run_rng` seed = base + `run_index` → collision-prone | FR-E5 |
| G-9 | high | End-use perturbations never applied via `run_ensemble` (no `df_15min`) | FR-E3/E7 |
| G-10 | low | `feasibility.py` dead import of `NeighborhoodResult` | FE-3 |
| G-11 | low | `timestamp_str = str(time.time())` — not a sortable/safe dir name | FE-3 |
| G-12 | med | Unbounded `lru_cache` on parquet loads → memory growth | NFR-4 |
| G-13 | high | Package not importable without `PYTHONPATH=src`; not installed | FR-E1 |

None of these block current notebook use; all must be resolved or
explicitly documented-as-accepted before the dependent platform features
build on top.
