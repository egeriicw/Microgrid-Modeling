# Domain Glossary — Ubiquitous Language

Shared vocabulary for the SNLG methodology and platform. Use these exact
terms in code, API, UI, and docs.

| Term | Definition |
|---|---|
| **Scenario / Config** | A named, versioned set of parameters describing the neighborhood to synthesize: building total & mix bounds, archetype filters, perturbation settings, feasibility constraints, ensemble/convergence controls, validation benchmarks, output options. Canonical form = structured JSON (`snlg.config.ScenarioConfig`). |
| **Archetype** | A building-stock template from NREL **ResStock** (residential) or **ComStock** (commercial), identified by `bldg_id`, carrying a full-year simulated load timeseries + characteristics. |
| **Archetype pool (`A_filtered`)** | The subset of archetypes passing a scenario's hard-gate filters, grouped by **category**. |
| **Category** | A named bucket of archetypes within a scenario (e.g. `mf_small`, `single_family_detached`, `commercial_private`). Has composition bounds (`min_fraction`/`max_fraction`) or a `fixed_count`, and a `source` (`resstock`/`comstock`). |
| **Composition** | The realized integer count of buildings per category for one Monte-Carlo run; sums exactly to `total_buildings`. |
| **Composition method** | `dirichlet` (bounded Dirichlet draw + simplex-box projection) or `uniform_renorm` (per-category uniform draw + renormalize). |
| **Building selection** | One drawn archetype instance for a run: `(bldg_id, category, source, unit_multiplier)`. Drawn **with replacement**. |
| **Unit multiplier** | For multifamily ResStock archetypes, the number of dwelling units (`in.geometry_building_number_units_mf`) the single-unit timeseries is scaled by. |
| **Perturbation** | Optional instance-level stochastic modification of a profile: **temporal shift** (circular hour roll, energy-conserving) and/or **end-use scaling** (lognormal multiplicative factor per end-use group). Off by default. |
| **End-use group** | `lighting`, `plug_loads`, `hvac` (cooling+heating), `hot_water` (hot_water+water_heater). |
| **Load profile** | An 8760-value hourly kWh/h `pandas` Series over one year with a `DatetimeIndex`. |
| **Realization / Monte-Carlo run** | One pass through composition → selection → load → perturb → aggregate for a single neighborhood draw. Indexed by `run_index` (0-based). |
| **NeighborhoodResult** | The output of one realization: residential/commercial/total profiles, composition, selections, scalar metrics, feasibility verdict, soft score. |
| **Aggregation** | Summing per-building hourly profiles into residential, commercial, and total neighborhood profiles (deterministic within a realization). |
| **Peak (kW)** | Max hourly value of a profile. |
| **Annual energy (kWh)** | Sum of a profile's 8760 values. |
| **Load factor** | `annual_energy / (peak · 8760)` ∈ (0, 1]. Higher = flatter load. |
| **Coincidence factor** | `neighborhood_peak / Σ(individual building peaks)` ∈ (0, 1]. Lower = more diversity. |
| **Diversity factor** | `1 / coincidence_factor` (≥ 1). |
| **Hard constraint** | A binary accept/reject bound on a system metric (`peak_kw`, `annual_energy_kwh`, `load_factor`). A realization failing any is **rejected** (excluded from the ensemble). |
| **Soft constraint / soft score `S(x)`** | `Σ wᵢ·fᵢ(x)`; a non-negative penalty for ranking feasible realizations. `0` = all soft targets met. |
| **Feasible run** | A realization passing all hard constraints. Only feasible runs enter `EnsembleResult.runs`. |
| **Ensemble** | The collection of feasible realizations for a scenario plus derived statistics. |
| **Ensemble mode** | `fixed` (run exactly `max_runs`) or `adaptive` (run until convergence or `max_runs`). |
| **Convergence** | Adaptive-mode stop condition: rolling mean of each tracked metric changes by `< epsilon` (relative) between the last `lookback_window` feasible runs and the `lookback_window` before them, after `min_stable_window` is satisfied. |
| **Convergence history** | Per tracked metric, the running mean over the first *k* feasible runs, for `k = 1..n_feasible`. |
| **Percentile envelope** | Per-hour P10/P50/P90 (configurable) across all feasible runs' total profiles — an 8760×3 series set. |
| **Load-duration curve** | A profile's 8760 values sorted descending, plotted against exceedance fraction (0→1). |
| **Representative case** | A single feasible run chosen to illustrate the ensemble: `median` (peak nearest the median), `high_peak` (max peak), `low_load_factor` (min LF). |
| **Diversity metrics** | Ensemble-level scalars: `mean_peak_kw`, `std_peak_kw`, `cv_peak`, `mean_annual_energy_kwh`, `mean_load_factor`. |
| **Diversity collapse** | Validation failure: `cv_peak < min_cv_peak` — realizations are nearly identical (RNG or bounds too tight). |
| **Pathological synchronization** | Validation failure: near-zero variance in load factor across runs — buildings always peak together. |
| **RNG layer** | One of three independent random streams: **L1 composition**, **L2 archetype**, **L3 perturbation**, each seeded separately for non-interference and reproducibility. |
| **Run (platform)** | A persisted, asynchronous execution of one scenario snapshot: `queued → running → succeeded | failed | cancelled`. Owns metrics, summary, artifacts, log. |
| **Config snapshot** | The frozen effective JSON config (base + launch overrides) stored on a `Run` for reproducibility. |
| **Override** | A launch-time change to a scenario for a single run (seeds, `max_runs`, `mode`) that does **not** mutate the stored config. |
| **Artifact** | A file output of a run stored in the object store (per-run CSV, compiled CSV, envelope CSV, figures, overview txt, full log), indexed in Postgres with `kind`, `object_key`, `sha256`. |
| **Engine version** | `snlg.__version__` recorded on every run; part of the determinism contract. |
| **Template** | A read-only seed scenario (e.g. "DC Multifamily Baseline") that users clone to start from. |
| **OEDI** | NREL's Open Energy Data Initiative — the S3/HTTPS source for ResStock/ComStock parquet + characteristics files. |
| **Upgrade** | ResStock/ComStock scenario id (`upgrade=0` = baseline building stock). |
| **`data_dirs`** | Mapping `{"resstock": Path, "comstock": Path}` locating the timeseries parquet trees; passed explicitly to the engine (no relative-path assumptions). |
