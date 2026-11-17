# Results JSON schema (`schema_version` "1.1")

Produced by `RunResult.to_dict()` (`src/microgrid/community_load/synthetic/run.py`); the `results` block is built by
`synthetic/results.py::build_results`. All numbers are computed in the engine; the browser only draws them.
Strict JSON: no NaN or Infinity (a value that cannot exist, such as a season with no data, is `null`).
Any change to the shape of this document bumps `schema_version` (and this file).

**Version history.** `1.1` adds `results.temperature` (FR-ENG-10) and `results.units.temperature = "degC"`;
nothing else changed and no number changed. A `1.0` document is a `1.1` document without those two keys, so a
reader can accept both by treating a missing `temperature` as `{"available": false, "reason": "schema 1.0"}`.
(There is no reader in the code base today: the API stores and serves the document as written.)
Because the config gained the temperature declaration, every 1.1 document's `config_snapshot.data` also carries the
five `temperature_*` fields (`temperature_source`, `temperature_column`, `temperature_units`,
`temperature_weather_file`, `temperature_time_column`; `"none"` and nulls when nothing is declared).

Conventions: power is **kW**, energy is **kWh**, and every key says which. A source value in kWh per 15-minute
interval is multiplied by 4 to give kW; kWh per hour is already kW. "Envelope" = at each time step, across the
ensemble's feasible runs: `mean_kw` (mean) and `p10_kw`, `p50_kw`, `p90_kw` (10th/50th/90th percentile, linear
interpolation, `numpy.percentile`). Only feasible runs (the ensemble members) are summarized. Arrays are in time order.

`to_dict(include_results=False)` returns the compact summary without the `results` block.

## Top level (`RunResult.to_dict()`)

| key | type | meaning |
|---|---|---|
| `schema_version` | string | `"1.1"` |
| `engine_version` | string | package version |
| `seeds` | `{composition_seed, archetype_seed, perturbation_seed}` ints | seeds used; supply as `rng` to replay |
| `config_snapshot` | object | `config_from_dict` shape; replayable |
| `pool_summary`, `datasets` | objects | unchanged from earlier versions |
| `metrics` | list of rows | one per feasible run: `run_index, peak_kw, annual_energy_kwh, load_factor, soft_score, feasible` |
| `converged` | bool | adaptive stop reached (fixed mode: always true when the runs completed) |
| `n_attempted`, `n_rejected` | ints | runs tried / rejected as infeasible |
| `diversity_metrics` | object | unchanged |
| `convergence` | object | see below |
| `results` | object | see below (omitted with `include_results=False`) |

## `convergence`

`{ "rule": "legacy_window" | "prd_default", "converged": bool, "checks": [check, ...] }`, one check per adaptive
convergence check, in order (empty in fixed mode). A check:

| key | meaning |
|---|---|
| `rule`, `epsilon`, `se_target` | rule name and thresholds used (`se_target` null for legacy) |
| `n_attempted`, `n_feasible` | counts when the check ran |
| `rel_change` | `{annual_energy_kwh, peak_kw, mean_profile}` relative change of the ensemble mean since the previous check; `{}` on the first check and for `legacy_window` |
| `rel_se_p50` | relative standard error of the P50 profile (`prd_default`), else null |
| `passed` | this check met every criterion |
| `consecutive_passes`, `required_passes` | current run of passing checks, and M |
| `converged` | `consecutive_passes >= required_passes` |

Progress events (`ProgressEvent.convergence`) carry the latest check in this same shape (null before the first check).

## `results`

| key | type | content |
|---|---|---|
| `schema_version`, `units` | | `units = {"power": "kW", "energy": "kWh", "temperature": "degC"}` |
| `n_runs` | int | feasible runs summarized. If 0, `time`, `envelopes`, `load_duration`, `seasonal_daily`, `scalars` are null and `by_building_type` is `{}` |
| `percentiles` | `[10, 50, 90]` | |
| `time` | `{hourly, 15min}` | each `{start (ISO local time), step_minutes, count}`; `15min` null if runs have no 15-minute data. Time stamps are `start + i * step_minutes` |
| `envelopes` | `{hourly, 15min}` | each `{total, residential, commercial}`, each an envelope `{mean_kw, p10_kw, p50_kw, p90_kw}` (arrays of length `time.<res>.count`). Residential + commercial = total in the mean (percentiles are not additive) |
| `by_building_type` | object | per composition category (building type) name: `{source ("resstock"/"comstock"), mean_annual_energy_kwh, hourly: envelope}`. Hourly only. A category absent from a run counts as zero. Categories sum to `total` in the mean |
| `load_duration` | `{hourly: {total, residential, commercial}}` | each run's hourly kW sorted high to low (index 0 = peak hour, index 8759 = lowest hour), then the envelope across runs. x-axis = rank / count |
| `seasonal_daily` | object | `seasons` (name -> months), `hours` (0..23), `profiles.{total,residential,commercial}.{winter,spring,summer,fall}` = envelope with 24 values: per run the mean kW at each hour of day over the season's days, then envelope across runs. Seasons: winter Dec-Feb, spring Mar-May, summer Jun-Aug, fall Sep-Nov |
| `scalars` | object | `peak_kw`, `annual_energy_kwh`, `load_factor`, `annual_energy_residential_kwh`, `annual_energy_commercial_kwh`: each `{mean, std (sample, ddof=1; 0 for one run), p10, p50, p90, min, max, n}`. `peak_kw` is the maximum hourly kW; `load_factor` = annual kWh / (peak kW x hours) |
| `energy_check` | `{checked, max_relative_difference, tolerance}` | engine self-check: per run, 15-minute annual kWh vs hourly annual kWh. A relative difference above `tolerance` (1e-5) raises an error instead of returning results |
| `temperature` | object | outdoor temperature that goes with the load (schema 1.1), see below. Always present |

## `temperature` (FR-ENG-10)

Always present, with the same keys whether or not a series is available (never silently omitted).

| key | meaning |
|---|---|
| `available` | true when the series below are filled |
| `reason` | why not, when `available` is false (no source declared; no feasible runs; not computed); else null |
| `units` | `"degC"` (degrees Celsius); a source declared in Fahrenheit is converted |
| `source` | what the config declared: `{"kind": "none"}`, or `{kind ("timeseries_column" / "weather_file"), column, declared_units ("C"/"F"), file (file name only, never a path; null for a column), time_column (null for a column)}` |
| `source_resolution` | `"15min"` or `"hourly"`: the step of the source time stamps; null when not available |
| `hourly` | array of `time.hourly.count` values (deg C), aligned to `time.hourly`; null when not available |
| `15min` | array of `time.15min.count` values aligned to `time.15min`; null when not available or the load has no 15-minute axis |
| `derivation` | `{hourly, 15min}` plain-language rule used for each series (below); null when not available |
| `consistency` | `{n_series_compared, max_abs_difference_c, tolerance_c, note}`: how many distinct sampled buildings' series were compared and how far apart they were (a weather file counts as one series); null when not available |

**Where the series comes from is declared, not guessed.** The scenario config's `data` section names it:
`temperature_source` = `none` (default) | `timeseries_column` (`temperature_column`, `temperature_units`) |
`weather_file` (`temperature_weather_file` (.csv or .parquet), `temperature_time_column`, `temperature_column`,
`temperature_units`). An incomplete declaration, or a field the chosen source would ignore, is a config error.
Where the real ResStock/ComStock data keeps temperature (a column in each building file, or a separate weather
file per county/location, and its column names and units) is **not confirmed [verify]**.

**Alignment rules.** Time stamps must match the load's exactly (no shifting, no time zones). From a 15-minute
source, `15min` = the source values and `hourly` = the mean of the four 15-minute values labelled within each hour
(the same grouping that sums the hourly load). From an hourly source, `hourly` = the source values and `15min` =
each hour's value held for its four intervals (no interpolation). Missing time stamps, duplicates, irregular steps
and missing or non-finite values are errors. Whether NREL time stamps mark the start or end of an interval, and
whether load and weather files use the same convention, is **[verify]**.

**"The" temperature when buildings could differ.** With `timeseries_column`, every distinct sampled building (of
the feasible runs) is read and compared with the first. They are expected to share one weather location within a
DC dataset **[verify]**. If any two differ by more than `tolerance_c` (0.01 deg C, an engineer's placeholder) the
run fails with a message naming the two buildings: the engine never picks or averages one silently, because that
choice is a methodology decision for the owner. With `weather_file`, the one file is assumed to apply to every
sampled building **[verify]**.

Temperature is read after the ensemble, from the files only; it never changes any load number.

## Per-building load (FR-ENG-13 b)

The document carries neighborhood totals only. Per sampled building hourly and 15-minute kWh are retrievable in the
library with `synthetic.ensemble.building_profiles(run, pool, cfg, data_dirs)` (same loading, multifamily scaling and
recorded perturbation as the run; the buildings sum to the run's totals). They are not part of this JSON document.

## Energy conservation (FR-ENG-5)

For every run the hourly profile is the sum of its four 15-minute values and the 15-minute total equals the
source (post multifamily scaling) energy. In the envelopes: `sum(hourly.mean_kw) * 1 h == sum(15min.mean_kw) * 0.25 h`.

## Not (yet) included

Coincidence and diversity factors and representative cases (FR-ENG-7) are not in this version; 15-minute resolution is provided for total/residential/commercial only (not per building type).

## Convergence formulas (rule `prd_default`)

Plain language: every `check_every` feasible runs (starting at `min_runs`) the engine compares the ensemble now with the
ensemble at the previous check, and asks whether the estimate has stopped moving and is precise enough. A rejected
(infeasible) run adds no information, so it never triggers a check. A check cannot happen twice for the same count.

Let K be the number of feasible runs, `E` the mean annual energy, `P` the mean peak kW, `m` the mean hourly profile
(a vector over the hours), and `prev` the value at the previous check.

- `rel_change.annual_energy_kwh = |E - E_prev| / |E_prev|`, and likewise for `peak_kw` with `P`.
- `rel_change.mean_profile = ||m - m_prev|| / ||m_prev||` (`||.||` is the root-sum-of-squares over hours).
- `rel_se_p50 = ||SE|| / ||P50||`, where `SE_h = 1.2533 * s_h / sqrt(K)`, `s_h` is the sample standard deviation across
  runs at hour h (n-1 divisor), and `P50` is the median profile. 1.2533 = sqrt(pi/2), the large-sample ratio of the
  standard error of a median to that of a mean for normally distributed data (an approximation).
- A check **passes** when all three `rel_change` values are strictly below `epsilon` and `rel_se_p50` is strictly below
  `se_target`. The first check has no previous check, so it cannot pass. If a previous value is (near) zero the
  denominator is taken as 1 (absolute change).
- **Converged** when `consecutive_checks` (M) checks in a row pass; one failed check resets the count to zero.
- A run profile containing NaN or infinity raises an error (it never silently fails to converge).

Defaults and status: `epsilon = 0.01` (carried over from the earlier rule), `consecutive_checks = 3` and
`se_target = 0.02` are **placeholders chosen by the engineer, not owner-approved numbers**; the L2-norm choice and the
normal-approximation median standard error are also placeholders. The owner approved the shape of the rule
(2026-11-06, provisional). `max_runs` defaults to 1000 for adaptive mode. `rule: legacy_window` (still the default
rule) keeps the earlier behaviour.
