# Results JSON schema (`schema_version` "1.0")

Produced by `RunResult.to_dict()` (`src/microgrid/community_load/synthetic/run.py`); the `results` block is built by
`synthetic/results.py::build_results`. All numbers are computed in the engine; the browser only draws them.
Strict JSON: no NaN or Infinity (a value that cannot exist, such as a season with no data, is `null`).
Any change to the shape of this document bumps `schema_version` (and this file).

Conventions: power is **kW**, energy is **kWh**, and every key says which. A source value in kWh per 15-minute
interval is multiplied by 4 to give kW; kWh per hour is already kW. "Envelope" = at each time step, across the
ensemble's feasible runs: `mean_kw` (mean) and `p10_kw`, `p50_kw`, `p90_kw` (10th/50th/90th percentile, linear
interpolation, `numpy.percentile`). Only feasible runs (the ensemble members) are summarized. Arrays are in time order.

`to_dict(include_results=False)` returns the compact summary without the `results` block.

## Top level (`RunResult.to_dict()`)

| key | type | meaning |
|---|---|---|
| `schema_version` | string | `"1.0"` |
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
| `schema_version`, `units` | | `units = {"power": "kW", "energy": "kWh"}` |
| `n_runs` | int | feasible runs summarized. If 0, `time`, `envelopes`, `load_duration`, `seasonal_daily`, `scalars` are null and `by_building_type` is `{}` |
| `percentiles` | `[10, 50, 90]` | |
| `time` | `{hourly, 15min}` | each `{start (ISO local time), step_minutes, count}`; `15min` null if runs have no 15-minute data. Time stamps are `start + i * step_minutes` |
| `envelopes` | `{hourly, 15min}` | each `{total, residential, commercial}`, each an envelope `{mean_kw, p10_kw, p50_kw, p90_kw}` (arrays of length `time.<res>.count`). Residential + commercial = total in the mean (percentiles are not additive) |
| `by_building_type` | object | per composition category (building type) name: `{source ("resstock"/"comstock"), mean_annual_energy_kwh, hourly: envelope}`. Hourly only. A category absent from a run counts as zero. Categories sum to `total` in the mean |
| `load_duration` | `{hourly: {total, residential, commercial}}` | each run's hourly kW sorted high to low (index 0 = peak hour, index 8759 = lowest hour), then the envelope across runs. x-axis = rank / count |
| `seasonal_daily` | object | `seasons` (name -> months), `hours` (0..23), `profiles.{total,residential,commercial}.{winter,spring,summer,fall}` = envelope with 24 values: per run the mean kW at each hour of day over the season's days, then envelope across runs. Seasons: winter Dec-Feb, spring Mar-May, summer Jun-Aug, fall Sep-Nov |
| `scalars` | object | `peak_kw`, `annual_energy_kwh`, `load_factor`, `annual_energy_residential_kwh`, `annual_energy_commercial_kwh`: each `{mean, std (sample, ddof=1; 0 for one run), p10, p50, p90, min, max, n}`. `peak_kw` is the maximum hourly kW; `load_factor` = annual kWh / (peak kW x hours) |
| `energy_check` | `{checked, max_relative_difference, tolerance}` | engine self-check: per run, 15-minute annual kWh vs hourly annual kWh. A relative difference above `tolerance` (1e-5) raises an error instead of returning results |

## Energy conservation (FR-ENG-5)

For every run the hourly profile is the sum of its four 15-minute values and the 15-minute total equals the
source (post multifamily scaling) energy. In the envelopes: `sum(hourly.mean_kw) * 1 h == sum(15min.mean_kw) * 0.25 h`.

## Not (yet) included

Coincidence and diversity factors, representative cases and the temperature series (FR-ENG-7 / FR-ENG-10) are not in
this version; 15-minute resolution is provided for total/residential/commercial only (not per building type).

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
