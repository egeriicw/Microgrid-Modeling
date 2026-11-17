// Display-side helpers for the results charts and their data tables. These only reshape numbers the API
// already produced (re-binning to hourly, sorting for a load-duration curve, summing for a share). Anything that
// changes the numbers themselves (sampling, percentiles over the ensemble, calibration) stays in the engine/API;
// the real API is expected to supply hourly series and duration curves directly (see report), at which point
// these helpers shrink.

export function percentile(values: ArrayLike<number>, p: number): number {
  if (values.length === 0) throw new Error("percentile: empty input");
  if (!(p >= 0 && p <= 100)) throw new Error(`percentile: p must be within [0, 100], got ${p}`);
  const sorted = Float64Array.from(values).sort();
  const rank = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo);
}

/** Mean of each group of 4 consecutive 15-minute values -> hourly values. */
export function hourlyMean(values: ArrayLike<number>): Float64Array {
  if (values.length % 4 !== 0) throw new Error(`hourlyMean: length ${values.length} is not a multiple of 4`);
  const out = new Float64Array(values.length / 4);
  for (let i = 0; i < out.length; i++) {
    out[i] = (values[4 * i] + values[4 * i + 1] + values[4 * i + 2] + values[4 * i + 3]) / 4;
  }
  return out;
}

export interface LoadDurationCurve {
  /** Percent of time the load is at or above `values[i]`, 0..100. */
  percent: Float64Array;
  /** Load sorted from highest to lowest. */
  values: Float64Array;
}

export function loadDurationCurve(load: ArrayLike<number>): LoadDurationCurve {
  const values = Float64Array.from(load).sort().reverse();
  const n = values.length;
  const percent = new Float64Array(n);
  for (let i = 0; i < n; i++) percent[i] = n === 1 ? 0 : (i / (n - 1)) * 100;
  return { percent, values };
}

export type Season = "winter" | "spring" | "summer" | "fall";
export const SEASONS: Season[] = ["winter", "spring", "summer", "fall"];
export type SeasonalProfile = Record<Season, Float64Array>;

function seasonOf(ms: number): Season {
  const m = new Date(ms).getUTCMonth();
  if (m === 11 || m <= 1) return "winter";
  if (m <= 4) return "spring";
  if (m <= 7) return "summer";
  return "fall";
}

/** Average load for each time-of-day slot, per season. Slot count follows the series step. */
export function seasonalDailyProfile(ts: ArrayLike<number>, load: ArrayLike<number>): SeasonalProfile {
  if (ts.length < 2) throw new Error("seasonalDailyProfile: need at least two points");
  const step = ts[1] - ts[0];
  const slots = Math.round(86_400_000 / step);
  const sums = {} as SeasonalProfile;
  const counts = {} as SeasonalProfile;
  for (const s of SEASONS) {
    sums[s] = new Float64Array(slots);
    counts[s] = new Float64Array(slots);
  }
  for (let i = 0; i < ts.length; i++) {
    const s = seasonOf(ts[i]);
    const slot = Math.floor((ts[i] % 86_400_000) / step);
    sums[s][slot] += load[i];
    counts[s][slot] += 1;
  }
  for (const s of SEASONS) {
    for (let k = 0; k < slots; k++) sums[s][k] = counts[s][k] ? sums[s][k] / counts[s][k] : 0;
  }
  return sums;
}

/** Energy in MWh for a kW series sampled every `stepMs`. */
export function annualEnergyMwh(kw: ArrayLike<number>, stepMs: number): number {
  let sum = 0;
  for (let i = 0; i < kw.length; i++) sum += kw[i];
  return (sum * (stepMs / 3_600_000)) / 1000;
}

export interface DailyRow {
  date: string;
  meanKw: number;
  peakKw: number;
  meanTempC: number;
}

export function dailySummary(ts: ArrayLike<number>, kw: ArrayLike<number>, tempC: ArrayLike<number>): DailyRow[] {
  const rows: DailyRow[] = [];
  let day = -1;
  let n = 0;
  let sum = 0;
  let peak = -Infinity;
  let tsum = 0;
  let date = "";
  const flush = () => {
    if (n > 0) rows.push({ date, meanKw: sum / n, peakKw: peak, meanTempC: tsum / n });
  };
  for (let i = 0; i < ts.length; i++) {
    const d = Math.floor(ts[i] / 86_400_000);
    if (d !== day) {
      flush();
      day = d;
      n = 0;
      sum = 0;
      peak = -Infinity;
      tsum = 0;
      date = new Date(ts[i]).toISOString().slice(0, 10);
    }
    n += 1;
    sum += kw[i];
    tsum += tempC[i];
    if (kw[i] > peak) peak = kw[i];
  }
  flush();
  return rows;
}
