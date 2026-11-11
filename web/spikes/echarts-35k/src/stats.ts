// Pure helpers: percentile, hourly aggregation, LTTB downsampling, load-duration curve, seasonal profile.
// In the real product the heavy numeric work belongs in the engine/API; these exist so the spike can
// (a) compute the band, (b) provide a server-style decimation fallback to compare with ECharts' own sampling.

export function percentile(values: ArrayLike<number>, p: number): number {
  if (values.length === 0) throw new Error("percentile: empty input");
  if (!(p >= 0 && p <= 100)) throw new Error(`percentile: p must be within [0, 100], got ${p}`);
  const sorted = Float64Array.from(values as ArrayLike<number>).sort();
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

/** Largest-Triangle-Three-Buckets. Returns the selected indices (ascending), length === threshold. */
export function lttb(x: ArrayLike<number>, y: ArrayLike<number>, threshold: number): Uint32Array {
  const n = x.length;
  if (threshold >= n || threshold < 3) return Uint32Array.from({ length: n }, (_, i) => i);
  const out = new Uint32Array(threshold);
  const bucket = (n - 2) / (threshold - 2);
  let a = 0;
  out[0] = 0;
  for (let i = 0; i < threshold - 2; i++) {
    const nextStart = Math.floor((i + 1) * bucket) + 1;
    const nextEnd = Math.min(Math.floor((i + 2) * bucket) + 1, n);
    let avgX = 0;
    let avgY = 0;
    for (let j = nextStart; j < nextEnd; j++) {
      avgX += x[j];
      avgY += y[j];
    }
    const cnt = nextEnd - nextStart;
    avgX /= cnt;
    avgY /= cnt;
    const start = Math.floor(i * bucket) + 1;
    const end = Math.floor((i + 1) * bucket) + 1;
    let best = -1;
    let pick = start;
    for (let j = start; j < end; j++) {
      const area = Math.abs((x[a] - avgX) * (y[j] - y[a]) - (x[a] - x[j]) * (avgY - y[a]));
      if (area > best) {
        best = area;
        pick = j;
      }
    }
    out[i + 1] = pick;
    a = pick;
  }
  out[threshold - 1] = n - 1;
  return out;
}

export interface LoadDurationCurve {
  /** Percent of time the load is at or above `values[i]`, 0..100. */
  percent: Float64Array;
  /** Load sorted from highest to lowest. */
  values: Float64Array;
}

export function loadDurationCurve(load: ArrayLike<number>): LoadDurationCurve {
  const values = Float64Array.from(load as ArrayLike<number>).sort().reverse();
  const n = values.length;
  const percent = new Float64Array(n);
  for (let i = 0; i < n; i++) percent[i] = n === 1 ? 0 : (i / (n - 1)) * 100;
  return { percent, values };
}

export type Season = "winter" | "spring" | "summer" | "fall";
export type SeasonalProfile = Record<Season, Float64Array>;

function seasonOf(ms: number): Season {
  const m = new Date(ms).getUTCMonth(); // 0 = Jan
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
  const sums: Record<Season, Float64Array> = {
    winter: new Float64Array(slots),
    spring: new Float64Array(slots),
    summer: new Float64Array(slots),
    fall: new Float64Array(slots),
  };
  const counts: Record<Season, Float64Array> = {
    winter: new Float64Array(slots),
    spring: new Float64Array(slots),
    summer: new Float64Array(slots),
    fall: new Float64Array(slots),
  };
  for (let i = 0; i < ts.length; i++) {
    const s = seasonOf(ts[i]);
    const slot = Math.floor((ts[i] % 86_400_000) / step);
    sums[s][slot] += load[i];
    counts[s][slot] += 1;
  }
  for (const s of Object.keys(sums) as Season[]) {
    for (let k = 0; k < slots; k++) sums[s][k] = counts[s][k] ? sums[s][k] / counts[s][k] : 0;
  }
  return sums;
}
