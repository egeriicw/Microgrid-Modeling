// Deterministic synthetic data: NOT real data and not a model of any real neighborhood. It only needs to
// have the right size and shape (seasonal + diurnal + weekday, weather-driven, noisy, with an ensemble band).
import { percentile } from "./stats";

export const YEAR_2018_START_MS = Date.UTC(2018, 0, 1);
export const STEP_15_MIN_MS = 15 * 60 * 1000;
export const STEP_1_H_MS = 60 * 60 * 1000;
const REALIZATIONS = 25;

export interface Dataset {
  /** Timestamps, ms since epoch (UTC). */
  ts: Float64Array;
  stepMs: number;
  /** kW */
  total: Float64Array;
  residential: Float64Array;
  commercial: Float64Array;
  p10: Float64Array;
  p50: Float64Array;
  p90: Float64Array;
  /** degrees C */
  temperature: Float64Array;
}

function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(rand: () => number): number {
  const u = Math.max(rand(), 1e-12);
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * rand());
}

export function generateDataset(opts: { points: 35040 | 8760 | number; seed?: number }): Dataset {
  const { points } = opts;
  if (points !== 35040 && points !== 8760) throw new Error(`points must be 35040 or 8760, got ${points}`);
  const stepMs = points === 35040 ? STEP_15_MIN_MS : STEP_1_H_MS;
  const rand = mulberry32(opts.seed ?? 1);

  const ts = new Float64Array(points);
  const temperature = new Float64Array(points);
  const residential = new Float64Array(points);
  const commercial = new Float64Array(points);
  const total = new Float64Array(points);
  const p10 = new Float64Array(points);
  const p50 = new Float64Array(points);
  const p90 = new Float64Array(points);
  const level = Array.from({ length: REALIZATIONS }, () => 1 + 0.05 * gaussian(rand));
  const scratch = new Float64Array(REALIZATIONS);

  let weather = 0; // slow AR(1) weather anomaly, deg C
  for (let i = 0; i < points; i++) {
    const t = YEAR_2018_START_MS + i * stepMs;
    ts[i] = t;
    const dayOfYear = (t - YEAR_2018_START_MS) / 86_400_000;
    const hour = (t % 86_400_000) / 3_600_000;
    const weekday = new Date(t).getUTCDay(); // 2018-01-01 is a Monday
    const isWeekend = weekday === 0 || weekday === 6;

    const k = stepMs / STEP_15_MIN_MS;
    weather = 0.998 ** k * weather + 0.12 * Math.sqrt(k) * gaussian(rand);
    const seasonalT = 14 - 13 * Math.cos((2 * Math.PI * (dayOfYear - 15)) / 365);
    const diurnalT = 4 * Math.sin((2 * Math.PI * (hour - 9)) / 24);
    const temp = seasonalT + diurnalT + weather;
    temperature[i] = temp;

    const cooling = Math.max(0, temp - 18);
    const heating = Math.max(0, 12 - temp);
    const resShape =
      0.55 + 0.25 * Math.exp(-(((hour - 7.5) / 2) ** 2)) + 0.45 * Math.exp(-(((hour - 19.5) / 2.5) ** 2));
    const comShape = isWeekend ? 0.35 : 0.3 + 0.7 / (1 + Math.exp(-(hour - 7.5) * 1.5)) / (1 + Math.exp((hour - 18) * 1.5));
    const res = 900 * resShape + 55 * cooling * resShape + 40 * heating * (0.5 + resShape);
    const com = 700 * comShape + 30 * cooling * comShape + 12 * heating * comShape;

    let rs = 0;
    let cs = 0;
    for (let r = 0; r < REALIZATIONS; r++) {
      const f = level[r] * (1 + 0.04 * gaussian(rand));
      const g = level[(r + 7) % REALIZATIONS] * (1 + 0.06 * gaussian(rand));
      rs += res * f;
      cs += com * g;
      scratch[r] = res * f + com * g;
    }
    residential[i] = rs / REALIZATIONS;
    commercial[i] = cs / REALIZATIONS;
    total[i] = residential[i] + commercial[i];
    p10[i] = percentile(scratch, 10);
    p50[i] = percentile(scratch, 50);
    p90[i] = percentile(scratch, 90);
  }
  return { ts, stepMs, total, residential, commercial, p10, p50, p90, temperature };
}
