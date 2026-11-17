// Translation between the API's JSON (types generated from api/openapi.json) and the UI's domain types.
// Only renaming and reshaping: every number shown is computed by the engine/API.
import type { components } from "./generated/openapi";
import {
  SEED_KEYS,
  TERMINAL_STATES,
  type LoadSeries,
  type ProgressPayload,
  type ResultsData,
  type RunEvent,
  type RunState,
  type RunStatus,
  type ScenarioConfig,
  type Seeds,
} from "./types";

type RunOut = components["schemas"]["RunOut"];
type RunDetail = components["schemas"]["RunDetail"];

const STATES: readonly RunState[] = ["queued", "running", "succeeded", "failed", "cancelled"];
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const num = (v: unknown): number | undefined => (typeof v === "number" && Number.isFinite(v) ? v : undefined);

export const isTerminal = (s: RunState): boolean => TERMINAL_STATES.includes(s);

const STATE_TEXT: Record<RunState, string> = {
  queued: "Queued",
  running: "Running",
  succeeded: "Finished",
  failed: "Failed",
  cancelled: "Cancelled",
};

export function seedsFrom(v: unknown): Seeds | null {
  if (!isObj(v)) return null;
  const out = {} as Seeds;
  for (const k of SEED_KEYS) {
    const n = num(v[k]);
    if (n === undefined) return null;
    out[k] = n;
  }
  return out;
}

/** Fold one engine progress payload into a run status (the counts are the engine's; nothing is derived here
 *  except rejected = attempted - feasible, which is how the engine defines a rejected run). */
export function applyProgress(s: RunStatus, p: ProgressPayload): RunStatus {
  const attempted = num(p.n_attempted) ?? s.completedRuns;
  const feasible = num(p.n_feasible) ?? s.feasible;
  return {
    ...s,
    state: s.state === "queued" ? "running" : s.state,
    stage: typeof p.message === "string" && p.message ? p.message : (p.stage ?? s.stage),
    completedRuns: attempted,
    feasible,
    rejected: Math.max(0, attempted - feasible),
    maxRuns: num(p.max_runs) ?? s.maxRuns,
    converged: p.converged === true,
  };
}

export function runStatusFromApi(r: RunOut | RunDetail): RunStatus {
  if (!STATES.includes(r.status)) throw new Error(`unexpected run status from the API: ${String(r.status)}`);
  const config = "composition_snapshot" in r && isObj(r.composition_snapshot) ? (r.composition_snapshot as ScenarioConfig) : null;
  const ensemble = config && isObj(config.ensemble) ? config.ensemble : null;
  const base: RunStatus = {
    runId: r.id,
    state: r.status,
    stage: STATE_TEXT[r.status],
    completedRuns: 0,
    maxRuns: num(ensemble?.max_runs) ?? null,
    feasible: 0,
    rejected: 0,
    converged: false,
    seeds: seedsFrom(r.seeds),
    config,
    engineVersion: r.engine_version,
    cancelRequested: r.cancel_requested,
    ...(r.error_message ? { message: r.error_message } : {}),
  };
  const withProgress = isObj(r.progress) && Object.keys(r.progress).length > 0 ? applyProgress(base, r.progress as ProgressPayload) : base;
  // The status from the server wins over the progress fold (a terminal run stays terminal).
  return { ...withProgress, state: r.status, stage: isTerminal(r.status) && r.status !== "succeeded" ? STATE_TEXT[r.status] : withProgress.stage };
}

/** Parse one SSE message from `/runs/{id}/events`; returns null for unknown event types. */
export function runEventFrom(event: string, data: string, id: string | undefined): RunEvent | null {
  const parsed: unknown = JSON.parse(data);
  if (!isObj(parsed)) throw new Error("malformed run event: data is not an object");
  if (event === "status") {
    const st = parsed.status;
    if (typeof st !== "string" || !STATES.includes(st as RunState)) throw new Error(`malformed status event: ${String(st)}`);
    return { type: "status", status: st as RunState };
  }
  if (event === "progress") {
    return {
      type: "progress",
      id: id ?? "",
      stage: String(parsed.stage ?? ""),
      message: String(parsed.message ?? ""),
      payload: isObj(parsed.payload) ? (parsed.payload as ProgressPayload) : {},
      ts: String(parsed.ts ?? ""),
    };
  }
  return null;
}

/** "product:release:weather:scenario:state" -> the weather part, upper-cased (e.g. AMY2018), if all ids agree. */
export function weatherYearFrom(datasets: unknown): string | null {
  if (!isObj(datasets)) return null;
  const years = new Set<string>();
  for (const v of Object.values(datasets)) {
    const id = isObj(v) ? v.dataset_id : undefined;
    if (typeof id !== "string") continue;
    const parts = id.split(id.includes(":") ? ":" : "/");
    if (parts.length === 5 && parts[2]) years.add(parts[2].toUpperCase());
  }
  return years.size === 1 ? [...years][0] : null;
}

function f64(v: unknown, count: number, what: string): Float64Array {
  if (!Array.isArray(v)) throw new Error(`results: ${what} is missing`);
  if (v.length !== count) throw new Error(`results: ${what} has length ${v.length}, expected ${count}`);
  const out = new Float64Array(count);
  for (let i = 0; i < count; i++) {
    const x = v[i];
    out[i] = typeof x === "number" ? x : Number.NaN; // the engine writes strict JSON; null would mean "no value"
  }
  return out;
}

/** The engine's local wall-clock ISO time (no offset) encoded as UTC milliseconds, so labels never shift. */
function wallClockMs(iso: string): number {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?/.exec(iso);
  if (!m) throw new Error(`results: unreadable start time ${iso}`);
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], m[6] ? +m[6] : 0);
}

/** Map the results document (docs/reference/results-schema.md) to the chart series. Prefers 15-minute data. */
export function resultsFromApi(runId: string, doc: unknown): ResultsData {
  if (!isObj(doc) || !isObj(doc.results)) throw new Error("results: the document has no results block");
  const r = doc.results;
  if (!isObj(r.time) || !isObj(r.envelopes)) {
    throw new Error(num(r.n_runs) === 0 ? "This run has no feasible runs, so there is no time series to show." : "results: no time series in the document");
  }
  const res = isObj(r.time["15min"]) && isObj(r.envelopes["15min"]) ? "15min" : "hourly";
  const time = r.time[res];
  const envs = r.envelopes[res];
  if (!isObj(time) || !isObj(envs)) throw new Error("results: no time series in the document");
  const count = num(time.count);
  const stepMin = num(time.step_minutes);
  if (count === undefined || stepMin === undefined || typeof time.start !== "string") throw new Error("results: incomplete time axis");
  const env = (k: string): Record<string, unknown> => {
    const e = envs[k];
    if (!isObj(e)) throw new Error(`results: envelope ${k} is missing`);
    return e;
  };
  const stepMs = stepMin * 60_000;
  const t0 = wallClockMs(time.start);
  const ts = new Float64Array(count);
  for (let i = 0; i < count; i++) ts[i] = t0 + i * stepMs;
  const total = env("total");
  const series: LoadSeries = {
    ts,
    stepMs,
    total: f64(total.mean_kw, count, "total mean"),
    residential: f64(env("residential").mean_kw, count, "residential mean"),
    commercial: f64(env("commercial").mean_kw, count, "commercial mean"),
    p10: f64(total.p10_kw, count, "total P10"),
    p50: f64(total.p50_kw, count, "total P50"),
    p90: f64(total.p90_kw, count, "total P90"),
    temperature: null,
  };
  return { runId, weatherYear: weatherYearFrom(doc.datasets), calibrationStatus: null, series };
}
