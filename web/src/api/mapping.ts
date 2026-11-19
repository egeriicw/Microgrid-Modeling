// Translation between the API's JSON (types generated from api/openapi.json) and the UI's domain types.
// Only renaming and reshaping: every number shown is computed by the engine/API.
import type { components } from "./generated/openapi";
import {
  SEED_KEYS,
  TERMINAL_STATES,
  type BuildingType,
  type Composition,
  type DataFiles,
  type LoadSeries,
  type ProgressPayload,
  type ResultsData,
  type RunEvent,
  type RunState,
  type RunStatus,
  type ScenarioConfig,
  type Seeds,
  type Session,
} from "./types";

type RunOut = components["schemas"]["RunOut"];
type RunDetail = components["schemas"]["RunDetail"];
type CompositionRequest = components["schemas"]["CompositionRequest"];
type CompositionEntryApi = components["schemas"]["CompositionEntry"];

const STATES: readonly RunState[] = ["queued", "running", "succeeded", "failed", "cancelled"];
const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const num = (v: unknown): number | undefined => (typeof v === "number" && Number.isFinite(v) ? v : undefined);
const strOrNull = (v: unknown): string | null => (typeof v === "string" ? v : null);

/** `GET /me` -> Session. Every field is checked: a session the browser cannot read is refused, never guessed. */
export function sessionFromMe(me: unknown): Session {
  const bad = (what: string) => new Error(`the server's account information could not be read (${what})`);
  if (!isObj(me)) throw bad("not an object");
  if (typeof me.subject !== "string" || !me.subject) throw bad("subject");
  if (typeof me.role !== "string" || !me.role) throw bad("role");
  if (!isObj(me.permissions) || !Object.values(me.permissions).every((v) => typeof v === "boolean")) throw bad("permissions");
  if (!Array.isArray(me.run_inputs) || !me.run_inputs.every((v) => typeof v === "string")) throw bad("run_inputs");
  if (typeof me.may_set_seeds !== "boolean") throw bad("may_set_seeds");
  const l = me.limits;
  if (!isObj(l)) throw bad("limits");
  const whole = (v: unknown, what: string): number => {
    if (typeof v !== "number" || !Number.isInteger(v) || v < 0) throw bad(what);
    return v;
  };
  let demoCaps: Session["limits"]["demoCaps"] = null;
  if (l.demo_caps !== null && l.demo_caps !== undefined) {
    if (!isObj(l.demo_caps)) throw bad("limits.demo_caps");
    demoCaps = { maxBuildings: whole(l.demo_caps.max_buildings, "demo_caps.max_buildings"), maxRuns: whole(l.demo_caps.max_runs, "demo_caps.max_runs") };
  }
  return {
    subject: me.subject,
    role: me.role,
    displayName: me.subject,
    permissions: { ...(me.permissions as Record<string, boolean>) },
    runInputs: [...(me.run_inputs as string[])],
    maySetSeeds: me.may_set_seeds,
    limits: {
      maxTotalBuildings: whole(l.max_total_buildings, "max_total_buildings"),
      maxEnsembleRuns: whole(l.max_ensemble_runs, "max_ensemble_runs"),
      defaultMinRuns: whole(l.default_min_runs, "default_min_runs"),
      demoCaps,
    },
  };
}

/** `GET /building-types` -> the form's catalogue (renaming only; inactive types are kept). */
export function buildingTypesFromApi(doc: unknown): BuildingType[] {
  if (!isObj(doc) || !Array.isArray(doc.items)) throw new Error("the building type list could not be read");
  return doc.items.map((t, i) => {
    const ok =
      isObj(t) &&
      typeof t.key === "string" &&
      typeof t.label === "string" &&
      (t.building_class === "residential" || t.building_class === "commercial") &&
      (t.source === "resstock" || t.source === "comstock") &&
      typeof t.active === "boolean" &&
      (t.description === null || t.description === undefined || typeof t.description === "string");
    if (!ok) throw new Error(`building type ${i + 1} in the server's list could not be read`);
    return {
      id: t.key as string,
      label: t.label as string,
      sector: t.building_class as BuildingType["sector"],
      source: t.source as BuildingType["source"],
      description: typeof t.description === "string" && t.description ? t.description : null,
      active: t.active as boolean,
    };
  });
}

/** The form's composition -> API `CompositionRequest`. No convergence rule is sent: the server's default applies. */
export function compositionToApi(c: Composition): CompositionRequest {
  return {
    name: c.name,
    total_buildings: c.totalBuildings,
    building_types: c.entries.map(
      (e): CompositionEntryApi =>
        e.mode === "count"
          ? { building_type: e.typeId, fixed_count: e.count }
          : { building_type: e.typeId, min_fraction: e.minFraction, max_fraction: e.maxFraction },
    ),
    max_runs: c.maxRuns,
    convergence: c.convergence,
  };
}

/** `RunDetail.data_files` -> DataFiles; null when the server did not send the block. */
export function dataFilesFrom(v: unknown): DataFiles | null {
  if (!isObj(v)) return null;
  const t = isObj(v.temperature) ? v.temperature : {};
  return {
    resstockCharacteristicsFile: strOrNull(v.resstock_characteristics_file),
    comstockCharacteristicsFile: strOrNull(v.comstock_characteristics_file),
    temperature: {
      source: strOrNull(t.source),
      file: strOrNull(t.file),
      column: strOrNull(t.column),
      timeColumn: strOrNull(t.time_column),
      units: strOrNull(t.units),
    },
  };
}

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
    converged: p.converged === undefined ? s.converged : typeof p.converged === "boolean" ? p.converged : null,
  };
}

export function runStatusFromApi(r: RunOut | RunDetail): RunStatus {
  if (!STATES.includes(r.status)) throw new Error(`unexpected run status from the API: ${String(r.status)}`);
  const config = "composition_snapshot" in r && isObj(r.composition_snapshot) ? (r.composition_snapshot as ScenarioConfig) : null;
  const ensemble = config && isObj(config.ensemble) ? config.ensemble : null;
  const mode = ensemble?.mode === "adaptive" || ensemble?.mode === "fixed" ? ensemble.mode : null;
  // The rule the engine says it used (the finished run's result), not the one written in the stored config.
  const result = "result" in r && isObj(r.result) ? r.result : null;
  const rule = result && isObj(result.convergence) && typeof result.convergence.rule === "string" ? result.convergence.rule : null;
  const base: RunStatus = {
    runId: r.id,
    state: r.status,
    stage: STATE_TEXT[r.status],
    completedRuns: 0,
    maxRuns: num(ensemble?.max_runs) ?? null,
    feasible: 0,
    rejected: 0,
    converged: null,
    seeds: seedsFrom(r.seeds),
    config,
    engineVersion: r.engine_version,
    cancelRequested: r.cancel_requested,
    ensembleMode: mode,
    convergenceRule: rule,
    dataFiles: "data_files" in r ? dataFilesFrom(r.data_files) : null,
    ...(r.error_message ? { message: r.error_message } : {}),
  };
  const withProgress = isObj(r.progress) && Object.keys(r.progress).length > 0 ? applyProgress(base, r.progress as ProgressPayload) : base;
  // The status from the server wins over the progress fold (a terminal run stays terminal).
  return { ...withProgress, state: r.status, stage: isTerminal(r.status) && r.status !== "succeeded" ? STATE_TEXT[r.status] : withProgress.stage };
}

/** Parse one SSE message from `/runs/{id}/events`; returns null for unknown event types. */
export function runEventFrom(event: string, data: string, id: string | undefined): RunEvent | null {
  if (event !== "status" && event !== "progress") return null; // checked before parsing: unknown data may not be JSON
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
  // Temperature (schema 1.1): always present in 1.1; a 1.0 document without it counts as not available.
  const t = r.temperature;
  let temperatureNote: string | null = null;
  if (!isObj(t)) {
    temperatureNote = "these results use schema 1.0, which has no temperature";
  } else if (t.available !== true) {
    temperatureNote = typeof t.reason === "string" && t.reason ? t.reason : "the results do not say why";
  } else {
    if (t.units !== "degC") throw new Error(`results: temperature units ${String(t.units)} are not degC`);
    series.temperature = f64(t[res], count, `temperature (${res})`);
  }
  return { runId, weatherYear: weatherYearFrom(doc.datasets), calibrationStatus: null, series, temperatureNote };
}
