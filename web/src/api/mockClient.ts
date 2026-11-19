// Mock ApiClient driven by fixtures, for tests and offline work (VITE_API_MODE=mock). It plays the server's part
// for roles (401/403/404) with the SAME rules and shapes as the real API (api/src/microgrid_api/routers/runs.py),
// so the UI is exercised against realistic behaviour. All HTTP lives in src/api/httpClient.ts.
import { BUILDING_TYPES, FILTER_OPTIONS, FULL_YEAR_POINTS, generateDataset } from "./fixtures";
import { applyProgress } from "./mapping";
import {
  ApiError,
  SEED_KEYS,
  type ApiClient,
  type Composition,
  type DataFiles,
  type Limits,
  type ResultsData,
  type Role,
  type RunEvent,
  type RunRequest,
  type RunStatus,
  type ScenarioConfig,
  type Seeds,
  type Session,
} from "./types";

export interface MockOptions {
  /** Runs the fake engine completes per progress tick. */
  runsPerTick?: number;
  /** Milliseconds between progress events on the stream. */
  tickMs?: number;
  /** Length of the fixture series (multiple of 4); default is a full year, 35,040 points. */
  pointsPerYear?: number;
  /** Keep the mock session in sessionStorage so a reload or deep link keeps the user signed in. */
  persistSession?: boolean;
}

/** Mock-only: a finished run with fixture results, so the results screen can be viewed without running. */
export const DEMO_RUN_ID = "demo-run";
/** Mock stand-in for the server's predefined (demo) compositions. */
export const PREDEFINED = ["dc_small_mixed"];
const DEMO_SEED = 12345;
const ROLES: Role[] = ["admin", "client", "demo"];
const SESSION_KEY = "microgrid.mockSession";

// Mirrors the API's defaults (api/src/microgrid_api/config.py, routers/me.py and the engine's default min_runs).
const LIMITS: Limits = { maxTotalBuildings: 5000, maxEnsembleRuns: 1000, defaultMinRuns: 20, demoCaps: null };
const DEMO_CAPS = { maxBuildings: 50, maxRuns: 20 };
const MOCK_DATA_FILES: DataFiles = {
  resstockCharacteristicsFile: "DC_upgrade0.xlsx",
  comstockCharacteristicsFile: "DC_upgrade0_agg.xlsx",
  temperature: { source: "timeseries_column", file: null, column: "out.outdoor_air_drybulb_temp_c", timeColumn: null, units: "C" },
};

/** What the mock's `GET /me` says for each role (the same rules as the API's `run_inputs` / `may_set_seeds`). */
function sessionFor(role: Role): Session {
  const demo = role === "demo";
  return {
    subject: `mock-${role}`,
    role,
    displayName: `Mock ${role}`,
    permissions: { "filters.use": role === "admin" },
    runInputs: demo ? ["composition_id"] : ["composition", "composition_id", "config"],
    maySetSeeds: !demo,
    limits: { ...LIMITS, demoCaps: demo ? { ...DEMO_CAPS } : null },
  };
}

function readStored(): Session | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    const v = raw ? (JSON.parse(raw) as { role?: unknown }) : null;
    return v && typeof v.role === "string" && ROLES.includes(v.role as Role) ? sessionFor(v.role as Role) : null;
  } catch {
    return null;
  }
}

function writeStored(s: Session | null): void {
  try {
    if (s) sessionStorage.setItem(SESSION_KEY, JSON.stringify({ role: s.role }));
    else sessionStorage.removeItem(SESSION_KEY);
  } catch {
    /* storage blocked: the session just lasts until reload */
  }
}

const unprocessable = (message: string) => new ApiError(422, message);

/** The API's composition rules (services/compositions.py, routers/runs.py caps), with the API's own wording. */
function checkComposition(c: Composition): void {
  const seen = new Set<string>();
  for (const e of c.entries) {
    if (seen.has(e.typeId)) throw unprocessable(`building type '${e.typeId}' is listed more than once`);
    seen.add(e.typeId);
    const t = BUILDING_TYPES.find((x) => x.id === e.typeId);
    if (!t) throw unprocessable(`unknown building type '${e.typeId}'; see GET /building-types for the catalogue`);
    if (!t.active) throw unprocessable(`building type '${e.typeId}' is not active yet and cannot be modelled`);
  }
  const total = c.totalBuildings;
  const fixed = c.entries.reduce((a, e) => a + (e.mode === "count" ? e.count : 0), 0);
  const ranged = c.entries.filter((e) => e.mode === "fraction");
  if (fixed > total) throw unprocessable(`the fixed counts add up to ${fixed}, more than total_buildings (${total})`);
  if (ranged.length === 0 && fixed !== total) {
    throw unprocessable(
      `every type has a fixed count and they add up to ${fixed}, not total_buildings (${total}); make them add up or give at least one type a fraction range`,
    );
  }
  if (ranged.length > 0) {
    const lo = ranged.reduce((a, e) => a + (e.mode === "fraction" ? e.minFraction : 0), 0);
    const hi = ranged.reduce((a, e) => a + (e.mode === "fraction" ? e.maxFraction : 0), 0);
    if (lo > 1 + 1e-9) throw unprocessable(`the minimum fractions of the ranged types add up to ${Number(lo.toPrecision(4))}, more than 1; no mix can satisfy them`);
    if (hi < 1 - 1e-9) throw unprocessable(`the maximum fractions of the ranged types add up to ${Number(hi.toPrecision(4))}, less than 1; no mix can satisfy them`);
  }
  if (c.convergence && c.maxRuns < LIMITS.defaultMinRuns) {
    throw unprocessable(
      `with convergence on, max_runs must be at least min_runs (${LIMITS.defaultMinRuns} unless you set it); got max_runs ${c.maxRuns}`,
    );
  }
  if (total > LIMITS.maxTotalBuildings) throw unprocessable(`composition.total_buildings exceeds the server cap of ${LIMITS.maxTotalBuildings}`);
  if (c.maxRuns > LIMITS.maxEnsembleRuns) throw unprocessable(`ensemble.max_runs exceeds the server cap of ${LIMITS.maxEnsembleRuns}`);
}

interface RunRecord {
  maxRuns: number;
  completed: number;
  cancelled: boolean;
  started: boolean;
  seeds: Seeds;
  config: ScenarioConfig;
  mode: "adaptive" | "fixed";
}

function stageFor(fraction: number): string {
  if (fraction < 0.2) return "Sampling buildings from the library";
  if (fraction < 0.8) return "Simulating and aggregating sampled communities";
  if (fraction < 1) return "Checking convergence";
  return "finished";
}

export function createMockClient(options: MockOptions = {}): ApiClient {
  const runsPerTick = options.runsPerTick ?? 25;
  const tickMs = options.tickMs ?? 250;
  const points = options.pointsPerYear ?? FULL_YEAR_POINTS;
  const persist = options.persistSession ?? false;
  let session: Session | null = persist ? readStored() : null;
  const runs = new Map<string, RunRecord>();
  const listeners = new Set<() => void>();
  let counter = 0;
  let seedCounter = 1000;
  let cached: ResultsData | null = null;

  const need = (): Session => {
    if (!session) throw new ApiError(401, "Sign in first.");
    return session;
  };
  const record = (runId: string): RunRecord => {
    need();
    const r = runs.get(runId);
    if (!r) throw new ApiError(404, "run not found");
    return r;
  };
  const results = (runId: string): ResultsData => {
    cached ??= {
      runId,
      weatherYear: "AMY2018",
      calibrationStatus: "Not calibrated: no site data uploaded for this run",
      series: generateDataset({ points, seed: 2018 }),
      temperatureNote: null,
    };
    return { ...cached, runId };
  };
  const payloadOf = (r: RunRecord) => {
    const rejected = Math.floor(r.completed / 10);
    const done = r.completed >= r.maxRuns;
    return {
      stage: done ? "finished" : "run",
      n_attempted: r.completed,
      n_feasible: r.completed - rejected,
      max_runs: r.maxRuns,
      converged: done ? true : null, // like the engine: null until a convergence check has run
      message: stageFor(r.completed / r.maxRuns),
    };
  };
  const statusOf = (runId: string, r: RunRecord): RunStatus => {
    const done = r.completed >= r.maxRuns;
    const base: RunStatus = {
      runId,
      state: "queued",
      stage: "Queued",
      completedRuns: 0,
      maxRuns: r.maxRuns,
      feasible: 0,
      rejected: 0,
      converged: null,
      seeds: { ...r.seeds },
      config: structuredClone(r.config),
      engineVersion: "mock",
      cancelRequested: r.cancelled,
      ensembleMode: r.mode,
      convergenceRule: null,
      dataFiles: structuredClone(MOCK_DATA_FILES),
    };
    const s = r.started ? applyProgress(base, payloadOf(r)) : base;
    if (r.cancelled) return { ...s, state: "cancelled", stage: "Cancelled" };
    if (r.started && done) return { ...s, state: "succeeded", convergenceRule: "legacy_window" }; // the engine reports it in the result
    return { ...s, state: !r.started ? "queued" : "running" };
  };
  const fillSeeds = (given: Partial<Seeds>): Seeds => {
    const out = {} as Seeds;
    for (const k of SEED_KEYS) out[k] = given[k] ?? ++seedCounter; // the real server draws fresh random seeds
    return out;
  };
  const addRun = (config: ScenarioConfig, seeds: Seeds): { runId: string } => {
    const ensemble = (config.ensemble ?? {}) as { max_runs?: number; mode?: string };
    const runId = `run-${++counter}`;
    runs.set(runId, {
      maxRuns: ensemble.max_runs ?? 50,
      completed: 0,
      cancelled: false,
      started: false,
      seeds,
      config: structuredClone(config),
      mode: ensemble.mode === "adaptive" ? "adaptive" : "fixed",
    });
    return { runId };
  };

  return {
    kind: "mock",
    async login(apiKey: string) {
      const role = apiKey.trim() as Role;
      if (!ROLES.includes(role)) throw new ApiError(401, "Your API key was not accepted. (Mock mode: use admin, client or demo.)");
      session = sessionFor(role);
      if (persist) writeStored(session);
      return session;
    },
    async logout() {
      session = null;
      if (persist) writeStored(null);
    },
    async getSession() {
      if (persist) session = readStored();
      return session;
    },
    onSignedOut(listener: () => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    async listBuildingTypes() {
      need();
      return BUILDING_TYPES;
    },
    async listFilterOptions() {
      return need().permissions["filters.use"] === true ? FILTER_OPTIONS : [];
    },
    async listPredefinedCompositions() {
      need();
      return [...PREDEFINED];
    },
    async startRun(request: RunRequest) {
      const s = need();
      const input = request.kind === "predefined" ? "composition_id" : request.kind === "replay" ? "config" : "composition";
      const sendsSeeds = request.kind === "replay" || (request.kind === "custom" && Object.keys(request.composition.seeds).length > 0);
      if (!s.runInputs.includes(input) || (sendsSeeds && !s.maySetSeeds)) {
        throw new ApiError(403, "the demo role may only run a predefined composition with its fixed seed");
      }
      if (request.kind === "predefined") {
        if (!PREDEFINED.includes(request.compositionId)) throw new ApiError(404, "unknown composition_id");
        const config = { scenario: { name: [request.compositionId] }, ensemble: { mode: "fixed", max_runs: 50 } };
        return addRun(config, s.role === "demo" ? fillSeeds({ composition_seed: DEMO_SEED, archetype_seed: DEMO_SEED, perturbation_seed: DEMO_SEED }) : fillSeeds({}));
      }
      if (request.kind === "replay") return addRun(request.config, { ...request.seeds });
      const c = request.composition;
      if (c.vintageFilter && s.permissions["filters.use"] !== true) throw new ApiError(403, `role '${s.role}' does not have the filters.use permission`);
      checkComposition(c);
      const config = {
        scenario: { name: c.name },
        composition: {
          total_buildings: c.totalBuildings,
          categories: Object.fromEntries(
            c.entries.map((e) => [e.typeId, e.mode === "count" ? { fixed_count: e.count } : { min_fraction: e.minFraction, max_fraction: e.maxFraction }]),
          ),
        },
        ensemble: { mode: c.convergence ? "adaptive" : "fixed", max_runs: c.maxRuns },
      };
      return addRun(config, fillSeeds(c.seeds));
    },
    async getRun(runId: string) {
      return statusOf(runId, record(runId));
    },
    async watchRun(runId: string, onEvent: (e: RunEvent) => void, signal: AbortSignal) {
      const r = record(runId);
      let id = 0;
      while (!signal.aborted) {
        if (r.cancelled) return onEvent({ type: "status", status: "cancelled" });
        if (r.started && r.completed >= r.maxRuns) return onEvent({ type: "status", status: "succeeded" });
        await new Promise((res) => setTimeout(res, tickMs));
        if (signal.aborted || r.cancelled) continue;
        r.started = true;
        r.completed = Math.min(r.maxRuns, r.completed + runsPerTick);
        const p = payloadOf(r);
        onEvent({ type: "progress", id: String(++id), stage: p.stage, message: p.message, payload: p, ts: new Date().toISOString() });
      }
    },
    async cancelRun(runId: string) {
      const r = record(runId);
      if (r.completed < r.maxRuns || !r.started) r.cancelled = true;
    },
    async getResults(runId: string) {
      need();
      if (runId === DEMO_RUN_ID) return results(runId);
      const r = record(runId);
      if (r.cancelled || !r.started || r.completed < r.maxRuns) throw new ApiError(404, "results not available");
      return results(runId);
    },
  };
}
