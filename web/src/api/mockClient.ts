// Mock ApiClient driven by fixtures, for tests and offline work (VITE_API_MODE=mock). It plays the server's part
// for roles (401/403/404) with the SAME rules and shapes as the real API (api/src/microgrid_api/routers/runs.py),
// so the UI is exercised against realistic behaviour. All HTTP lives in src/api/httpClient.ts.
import { BUILDING_TYPES, FILTER_OPTIONS, FULL_YEAR_POINTS, generateDataset } from "./fixtures";
import { applyProgress } from "./mapping";
import {
  ApiError,
  SEED_KEYS,
  type ApiClient,
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

function readStored(): Session | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    const v = raw ? (JSON.parse(raw) as Session) : null;
    return v && v.role && ROLES.includes(v.role) ? v : null;
  } catch {
    return null;
  }
}

function writeStored(s: Session | null): void {
  try {
    if (s) sessionStorage.setItem(SESSION_KEY, JSON.stringify(s));
    else sessionStorage.removeItem(SESSION_KEY);
  } catch {
    /* storage blocked: the session just lasts until reload */
  }
}

interface RunRecord {
  maxRuns: number;
  completed: number;
  cancelled: boolean;
  started: boolean;
  seeds: Seeds;
  config: ScenarioConfig;
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
      converged: done,
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
      converged: false,
      seeds: { ...r.seeds },
      config: structuredClone(r.config),
      engineVersion: "mock",
      cancelRequested: r.cancelled,
    };
    const s = r.started ? applyProgress(base, payloadOf(r)) : base;
    if (r.cancelled) return { ...s, state: "cancelled", stage: "Cancelled" };
    return { ...s, state: !r.started ? "queued" : done ? "succeeded" : "running" };
  };
  const fillSeeds = (given: Partial<Seeds>): Seeds => {
    const out = {} as Seeds;
    for (const k of SEED_KEYS) out[k] = given[k] ?? ++seedCounter; // the real server draws fresh random seeds
    return out;
  };
  const addRun = (config: ScenarioConfig, seeds: Seeds): { runId: string } => {
    const ensemble = (config.ensemble ?? {}) as { max_runs?: number };
    const runId = `run-${++counter}`;
    runs.set(runId, { maxRuns: ensemble.max_runs ?? 50, completed: 0, cancelled: false, started: false, seeds, config: structuredClone(config) });
    return { runId };
  };

  return {
    kind: "mock",
    async login(apiKey: string) {
      const role = apiKey.trim() as Role;
      if (!ROLES.includes(role)) throw new ApiError(401, "Your API key was not accepted. (Mock mode: use admin, client or demo.)");
      session = { role, displayName: `Mock ${role}` };
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
      return need().role === "admin" ? FILTER_OPTIONS : [];
    },
    async listPredefinedCompositions() {
      need();
      return [...PREDEFINED];
    },
    async startRun(request: RunRequest) {
      const role = need().role;
      if (request.kind === "predefined") {
        if (!PREDEFINED.includes(request.compositionId)) throw new ApiError(404, "unknown composition_id");
        const config = { scenario: { name: [request.compositionId] }, ensemble: { max_runs: 50 } };
        return addRun(config, role === "demo" ? fillSeeds({ composition_seed: DEMO_SEED, archetype_seed: DEMO_SEED, perturbation_seed: DEMO_SEED }) : fillSeeds({}));
      }
      if (role === "demo") throw new ApiError(403, "the demo role may only run a predefined composition with its fixed seed");
      if (request.kind === "replay") return addRun(request.config, { ...request.seeds });
      const c = request.composition;
      if (c.vintageFilter && role !== "admin") throw new ApiError(403, "attribute filters are admin-only");
      const config = {
        scenario: { name: [c.name] },
        composition: { total_buildings: c.totalBuildings, categories: Object.fromEntries(c.entries.map((e) => [e.typeId, e])) },
        ensemble: { max_runs: c.maxRuns },
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
