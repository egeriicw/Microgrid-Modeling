// Mock ApiClient driven by fixtures. It also plays the server's part for roles (401/403) so the UI is exercised
// against realistic failures. Replace with the generated client; keep all HTTP inside src/api/.
import { BUILDING_TYPES, FILTER_OPTIONS, FULL_YEAR_POINTS, generateDataset } from "./fixtures";
import {
  ApiError,
  type ApiClient,
  type Composition,
  type ResultsData,
  type Role,
  type RunStatus,
  DEMO_RUN_ID,
  type Session,
} from "./types";

export interface MockOptions {
  /** Runs the fake engine completes on each status poll. */
  runsPerPoll?: number;
  /** Length of the fixture series (multiple of 4); default is a full year, 35,040 points. */
  pointsPerYear?: number;
  /** Keep the mock session in sessionStorage so a reload or deep link keeps the user signed in. */
  persistSession?: boolean;
}

const SESSION_KEY = "microgrid.mockSession";

function readStored(): Session | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    const v = raw ? (JSON.parse(raw) as Session) : null;
    return v && ["admin", "client", "demo"].includes(v.role) ? v : null;
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
}

function stageFor(fraction: number): string {
  if (fraction < 0.2) return "Sampling buildings from the library";
  if (fraction < 0.8) return "Simulating and aggregating sampled communities";
  if (fraction < 1) return "Checking convergence";
  return "Done";
}

export function createMockClient(options: MockOptions = {}): ApiClient {
  const runsPerPoll = options.runsPerPoll ?? 25;
  const points = options.pointsPerYear ?? FULL_YEAR_POINTS;
  const persist = options.persistSession ?? false;
  let session: Session | null = persist ? readStored() : null;
  const runs = new Map<string, RunRecord>();
  let counter = 0;
  let cached: ResultsData | null = null;

  const need = (): Session => {
    if (!session) throw new ApiError(401, "Sign in first.");
    return session;
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
  const statusOf = (runId: string, r: RunRecord): RunStatus => {
    const done = r.completed >= r.maxRuns;
    const rejected = Math.floor(r.completed / 10);
    return {
      runId,
      state: r.cancelled ? "cancelled" : done ? "succeeded" : "running",
      stage: r.cancelled ? "Cancelled" : stageFor(r.completed / r.maxRuns),
      completedRuns: r.completed,
      maxRuns: r.maxRuns,
      feasible: r.completed - rejected,
      rejected,
      converged: done && !r.cancelled,
    };
  };

  return {
    async login(role: Role) {
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
    async listBuildingTypes() {
      need();
      return BUILDING_TYPES;
    },
    async listFilterOptions() {
      return need().role === "admin" ? FILTER_OPTIONS : [];
    },
    async startRun(composition: Composition) {
      if (need().role === "demo") throw new ApiError(403, "Demo accounts cannot start runs.");
      if (composition.vintageFilter && session?.role !== "admin") throw new ApiError(403, "Filters are admin-only.");
      const runId = `run-${++counter}`;
      runs.set(runId, { maxRuns: composition.maxRuns, completed: 0, cancelled: false });
      return { runId };
    },
    async getRun(runId: string) {
      need();
      const r = runs.get(runId);
      if (!r) throw new ApiError(404, `Run ${runId} not found.`);
      if (!r.cancelled) r.completed = Math.min(r.maxRuns, r.completed + runsPerPoll);
      return statusOf(runId, r);
    },
    async cancelRun(runId: string) {
      if (need().role === "demo") throw new ApiError(403, "Demo accounts cannot cancel runs.");
      const r = runs.get(runId);
      if (!r) throw new ApiError(404, `Run ${runId} not found.`);
      r.cancelled = true;
    },
    async getResults(runId: string) {
      need();
      if (runId === DEMO_RUN_ID) return results(runId);
      const r = runs.get(runId);
      if (!r) throw new ApiError(404, `Run ${runId} not found.`);
      if (r.cancelled || r.completed < r.maxRuns) throw new ApiError(409, `Run ${runId} has no results (not finished).`);
      return results(runId);
    },
  };
}
