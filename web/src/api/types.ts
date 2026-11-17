// Domain types and the ApiClient interface. The UI depends only on this file. Two implementations: the mock
// (fixtures, for tests and offline work) and HttpApiClient (the real API, typed from src/api/generated/).
// The real API (api/openapi.json) is the source of truth for every shape here.
// Business rules (sampling, convergence, feasibility, role enforcement) live in the engine/API, not here.

export type Role = "admin" | "client" | "demo";

export interface Session {
  /**
   * The caller's role, or null when the server does not report it. The real API has no "who am I" endpoint yet,
   * so the HTTP client cannot know the role; the UI then shows every control and the server enforces permissions.
   */
  role: Role | null;
  displayName: string;
}

export interface BuildingType {
  id: string;
  label: string;
  sector: "residential" | "commercial";
}

export interface FilterOption {
  id: string;
  label: string;
  values: string[];
}

export type EntryMode = "fraction" | "count";

/**
 * Mirrors the engine's category config: either a fixed count, or a fraction range (min..max share of the buildings
 * left after fixed counts are taken out). See src/microgrid/community_load/synthetic/composition.py.
 */
export type CompositionEntry =
  | { typeId: string; mode: "fraction"; minFraction: number; maxFraction: number }
  | { typeId: string; mode: "count"; count: number };

/** The three engine seeds (API `RunCreate.seeds`, engine `rng` section). */
export const SEED_KEYS = ["composition_seed", "archetype_seed", "perturbation_seed"] as const;
export type SeedKey = (typeof SEED_KEYS)[number];
export type Seeds = Record<SeedKey, number>;

export interface Composition {
  name: string;
  totalBuildings: number;
  entries: CompositionEntry[];
  /** Seeds left out are drawn fresh by the server (FR-ENG-4: random by default, always recorded). */
  seeds: Partial<Seeds>;
  maxRuns: number;
  /** Admin-only in the MVP (FR-ENG-11); the server rejects it for other roles. */
  vintageFilter?: string;
}

/** A scenario config as the API stores it (`composition_snapshot`), without the server-built `data` section. */
export type ScenarioConfig = Record<string, unknown>;

/**
 * What to run. Mirrors the API's RunCreate (exactly one of config / composition id):
 * - `predefined`: a server-side composition by name (the only kind the demo role may start);
 * - `replay`: a stored config with explicit seeds (FR-ENG-4 d);
 * - `custom`: the composition form. The real API takes a full scenario config, and building one needs the
 *   building-type catalogue the API does not offer yet, so the HTTP client rejects this kind (see report).
 */
export type RunRequest =
  | { kind: "predefined"; compositionId: string }
  | { kind: "replay"; config: ScenarioConfig; seeds: Seeds }
  | { kind: "custom"; composition: Composition };

/** API run statuses (RunOut.status). */
export type RunState = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export const TERMINAL_STATES: readonly RunState[] = ["succeeded", "failed", "cancelled"];

export interface RunStatus {
  runId: string;
  state: RunState;
  /** Human-readable stage text from the engine. */
  stage: string;
  completedRuns: number;
  /** Null until the server reports it (before the first progress event of a run whose config is unknown). */
  maxRuns: number | null;
  feasible: number;
  rejected: number;
  converged: boolean;
  /** The server's error message for a failed run. */
  message?: string;
  /** Seeds the run used (resolved by the server at creation, so present from the start). */
  seeds: Seeds | null;
  /** The stored config (no server paths), used to replay the run; null when the server did not return it. */
  config: ScenarioConfig | null;
  engineVersion: string;
  cancelRequested: boolean;
}

/** Progress payload of one engine event (engine ProgressEvent, stored by the worker as JSON). */
export interface ProgressPayload {
  stage?: string;
  run_index?: number;
  n_attempted?: number;
  n_feasible?: number;
  max_runs?: number;
  converged?: boolean | null;
  message?: string;
  convergence?: Record<string, unknown> | null;
}

/** One server-sent event from `/runs/{id}/events`. */
export type RunEvent =
  | { type: "progress"; id: string; stage: string; message: string; payload: ProgressPayload; ts: string }
  | { type: "status"; status: RunState };

/** A year of data. Arrays are parallel and have the same length (35,040 for a full year at 15 minutes). */
export interface LoadSeries {
  /** ms since epoch; the engine's local wall-clock time is encoded as if it were UTC (labels say "UTC"). */
  ts: Float64Array;
  stepMs: number;
  /** kW (ensemble mean) */
  total: Float64Array;
  residential: Float64Array;
  commercial: Float64Array;
  p10: Float64Array;
  p50: Float64Array;
  p90: Float64Array;
  /** degrees C; null when the results do not include temperature (the real engine does not yet). */
  temperature: Float64Array | null;
}

export interface ResultsData {
  runId: string;
  /** e.g. AMY2018 = actual 2018 weather, not a typical year. Null when the results do not say. */
  weatherYear: string | null;
  /** Null when the API does not report it. */
  calibrationStatus: string | null;
  series: LoadSeries;
}

/** One field-level message from a 422 response (`loc` joined with "."). */
export interface FieldError {
  field: string;
  message: string;
}

export class ApiError extends Error {
  /** HTTP status; 0 when the API could not be reached. */
  readonly status: number;
  readonly fieldErrors: FieldError[];
  constructor(status: number, message: string, fieldErrors: FieldError[] = []) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.fieldErrors = fieldErrors;
  }
}

export interface ApiClient {
  /** "mock" for the fixture client, "http" for the real API. */
  readonly kind: "mock" | "http";
  /** Validates the key against the API; rejects with ApiError(401) if it is wrong. */
  login(apiKey: string): Promise<Session>;
  logout(): Promise<void>;
  getSession(): Promise<Session | null>;
  /** Called when the server rejects the stored key (401); returns an unsubscribe function. */
  onSignedOut(listener: () => void): () => void;
  listBuildingTypes(): Promise<BuildingType[]>;
  /** Empty unless the caller may use filters (admin in the MVP). */
  listFilterOptions(): Promise<FilterOption[]>;
  /** Names of the predefined compositions every role may run. */
  listPredefinedCompositions(): Promise<string[]>;
  startRun(request: RunRequest): Promise<{ runId: string }>;
  getRun(runId: string): Promise<RunStatus>;
  /**
   * Streams the run's events until the run is terminal (resolves) or `signal` aborts (resolves). Reconnects after
   * a dropped connection, resuming after the last event seen. Rejects with ApiError for 401/403/404/422.
   */
  watchRun(runId: string, onEvent: (e: RunEvent) => void, signal: AbortSignal): Promise<void>;
  cancelRun(runId: string): Promise<void>;
  getResults(runId: string): Promise<ResultsData>;
}
