// Domain types and the ApiClient interface. The UI depends only on this file; the mock client implements it
// today and a client generated from api/openapi.json will replace it later (separate story).
// Business rules (sampling, convergence, feasibility, role enforcement) live in the engine/API, not here.

export type Role = "admin" | "client" | "demo";

export interface Session {
  role: Role;
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

export interface CompositionEntry {
  typeId: string;
  mode: EntryMode;
  value: number;
}

export interface Composition {
  name: string;
  totalBuildings: number;
  entries: CompositionEntry[];
  seed: number;
  maxRuns: number;
  /** Admin-only in the MVP (FR-ENG-11); the server rejects it for other roles. */
  vintageFilter?: string;
}

export type RunState = "running" | "succeeded" | "failed" | "cancelled";

export interface RunStatus {
  runId: string;
  state: RunState;
  /** Human-readable stage text from the engine. */
  stage: string;
  completedRuns: number;
  maxRuns: number;
  feasible: number;
  rejected: number;
  converged: boolean;
  message?: string;
}

/** A year of 15-minute data. Arrays are parallel and have the same length (35,040 for a full year). */
export interface LoadSeries {
  /** ms since epoch (UTC) */
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

export interface ResultsData {
  runId: string;
  /** AMY2018 = actual 2018 weather, not a typical year. */
  weatherYear: string;
  calibrationStatus: string;
  series: LoadSeries;
}

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export interface ApiClient {
  login(role: Role): Promise<Session>;
  logout(): Promise<void>;
  getSession(): Promise<Session | null>;
  listBuildingTypes(): Promise<BuildingType[]>;
  /** Empty unless the caller may use filters (admin in the MVP). */
  listFilterOptions(): Promise<FilterOption[]>;
  startRun(composition: Composition): Promise<{ runId: string }>;
  getRun(runId: string): Promise<RunStatus>;
  cancelRun(runId: string): Promise<void>;
  getResults(runId: string): Promise<ResultsData>;
}

/** Id of the predefined demo run every role may view. */
export const DEMO_RUN_ID = "demo-run";
