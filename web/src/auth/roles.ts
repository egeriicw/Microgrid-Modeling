import type { Session } from "../api/types";

// What the UI offers, derived ONLY from the server's session (`GET /me`): its run inputs, seed permission and
// permission flags. The role name is never interpreted here. This only hides controls; the API enforces every rule.

export interface Capabilities {
  /** The composition form (`POST /runs` with `composition`). */
  composeCustom: boolean;
  /** Predefined compositions (`composition_id`). */
  runPredefined: boolean;
  /** Replay with the same seeds: posts the stored config with explicit seeds. */
  replay: boolean;
  /** Seed fields in the form. */
  setSeeds: boolean;
  /** Building-attribute filters (`filters.use`). */
  useFilters: boolean;
}

export function capabilitiesOf(s: Session | null): Capabilities {
  if (!s) return { composeCustom: false, runPredefined: false, replay: false, setSeeds: false, useFilters: false };
  return {
    composeCustom: s.runInputs.includes("composition"),
    runPredefined: s.runInputs.includes("composition_id"),
    replay: s.runInputs.includes("config") && s.maySetSeeds,
    setSeeds: s.maySetSeeds,
    useFilters: s.permissions["filters.use"] === true,
  };
}
