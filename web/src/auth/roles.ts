import type { Role } from "../api/types";

// What the UI shows per role (PRD section 4). This only hides controls; the API enforces every permission and
// must reject anything the UI hides. `null` = the server did not report the role (real API today): show the
// controls and let the server answer 403 rather than guess.

/** Custom compositions and replays (the server limits demo to predefined compositions). */
export const canStartRuns = (r: Role | null): boolean => r !== "demo";
/** In the MVP the building-attribute filter feature is Admin-only; unknown role: hidden (no API for options yet). */
export const canUseFilters = (r: Role | null): boolean => r === "admin";
