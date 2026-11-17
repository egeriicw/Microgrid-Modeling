import type { Role } from "../api/types";

// What the UI shows per role (PRD section 4). This only hides controls; the API enforces every permission and
// must reject anything the UI hides.
export const canStartRuns = (r: Role): boolean => r === "admin" || r === "client";
/** In the MVP the building-attribute filter feature is Admin-only. */
export const canUseFilters = (r: Role): boolean => r === "admin";
