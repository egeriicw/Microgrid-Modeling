import { SEED_KEYS, type Composition, type Limits, type SeedKey } from "../../api/types";
import { MAX_NAME, hasForbiddenNameChar, pyBlank, pyLength } from "../../api/serverText";

export { MAX_NAME };

/** Problems on one building type's own fields (shown on that field, naming the type). */
export interface EntryErrors {
  count?: string;
  min?: string;
  max?: string;
}

export type CompositionErrors = Partial<Record<"name" | "totalBuildings" | "entries" | "maxRuns" | SeedKey, string>> & {
  /** Per building type (by id): problems on that type's fields. Absent when there are none. */
  byType?: Record<string, EntryErrors>;
};

/** Each seed fits an unsigned 32-bit integer (the API's RunCreate rule, 0..2^32-1). */
export const MAX_SEED = 4_294_967_295;
/** The API schema's upper bound on any count (composition_schemas.MAX_COUNT); the server's caps in `limits` are lower. */
export const MAX_COUNT = 1_000_000;

/**
 * Strict parse of typed text: optional minus, then digits with an optional decimal part, or a leading-dot decimal
 * (".25" = 0.25). Refuses exponents ("1e-1"), hex, percent signs, decimal commas, surrounding spaces and empty text,
 * which `Number()` would accept or silently turn into 0.
 */
export function parseNumber(raw: string): number {
  return /^-?(\d+(\.\d+)?|\.\d+)$/.test(raw) ? Number(raw) : Number.NaN;
}

const isWhole = (n: number) => Number.isInteger(n);

/** Every problem message, field and group alike (for the "N problems" summary). */
export function problemList(e: CompositionErrors): string[] {
  const out: string[] = [];
  for (const [k, v] of Object.entries(e)) {
    if (k === "byType") continue;
    if (typeof v === "string") out.push(v);
  }
  for (const t of Object.values(e.byType ?? {})) for (const v of [t.count, t.min, t.max]) if (v) out.push(v);
  return out;
}

function shareProblem(v: number, label: string, which: "smallest" | "largest"): string | undefined {
  if (!Number.isFinite(v)) return `${label}: enter the ${which} share as a number from 0 to 1 (for example 0.25).`;
  if (v < 0 || v > 1) return `${label}: the ${which} share must be between 0 and 1.`;
  return undefined;
}

/**
 * Mirror of the API's composition rules (api/src/microgrid_api/composition_schemas.py and services/compositions.py),
 * so problems show before submitting. The server stays the authority and its messages are always shown.
 *  - per type: an exact number is a whole number >= 0; a share range has 0 <= smallest <= largest <= 1;
 *  - exact numbers are placed first; their sum must not exceed the total; if every type has an exact number they must
 *    add up to the total; the share-range types share the remainder, so sum(smallest) <= 1 <= sum(largest).
 */
function checkEntries(c: Composition, labelOf: (id: string) => string): { group?: string; byType: Record<string, EntryErrors> } {
  const byType: Record<string, EntryErrors> = {};
  const add = (id: string, field: keyof EntryErrors, msg: string | undefined) => {
    if (msg) byType[id] = { ...byType[id], [field]: msg };
  };
  for (const e of c.entries) {
    const l = labelOf(e.typeId);
    if (e.mode === "count") {
      if (!Number.isFinite(e.count)) add(e.typeId, "count", `${l}: enter the number of buildings as a whole number.`);
      else if (!isWhole(e.count) || e.count < 0) add(e.typeId, "count", `${l}: the number of buildings must be a whole number, 0 or more.`);
      else if (e.count > MAX_COUNT) add(e.typeId, "count", `${l}: the number of buildings can be at most ${MAX_COUNT}.`);
    } else {
      add(e.typeId, "min", shareProblem(e.minFraction, l, "smallest"));
      add(e.typeId, "max", shareProblem(e.maxFraction, l, "largest"));
      if (!byType[e.typeId] && e.minFraction > e.maxFraction) {
        add(e.typeId, "min", `${l}: the smallest share (${e.minFraction}) must not be larger than the largest share (${e.maxFraction}).`);
      }
    }
  }
  const ids = c.entries.map((x) => x.typeId);
  const repeated = ids.find((id, i) => ids.indexOf(id) !== i);
  if (repeated !== undefined) return { group: `A building type is listed more than once (${labelOf(repeated)}).`, byType };
  if (Object.keys(byType).length > 0) return { byType }; // sums of unreadable numbers would only add noise

  const counts = c.entries.flatMap((x) => (x.mode === "count" ? [x.count] : []));
  const ranges = c.entries.flatMap((x) => (x.mode === "fraction" ? [x] : []));
  const fixed = counts.reduce((a, b) => a + b, 0);
  const total = c.totalBuildings;
  if (!Number.isFinite(total) || !isWhole(total) || total < 1) return { byType }; // the total's own message is shown
  if (fixed > total) return { group: `The exact numbers add up to ${fixed}, which exceeds the total number of buildings (${total}).`, byType };
  if (ranges.length === 0) {
    const left = total - fixed;
    return left === 0
      ? { byType }
      : {
          group: `Every type has an exact number and they add up to ${fixed}, not ${total}; ${left} building(s) are unassigned. Make them add up, or give at least one type a share range.`,
          byType,
        };
  }
  const sumMin = ranges.reduce((a, x) => a + x.minFraction, 0);
  const sumMax = ranges.reduce((a, x) => a + x.maxFraction, 0);
  if (sumMin > 1 + 1e-9) return { group: `The smallest shares add up to more than 1 (${Number(sumMin.toFixed(4))}), so no mix is possible.`, byType };
  if (sumMax < 1 - 1e-9) return { group: `The largest shares add up to less than 1 (${Number(sumMax.toFixed(4))}), so no mix is possible.`, byType };
  return { byType };
}

/**
 * Early feedback while typing; reports EVERY current problem. `limits` are the server's caps from `GET /me`; without
 * them only the API schema's fixed bounds are checked. `labelOf` names a building type in messages (default: its id).
 * These never encode methodology (no sampling or convergence rules beyond what the API states in its own limits).
 */
export function validateComposition(c: Composition, limits?: Limits, labelOf: (id: string) => string = (id) => id): CompositionErrors {
  const e: CompositionErrors = {};
  // The server's order: at least one character, not only whitespace (Python's strip), then the character rules.
  if (c.name === "" || pyBlank(c.name)) e.name = "Enter a name for this composition.";
  else if (pyLength(c.name) > MAX_NAME) e.name = `The name can have at most ${MAX_NAME} characters.`;
  else if (hasForbiddenNameChar(c.name)) e.name = "The name cannot contain control, line-break or text-direction characters.";

  const maxTotal = Math.min(MAX_COUNT, limits?.maxTotalBuildings ?? MAX_COUNT);
  if (!Number.isFinite(c.totalBuildings)) e.totalBuildings = "Total buildings: enter a whole number.";
  else if (!isWhole(c.totalBuildings)) e.totalBuildings = "Total buildings must be a whole number.";
  else if (c.totalBuildings < 1) e.totalBuildings = "Total buildings must be at least 1.";
  else if (c.totalBuildings > maxTotal) e.totalBuildings = `Total buildings can be at most ${maxTotal} on this server.`;

  if (c.entries.length === 0) {
    e.entries = "Include at least one building type.";
  } else {
    const { group, byType } = checkEntries(c, labelOf);
    if (group) e.entries = group;
    if (Object.keys(byType).length > 0) e.byType = byType;
  }

  for (const k of SEED_KEYS) {
    const v = c.seeds[k];
    if (v === undefined) continue; // blank: the server draws a fresh random seed and records it
    if (!Number.isFinite(v) || !isWhole(v) || v < 0) e[k] = "Seed must be a whole number, 0 or more.";
    else if (v > MAX_SEED) e[k] = `Seed must be at most ${MAX_SEED}.`;
  }

  const maxRuns = Math.min(MAX_COUNT, limits?.maxEnsembleRuns ?? MAX_COUNT);
  if (!Number.isFinite(c.maxRuns) || !isWhole(c.maxRuns) || c.maxRuns < 1) e.maxRuns = "Number of runs must be a whole number, at least 1.";
  else if (c.maxRuns > maxRuns) e.maxRuns = `Number of runs can be at most ${maxRuns} on this server.`;
  else if (c.convergence && limits && c.maxRuns < limits.defaultMinRuns) {
    e.maxRuns = `With convergence on, the number of runs must be at least ${limits.defaultMinRuns} (the server's minimum number of runs).`;
  }
  return e;
}
