import { SEED_KEYS, type Composition, type Limits, type SeedKey } from "../../api/types";

export type CompositionErrors = Partial<Record<"name" | "totalBuildings" | "entries" | "maxRuns" | SeedKey, string>>;

/** Each seed fits an unsigned 32-bit integer (the API's RunCreate rule, 0..2^32-1). */
export const MAX_SEED = 4_294_967_295;
/** The API schema's upper bound on any count (composition_schemas.MAX_COUNT); the server's caps in `limits` are lower. */
export const MAX_COUNT = 1_000_000;
/** The API's limit on a composition name (composition_schemas.MAX_NAME). */
export const MAX_NAME = 120;
// The characters the API refuses in a name: C0/C1 controls, DEL, line/paragraph separators, bidi controls.
const FORBIDDEN_RANGES: [number, number][] = [
  [0x00, 0x1f],
  [0x7f, 0x9f],
  [0x2028, 0x2029],
  [0x202a, 0x202e],
  [0x2066, 0x2069],
];
const hasForbiddenChar = (s: string): boolean => {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (FORBIDDEN_RANGES.some(([lo, hi]) => c >= lo && c <= hi)) return true;
  }
  return false;
};

/**
 * Strict parse of typed text: optional minus, digits, optional decimal part. Rejects hex ("0x10"), exponents ("1e3"),
 * surrounding whitespace, empty text, "Infinity" and thousands separators, all of which `Number()` would accept or
 * silently turn into 0.
 */
export function parseNumber(raw: string): number {
  return /^-?\d+(\.\d+)?$/.test(raw) ? Number(raw) : Number.NaN;
}

const isWhole = (n: number) => Number.isInteger(n);

/**
 * Mirror of the API's composition rules (api/src/microgrid_api/services/compositions.py check_category_counts), so
 * problems show before submitting. The server stays the authority and its messages are always shown.
 *  1. fixed-count types are taken out first; their sum must not exceed the total;
 *  2. if every type has a fixed count, they must add up to the total exactly;
 *  3. the ranged types share the remainder: 0 <= min <= max <= 1 each, and sum(min) <= 1 <= sum(max), checked
 *     whenever there is a ranged type (also when the fixed counts use up the total, as the API does).
 */
function checkEntries(c: Composition): string | undefined {
  const ids = c.entries.map((x) => x.typeId);
  const repeated = ids.find((id, i) => ids.indexOf(id) !== i);
  if (repeated !== undefined) return `A building type is listed more than once (${repeated}).`;
  const counts = c.entries.filter((x) => x.mode === "count");
  const ranges = c.entries.filter((x) => x.mode === "fraction");
  const countValues = counts.map((x) => x.count);
  if (countValues.some((v) => !Number.isFinite(v))) return "Every exact number needs a number.";
  if (countValues.some((v) => !isWhole(v) || v < 0)) return "Exact numbers must be whole numbers, 0 or more.";
  const fixed = countValues.reduce((a, b) => a + b, 0);
  if (Number.isFinite(c.totalBuildings) && fixed > c.totalBuildings) {
    return `The exact numbers add up to ${fixed}, which exceeds the total number of buildings (${c.totalBuildings}).`;
  }
  if (!Number.isFinite(c.totalBuildings) || c.totalBuildings < 1) return undefined; // the total's own error is shown
  if (ranges.length === 0) {
    const left = c.totalBuildings - fixed;
    return left === 0
      ? undefined
      : `Every type has an exact number and they add up to ${fixed}, not ${c.totalBuildings}; ${left} building(s) are unassigned. Make them add up, or give at least one type a share range.`;
  }
  if (ranges.some((x) => !Number.isFinite(x.minFraction) || !Number.isFinite(x.maxFraction))) return "Every share needs a number.";
  if (ranges.some((x) => x.minFraction < 0 || x.maxFraction > 1)) return "Shares must be between 0 and 1.";
  if (ranges.some((x) => x.minFraction > x.maxFraction)) return "Each minimum share must be at most its maximum.";
  const sumMin = ranges.reduce((a, x) => a + x.minFraction, 0);
  const sumMax = ranges.reduce((a, x) => a + x.maxFraction, 0);
  if (sumMin > 1 + 1e-9) return `The minimum shares add up to more than 1 (${Number(sumMin.toFixed(4))}), so no mix is possible.`;
  if (sumMax < 1 - 1e-9) return `The maximum shares add up to less than 1 (${Number(sumMax.toFixed(4))}), so no mix is possible.`;
  return undefined;
}

/**
 * Early feedback while typing. `limits` are the server's caps from `GET /me`; without them only the API schema's
 * fixed bounds are checked. These never encode methodology (no sampling or convergence rules beyond what the API
 * states in its own limits).
 */
export function validateComposition(c: Composition, limits?: Limits): CompositionErrors {
  const e: CompositionErrors = {};
  if (c.name.trim() === "") e.name = "Enter a name for this composition.";
  else if (c.name.length > MAX_NAME) e.name = `The name can have at most ${MAX_NAME} characters.`;
  else if (hasForbiddenChar(c.name)) e.name = "The name cannot contain control, line-break or text-direction characters.";

  const maxTotal = Math.min(MAX_COUNT, limits?.maxTotalBuildings ?? MAX_COUNT);
  if (!Number.isFinite(c.totalBuildings)) e.totalBuildings = "Enter a number.";
  else if (!isWhole(c.totalBuildings)) e.totalBuildings = "Total buildings must be a whole number.";
  else if (c.totalBuildings < 1) e.totalBuildings = "Total buildings must be at least 1.";
  else if (c.totalBuildings > maxTotal) e.totalBuildings = `Total buildings can be at most ${maxTotal} on this server.`;

  if (c.entries.length === 0) {
    e.entries = "Include at least one building type.";
  } else {
    e.entries = checkEntries(c);
    if (e.entries === undefined) delete e.entries;
  }

  for (const k of SEED_KEYS) {
    const v = c.seeds[k];
    if (v === undefined) continue; // blank: the server draws a fresh random seed and records it
    if (!Number.isFinite(v) || !isWhole(v) || v < 0) e[k] = "Seed must be a whole number, 0 or more.";
    else if (v > MAX_SEED) e[k] = `Seed must be at most ${MAX_SEED}.`;
  }

  const maxRuns = Math.min(MAX_COUNT, limits?.maxEnsembleRuns ?? MAX_COUNT);
  if (!Number.isFinite(c.maxRuns) || !isWhole(c.maxRuns) || c.maxRuns < 1) e.maxRuns = "Maximum runs must be a whole number, at least 1.";
  else if (c.maxRuns > maxRuns) e.maxRuns = `Maximum runs can be at most ${maxRuns} on this server.`;
  else if (c.convergence && limits && c.maxRuns < limits.defaultMinRuns) {
    e.maxRuns = `With convergence on, maximum runs must be at least ${limits.defaultMinRuns} (the server's minimum number of runs).`;
  }
  return e;
}
