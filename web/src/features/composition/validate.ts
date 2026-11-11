import type { Composition } from "../../api/types";

export type CompositionErrors = Partial<Record<"name" | "totalBuildings" | "entries" | "seed" | "maxRuns", string>>;

/** UI limits (the API is authoritative and may differ): seed fits an unsigned 32-bit integer; max runs is capped. */
export const MAX_SEED = 4_294_967_295;
export const MAX_RUNS_LIMIT = 1000;

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
 * Mirror of the engine's composition rule (composition.py / config.py):
 *  1. fixed-count types are taken out first; their sum must not exceed the total;
 *  2. the remainder (total - fixed) is split among the fraction-range types, each with min..max as a share of the
 *     remainder, so 0 <= min <= max <= 1 per type and sum(min) <= 1 <= sum(max) overall;
 *  3. if the remainder is 0, fraction types get 0 buildings and their ranges are ignored (not an error);
 *  4. with no fraction types at all, fixed counts must add up exactly to the total (UI rule, stricter than the engine).
 */
function checkEntries(c: Composition): string | undefined {
  const counts = c.entries.filter((x) => x.mode === "count");
  const ranges = c.entries.filter((x) => x.mode === "fraction");
  const countValues = counts.map((x) => x.count);
  if (countValues.some((v) => !Number.isFinite(v))) return "Every fixed count needs a number.";
  if (countValues.some((v) => !isWhole(v) || v < 0)) return "Fixed counts must be whole numbers, 0 or more.";
  const fixed = countValues.reduce((a, b) => a + b, 0);
  if (fixed > c.totalBuildings) return "Fixed counts exceed the total number of buildings.";
  if (!Number.isFinite(c.totalBuildings) || c.totalBuildings < 1) return undefined; // the total's own error is shown
  const remainder = c.totalBuildings - fixed;
  if (ranges.length === 0) {
    return remainder === 0 ? undefined : `Fixed counts must add up to the total (${c.totalBuildings}); ${remainder} building(s) are unassigned.`;
  }
  if (remainder === 0) return undefined;
  if (ranges.some((x) => !Number.isFinite(x.minFraction) || !Number.isFinite(x.maxFraction))) return "Every fraction needs a number.";
  if (ranges.some((x) => x.minFraction < 0 || x.maxFraction > 1)) return "Fractions must be between 0 and 1.";
  if (ranges.some((x) => x.minFraction > x.maxFraction)) return "Each minimum fraction must be at most its maximum.";
  const sumMin = ranges.reduce((a, x) => a + x.minFraction, 0);
  const sumMax = ranges.reduce((a, x) => a + x.maxFraction, 0);
  if (sumMin > 1 + 1e-9) return `The minimum fractions add up to more than 1 (${Number(sumMin.toFixed(4))}), so no composition is possible.`;
  if (sumMax < 1 - 1e-9) return `The maximum fractions add up to less than 1 (${Number(sumMax.toFixed(4))}), so no composition is possible.`;
  return undefined;
}

/**
 * Input-shape checks for instant feedback while typing. The API re-validates everything and is the authority;
 * these never encode methodology (no sampling or feasibility rules here).
 */
export function validateComposition(c: Composition): CompositionErrors {
  const e: CompositionErrors = {};
  if (c.name.trim() === "") e.name = "Enter a name for this composition.";

  if (!Number.isFinite(c.totalBuildings)) e.totalBuildings = "Enter a number.";
  else if (!isWhole(c.totalBuildings)) e.totalBuildings = "Total buildings must be a whole number.";
  else if (c.totalBuildings < 1) e.totalBuildings = "Total buildings must be at least 1.";

  if (c.entries.length === 0) {
    e.entries = "Add at least one building type.";
  } else {
    e.entries = checkEntries(c);
    if (e.entries === undefined) delete e.entries;
  }

  if (!Number.isFinite(c.seed) || !isWhole(c.seed) || c.seed < 0) e.seed = "Seed must be a whole number, 0 or more.";
  else if (c.seed > MAX_SEED) e.seed = `Seed must be at most ${MAX_SEED}.`;
  if (!Number.isFinite(c.maxRuns) || !isWhole(c.maxRuns) || c.maxRuns < 1) e.maxRuns = "Maximum runs must be a whole number, at least 1.";
  else if (c.maxRuns > MAX_RUNS_LIMIT) e.maxRuns = `Maximum runs must be at most ${MAX_RUNS_LIMIT}.`;
  return e;
}
