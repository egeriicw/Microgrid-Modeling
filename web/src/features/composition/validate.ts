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
  } else if (c.entries.some((x) => !Number.isFinite(x.value))) {
    e.entries = "Every building type needs a number.";
  } else {
    const fractions = c.entries.filter((x) => x.mode === "fraction");
    const counts = c.entries.filter((x) => x.mode === "count");
    const fixed = counts.reduce((sum, x) => sum + x.value, 0);
    const fractionSum = fractions.reduce((sum, x) => sum + x.value, 0);
    if (fractions.some((x) => x.value < 0 || x.value > 1)) {
      e.entries = "Fractions must be between 0 and 1.";
    } else if (counts.some((x) => !isWhole(x.value) || x.value < 0)) {
      e.entries = "Fixed counts must be whole numbers, 0 or more.";
    } else if (fixed > c.totalBuildings) {
      e.entries = "Fixed counts exceed the total number of buildings.";
    } else if (Number.isFinite(c.totalBuildings) && c.totalBuildings > 0) {
      // Rule: a fraction is a share of the TOTAL. Fixed counts plus fraction shares must account for every building,
      // i.e. fractions must sum to 1 - fixed/total. With no fixed counts that is "fractions sum to 1".
      const needed = 1 - fixed / c.totalBuildings;
      if (Math.abs(fractionSum - needed) > 1e-6) {
        e.entries =
          counts.length === 0
            ? "Fractions must sum to 1."
            : `Fixed counts plus fractions must add up to the total: fractions must sum to ${Number(needed.toFixed(6))}.`;
      }
    }
  }

  if (!Number.isFinite(c.seed) || !isWhole(c.seed) || c.seed < 0) e.seed = "Seed must be a whole number, 0 or more.";
  else if (c.seed > MAX_SEED) e.seed = `Seed must be at most ${MAX_SEED}.`;
  if (!Number.isFinite(c.maxRuns) || !isWhole(c.maxRuns) || c.maxRuns < 1) e.maxRuns = "Maximum runs must be a whole number, at least 1.";
  else if (c.maxRuns > MAX_RUNS_LIMIT) e.maxRuns = `Maximum runs must be at most ${MAX_RUNS_LIMIT}.`;
  return e;
}
