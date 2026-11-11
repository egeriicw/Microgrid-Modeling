import type { Composition } from "../../api/types";

export type CompositionErrors = Partial<Record<"name" | "totalBuildings" | "entries" | "seed" | "maxRuns", string>>;

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
    if (fractions.some((x) => x.value < 0 || x.value > 1)) {
      e.entries = "Fractions must be between 0 and 1.";
    } else if (counts.some((x) => !isWhole(x.value) || x.value < 0)) {
      e.entries = "Fixed counts must be whole numbers, 0 or more.";
    } else if (counts.reduce((s, x) => s + x.value, 0) > c.totalBuildings) {
      e.entries = "Fixed counts exceed the total number of buildings.";
    } else if (counts.length === 0 && Math.abs(fractions.reduce((s, x) => s + x.value, 0) - 1) > 1e-6) {
      e.entries = "Fractions must sum to 1.";
    }
  }

  if (!Number.isFinite(c.seed) || !isWhole(c.seed) || c.seed < 0) e.seed = "Seed must be a whole number, 0 or more.";
  if (!Number.isFinite(c.maxRuns) || !isWhole(c.maxRuns) || c.maxRuns < 1) e.maxRuns = "Maximum runs must be a whole number, at least 1.";
  return e;
}
