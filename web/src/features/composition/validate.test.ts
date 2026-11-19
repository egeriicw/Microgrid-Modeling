import { describe, expect, it } from "vitest";
import { MAX_COUNT, MAX_SEED, parseNumber, problemList, validateComposition } from "./validate";
import type { Composition, CompositionEntry, Limits } from "../../api/types";

const range = (typeId: string, minFraction: number, maxFraction: number): CompositionEntry => ({ typeId, mode: "fraction", minFraction, maxFraction });
const fixed = (typeId: string, count: number): CompositionEntry => ({ typeId, mode: "count", count });

const ok: Composition = {
  name: "demo",
  totalBuildings: 100,
  seeds: {},
  maxRuns: 200,
  convergence: false,
  entries: [range("sf", 0.2, 0.8), range("office", 0, 1)],
};
const withEntries = (entries: CompositionEntry[], total = 100): Composition => ({ ...ok, totalBuildings: total, entries });

// Mirrors the engine (src/microgrid/community_load/synthetic/composition.py and config.py): fixed-count types are
// removed first; the remaining buildings (total - sum of fixed) are split among the fraction-range types, each with a
// min and max share of that remainder; feasibility needs sum(min) <= 1 <= sum(max).
describe("validateComposition (input-shape checks; the API is the authority)", () => {
  it("accepts a valid composition", () => {
    expect(validateComposition(ok)).toEqual({});
  });
  it("requires a name", () => {
    expect(validateComposition({ ...ok, name: " " }).name).toMatch(/name/i);
  });
  it("requires total buildings to be a positive whole number", () => {
    expect(validateComposition({ ...ok, totalBuildings: 0 }).totalBuildings).toMatch(/at least 1/i);
    expect(validateComposition({ ...ok, totalBuildings: 2.5 }).totalBuildings).toMatch(/whole/i);
    expect(validateComposition({ ...ok, totalBuildings: Number.NaN }).totalBuildings).toMatch(/number/i);
  });
  it("requires at least one entry", () => {
    expect(validateComposition({ ...ok, entries: [] }).entries).toMatch(/at least one/i);
  });
  it("requires 0 <= smallest <= largest <= 1 for each share range, on that type's own field", () => {
    expect(validateComposition(withEntries([range("a", -0.1, 1)])).byType?.a.min).toMatch(/a: the smallest share must be between 0 and 1/i);
    expect(validateComposition(withEntries([range("a", 0, 1.5)])).byType?.a.max).toMatch(/a: the largest share must be between 0 and 1/i);
    expect(validateComposition(withEntries([range("a", 0.6, 0.4)])).byType?.a.min).toMatch(/smallest share \(0\.6\) must not be larger than the largest share \(0\.4\)/i);
    expect(validateComposition(withEntries([range("a", Number.NaN, 1)])).byType?.a.min).toMatch(/enter the smallest share as a number/i);
  });
  it("rejects ranges that cannot sum to 1 over the remainder", () => {
    expect(validateComposition(withEntries([range("a", 0.7, 1), range("b", 0.5, 1)])).entries).toMatch(/smallest shares add up to more than 1/i);
    expect(validateComposition(withEntries([range("a", 0, 0.3), range("b", 0, 0.3)])).entries).toMatch(/largest shares add up to less than 1/i);
  });
  it("accepts ranges whose min sum is <= 1 <= max sum, including exactly 1", () => {
    expect(validateComposition(withEntries([range("a", 0.5, 0.5), range("b", 0.5, 0.5)]))).toEqual({});
    expect(validateComposition(withEntries([range("a", 0, 1)]))).toEqual({});
  });
  it("requires fixed counts to be whole, non-negative and not above the total", () => {
    expect(validateComposition(withEntries([fixed("a", 101), range("b", 0, 1)])).entries).toMatch(/exceed/i);
    expect(validateComposition(withEntries([fixed("a", 1.5), range("b", 0, 1)])).byType?.a.count).toMatch(/a: .*whole/i);
    expect(validateComposition(withEntries([fixed("a", -1), range("b", 0, 1)])).byType?.a.count).toMatch(/a: .*whole/i);
  });
  it("fixed counts plus ranges: ranges apply to the REMAINDER (60 fixed + range 0..1 of 100 is valid)", () => {
    expect(validateComposition(withEntries([fixed("a", 60), range("b", 0, 1)]))).toEqual({});
    // feasibility is judged on the ranges themselves, not on the fixed part
    expect(validateComposition(withEntries([fixed("a", 60), range("b", 0, 0.3), range("c", 0, 0.3)])).entries).toMatch(/less than 1/i);
  });
  it("counts only must sum exactly to the total", () => {
    expect(validateComposition(withEntries([fixed("a", 40), fixed("b", 60)]))).toEqual({});
    expect(validateComposition(withEntries([fixed("a", 40), fixed("b", 50)])).entries).toMatch(/add up/i);
  });
  it("checks the ranges even when fixed counts use up the total (the API does: sum of minimums must still be <= 1)", () => {
    expect(validateComposition(withEntries([fixed("a", 100), range("b", 0.9, 1), range("c", 0.9, 1)])).entries).toMatch(/smallest shares add up to more than 1/i);
    expect(validateComposition(withEntries([fixed("a", 100), range("b", 0, 1)]))).toEqual({});
  });
  it("validates seeds (optional, each 0..2^32-1) and max runs, with upper bounds", () => {
    expect(validateComposition({ ...ok, seeds: { composition_seed: -1 } }).composition_seed).toMatch(/0 or more/i);
    expect(validateComposition({ ...ok, seeds: { archetype_seed: 1.5 } }).archetype_seed).toMatch(/whole/i);
    expect(validateComposition({ ...ok, seeds: { perturbation_seed: Number.NaN } }).perturbation_seed).toMatch(/whole/i);
    expect(validateComposition({ ...ok, maxRuns: 0 }).maxRuns).toMatch(/at least 1/i);
    expect(validateComposition({ ...ok, seeds: { composition_seed: MAX_SEED + 1 } }).composition_seed).toMatch(/at most/i);
    expect(validateComposition({ ...ok, seeds: { composition_seed: MAX_SEED, archetype_seed: 0, perturbation_seed: 7 } })).toEqual({});
    expect(validateComposition({ ...ok, seeds: {} })).toEqual({});
    expect(validateComposition({ ...ok, maxRuns: MAX_COUNT + 1 }).maxRuns).toMatch(/at most/i);
    expect(validateComposition({ ...ok, maxRuns: MAX_COUNT })).toEqual({});
  });
});

describe("validateComposition with the server's limits (GET /me) and convergence", () => {
  const limits: Limits = { maxTotalBuildings: 500, maxEnsembleRuns: 300, defaultMinRuns: 20, demoCaps: null };
  it("uses the server's caps for total buildings and max runs", () => {
    expect(validateComposition({ ...ok, totalBuildings: 501 }, limits).totalBuildings).toMatch(/at most 500/);
    expect(validateComposition({ ...ok, totalBuildings: 500, entries: [range("a", 0, 1)] }, limits)).toEqual({});
    expect(validateComposition({ ...ok, maxRuns: 301 }, limits).maxRuns).toMatch(/at most 300/);
    expect(validateComposition({ ...ok, maxRuns: 300 }, limits)).toEqual({});
  });
  it("with convergence on, max runs must be at least the server's default minimum", () => {
    expect(validateComposition({ ...ok, convergence: true, maxRuns: 19 }, limits).maxRuns).toMatch(/at least 20/);
    expect(validateComposition({ ...ok, convergence: true, maxRuns: 20 }, limits)).toEqual({});
    expect(validateComposition({ ...ok, convergence: false, maxRuns: 19 }, limits)).toEqual({});
  });
  it("mirrors the server's name rules: at most 120 characters, no control or text-direction characters", () => {
    expect(validateComposition({ ...ok, name: "x".repeat(121) }).name).toMatch(/120/);
    expect(validateComposition({ ...ok, name: "x".repeat(120) })).toEqual({});
    expect(validateComposition({ ...ok, name: "a\nb" }).name).toMatch(/control|line|direction/i);
    expect(validateComposition({ ...ok, name: `a${String.fromCharCode(0x202e)}b` }).name).toMatch(/control|line|direction/i);
  });
  it("counts the name in characters like the server (an emoji is one), and trims like the server (U+FEFF is not space)", () => {
    expect(validateComposition({ ...ok, name: "\u{1F600}".repeat(120) }).name).toBeUndefined();
    expect(validateComposition({ ...ok, name: "\u{1F600}".repeat(121) }).name).toMatch(/120/);
    expect(validateComposition({ ...ok, name: "\uFEFF" }).name).toBeUndefined();
    expect(validateComposition({ ...ok, name: "\u00A0\u3000" }).name).toMatch(/enter a name/i);
  });
  it("a type listed twice is refused (the server refuses it too)", () => {
    expect(validateComposition(withEntries([range("a", 0, 1), range("a", 0, 1)])).entries).toMatch(/more than once/i);
  });
});

describe("validateComposition reports every problem, each on its own field and naming the building type", () => {
  const labels: Record<string, string> = { a: "Single-family detached", b: "Office" };
  it("collects all field problems at once, not only the first", () => {
    const e = validateComposition(
      { ...ok, name: "", totalBuildings: 0, maxRuns: 0, entries: [fixed("a", 1.5), range("b", 2, 3)] },
      undefined,
      (id) => labels[id],
    );
    expect(e.name).toBeTruthy();
    expect(e.totalBuildings).toBeTruthy();
    expect(e.maxRuns).toBeTruthy();
    expect(e.byType?.a.count).toMatch(/^Single-family detached: /);
    expect(e.byType?.b.min).toMatch(/^Office: the smallest share/);
    expect(e.byType?.b.max).toMatch(/^Office: the largest share/);
    expect(problemList(e)).toHaveLength(6);
  });
  it("a valid composition has no problems", () => {
    expect(problemList(validateComposition(ok))).toEqual([]);
  });
});

describe("parseNumber (typed text -> number, strict)", () => {
  it("accepts plain decimals and negatives", () => {
    expect(parseNumber("42")).toBe(42);
    expect(parseNumber("0.25")).toBe(0.25);
    expect(parseNumber("-3")).toBe(-3);
  });
  it("rejects hex, exponent, whitespace, empty and junk", () => {
    for (const bad of ["0x10", "1e3", "1e-1", " 5", "5 ", "", "  ", "1,000", "0,25", "25%", "Infinity", "+5", "5.", ".", "-."]) {
      expect(parseNumber(bad), bad).toBeNaN();
    }
  });
  it("accepts a leading-dot decimal: '.25' is 0.25, '-.5' is -0.5", () => {
    expect(parseNumber(".25")).toBe(0.25);
    expect(parseNumber("-.5")).toBe(-0.5);
  });
});
