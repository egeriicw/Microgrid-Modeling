import { describe, expect, it } from "vitest";
import { MAX_RUNS_LIMIT, MAX_SEED, parseNumber, validateComposition } from "./validate";
import type { Composition, CompositionEntry } from "../../api/types";

const range = (typeId: string, minFraction: number, maxFraction: number): CompositionEntry => ({ typeId, mode: "fraction", minFraction, maxFraction });
const fixed = (typeId: string, count: number): CompositionEntry => ({ typeId, mode: "count", count });

const ok: Composition = {
  name: "demo",
  totalBuildings: 100,
  seed: 1,
  maxRuns: 200,
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
  it("requires 0 <= min <= max <= 1 for each fraction range", () => {
    expect(validateComposition(withEntries([range("a", -0.1, 1)])).entries).toMatch(/between 0 and 1/i);
    expect(validateComposition(withEntries([range("a", 0, 1.5)])).entries).toMatch(/between 0 and 1/i);
    expect(validateComposition(withEntries([range("a", 0.6, 0.4)])).entries).toMatch(/minimum.*maximum|at most/i);
    expect(validateComposition(withEntries([range("a", Number.NaN, 1)])).entries).toMatch(/number/i);
  });
  it("rejects ranges that cannot sum to 1 over the remainder", () => {
    expect(validateComposition(withEntries([range("a", 0.7, 1), range("b", 0.5, 1)])).entries).toMatch(/minimum.*more than 1/i);
    expect(validateComposition(withEntries([range("a", 0, 0.3), range("b", 0, 0.3)])).entries).toMatch(/maximum.*less than 1/i);
  });
  it("accepts ranges whose min sum is <= 1 <= max sum, including exactly 1", () => {
    expect(validateComposition(withEntries([range("a", 0.5, 0.5), range("b", 0.5, 0.5)]))).toEqual({});
    expect(validateComposition(withEntries([range("a", 0, 1)]))).toEqual({});
  });
  it("requires fixed counts to be whole, non-negative and not above the total", () => {
    expect(validateComposition(withEntries([fixed("a", 101), range("b", 0, 1)])).entries).toMatch(/exceed/i);
    expect(validateComposition(withEntries([fixed("a", 1.5), range("b", 0, 1)])).entries).toMatch(/whole/i);
    expect(validateComposition(withEntries([fixed("a", -1), range("b", 0, 1)])).entries).toMatch(/whole/i);
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
  it("when fixed counts use up the total, fraction ranges are ignored (those types get 0), not rejected", () => {
    expect(validateComposition(withEntries([fixed("a", 100), range("b", 0.9, 1), range("c", 0.9, 1)]))).toEqual({});
  });
  it("validates seed and max runs, with upper bounds", () => {
    expect(validateComposition({ ...ok, seed: -1 }).seed).toMatch(/0 or more/i);
    expect(validateComposition({ ...ok, maxRuns: 0 }).maxRuns).toMatch(/at least 1/i);
    expect(validateComposition({ ...ok, seed: MAX_SEED + 1 }).seed).toMatch(/at most/i);
    expect(validateComposition({ ...ok, seed: MAX_SEED })).toEqual({});
    expect(validateComposition({ ...ok, maxRuns: MAX_RUNS_LIMIT + 1 }).maxRuns).toMatch(/at most/i);
    expect(validateComposition({ ...ok, maxRuns: MAX_RUNS_LIMIT })).toEqual({});
  });
});

describe("parseNumber (typed text -> number, strict)", () => {
  it("accepts plain decimals and negatives", () => {
    expect(parseNumber("42")).toBe(42);
    expect(parseNumber("0.25")).toBe(0.25);
    expect(parseNumber("-3")).toBe(-3);
  });
  it("rejects hex, exponent, whitespace, empty and junk", () => {
    for (const bad of ["0x10", "1e3", " 5", "5 ", "", "  ", "1,000", "Infinity", "+5", ".5", "5."]) {
      expect(parseNumber(bad), bad).toBeNaN();
    }
  });
});
