import { describe, expect, it } from "vitest";
import { MAX_COUNT, MAX_SEED, parseNumber, validateComposition } from "./validate";
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
  it("checks the ranges even when fixed counts use up the total (the API does: sum of minimums must still be <= 1)", () => {
    expect(validateComposition(withEntries([fixed("a", 100), range("b", 0.9, 1), range("c", 0.9, 1)])).entries).toMatch(/minimum.*more than 1/i);
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
  it("a type listed twice is refused (the server refuses it too)", () => {
    expect(validateComposition(withEntries([range("a", 0, 1), range("a", 0, 1)])).entries).toMatch(/more than once/i);
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
