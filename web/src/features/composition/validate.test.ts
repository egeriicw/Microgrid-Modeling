import { describe, expect, it } from "vitest";
import { validateComposition } from "./validate";
import type { Composition } from "../../api/types";

const ok: Composition = {
  name: "demo",
  totalBuildings: 100,
  seed: 1,
  maxRuns: 200,
  entries: [
    { typeId: "sf", mode: "fraction", value: 0.6 },
    { typeId: "office", mode: "fraction", value: 0.4 },
  ],
};

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
  it("requires fractions within 0..1 and summing to 1 when all entries are fractions", () => {
    const bad = { ...ok, entries: [{ typeId: "sf", mode: "fraction" as const, value: 1.5 }] };
    expect(validateComposition(bad).entries).toMatch(/between 0 and 1/i);
    const sum = { ...ok, entries: [{ typeId: "sf", mode: "fraction" as const, value: 0.5 }] };
    expect(validateComposition(sum).entries).toMatch(/sum to 1/i);
  });
  it("requires fixed counts to be whole and not exceed the total", () => {
    const big = { ...ok, entries: [{ typeId: "sf", mode: "count" as const, value: 101 }] };
    expect(validateComposition(big).entries).toMatch(/exceed/i);
    const frac = { ...ok, entries: [{ typeId: "sf", mode: "count" as const, value: 1.5 }] };
    expect(validateComposition(frac).entries).toMatch(/whole/i);
  });
  it("requires at least one entry", () => {
    expect(validateComposition({ ...ok, entries: [] }).entries).toMatch(/at least one/i);
  });
  it("validates seed and max runs", () => {
    expect(validateComposition({ ...ok, seed: -1 }).seed).toMatch(/0 or more/i);
    expect(validateComposition({ ...ok, maxRuns: 0 }).maxRuns).toMatch(/at least 1/i);
  });
});
