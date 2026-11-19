// W1-WEB2 independent QA: rules parity between the SERVER (the truth), the form's validateComposition and the
// mock client, for the same composition inputs.
//
// The "server" column was produced by running the API's own CompositionRequest schema + services.compositions
// .build_scenario + the run router's caps (5000 buildings, 1000 runs, default_min_runs 20) on these exact inputs
// (scratch script, QA review 2026-11-19). Keys are mapped to the mock catalogue (sf_detached->sf, mf_small->mf,
// mf_mid->office); the rules do not depend on the key.
//
// Every disagreement is a defect: the form blocking what the server accepts, or the mock accepting what the server
// refuses (the mock is the demo-data mode and the test double). Known disagreements are `it.fails`.
import { describe, expect, it } from "vitest";
import { createMockClient } from "../api/mockClient";
import type { Composition, CompositionEntry, Limits } from "../api/types";
import { parseNumber, validateComposition } from "../features/composition/validate";

const LIMITS: Limits = { maxTotalBuildings: 5000, maxEnsembleRuns: 1000, defaultMinRuns: 20, demoCaps: null };

const F = (typeId: string, count: number): CompositionEntry => ({ typeId, mode: "count", count });
const R = (typeId: string, minFraction: number, maxFraction: number): CompositionEntry => ({ typeId, mode: "fraction", minFraction, maxFraction });
const r1 = [R("sf", 0, 1)];

interface Case {
  id: string;
  c: Partial<Composition>;
  server: "accept" | "refuse";
}

const base = (c: Partial<Composition>): Composition => ({
  name: "n",
  totalBuildings: 10,
  entries: r1,
  seeds: {},
  maxRuns: 10,
  convergence: false,
  ...c,
});

const CASES: Case[] = [
  { id: "fixed_equal", c: { entries: [F("sf", 6), F("mf", 4)] }, server: "accept" },
  { id: "fixed_above", c: { entries: [F("sf", 6), F("mf", 5)] }, server: "refuse" },
  { id: "fixed_below", c: { entries: [F("sf", 6), F("mf", 3)] }, server: "refuse" },
  { id: "hybrid", c: { entries: [F("sf", 4), R("mf", 0, 1)] }, server: "accept" },
  { id: "hybrid_fixed_fills_total", c: { entries: [F("sf", 10), R("mf", 0, 1)] }, server: "accept" },
  { id: "hybrid_fixed_above", c: { entries: [F("sf", 11), R("mf", 0, 1)] }, server: "refuse" },
  { id: "min_sum_exactly_1", c: { entries: [R("sf", 0.5, 1), R("mf", 0.5, 1)] }, server: "accept" },
  { id: "min_sum_float_1", c: { entries: [R("sf", 0.1, 1), R("mf", 0.2, 1), R("office", 0.7, 1)] }, server: "accept" },
  { id: "min_sum_just_above", c: { entries: [R("sf", 0.5, 1), R("mf", 0.5000001, 1)] }, server: "refuse" },
  { id: "min_sum_just_below", c: { entries: [R("sf", 0.5, 1), R("mf", 0.4999999, 1)] }, server: "accept" },
  { id: "max_sum_below_1", c: { entries: [R("sf", 0, 0.4), R("mf", 0, 0.5)] }, server: "refuse" },
  { id: "max_sum_exactly_1", c: { entries: [R("sf", 0, 0.5), R("mf", 0, 0.5)] }, server: "accept" },
  { id: "fixed_zero", c: { entries: [F("sf", 0), R("mf", 0, 1)] }, server: "accept" },
  { id: "fixed_zero_only_fixed", c: { entries: [F("sf", 0), F("mf", 10)] }, server: "accept" },
  { id: "total_1", c: { totalBuildings: 1 }, server: "accept" },
  { id: "total_0", c: { totalBuildings: 0 }, server: "refuse" },
  { id: "total_at_cap", c: { totalBuildings: 5000 }, server: "accept" },
  { id: "total_above_cap", c: { totalBuildings: 5001 }, server: "refuse" },
  { id: "max_runs_at_cap", c: { maxRuns: 1000 }, server: "accept" },
  { id: "max_runs_above_cap", c: { maxRuns: 1001 }, server: "refuse" },
  { id: "max_runs_0", c: { maxRuns: 0 }, server: "refuse" },
  { id: "conv_below_min", c: { maxRuns: 19, convergence: true }, server: "refuse" },
  { id: "conv_equal_min", c: { maxRuns: 20, convergence: true }, server: "accept" },
  { id: "conv_above_min", c: { maxRuns: 21, convergence: true }, server: "accept" },
  { id: "conv_off_small", c: { maxRuns: 1 }, server: "accept" },
  { id: "min_gt_max", c: { entries: [R("sf", 0.6, 0.5), R("mf", 0, 1)] }, server: "refuse" },
  { id: "max_above_1", c: { entries: [R("sf", 0, 1.5)] }, server: "refuse" },
  { id: "min_negative", c: { entries: [R("sf", -0.1, 1)] }, server: "refuse" },
  { id: "fixed_fractional", c: { entries: [F("sf", 2.5), R("mf", 0, 1)] }, server: "refuse" },
  { id: "inactive_type", c: { entries: [R("civic", 0, 1)] }, server: "refuse" },
  { id: "name_blank", c: { name: "" }, server: "refuse" },
  { id: "name_spaces", c: { name: "   " }, server: "refuse" },
  { id: "name_nbsp", c: { name: " " }, server: "refuse" },
  { id: "name_120", c: { name: "a".repeat(120) }, server: "accept" },
  { id: "name_121", c: { name: "a".repeat(121) }, server: "refuse" },
  { id: "name_control", c: { name: "a\x07b" }, server: "refuse" },
  { id: "name_tab", c: { name: "a\tb" }, server: "refuse" },
  { id: "name_rlo", c: { name: "a‮b" }, server: "refuse" },
  { id: "name_lri", c: { name: "a⁦b" }, server: "refuse" },
  { id: "name_lsep", c: { name: "a b" }, server: "refuse" },
  { id: "name_bom_only", c: { name: "﻿" }, server: "accept" },
  { id: "name_zwsp_only", c: { name: "​" }, server: "accept" },
  { id: "name_rlm", c: { name: "a‏b" }, server: "accept" },
  { id: "name_alm", c: { name: "a؜b" }, server: "accept" },
  { id: "name_emoji_61", c: { name: "\u{1F600}".repeat(61) }, server: "accept" },
  { id: "name_emoji_120", c: { name: "\u{1F600}".repeat(120) }, server: "accept" },
];

// Known disagreements (defects). Form: Python counts code points and does not strip U+FEFF; JS .length counts UTF-16
// units and .trim() strips U+FEFF.
// Fixed in the W1-WEB2 fix pass (D3, D4): none left.
const FORM_DIFFERS = new Set<string>([]);
// Mock: checks only the count/fraction sums, types and caps; none of the schema rules (name, total >= 1, per-entry
// range, whole numbers, max_runs >= 1).
// Fixed in the W1-WEB2 fix pass (D1): none left.
const MOCK_DIFFERS = new Set<string>([]);

const formVerdict = (c: Composition) => (Object.keys(validateComposition(c, LIMITS)).length === 0 ? "accept" : "refuse");

async function mockVerdict(c: Composition): Promise<"accept" | "refuse"> {
  const client = createMockClient();
  await client.login("client");
  try {
    await client.startRun({ kind: "custom", composition: c });
    return "accept";
  } catch {
    return "refuse";
  }
}

describe("QA W1-WEB2 rules parity: form validation vs the server", () => {
  // The form never offers an inactive type (the page filters them out; covered by a UI test), so validate() is not
  // asked about one.
  for (const t of CASES.filter((x) => x.id !== "inactive_type")) {
    const run = FORM_DIFFERS.has(t.id) ? it.fails : it;
    run(`form ${t.server}s ${t.id} like the server`, () => {
      expect(formVerdict(base(t.c))).toBe(t.server);
    });
  }
});

describe("QA W1-WEB2 rules parity: mock client vs the server", () => {
  for (const t of CASES) {
    const run = MOCK_DIFFERS.has(t.id) ? it.fails : it;
    run(`mock ${t.server}s ${t.id} like the server`, async () => {
      expect(await mockVerdict(base(t.c))).toBe(t.server);
    });
  }
});

describe("QA W1-WEB2 typed fractions (the form parses text; the server receives numbers)", () => {
  it("reads plain decimals", () => {
    expect(parseNumber("0.25")).toBe(0.25);
    expect(parseNumber("-0.1")).toBe(-0.1);
  });
  it("refuses, never misreads, ambiguous text: '25%', '0,25', empty", () => {
    for (const s of ["25%", "0,25", "", " 0.25"]) expect(Number.isNaN(parseNumber(s))).toBe(true);
  });
  // A person typing ".25" or "1e-1" means 0.25 / 0.1, which the server accepts; the form says "needs a number".
  // Usability defect (LOW): either accept them or say what format is expected.
  it("accepts '.25' (leading-dot decimal) as 0.25", () => {
    expect(parseNumber(".25")).toBe(0.25);
  });
});
