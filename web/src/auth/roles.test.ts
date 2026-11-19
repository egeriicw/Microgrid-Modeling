import { describe, expect, it } from "vitest";
import type { Session } from "../api/types";
import { capabilitiesOf } from "./roles";

const session = (over: Partial<Session> = {}): Session => ({
  subject: "s",
  role: "client",
  displayName: "s",
  permissions: { "filters.use": false },
  runInputs: ["composition", "composition_id", "config"],
  maySetSeeds: true,
  limits: { maxTotalBuildings: 5000, maxEnsembleRuns: 1000, defaultMinRuns: 20, demoCaps: null },
  ...over,
});

describe("capabilities come only from the server's session (GET /me), never from the role name", () => {
  it("nothing without a session", () => {
    expect(capabilitiesOf(null)).toEqual({ composeCustom: false, runPredefined: false, replay: false, setSeeds: false, useFilters: false });
  });

  it("a client with every run input", () => {
    expect(capabilitiesOf(session())).toEqual({ composeCustom: true, runPredefined: true, replay: true, setSeeds: true, useFilters: false });
  });

  it("demo: predefined only, no seeds, no replay", () => {
    expect(capabilitiesOf(session({ role: "demo", runInputs: ["composition_id"], maySetSeeds: false }))).toEqual({
      composeCustom: false,
      runPredefined: true,
      replay: false,
      setSeeds: false,
      useFilters: false,
    });
  });

  it("follows the flags, not the role name (an 'admin' without filters.use gets no filters)", () => {
    expect(capabilitiesOf(session({ role: "admin" })).useFilters).toBe(false);
    expect(capabilitiesOf(session({ role: "client", permissions: { "filters.use": true } })).useFilters).toBe(true);
  });

  it("replay needs both the config input and permission to set seeds", () => {
    expect(capabilitiesOf(session({ maySetSeeds: false })).replay).toBe(false);
    expect(capabilitiesOf(session({ runInputs: ["composition"] })).replay).toBe(false);
  });
});
