import { describe, expect, it } from "vitest";
import { zoomWindow } from "./zoom";

describe("zoomWindow (percent window on the time axis)", () => {
  it("zooms in to half the width around the centre", () => {
    expect(zoomWindow([0, 100], "in")).toEqual([25, 75]);
  });
  it("zooms out to double the width, clamped to 0..100", () => {
    expect(zoomWindow([25, 75], "out")).toEqual([0, 100]);
    expect(zoomWindow([40, 50], "out")).toEqual([35, 55]);
  });
  it("pans by a quarter of the width and stays inside the range", () => {
    expect(zoomWindow([20, 40], "right")).toEqual([25, 45]);
    expect(zoomWindow([0, 20], "left")).toEqual([0, 20]);
    expect(zoomWindow([90, 100], "right")).toEqual([90, 100]);
  });
  it("reset returns the full range", () => {
    expect(zoomWindow([10, 20], "reset")).toEqual([0, 100]);
  });
  it("never zooms in below a minimum width", () => {
    const [a, b] = zoomWindow([50, 50.05], "in");
    expect(b - a).toBeCloseTo(0.05, 9);
  });
});
