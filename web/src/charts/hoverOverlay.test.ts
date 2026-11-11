import { describe, expect, it, vi } from "vitest";
import { generateDataset } from "../api/fixtures";
import { attachHoverOverlay } from "./hoverOverlay";

const series = generateDataset({ points: 96 * 3, seed: 1 });

function fakeChart(inside: boolean, ts: number) {
  const handlers: Record<string, (e: { offsetX: number; offsetY: number }) => void> = {};
  const chart = {
    getZr: () => ({
      on: (name: string, h: (e: { offsetX: number; offsetY: number }) => void) => (handlers[name] = h),
      off: vi.fn(),
    }),
    containPixel: () => inside,
    convertFromPixel: () => ts,
  };
  return { chart, handlers };
}

describe("attachHoverOverlay (DOM tooltip, no chart repaint)", () => {
  it("shows load (kW) and temperature (deg C) at the hovered timestamp, with a live-region role", () => {
    const host = document.createElement("div");
    const { chart, handlers } = fakeChart(true, series.ts[50]);
    const detach = attachHoverOverlay(chart as never, host, () => series, ["total", "residential"]);
    handlers.mousemove({ offsetX: 120, offsetY: 40 });
    const tip = host.querySelector("[role=status]") as HTMLElement;
    expect(tip.style.display).toBe("block");
    expect(tip.textContent).toContain("Total");
    expect(tip.textContent).toContain(series.total[50].toFixed(1));
    expect(tip.textContent).toContain("kW");
    expect(tip.textContent).toContain(series.temperature[50].toFixed(1));
    expect(tip.textContent).toContain("°C");
    detach();
    expect(host.querySelector("[role=status]")).toBeNull();
  });

  it("hides outside the plot area and on mouse out", () => {
    const host = document.createElement("div");
    const { chart, handlers } = fakeChart(false, series.ts[0]);
    attachHoverOverlay(chart as never, host, () => series, ["total"]);
    handlers.mousemove({ offsetX: 1, offsetY: 1 });
    expect((host.querySelector("[role=status]") as HTMLElement).style.display).toBe("none");
  });

  it("tap-to-inspect: a click pins the tooltip through globalout; tapping outside the plot unpins it", () => {
    const host = document.createElement("div");
    let inside = true;
    const handlers: Record<string, (e: { offsetX: number; offsetY: number }) => void> = {};
    const chart = {
      getZr: () => ({ on: (n: string, h: never) => (handlers[n] = h), off: vi.fn() }),
      containPixel: () => inside,
      convertFromPixel: () => series.ts[10],
    };
    attachHoverOverlay(chart as never, host, () => series, ["total"]);
    const tip = host.querySelector("[role=status]") as HTMLElement;
    handlers.click({ offsetX: 50, offsetY: 50 });
    expect(tip.style.display).toBe("block");
    handlers.globalout({ offsetX: 0, offsetY: 0 }); // finger lifted
    expect(tip.style.display).toBe("block");
    inside = false;
    handlers.click({ offsetX: 1, offsetY: 1 });
    expect(tip.style.display).toBe("none");
  });
});
