// "Overlay" tooltip from the spike: a crosshair and tooltip drawn as plain DOM so a hover never repaints the canvas
// (about 8 ms vs 100-3000 ms for the built-in tooltip on 35,040 points). Built with textContent, never innerHTML.
import { formatTooltip, type LoadKey } from "../features/results/chartOptions";
import type { LoadSeries } from "../api/types";
import type { ChartLike } from "./EChart";

export function attachHoverOverlay(
  chart: ChartLike,
  host: HTMLElement,
  getData: () => LoadSeries,
  keys: LoadKey[],
  gridTop = 72,
): () => void {
  const line = document.createElement("div");
  line.setAttribute("aria-hidden", "true");
  line.style.cssText = `position:absolute;top:${gridTop}px;bottom:80px;width:1px;background:#555;pointer-events:none;display:none`;
  const tip = document.createElement("div");
  tip.setAttribute("role", "status");
  tip.style.cssText =
    "position:absolute;pointer-events:none;display:none;background:#fff;color:#000;border:1px solid #888;padding:4px 8px;font-size:12px;white-space:nowrap;z-index:2";
  host.append(line, tip);

  // Tap-to-inspect: a click/tap pins the tooltip (touch has no hover and fires globalout when the finger lifts).
  // Tapping outside the plot area unpins it. Mouse users still get the live hover.
  let pinned = false;
  const hide = () => {
    line.style.display = "none";
    tip.style.display = "none";
  };
  const out = () => {
    if (!pinned) hide();
  };
  const move = (e: { offsetX: number; offsetY: number }, force = false) => {
    if (pinned && !force) return;
    if (!chart.containPixel("grid", [e.offsetX, e.offsetY])) return hide();
    const t = chart.convertFromPixel({ xAxisIndex: 0 }, e.offsetX) as unknown as number;
    const c = formatTooltip(t, getData(), keys);
    const rows = [c.time, ...c.loads.map((l) => `${l.name}: ${l.kw.toFixed(1)} kW`), `Temperature: ${c.temperatureC.toFixed(1)} °C`];
    tip.replaceChildren(
      ...rows.map((r) => {
        const div = document.createElement("div");
        div.textContent = r;
        return div;
      }),
    );
    line.style.left = `${e.offsetX}px`;
    tip.style.left = `${e.offsetX + 12}px`;
    tip.style.top = `${e.offsetY + 12}px`;
    line.style.display = "block";
    tip.style.display = "block";
  };

  const tap = (e: { offsetX: number; offsetY: number }) => {
    pinned = chart.containPixel("grid", [e.offsetX, e.offsetY]);
    move(e, true);
  };

  const zr = chart.getZr();
  zr.on("mousemove", move);
  zr.on("click", tap);
  zr.on("globalout", out);
  return () => {
    zr.off("mousemove", move);
    zr.off("click", tap);
    zr.off("globalout", out);
    line.remove();
    tip.remove();
  };
}
