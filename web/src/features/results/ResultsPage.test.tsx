import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createMockClient } from "../../api/mockClient";
import type { ApiClient } from "../../api/types";
import { renderApp } from "../../test/utils";

// ECharts needs a real canvas; jsdom has none. The fake records the options it was given and hands back a
// fake chart so the zoom controls can be checked.
const zoomState = { start: 0, end: 100 };
const dispatchAction = vi.fn((p: { start: number; end: number }) => {
  zoomState.start = p.start;
  zoomState.end = p.end;
});
const fakeChart = {
  dispatchAction,
  on: vi.fn(),
  off: vi.fn(),
  getOption: () => ({ dataZoom: [{ ...zoomState }] }),
  getZr: () => ({ on: vi.fn(), off: vi.fn() }),
  containPixel: () => false,
  convertFromPixel: () => 0,
};
vi.mock("../../charts/EChart", async () => {
  const { useEffect } = await import("react");
  return {
    EChart: (props: { option: { series?: unknown[] }; ariaLabel: string; onReady?: (c: unknown) => void }) => {
      const { onReady } = props;
      useEffect(() => onReady?.(fakeChart), [onReady]);
      return <div role="img" aria-label={props.ariaLabel} data-series={props.option.series?.length ?? 0} />;
    },
  };
});

beforeEach(() => {
  dispatchAction.mockClear();
  Object.assign(zoomState, { start: 0, end: 100 });
});

describe("results screen", () => {
  async function open(role: "admin" | "client" | "demo" = "client") {
    await renderApp({ route: "/results/demo-run", role });
    await screen.findByText(/methodology demonstration/i); // results have loaded
  }

  it("shows the accuracy caveat and the AMY2018 weather-year label (FR-WEB-8)", async () => {
    await open();
    expect(
      screen.getByText(/methodology demonstration with benchmark checks.*not validated for engineering or investment decisions/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/weather year: AMY2018/i)).toBeInTheDocument();
    expect(screen.getByText(/actual 2018 weather, not a typical year/i)).toBeInTheDocument();
    expect(screen.getByText(/calibration status/i)).toBeInTheDocument();
  });

  it("shows a progress indicator while results load", async () => {
    await renderApp({ route: "/results/demo-run", role: "client" });
    expect(screen.getByRole("progressbar", { name: /loading results/i })).toBeInTheDocument();
    await screen.findByText(/methodology demonstration/i);
    expect(screen.queryByRole("progressbar", { name: /loading results/i })).not.toBeInTheDocument();
  });

  it("renders time series, load-duration, seasonal profile and residential/commercial split", async () => {
    await open();
    expect(screen.getByRole("heading", { name: /load over the year/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /load-duration curve/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /seasonal daily profile/i })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /residential and commercial split/i })).toBeInTheDocument();
    expect(screen.getAllByRole("img")).toHaveLength(4);
  });

  it("toggles hourly / 15-minute resolution", async () => {
    const user = userEvent.setup();
    await open();
    const hourly = screen.getByRole("button", { name: /hourly/i });
    const quarter = screen.getByRole("button", { name: /15-minute/i });
    expect(hourly).toHaveAttribute("aria-pressed", "true"); // hourly by default: 15-minute lines are unreadable at full-year zoom
    await user.click(quarter);
    expect(quarter).toHaveAttribute("aria-pressed", "true");
    expect(hourly).toHaveAttribute("aria-pressed", "false");
  });

  it("adds the P10-P90 band and removes the temperature overlay via switches", async () => {
    const user = userEvent.setup();
    await open();
    const chart = () => screen.getByRole("img", { name: /load over the year/i });
    const base = Number(chart().getAttribute("data-series"));
    await user.click(screen.getByRole("checkbox", { name: /p10.*p90 band/i }));
    expect(Number(chart().getAttribute("data-series"))).toBeGreaterThan(base);
    const withBand = Number(chart().getAttribute("data-series"));
    await user.click(screen.getByRole("checkbox", { name: /temperature overlay/i }));
    expect(Number(chart().getAttribute("data-series"))).toBe(withBand - 1);
  });

  it("has keyboard-operable zoom controls that drive the chart", async () => {
    const user = userEvent.setup();
    await open();
    await user.click(screen.getByRole("button", { name: /zoom in/i }));
    expect(dispatchAction).toHaveBeenLastCalledWith({ type: "dataZoom", start: 25, end: 75 });
    await user.click(screen.getByRole("button", { name: /pan right/i }));
    expect(dispatchAction).toHaveBeenLastCalledWith({ type: "dataZoom", start: 37.5, end: 87.5 });
    await user.click(screen.getByRole("button", { name: /reset zoom/i }));
    expect(dispatchAction).toHaveBeenLastCalledWith({ type: "dataZoom", start: 0, end: 100 });
  });

  it("supports keyboard shortcuts on the focusable chart region", async () => {
    const user = userEvent.setup();
    await open();
    const region = screen.getByRole("group", { name: /time series chart/i });
    expect(region).toHaveAttribute("tabindex", "0");
    region.focus();
    await user.keyboard("+");
    expect(dispatchAction).toHaveBeenLastCalledWith({ type: "dataZoom", start: 25, end: 75 });
  });

  it("says so when the API reports no weather year, calibration status or temperature (real API today)", async () => {
    const base = createMockClient({ pointsPerYear: 96 * 20 });
    const client: ApiClient = {
      ...base,
      getResults: async (id) => {
        const r = await base.getResults(id);
        return { ...r, weatherYear: null, calibrationStatus: null, series: { ...r.series, temperature: null } };
      },
    };
    await renderApp({ route: "/results/demo-run", role: "client", client });
    await screen.findByText(/methodology demonstration/i); // the caveat is still shown
    expect(screen.getByText(/weather year: not reported/i)).toBeInTheDocument();
    expect(screen.getByText(/calibration status: not reported/i)).toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /temperature overlay/i })).not.toBeInTheDocument();
    expect(screen.getByText(/these results do not include temperature/i)).toBeInTheDocument();
    expect(screen.queryByText(/AMY2018/)).not.toBeInTheDocument();
  });

  it("gives the server's reason when temperature is not available, as text", async () => {
    const base = createMockClient({ pointsPerYear: 96 * 20 });
    const reason = "no temperature source declared <b>(config)</b>";
    const client: ApiClient = {
      ...base,
      getResults: async (id) => {
        const r = await base.getResults(id);
        return { ...r, temperatureNote: reason, series: { ...r.series, temperature: null } };
      },
    };
    const { container } = await renderApp({ route: "/results/demo-run", role: "client", client });
    expect(await screen.findByText(/these results do not include temperature/i)).toHaveTextContent(reason);
    expect(container.querySelector("main b")).toBeNull();
  });

  it("shows the run's data files on the results screen when the server reports them", async () => {
    const base = createMockClient({ runsPerTick: 50, tickMs: 1, pointsPerYear: 96 * 20 });
    await base.login("client");
    const { runId } = await base.startRun({
      kind: "custom",
      composition: { name: "t", totalBuildings: 5, seeds: {}, maxRuns: 50, convergence: false, entries: [{ typeId: "sf", mode: "fraction", minFraction: 0, maxFraction: 1 }] },
    });
    await base.watchRun(runId, () => undefined, new AbortController().signal);
    await renderApp({ route: `/results/${runId}`, role: "client", client: base });
    const panel = await screen.findByRole("region", { name: /data used/i });
    expect(panel).toHaveTextContent(/DC_upgrade0\.xlsx/);
  });

  it("labels time as the data's local clock time, not as UTC (the results document has no time zone)", async () => {
    const user = userEvent.setup();
    await open();
    const section = screen.getByRole("region", { name: /load over the year/i });
    await user.click(within(section).getByRole("checkbox", { name: /show data table/i }));
    expect(within(section).getByRole("columnheader", { name: /date \(local time\)/i })).toBeInTheDocument();
    expect(screen.queryByText(/\(UTC\)/)).toBeNull();
  });

  it("offers a data-table alternative for each chart", async () => {
    const user = userEvent.setup();
    await open();
    const section = screen.getByRole("region", { name: /residential and commercial split/i });
    await user.click(within(section).getByRole("checkbox", { name: /show data table/i }));
    const table = within(section).getByRole("table");
    expect(within(table).getByText("Residential")).toBeInTheDocument();
    expect(within(table).getByText("Commercial")).toBeInTheDocument();
    expect(table).toHaveAccessibleName(/AMY2018/);
  });
});
