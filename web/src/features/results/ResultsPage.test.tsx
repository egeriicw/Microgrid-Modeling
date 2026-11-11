import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
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
    expect(quarter).toHaveAttribute("aria-pressed", "true");
    await user.click(hourly);
    expect(hourly).toHaveAttribute("aria-pressed", "true");
    expect(quarter).toHaveAttribute("aria-pressed", "false");
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
