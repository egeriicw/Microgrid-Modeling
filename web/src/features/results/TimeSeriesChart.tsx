import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { Button, ButtonGroup, Checkbox, FormControlLabel, ToggleButton, ToggleButtonGroup, Typography } from "@mui/material";
import type { LoadSeries } from "../../api/types";
import { EChart, type ChartLike } from "../../charts/EChart";
import { attachHoverOverlay } from "../../charts/hoverOverlay";
import { buildTimeSeriesOption, viewAtResolution, type LoadKey, type Resolution } from "./chartOptions";
import { ChartCard } from "./ChartCard";
import { dailyTable } from "./tables";
import { zoomWindow, type ZoomAction, type ZoomWindow } from "./zoom";

const KEYS: LoadKey[] = ["total", "residential", "commercial"];
const KEY_ACTIONS: Record<string, ZoomAction> = { "+": "in", "=": "in", "-": "out", ArrowLeft: "left", ArrowRight: "right", "0": "reset", Home: "reset" };

export function TimeSeriesChart({ series }: { series: LoadSeries }) {
  const [resolution, setResolution] = useState<Resolution>("15min");
  const [showBand, setShowBand] = useState(false);
  const [showTemperature, setShowTemperature] = useState(true);
  const [chart, setChart] = useState<ChartLike | null>(null);
  const host = useRef<HTMLDivElement>(null);

  const option = useMemo(
    () => buildTimeSeriesOption({ series, resolution, showBand, showTemperature }),
    [series, resolution, showBand, showTemperature],
  );

  // Hover: DOM overlay showing load (kW) and temperature (deg C) at the same timestamp.
  useEffect(() => {
    if (!chart || !host.current) return;
    return attachHoverOverlay(chart, host.current, () => viewAtResolution(series, resolution), KEYS);
  }, [chart, series, resolution]);

  const zoom = useCallback(
    (action: ZoomAction) => {
      if (!chart) return;
      // The chart is the source of truth: mouse wheel, slider and these controls all move the same window.
      const dz = chart.getOption().dataZoom?.[0];
      const current: ZoomWindow = [dz?.start ?? 0, dz?.end ?? 100];
      const [start, end] = zoomWindow(current, action);
      chart.dispatchAction({ type: "dataZoom", start, end });
    },
    [chart],
  );

  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    const action = KEY_ACTIONS[e.key];
    if (!action || e.target !== e.currentTarget) return;
    e.preventDefault();
    zoom(action);
  };

  const controls = (
    <>
      <ToggleButtonGroup exclusive size="small" value={resolution} aria-label="Resolution" onChange={(_, v: Resolution | null) => v && setResolution(v)}>
        <ToggleButton value="15min">15-minute</ToggleButton>
        <ToggleButton value="1h">Hourly</ToggleButton>
      </ToggleButtonGroup>
      <FormControlLabel control={<Checkbox checked={showBand} onChange={(e) => setShowBand(e.target.checked)} />} label="P10-P90 band" />
      <FormControlLabel control={<Checkbox checked={showTemperature} onChange={(e) => setShowTemperature(e.target.checked)} />} label="Temperature overlay" />
      <ButtonGroup size="small" aria-label="Zoom and pan">
        <Button onClick={() => zoom("in")}>Zoom in</Button>
        <Button onClick={() => zoom("out")}>Zoom out</Button>
        <Button onClick={() => zoom("left")}>Pan left</Button>
        <Button onClick={() => zoom("right")}>Pan right</Button>
        <Button onClick={() => zoom("reset")}>Reset zoom</Button>
      </ButtonGroup>
    </>
  );

  return (
    <ChartCard title="Load over the year" subtitle="Weather year AMY2018" controls={controls} table={() => dailyTable(series)}>
      <div
        ref={host}
        role="group"
        aria-label="Time series chart"
        aria-describedby="ts-keys"
        tabIndex={0}
        onKeyDown={onKeyDown}
        style={{ position: "relative" }}
      >
        <EChart option={option} ariaLabel="Load over the year, total, residential and commercial, with temperature, weather year AMY2018" height={440} onReady={setChart} />
      </div>
      <Typography id="ts-keys" variant="caption" color="text.secondary">
        Keyboard: focus the chart, then + and - to zoom, left and right arrows to pan, 0 to reset. Hover shows load and temperature together; the data table below gives the same numbers.
      </Typography>
    </ChartCard>
  );
}
