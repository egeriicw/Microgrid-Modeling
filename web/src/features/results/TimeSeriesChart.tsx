import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import { Box, Button, ButtonGroup, Checkbox, FormControlLabel, FormHelperText, ToggleButton, ToggleButtonGroup, Typography, useMediaQuery } from "@mui/material";
import type { LoadSeries } from "../../api/types";
import { EChart, type ChartLike } from "../../charts/EChart";
import { attachHoverOverlay } from "../../charts/hoverOverlay";
import { BAND_NAME, GRID_TOP, buildTimeSeriesOption, viewAtResolution, type LoadKey, type Resolution } from "./chartOptions";
import { ChartCard } from "./ChartCard";
import { dailyTable } from "./tables";
import { zoomWindow, type ZoomAction, type ZoomWindow } from "./zoom";

export const BAND_HELP =
  "The shaded band covers the middle 80% of the sampled neighbourhoods: 1 in 10 runs fall below it and 1 in 10 above it. The white dashed line inside it is P50, the median run.";
// 44 px touch targets on phones (WCAG 2.5.5); desktop keeps MUI's compact sizes.
const TOUCH = { minHeight: { xs: 44, sm: "auto" } } as const;
const TOUCH_CHECK = { p: { xs: "10px", sm: "9px" } } as const;
const KEY_ACTIONS: Record<string, ZoomAction> = { "+": "in", "=": "in", "-": "out", ArrowLeft: "left", ArrowRight: "right", "0": "reset", Home: "reset" };

export function TimeSeriesChart({ series, weatherYear, temperatureNote = null }: { series: LoadSeries; weatherYear: string; temperatureNote?: string | null }) {
  // Hourly by default: a full year of 15-minute lines is a solid block at this width. Switch to 15-minute and zoom in.
  const [resolution, setResolution] = useState<Resolution>("1h");
  const compact = useMediaQuery("(max-width:600px)");
  // Default view: Total with its uncertainty band; the parts and temperature are opt-in.
  const [showBand, setShowBand] = useState(true);
  const [showResidential, setShowResidential] = useState(false);
  const [showCommercial, setShowCommercial] = useState(false);
  const [showTemperature, setShowTemperature] = useState(false);
  const [chart, setChart] = useState<ChartLike | null>(null);
  const host = useRef<HTMLDivElement>(null);
  const hasTemp = series.temperature !== null;

  const loads = useMemo<LoadKey[]>(
    () => ["total", ...(showResidential ? (["residential"] as const) : []), ...(showCommercial ? (["commercial"] as const) : [])],
    [showResidential, showCommercial],
  );
  const option = useMemo(
    () => buildTimeSeriesOption({ series, resolution, loads, showBand, showTemperature, compact }),
    [series, resolution, loads, showBand, showTemperature, compact],
  );

  // Hover: DOM overlay showing load (kW) and temperature (deg C) at the same timestamp.
  useEffect(() => {
    if (!chart || !host.current) return;
    return attachHoverOverlay(chart, host.current, () => viewAtResolution(series, resolution), loads, compact ? GRID_TOP.compact : GRID_TOP.wide);
  }, [chart, series, resolution, compact, loads]);

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
        <ToggleButton value="1h" sx={TOUCH}>Hourly</ToggleButton>
        <ToggleButton value="15min" sx={TOUCH}>15-minute</ToggleButton>
      </ToggleButtonGroup>
      <Box sx={{ width: "100%" }}>
        <FormControlLabel
          control={<Checkbox checked={showBand} onChange={(e) => setShowBand(e.target.checked)} sx={TOUCH_CHECK} slotProps={{ input: { "aria-describedby": "band-help" } }} />}
          label={BAND_NAME}
        />
        <FormHelperText id="band-help" sx={{ mt: 0, mb: 1 }}>{BAND_HELP}</FormHelperText>
      </Box>
      <Typography variant="body2" component="span" sx={{ fontWeight: 600 }}>Also show:</Typography>
      <FormControlLabel control={<Checkbox checked={showResidential} onChange={(e) => setShowResidential(e.target.checked)} sx={TOUCH_CHECK} />} label="Residential" />
      <FormControlLabel control={<Checkbox checked={showCommercial} onChange={(e) => setShowCommercial(e.target.checked)} sx={TOUCH_CHECK} />} label="Commercial" />
      {hasTemp ? (
        <FormControlLabel control={<Checkbox checked={showTemperature} onChange={(e) => setShowTemperature(e.target.checked)} sx={TOUCH_CHECK} />} label="Temperature" />
      ) : (
        <Typography variant="body2">
          {temperatureNote ? `These results do not include temperature (the server says: ${temperatureNote}).` : "These results do not include temperature."}
        </Typography>
      )}
      <ButtonGroup size="small" aria-label="Zoom and pan" sx={{ flexWrap: "wrap" }}>
        <Button sx={TOUCH} onClick={() => zoom("in")}>Zoom in</Button>
        <Button sx={TOUCH} onClick={() => zoom("out")}>Zoom out</Button>
        <Button sx={TOUCH} onClick={() => zoom("left")}>Pan left</Button>
        <Button sx={TOUCH} onClick={() => zoom("right")}>Pan right</Button>
        <Button sx={TOUCH} onClick={() => zoom("reset")}>Reset zoom</Button>
      </ButtonGroup>
    </>
  );

  return (
    <ChartCard title="Load over the year" subtitle={`Weather year ${weatherYear}`} controls={controls} table={() => dailyTable(series, weatherYear)}>
      <div
        ref={host}
        role="group"
        aria-label="Time series chart"
        aria-describedby="ts-keys"
        tabIndex={0}
        onKeyDown={onKeyDown}
        style={{ position: "relative" }}
      >
        <EChart option={option} ariaLabel={`Load over the year: ${loads.join(", ")}${showBand ? ", with the uncertainty band" : ""}${hasTemp && showTemperature ? ", with temperature" : ""}, weather year ${weatherYear}`} height={440} onReady={setChart} />
      </div>
      <Typography id="ts-keys" variant="caption" color="text.secondary" component="p" sx={{ mt: 1, mb: 1 }}>
        Keyboard: focus the chart, then + and - to zoom, left and right arrows to pan, 0 to reset. Hover (mouse) or tap (touch) shows load{hasTemp ? " and temperature" : ""} together; the data table below gives the same numbers.
      </Typography>
    </ChartCard>
  );
}
