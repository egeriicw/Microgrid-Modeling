import { useMemo } from "react";
import { Alert, Box, Chip, LinearProgress, Typography, useMediaQuery } from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { EChart } from "../../charts/EChart";
import { buildLoadDurationOption, buildSeasonalOption, buildSplitOption } from "./chartOptions";
import { ChartCard } from "./ChartCard";
import { loadDurationTable, seasonalTable, splitTable } from "./tables";
import { TimeSeriesChart } from "./TimeSeriesChart";

// FR-WEB-8: shown on every results view.
export const ACCURACY_CAVEAT = "Methodology demonstration with benchmark checks — not validated for engineering or investment decisions.";

/** AMY<year> = actual weather of that year (not a typical year); anything else is described neutrally. */
export function weatherYearNote(weatherYear: string): string {
  const m = /^AMY(\d{4})$/.exec(weatherYear);
  return m
    ? `${weatherYear} is actual ${m[1]} weather, not a typical year.`
    : `Weather basis: ${weatherYear}. Check whether this is an actual year or a typical year before comparing.`;
}

export function ResultsPage() {
  const { runId = "" } = useParams();
  const client = useApiClient();
  const q = useQuery({ queryKey: ["results", runId], queryFn: () => client.getResults(runId), staleTime: Infinity });
  const compact = useMediaQuery("(max-width:600px)");
  const series = q.data?.series;
  const wy = q.data?.weatherYear ?? "";

  const ldc = useMemo(() => (series ? buildLoadDurationOption(series, compact) : null), [series, compact]);
  const seasonal = useMemo(() => (series ? buildSeasonalOption(series, compact) : null), [series, compact]);
  const split = useMemo(() => (series ? buildSplitOption(series) : null), [series]);

  return (
    <Box>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 1 }}>Results for run {runId}</Typography>
      {q.isPending && <LinearProgress aria-label="Loading results" />}
      {q.isError && <Alert severity="error">{q.error.message}</Alert>}
      {q.data && series && ldc && seasonal && split && (
        <>
          <Alert role="note" severity="warning" sx={{ mb: 2 }}>{ACCURACY_CAVEAT}</Alert>
          <Box sx={{ display: "flex", gap: 1, alignItems: "center", flexWrap: "wrap", mb: 2 }}>
            <Chip color="primary" label={`Weather year: ${q.data.weatherYear}`} />
            <Typography variant="body2">{weatherYearNote(wy)}</Typography>
            <Typography variant="body2">Calibration status: {q.data.calibrationStatus}</Typography>
          </Box>

          <TimeSeriesChart series={series} weatherYear={wy} />

          <ChartCard title="Load-duration curve" subtitle={`Weather year ${wy}`} table={() => loadDurationTable(series, wy)}>
            <EChart option={ldc} ariaLabel={`Load-duration curve for total, residential and commercial load, weather year ${wy}`} />
          </ChartCard>

          <ChartCard title="Seasonal daily profile" subtitle={`Weather year ${wy}`} table={() => seasonalTable(series, wy)}>
            <EChart option={seasonal} ariaLabel={`Average daily profile of total load by season, weather year ${wy}`} />
          </ChartCard>

          <ChartCard title="Residential and commercial split" subtitle={`Weather year ${wy}`} table={() => splitTable(series, wy)}>
            <EChart option={split} height={300} ariaLabel={`Annual energy in MWh, residential versus commercial, weather year ${wy}`} />
          </ChartCard>
        </>
      )}
    </Box>
  );
}
