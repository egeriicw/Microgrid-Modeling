import { useMemo } from "react";
import { Alert, Box, Chip, LinearProgress, Typography } from "@mui/material";
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

export function ResultsPage() {
  const { runId = "" } = useParams();
  const client = useApiClient();
  const q = useQuery({ queryKey: ["results", runId], queryFn: () => client.getResults(runId), staleTime: Infinity });
  const series = q.data?.series;

  const ldc = useMemo(() => (series ? buildLoadDurationOption(series) : null), [series]);
  const seasonal = useMemo(() => (series ? buildSeasonalOption(series) : null), [series]);
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
            <Typography variant="body2">The {q.data.weatherYear} dataset uses actual 2018 weather, not a typical year.</Typography>
            <Typography variant="body2">Calibration status: {q.data.calibrationStatus}</Typography>
          </Box>

          <TimeSeriesChart series={series} />

          <ChartCard title="Load-duration curve" subtitle="Weather year AMY2018" table={() => loadDurationTable(series)}>
            <EChart option={ldc} ariaLabel="Load-duration curve for total, residential and commercial load, weather year AMY2018" />
          </ChartCard>

          <ChartCard title="Seasonal daily profile" subtitle="Weather year AMY2018" table={() => seasonalTable(series)}>
            <EChart option={seasonal} ariaLabel="Average daily profile of total load by season, weather year AMY2018" />
          </ChartCard>

          <ChartCard title="Residential and commercial split" subtitle="Weather year AMY2018" table={() => splitTable(series)}>
            <EChart option={split} height={300} ariaLabel="Annual energy in MWh, residential versus commercial, weather year AMY2018" />
          </ChartCard>
        </>
      )}
    </Box>
  );
}
