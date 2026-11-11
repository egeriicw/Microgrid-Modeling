import { Alert, Box, Button, LinearProgress, Paper, Typography } from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link as RouterLink, useParams } from "react-router-dom";
import { useApiClient } from "../../api/context";
import type { RunStatus } from "../../api/types";

const isTerminal = (s: RunStatus | undefined) => s !== undefined && s.state !== "running";

export function RunPage({ pollMs = 1000 }: { pollMs?: number }) {
  const { runId = "" } = useParams();
  const client = useApiClient();
  const qc = useQueryClient();
  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => client.getRun(runId),
    refetchInterval: (q) => (isTerminal(q.state.data) ? false : pollMs),
  });
  const cancel = useMutation({
    mutationFn: () => client.cancelRun(runId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["run", runId] }),
  });

  const s = run.data;
  const pct = s ? Math.round((100 * s.completedRuns) / s.maxRuns) : 0;

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 2 }}>Run {runId}</Typography>
      {run.isError && <Alert severity="error">{run.error.message}</Alert>}
      {!run.isError && (
        <>
          <LinearProgress variant="determinate" value={pct} aria-label="Run progress" sx={{ height: 10, borderRadius: 1 }} />
          <Typography data-testid="stage-text" sx={{ mt: 1 }} aria-live="polite">{s ? s.stage : "Starting"}</Typography>
          {s && (
            <Box sx={{ mt: 2 }}>
              <Typography>Runs completed: {s.completedRuns} of {s.maxRuns} ({pct}%)</Typography>
              <Typography>Feasible: {s.feasible}</Typography>
              <Typography>Rejected: {s.rejected}</Typography>
              <Typography>Convergence: {s.converged ? "converged" : "not yet converged"}</Typography>
              {s.state === "failed" && <Alert severity="error" sx={{ mt: 2 }}>{s.message ?? "The run failed."}</Alert>}
            </Box>
          )}
          <Box sx={{ mt: 3, display: "flex", gap: 2 }}>
            {s?.state === "running" && (
              <Button variant="outlined" color="inherit" onClick={() => cancel.mutate()} disabled={cancel.isPending}>Cancel run</Button>
            )}
            {s?.state === "succeeded" && (
              <Button variant="contained" component={RouterLink} to={`/results/${runId}`}>View results</Button>
            )}
            {(s?.state === "cancelled" || s?.state === "failed") && (
              <Button component={RouterLink} to="/composition">Back to composition</Button>
            )}
          </Box>
          {cancel.isError && <Alert severity="error" sx={{ mt: 2 }}>{cancel.error.message}</Alert>}
        </>
      )}
    </Paper>
  );
}
