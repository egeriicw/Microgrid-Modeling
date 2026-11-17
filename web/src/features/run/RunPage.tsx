import { useEffect, useState } from "react";
import { Alert, Box, Button, LinearProgress, Paper, Table, TableBody, TableCell, TableHead, TableRow, Typography } from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link as RouterLink, useNavigate, useParams } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { applyProgress, isTerminal } from "../../api/mapping";
import { ApiError, SEED_KEYS, type RunStatus } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";

const terminal = (s: RunStatus | undefined) => s !== undefined && isTerminal(s.state);

/** Only the API's own answer is reported; "converged" appears only when the API says true. */
function convergenceText(s: RunStatus): string {
  if (s.converged === true) return "converged";
  if (s.converged === null) return isTerminal(s.state) ? "not reported" : "not checked yet";
  if (s.state === "succeeded") return "did not converge (stopped at the run limit)";
  if (isTerminal(s.state)) return "did not converge (the run stopped)";
  return "not converged yet";
}

export function RunPage({ pollMs = 1000 }: { pollMs?: number }) {
  const { runId = "" } = useParams();
  // Keyed by run id so a replay (same route, new id) starts with fresh stream state.
  return <RunView key={runId} runId={runId} pollMs={pollMs} />;
}

function RunView({ runId, pollMs }: { runId: string; pollMs: number }) {
  const client = useApiClient();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const { can } = useAuth();
  const key = ["run", runId];
  // Live progress comes from the server-sent event stream; if that stream fails we poll the run instead.
  const [streamFailed, setStreamFailed] = useState(false);
  const run = useQuery({
    queryKey: key,
    queryFn: () => client.getRun(runId),
    refetchInterval: (q) => (streamFailed && !terminal(q.state.data) ? pollMs : false),
  });
  const live = run.data !== undefined && !terminal(run.data);

  useEffect(() => {
    if (!live) return;
    const ctrl = new AbortController();
    const refresh = () => void qc.invalidateQueries({ queryKey: ["run", runId] });
    client
      .watchRun(
        runId,
        (ev) => {
          if (ev.type === "progress") {
            qc.setQueryData<RunStatus>(["run", runId], (prev) => (prev ? applyProgress(prev, { message: ev.message, ...ev.payload }) : prev));
          } else refresh();
        },
        ctrl.signal,
      )
      .then(() => {
        if (!ctrl.signal.aborted) refresh();
      })
      .catch((e: unknown) => {
        if (ctrl.signal.aborted || (e instanceof ApiError && e.status === 401)) return;
        setStreamFailed(true);
      });
    return () => ctrl.abort();
  }, [live, runId, client, qc]);

  const cancel = useMutation({
    mutationFn: () => client.cancelRun(runId),
    onSuccess: () => qc.invalidateQueries({ queryKey: key }),
  });
  const replay = useMutation({
    mutationFn: (s: RunStatus) => client.startRun({ kind: "replay", config: s.config!, seeds: s.seeds! }),
    onSuccess: ({ runId: next }) => navigate(`/runs/${next}`),
  });

  const s = run.data;
  const pct = s && s.maxRuns ? Math.min(100, Math.round((100 * s.completedRuns) / s.maxRuns)) : null;

  return (
    <Paper sx={{ p: 3 }}>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 2 }}>Run {runId}</Typography>
      {run.isError && <Alert severity="error">{run.error.message}</Alert>}
      {!run.isError && (
        <>
          {pct === null ? (
            <LinearProgress aria-label="Run progress" sx={{ height: 10, borderRadius: 1 }} />
          ) : (
            <LinearProgress variant="determinate" value={pct} aria-label="Run progress" sx={{ height: 10, borderRadius: 1 }} />
          )}
          <Typography data-testid="stage-text" sx={{ mt: 1 }} aria-live="polite">{s ? s.stage : "Starting"}</Typography>
          {live && s?.cancelRequested && (
            <Typography role="status" sx={{ mt: 1 }}>Cancelling… the run stops after its current step.</Typography>
          )}
          {streamFailed && live && (
            <Typography variant="body2" color="text.secondary">Live updates are unavailable; checking for progress every few seconds.</Typography>
          )}
          {s && (
            <Box sx={{ mt: 2 }}>
              <Typography>
                Runs completed: {s.completedRuns} of {s.maxRuns ?? "?"}{pct !== null ? ` (${pct}%)` : ""}
              </Typography>
              <Typography>Feasible: {s.feasible}</Typography>
              <Typography>Rejected: {s.rejected}</Typography>
              <Typography>Convergence: {convergenceText(s)}</Typography>
              {s.state === "failed" && <Alert severity="error" sx={{ mt: 2 }}>{s.message ?? "The run failed."}</Alert>}
            </Box>
          )}
          <Box sx={{ mt: 3, display: "flex", gap: 2, flexWrap: "wrap" }}>
            {live && !s?.cancelRequested && (
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

          {s?.seeds && (
            <Box sx={{ mt: 3 }}>
              <Typography variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Seeds</Typography>
              <Typography variant="body2" sx={{ mb: 1 }}>
                The random seeds this run used. The same seeds, engine version ({s.engineVersion}) and data give the same result.
              </Typography>
              <Table size="small" aria-label="Seeds used by this run" sx={{ maxWidth: 420 }}>
                <TableHead>
                  <TableRow><TableCell>Seed</TableCell><TableCell align="right">Value</TableCell></TableRow>
                </TableHead>
                <TableBody>
                  {SEED_KEYS.map((k) => (
                    <TableRow key={k}><TableCell>{k}</TableCell><TableCell align="right">{s.seeds![k]}</TableCell></TableRow>
                  ))}
                </TableBody>
              </Table>
              {can.startRuns && s.config && (
                <Button sx={{ mt: 2 }} variant="outlined" onClick={() => replay.mutate(s)} disabled={replay.isPending}>
                  Replay with same seeds
                </Button>
              )}
              {replay.isPending && <LinearProgress aria-label="Starting the replay" sx={{ mt: 1 }} />}
              {replay.isError && <Alert severity="error" sx={{ mt: 2 }}>{replay.error.message}</Alert>}
            </Box>
          )}
        </>
      )}
    </Paper>
  );
}
