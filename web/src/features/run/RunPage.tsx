import { useEffect, useState } from "react";
import { Alert, Box, Button, LinearProgress, Paper, Table, TableBody, TableCell, TableHead, TableRow, Typography } from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link as RouterLink, useNavigate, useParams } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { applyProgress, isTerminal } from "../../api/mapping";
import { ApiError, SEED_KEYS, type RunState, type RunStatus } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { AccuracyCaveat } from "../results/caveat";
import { SEED_INFO } from "../seeds";
import { DataUsedPanel } from "./DataUsedPanel";

const terminal = (s: RunStatus | undefined) => s !== undefined && isTerminal(s.state);

const HEADINGS: Record<RunState, string> = {
  queued: "Estimate in progress",
  running: "Estimate in progress",
  succeeded: "Estimate finished",
  cancelled: "Estimate cancelled",
  failed: "Estimate failed",
};

export const RUN_EXPLANATION =
  "Each run builds one plausible neighbourhood by sampling real buildings; the results show the spread across all of them.";
export const NO_RESULTS_SAVED = "No results were saved.";

/** Only the API's own answer is reported; "converged" appears only when the API says true. */
function convergenceText(s: RunStatus): string {
  // Fixed mode: the engine marks a completed run "converged", which would mislead; convergence was simply off.
  if (s.ensembleMode === "fixed") return "off (a fixed number of runs)";
  if (s.converged === true) return "converged";
  if (s.converged === null) return isTerminal(s.state) ? "not reported" : "not checked yet";
  if (s.state === "succeeded") return "did not converge (stopped at the run limit)";
  if (isTerminal(s.state)) return "did not converge (the run stopped)";
  return "not converged yet";
}

const ofMax = (s: RunStatus) => `${s.completedRuns} of ${s.maxRuns ?? "?"}`;

/** The status line in plain words (the engine's raw stage words, such as "finished", are never shown alone). */
export function plainStage(s: RunStatus | undefined): string {
  if (!s) return "Starting…";
  switch (s.state) {
    case "queued":
      return "Waiting for the server to start this estimate (queued).";
    case "running":
      return "Building and simulating sampled neighbourhoods…";
    case "succeeded":
      return s.rejected === 0
        ? `Finished. All ${s.completedRuns} sampled neighbourhoods were simulated.`
        : `Finished. ${s.completedRuns} sampled neighbourhoods were tried; ${s.feasible} were valid and ${s.rejected} could not be built and were skipped.`;
    case "cancelled":
      return `Cancelled at ${ofMax(s)} runs. ${NO_RESULTS_SAVED}`;
    case "failed":
      return "The estimate failed. The server's reason is below.";
  }
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
  const stopped = s?.state === "cancelled" || s?.state === "failed";
  const replayLabel = stopped ? "Start again with the same seeds" : "Replay with same seeds";

  return (
    <Paper sx={{ p: { xs: 2, sm: 3 } }}>
      <Typography variant="h1" sx={{ fontSize: "1.75rem" }}>{s ? HEADINGS[s.state] : "Estimate"}</Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1, overflowWrap: "anywhere" }}>Run {runId}</Typography>
      <Typography variant="body2" sx={{ mb: 2 }}>{RUN_EXPLANATION}</Typography>
      {run.isError && <Alert severity="error">{run.error.message}</Alert>}
      {!run.isError && (
        <>
          {pct === null ? (
            <LinearProgress aria-label="Run progress" sx={{ height: 10, borderRadius: 1 }} />
          ) : (
            <LinearProgress variant="determinate" value={pct} aria-label="Run progress" sx={{ height: 10, borderRadius: 1 }} />
          )}
          <Typography data-testid="stage-text" sx={{ mt: 1, fontWeight: 600 }} aria-live="polite">{plainStage(s)}</Typography>
          {s?.state === "running" && s.stage && !/^finished/i.test(s.stage) && (
            <Typography variant="body2" color="text.secondary">Latest step from the engine: {s.stage}</Typography>
          )}
          {live && s?.cancelRequested && (
            <Typography role="status" sx={{ mt: 1 }}>Cancelling… the run stops after its current step.</Typography>
          )}
          {streamFailed && live && (
            <Typography variant="body2" color="text.secondary">Live updates are unavailable; checking for progress every few seconds.</Typography>
          )}
          {s && (
            <Box sx={{ mt: 2 }}>
              <Typography>
                Neighbourhoods simulated (runs): {ofMax(s)}{pct !== null ? ` (${pct}%)` : ""}
              </Typography>
              <Typography>Valid runs: {s.feasible} (sampled neighbourhoods that fit the rules)</Typography>
              <Typography>Rejected runs: {s.rejected} (neighbourhoods that could not be built and were skipped)</Typography>
              <Typography>Convergence: {convergenceText(s)}</Typography>
              {s.convergenceRule && s.ensembleMode !== "fixed" && (
                <Typography variant="body2">Convergence rule used: {s.convergenceRule} (as reported by the engine for this run)</Typography>
              )}
              {s.state === "failed" && (
                <Alert severity="error" sx={{ mt: 2 }}>
                  {s.message ?? "The server did not give a reason."} {NO_RESULTS_SAVED}
                </Alert>
              )}
            </Box>
          )}
          <Box sx={{ mt: 3, display: "flex", gap: 2, flexWrap: "wrap", alignItems: "center" }}>
            {live && !s?.cancelRequested && (
              <Button variant="outlined" color="inherit" onClick={() => cancel.mutate()} disabled={cancel.isPending}>Cancel run</Button>
            )}
            {s?.state === "succeeded" && (
              <Button variant="contained" component={RouterLink} to={`/results/${runId}`}>View results</Button>
            )}
            {stopped && (
              <Button component={RouterLink} to="/composition">Back to composition</Button>
            )}
          </Box>
          {s?.state === "succeeded" && <AccuracyCaveat compact />}
          {cancel.isError && <Alert severity="error" sx={{ mt: 2 }}>{cancel.error.message}</Alert>}

          {s?.seeds && (
            <Box sx={{ mt: 3 }}>
              <Typography variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Random seeds (only needed to repeat this run exactly)</Typography>
              <Typography variant="body2" sx={{ mb: 1 }}>
                The same seeds, engine version ({s.engineVersion}) and data give the same result.
              </Typography>
              <Table size="small" aria-label="Random seeds used by this run" sx={{ maxWidth: 560 }}>
                <TableHead>
                  <TableRow><TableCell>Seed</TableCell><TableCell align="right">Value</TableCell></TableRow>
                </TableHead>
                <TableBody>
                  {SEED_KEYS.map((k) => (
                    <TableRow key={k}>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>{SEED_INFO[k].label}</Typography>
                        <Typography variant="body2">{SEED_INFO[k].explanation}</Typography>
                        <Typography variant="caption" color="text.secondary" component="p">{k}</Typography>
                      </TableCell>
                      <TableCell align="right">{s.seeds![k]}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
              {can.replay && s.config && (
                <Button sx={{ mt: 2 }} variant="outlined" onClick={() => replay.mutate(s)} disabled={replay.isPending}>
                  {replayLabel}
                </Button>
              )}
              {replay.isPending && <LinearProgress aria-label="Starting the replay" sx={{ mt: 1 }} />}
              {replay.isError && <Alert severity="error" sx={{ mt: 2 }}>{replay.error.message}</Alert>}
            </Box>
          )}
          {s?.dataFiles && <DataUsedPanel files={s.dataFiles} />}
        </>
      )}
    </Paper>
  );
}
