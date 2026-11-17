import { useMemo, useState } from "react";
import { Alert, Box, Button, FormHelperText, LinearProgress, Link as MuiLink, Paper, TextField, Typography } from "@mui/material";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link as RouterLink, useNavigate } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { DEMO_RUN_ID, type Composition, type CompositionEntry, type EntryMode } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { parseNumber as num, validateComposition } from "./validate";

interface EntryForm {
  typeId: string;
  mode: EntryMode;
  min: string;
  max: string;
  count: string;
}

export function CompositionPage() {
  const client = useApiClient();
  const { can } = useAuth();
  const navigate = useNavigate();
  const types = useQuery({ queryKey: ["buildingTypes"], queryFn: () => client.listBuildingTypes() });
  const filters = useQuery({ queryKey: ["filterOptions"], queryFn: () => client.listFilterOptions(), enabled: can.useFilters });

  const [name, setName] = useState("My community");
  const [total, setTotal] = useState("100");
  const [seed, setSeed] = useState("1");
  const [maxRuns, setMaxRuns] = useState("200");
  const [vintage, setVintage] = useState("Any");
  const [overrides, setOverrides] = useState<Record<string, Partial<EntryForm>>>({});

  const entries: EntryForm[] = useMemo(
    () =>
      (types.data ?? []).map((t) => ({
        typeId: t.id,
        mode: "fraction" as EntryMode,
        min: "0", // engine defaults: 0..1
        max: "1",
        count: "0",
        ...overrides[t.id],
      })),
    [types.data, overrides],
  );

  const composition: Composition = {
    name,
    totalBuildings: num(total),
    seed: num(seed),
    maxRuns: num(maxRuns),
    entries: entries.map((e): CompositionEntry =>
      e.mode === "count"
        ? { typeId: e.typeId, mode: "count", count: num(e.count) }
        : { typeId: e.typeId, mode: "fraction", minFraction: num(e.min), maxFraction: num(e.max) },
    ),
    ...(can.useFilters && vintage !== "Any" ? { vintageFilter: vintage } : {}),
  };
  const errors = validateComposition(composition);
  const invalid = Object.keys(errors).length > 0;

  const start = useMutation({
    mutationFn: () => client.startRun(composition),
    onSuccess: ({ runId }) => navigate(`/runs/${runId}`),
  });

  if (!can.startRuns) {
    return (
      <Box>
        <Typography variant="h1" sx={{ fontSize: "1.75rem" }}>Composition</Typography>
        <Alert severity="info" sx={{ mt: 2 }}>
          Demo accounts cannot start runs.{" "}
          <MuiLink component={RouterLink} to={`/results/${DEMO_RUN_ID}`}>View the demo results</MuiLink>.
        </Alert>
      </Box>
    );
  }

  const setEntry = (typeId: string, patch: Partial<EntryForm>) =>
    setOverrides((o) => ({ ...o, [typeId]: { ...o[typeId], ...patch } }));

  return (
    <Paper component="form" sx={{ p: 3 }} onSubmit={(e) => { e.preventDefault(); if (!invalid) start.mutate(); }} noValidate>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 2 }}>Define a community composition</Typography>
      {(types.isPending || start.isPending) && <LinearProgress aria-label={start.isPending ? "Starting run" : "Loading building types"} sx={{ mb: 2 }} />}
      {types.isError && <Alert severity="error" sx={{ mb: 2 }}>{types.error.message}</Alert>}

      <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "2fr 1fr" }, mb: 3 }}>
        <TextField label="Composition name" value={name} onChange={(e) => setName(e.target.value)} error={!!errors.name} helperText={errors.name} />
        <TextField label="Total buildings" value={total} onChange={(e) => setTotal(e.target.value)} error={!!errors.totalBuildings} helperText={errors.totalBuildings} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
      </Box>

      <Typography variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Building types</Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>
        Fixed-count types are taken out first. The remaining buildings are shared among the other types, each within its min to max fraction of that remainder.
      </Typography>
      {(types.data ?? []).map((t) => {
        const e = entries.find((x) => x.typeId === t.id)!;
        return (
          <Box key={t.id} sx={{ display: "flex", gap: 2, mb: 1.5, alignItems: "center", flexWrap: "wrap" }}>
            <Typography sx={{ minWidth: 200 }}>{t.label} <Typography component="span" variant="caption">({t.sector})</Typography></Typography>
            <TextField
              select
              size="small"
              label={`${t.label} mode`}
              value={e.mode}
              onChange={(ev) => setEntry(t.id, { mode: ev.target.value as EntryMode })}
              slotProps={{ select: { native: true } }}
              sx={{ minWidth: 150 }}
            >
              <option value="fraction">Fraction range</option>
              <option value="count">Fixed count</option>
            </TextField>
            {e.mode === "count" ? (
              <TextField size="small" label={`${t.label} count`} value={e.count} onChange={(ev) => setEntry(t.id, { count: ev.target.value })} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
            ) : (
              <>
                <TextField size="small" label={`${t.label} min fraction`} value={e.min} onChange={(ev) => setEntry(t.id, { min: ev.target.value })} slotProps={{ htmlInput: { inputMode: "decimal" } }} />
                <TextField size="small" label={`${t.label} max fraction`} value={e.max} onChange={(ev) => setEntry(t.id, { max: ev.target.value })} slotProps={{ htmlInput: { inputMode: "decimal" } }} />
              </>
            )}
          </Box>
        );
      })}
      {errors.entries && <FormHelperText error role="alert">{errors.entries}</FormHelperText>}

      <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "1fr 1fr" }, my: 3 }}>
        <TextField label="Random seed" value={seed} onChange={(e) => setSeed(e.target.value)} error={!!errors.seed} helperText={errors.seed ?? "Same seed, same result."} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
        <TextField label="Maximum runs" value={maxRuns} onChange={(e) => setMaxRuns(e.target.value)} error={!!errors.maxRuns} helperText={errors.maxRuns ?? "Upper limit on Monte-Carlo samples."} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
      </Box>

      {can.useFilters && filters.data && filters.data.length > 0 && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Building filters (admin only)</Typography>
          {filters.data.map((f) => (
            <TextField key={f.id} select size="small" label={f.label} value={vintage} onChange={(e) => setVintage(e.target.value)} slotProps={{ select: { native: true } }} sx={{ minWidth: 200 }}>
              {f.values.map((v) => <option key={v} value={v}>{v}</option>)}
            </TextField>
          ))}
        </Box>
      )}

      {start.isError && <Alert severity="error" sx={{ mb: 2 }}>{start.error.message}</Alert>}
      <Button type="submit" variant="contained" disabled={invalid || start.isPending || types.isPending}>Start run</Button>
    </Paper>
  );
}
