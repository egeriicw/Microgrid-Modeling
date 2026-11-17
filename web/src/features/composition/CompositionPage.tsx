import { useMemo, useState } from "react";
import { Alert, Box, Button, FormHelperText, LinearProgress, List, ListItem, Paper, TextField, Typography } from "@mui/material";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { ApiError, SEED_KEYS, type Composition, type CompositionEntry, type EntryMode, type SeedKey, type Seeds } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { parseNumber as num, validateComposition } from "./validate";

interface EntryForm {
  typeId: string;
  mode: EntryMode;
  min: string;
  max: string;
  count: string;
}

const SEED_LABELS: Record<SeedKey, string> = {
  composition_seed: "Composition seed",
  archetype_seed: "Archetype seed",
  perturbation_seed: "Perturbation seed",
};

/** An API error with the server's field messages (422) listed under it. */
export function ErrorAlert({ error }: { error: Error }) {
  const fields = error instanceof ApiError ? error.fieldErrors : [];
  return (
    <Alert severity="error" sx={{ mb: 2 }}>
      {fields.length > 0 ? "The server did not accept the request:" : error.message}
      {fields.length > 0 && (
        <List dense disablePadding>
          {fields.map((f, i) => (
            <ListItem key={i} disableGutters sx={{ display: "list-item", listStyle: "disc", ml: 2 }}>
              {f.field ? `${f.field}: ` : ""}
              {f.message}
            </ListItem>
          ))}
        </List>
      )}
    </Alert>
  );
}

/** Predefined compositions (server-side). Every role may run these; for demo they are the only option. */
function PredefinedSection() {
  const client = useApiClient();
  const navigate = useNavigate();
  const list = useQuery({ queryKey: ["predefined"], queryFn: () => client.listPredefinedCompositions() });
  const start = useMutation({
    mutationFn: (compositionId: string) => client.startRun({ kind: "predefined", compositionId }),
    onSuccess: ({ runId }) => navigate(`/runs/${runId}`),
  });
  return (
    <Paper component="section" aria-labelledby="predefined-h" sx={{ p: 3, mb: 3 }}>
      <Typography id="predefined-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Predefined compositions</Typography>
      {(list.isPending || start.isPending) && <LinearProgress aria-label={start.isPending ? "Starting run" : "Loading predefined compositions"} sx={{ mb: 2 }} />}
      {list.isError && <ErrorAlert error={list.error} />}
      {list.data?.length === 0 && <Typography variant="body2">The server has no predefined compositions.</Typography>}
      <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap" }}>
        {list.data?.map((name) => (
          <Button key={name} variant="outlined" onClick={() => start.mutate(name)} disabled={start.isPending} aria-label={`Run ${name}`}>
            {name}
          </Button>
        ))}
      </Box>
      {start.isError && <Box sx={{ mt: 2 }}><ErrorAlert error={start.error} /></Box>}
    </Paper>
  );
}

function CustomForm() {
  const client = useApiClient();
  const { can } = useAuth();
  const navigate = useNavigate();
  const types = useQuery({ queryKey: ["buildingTypes"], queryFn: () => client.listBuildingTypes() });
  const filters = useQuery({ queryKey: ["filterOptions"], queryFn: () => client.listFilterOptions(), enabled: can.useFilters });

  const [name, setName] = useState("My community");
  const [total, setTotal] = useState("100");
  const [seedText, setSeedText] = useState<Record<SeedKey, string>>({ composition_seed: "", archetype_seed: "", perturbation_seed: "" });
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

  const seeds: Partial<Seeds> = {};
  for (const k of SEED_KEYS) if (seedText[k].trim() !== "") seeds[k] = num(seedText[k]);

  const composition: Composition = {
    name,
    totalBuildings: num(total),
    seeds,
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
    mutationFn: () => client.startRun({ kind: "custom", composition }),
    onSuccess: ({ runId }) => navigate(`/runs/${runId}`),
  });

  const setEntry = (typeId: string, patch: Partial<EntryForm>) =>
    setOverrides((o) => ({ ...o, [typeId]: { ...o[typeId], ...patch } }));

  if (types.isError) {
    return (
      <Paper component="section" aria-labelledby="custom-h" sx={{ p: 3 }}>
        <Typography id="custom-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Custom composition</Typography>
        <Alert severity="info">{types.error.message}</Alert>
      </Paper>
    );
  }

  return (
    <Paper component="form" aria-labelledby="custom-h" sx={{ p: 3 }} onSubmit={(e) => { e.preventDefault(); if (!invalid) start.mutate(); }} noValidate>
      <Typography id="custom-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 2 }}>Custom composition</Typography>
      {(types.isPending || start.isPending) && <LinearProgress aria-label={start.isPending ? "Starting run" : "Loading building types"} sx={{ mb: 2 }} />}

      <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "2fr 1fr" }, mb: 3 }}>
        <TextField label="Composition name" value={name} onChange={(e) => setName(e.target.value)} error={!!errors.name} helperText={errors.name} />
        <TextField label="Total buildings" value={total} onChange={(e) => setTotal(e.target.value)} error={!!errors.totalBuildings} helperText={errors.totalBuildings} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
      </Box>

      <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Building types</Typography>
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

      <Box sx={{ my: 3 }}>
        <TextField label="Maximum runs" value={maxRuns} onChange={(e) => setMaxRuns(e.target.value)} error={!!errors.maxRuns} helperText={errors.maxRuns ?? "Upper limit on Monte-Carlo samples."} slotProps={{ htmlInput: { inputMode: "numeric" } }} />
      </Box>

      <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Seeds (optional)</Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>
        Leave a seed blank for a fresh random one. The run screen shows every seed used, so any run can be replayed.
      </Typography>
      <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "1fr 1fr 1fr" }, mb: 3 }}>
        {SEED_KEYS.map((k) => (
          <TextField
            key={k}
            label={SEED_LABELS[k]}
            value={seedText[k]}
            onChange={(e) => setSeedText((s) => ({ ...s, [k]: e.target.value }))}
            error={!!errors[k]}
            helperText={errors[k] ?? "Blank = random"}
            slotProps={{ htmlInput: { inputMode: "numeric" } }}
          />
        ))}
      </Box>

      {can.useFilters && filters.data && filters.data.length > 0 && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Building filters (admin only)</Typography>
          {filters.data.map((f) => (
            <TextField key={f.id} select size="small" label={f.label} value={vintage} onChange={(e) => setVintage(e.target.value)} slotProps={{ select: { native: true } }} sx={{ minWidth: 200 }}>
              {f.values.map((v) => <option key={v} value={v}>{v}</option>)}
            </TextField>
          ))}
        </Box>
      )}

      {start.isError && <ErrorAlert error={start.error} />}
      <Button type="submit" variant="contained" disabled={invalid || start.isPending || types.isPending}>Start run</Button>
    </Paper>
  );
}

export function CompositionPage() {
  const { can } = useAuth();
  return (
    <Box>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 2 }}>Define a community composition</Typography>
      {!can.startRuns && (
        <Alert severity="info" sx={{ mb: 2 }}>Demo accounts can run the predefined compositions only.</Alert>
      )}
      <PredefinedSection />
      {can.startRuns && <CustomForm />}
    </Box>
  );
}
