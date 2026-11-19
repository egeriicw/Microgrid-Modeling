import { useState } from "react";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  FormControlLabel,
  FormHelperText,
  LinearProgress,
  List,
  ListItem,
  Paper,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { useApiClient } from "../../api/context";
import { ApiError, SEED_KEYS, type BuildingType, type Composition, type CompositionEntry, type EntryMode, type SeedKey, type Seeds } from "../../api/types";
import { useAuth } from "../../auth/AuthContext";
import { parseNumber as num, validateComposition } from "./validate";

interface EntryForm {
  included: boolean;
  mode: EntryMode;
  min: string;
  max: string;
  count: string;
}

// A type starts excluded; once included it starts as a full share range (the API's own defaults, 0 to 1).
const NEW_ENTRY: EntryForm = { included: false, mode: "fraction", min: "0", max: "1", count: "0" };

const SEED_LABELS: Record<SeedKey, string> = {
  composition_seed: "Composition seed",
  archetype_seed: "Archetype seed",
  perturbation_seed: "Perturbation seed",
};

const SECTORS: { sector: BuildingType["sector"]; title: string }[] = [
  { sector: "residential", title: "Residential building types" },
  { sector: "commercial", title: "Commercial building types" },
];

/** An API error with the server's field messages (422) listed under it. Server text is rendered as text. */
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

function TypeRow({ type, entry, onChange }: { type: BuildingType; entry: EntryForm; onChange: (patch: Partial<EntryForm>) => void }) {
  const l = type.label;
  return (
    <Box sx={{ display: "flex", gap: 2, mb: 1.5, alignItems: "center", flexWrap: "wrap" }}>
      <Box sx={{ minWidth: 240, flex: "1 1 240px" }}>
        <FormControlLabel
          control={<Checkbox checked={type.active && entry.included} disabled={!type.active} onChange={(e) => onChange({ included: e.target.checked })} />}
          label={`Include ${l}`}
        />
        {type.description && <Typography variant="body2" color="text.secondary" sx={{ ml: 4 }}>{type.description}</Typography>}
        {!type.active && (
          <Typography variant="body2" color="text.secondary" sx={{ ml: 4 }}>
            Not available yet: the server cannot model this type, so it cannot be included.
          </Typography>
        )}
      </Box>
      {type.active && entry.included && (
        <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", flex: "2 1 320px" }}>
          <TextField
            select
            size="small"
            label="Set by"
            value={entry.mode}
            onChange={(ev) => onChange({ mode: ev.target.value as EntryMode })}
            slotProps={{ select: { native: true }, htmlInput: { "aria-label": `${l} mode` } }}
            sx={{ minWidth: 150 }}
          >
            <option value="fraction">Share range</option>
            <option value="count">Exact number</option>
          </TextField>
          {entry.mode === "count" ? (
            <TextField size="small" label="Number of buildings" value={entry.count} onChange={(ev) => onChange({ count: ev.target.value })} slotProps={{ htmlInput: { inputMode: "numeric", "aria-label": `${l} number of buildings` } }} sx={{ maxWidth: 190 }} />
          ) : (
            <>
              <TextField size="small" label="Minimum share" value={entry.min} onChange={(ev) => onChange({ min: ev.target.value })} slotProps={{ htmlInput: { inputMode: "decimal", "aria-label": `${l} minimum share` } }} sx={{ maxWidth: 150 }} />
              <TextField size="small" label="Maximum share" value={entry.max} onChange={(ev) => onChange({ max: ev.target.value })} slotProps={{ htmlInput: { inputMode: "decimal", "aria-label": `${l} maximum share` } }} sx={{ maxWidth: 150 }} />
            </>
          )}
        </Box>
      )}
    </Box>
  );
}

function CustomForm() {
  const client = useApiClient();
  const { can, session } = useAuth();
  const navigate = useNavigate();
  const types = useQuery({ queryKey: ["buildingTypes"], queryFn: () => client.listBuildingTypes() });
  const filters = useQuery({ queryKey: ["filterOptions"], queryFn: () => client.listFilterOptions(), enabled: can.useFilters });
  const limits = session?.limits;

  const [name, setName] = useState("My community");
  const [total, setTotal] = useState("100");
  const [seedText, setSeedText] = useState<Record<SeedKey, string>>({ composition_seed: "", archetype_seed: "", perturbation_seed: "" });
  const [maxRuns, setMaxRuns] = useState("200");
  const [convergence, setConvergence] = useState(false);
  const [vintage, setVintage] = useState("Any");
  const [forms, setForms] = useState<Record<string, EntryForm>>({});

  const entryOf = (id: string): EntryForm => forms[id] ?? NEW_ENTRY;
  const setEntry = (id: string, patch: Partial<EntryForm>) => setForms((f) => ({ ...f, [id]: { ...(f[id] ?? NEW_ENTRY), ...patch } }));

  const seeds: Partial<Seeds> = {};
  if (can.setSeeds) for (const k of SEED_KEYS) if (seedText[k].trim() !== "") seeds[k] = num(seedText[k]);

  const included = (types.data ?? []).filter((t) => t.active && entryOf(t.id).included);
  const composition: Composition = {
    name,
    totalBuildings: num(total),
    seeds,
    maxRuns: num(maxRuns),
    convergence,
    entries: included.map((t): CompositionEntry => {
      const e = entryOf(t.id);
      return e.mode === "count"
        ? { typeId: t.id, mode: "count", count: num(e.count) }
        : { typeId: t.id, mode: "fraction", minFraction: num(e.min), maxFraction: num(e.max) };
    }),
    ...(can.useFilters && vintage !== "Any" ? { vintageFilter: vintage } : {}),
  };
  const errors = validateComposition(composition, limits);
  const invalid = Object.keys(errors).length > 0;

  const start = useMutation({
    mutationFn: () => client.startRun({ kind: "custom", composition }),
    onSuccess: ({ runId }) => navigate(`/runs/${runId}`),
  });

  if (types.isError) {
    return (
      <Paper component="section" aria-labelledby="custom-h" sx={{ p: 3 }}>
        <Typography id="custom-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Your own composition</Typography>
        <Alert severity="info">The building types could not be loaded, so a composition cannot be built here right now: {types.error.message}</Alert>
      </Paper>
    );
  }

  return (
    <Paper component="form" aria-labelledby="custom-h" sx={{ p: { xs: 2, sm: 3 } }} onSubmit={(e) => { e.preventDefault(); if (!invalid) start.mutate(); }} noValidate>
      <Typography id="custom-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 2 }}>Your own composition</Typography>
      {(types.isPending || start.isPending) && <LinearProgress aria-label={start.isPending ? "Starting run" : "Loading building types"} sx={{ mb: 2 }} />}

      <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "2fr 1fr" }, mb: 3 }}>
        <TextField label="Composition name" value={name} onChange={(e) => setName(e.target.value)} error={!!errors.name} helperText={errors.name} />
        <TextField
          label="Total buildings"
          value={total}
          onChange={(e) => setTotal(e.target.value)}
          error={!!errors.totalBuildings}
          helperText={errors.totalBuildings ?? (limits ? `Up to ${limits.maxTotalBuildings} on this server.` : undefined)}
          slotProps={{ htmlInput: { inputMode: "numeric" } }}
        />
      </Box>

      <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Building types</Typography>
      <Typography variant="body2" sx={{ mb: 2 }}>
        Tick the types in your neighbourhood. Give a type an exact number of buildings, or a share range: types with an exact
        number are placed first, and the buildings left over are shared among the share-range types, each between its minimum
        and maximum share of that remainder (0.25 = a quarter).
      </Typography>
      {SECTORS.map(({ sector, title }) => {
        const list = (types.data ?? []).filter((t) => t.sector === sector);
        if (list.length === 0) return null;
        return (
          <Box key={sector} component="fieldset" sx={{ border: 0, p: 0, m: 0, mb: 2, minWidth: 0 }}>
            <Typography component="legend" sx={{ fontWeight: 600, mb: 1 }}>{title}</Typography>
            {list.map((t) => <TypeRow key={t.id} type={t} entry={entryOf(t.id)} onChange={(p) => setEntry(t.id, p)} />)}
          </Box>
        );
      })}
      {errors.entries && <FormHelperText error role="alert" sx={{ mb: 2 }}>{errors.entries}</FormHelperText>}

      <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1, mt: 2 }}>Run settings</Typography>
      <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", alignItems: "flex-start", mb: 3 }}>
        <TextField
          label="Maximum runs"
          value={maxRuns}
          onChange={(e) => setMaxRuns(e.target.value)}
          error={!!errors.maxRuns}
          helperText={errors.maxRuns ?? `How many sampled neighbourhoods at most${limits ? ` (up to ${limits.maxEnsembleRuns} on this server)` : ""}.`}
          slotProps={{ htmlInput: { inputMode: "numeric" } }}
        />
        <Box>
          <FormControlLabel control={<Switch checked={convergence} onChange={(e) => setConvergence(e.target.checked)} />} label="Convergence: stop early when the results settle" />
          <FormHelperText>
            {convergence
              ? `The server's standard convergence rule decides when to stop${limits ? `; it needs at least ${limits.defaultMinRuns} runs` : ""}.`
              : "Off: exactly the maximum number of runs is made."}
          </FormHelperText>
        </Box>
      </Box>

      {can.setSeeds && (
        <>
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
        </>
      )}

      {can.useFilters && filters.data && filters.data.length > 0 && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Building filters</Typography>
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
      {!can.composeCustom && (
        <Alert severity="info" sx={{ mb: 2 }}>Demo accounts can run the predefined compositions only.</Alert>
      )}
      {can.runPredefined && <PredefinedSection />}
      {can.composeCustom && <CustomForm />}
    </Box>
  );
}
