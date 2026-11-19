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
import { SEED_INFO } from "../seeds";
import { parseNumber as num, problemList, validateComposition, type EntryErrors } from "./validate";

interface EntryForm {
  included: boolean;
  mode: EntryMode;
  min: string;
  max: string;
  count: string;
}

// A type starts excluded; once included it starts as a full share range (the API's own defaults, 0 to 1).
const NEW_ENTRY: EntryForm = { included: false, mode: "fraction", min: "0", max: "1", count: "0" };

const SECTORS: { sector: BuildingType["sector"]; title: string }[] = [
  { sector: "residential", title: "Residential building types" },
  { sector: "commercial", title: "Commercial building types" },
];

export const PREDEFINED_ONLY_NOTICE = "This account can run the ready-made neighbourhoods below. Pick one and press Run.";
export const INACTIVE_TYPE_NOTE = "Not available yet: this building type will be added later.";

/**
 * A readable name for a ready-made neighbourhood's id, for display only (the API sends ids only): separators become
 * spaces, then sentence case. "DC_SMALL_MIXED" -> "Dc small mixed". The id itself is still shown as secondary text.
 */
export function readableId(id: string): string {
  const words = id.replace(/[_\-.]+/g, " ").trim().toLowerCase();
  return words ? words[0].toUpperCase() + words.slice(1) : id;
}

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

/** Ready-made (predefined, server-side) neighbourhoods. For some accounts they are the only option. */
function PredefinedSection() {
  const client = useApiClient();
  const navigate = useNavigate();
  const list = useQuery({ queryKey: ["predefined"], queryFn: () => client.listPredefinedCompositions() });
  const start = useMutation({
    mutationFn: (compositionId: string) => client.startRun({ kind: "predefined", compositionId }),
    onSuccess: ({ runId }) => navigate(`/runs/${runId}`),
  });
  return (
    <Paper component="section" aria-labelledby="predefined-h" sx={{ p: { xs: 2, sm: 3 }, mb: 3 }}>
      <Typography id="predefined-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Ready-made neighbourhoods</Typography>
      {(list.isPending || start.isPending) && <LinearProgress aria-label={start.isPending ? "Starting run" : "Loading ready-made neighbourhoods"} sx={{ mb: 2 }} />}
      {list.isError && <ErrorAlert error={list.error} />}
      {list.data?.length === 0 && <Typography variant="body2">The server has no ready-made neighbourhoods.</Typography>}
      <Box component="ul" sx={{ listStyle: "none", p: 0, m: 0, display: "grid", gap: 1.5 }}>
        {list.data?.map((id) => {
          const label = readableId(id);
          return (
            <Box component="li" key={id} sx={{ display: "flex", gap: 2, alignItems: "center", flexWrap: "wrap" }}>
              <Box sx={{ flex: "1 1 200px" }}>
                <Typography>{label}</Typography>
                <Typography variant="caption" color="text.secondary">{id}</Typography>
              </Box>
              <Button variant="outlined" onClick={() => start.mutate(id)} disabled={start.isPending} aria-label={`Run this neighbourhood: ${label}`} sx={{ minHeight: 44 }}>
                Run this neighbourhood
              </Button>
            </Box>
          );
        })}
      </Box>
      {start.isError && <Box sx={{ mt: 2 }}><ErrorAlert error={start.error} /></Box>}
    </Paper>
  );
}

const FIELD_WIDTH = { width: { xs: "100%", sm: 210 } };

function TypeRow({ type, entry, errors, onChange }: { type: BuildingType; entry: EntryForm; errors?: EntryErrors; onChange: (patch: Partial<EntryForm>) => void }) {
  const l = type.label;
  return (
    <Box sx={{ display: "flex", gap: 2, mb: 1.5, alignItems: "flex-start", flexWrap: "wrap" }}>
      <Box sx={{ minWidth: 240, flex: "1 1 240px" }}>
        <FormControlLabel
          control={
            <Checkbox
              checked={type.active && entry.included}
              disabled={!type.active}
              onChange={(e) => onChange({ included: e.target.checked })}
              slotProps={{ input: { "aria-label": `Include ${l}` } }}
              sx={{ p: "10px" /* 44 px touch target */ }}
            />
          }
          label={l}
        />
        {type.description && <Typography variant="body2" color="text.secondary" sx={{ ml: 5.5 }}>{type.description}</Typography>}
        {!type.active && <Typography variant="body2" color="text.secondary" sx={{ ml: 5.5 }}>{INACTIVE_TYPE_NOTE}</Typography>}
      </Box>
      {type.active && entry.included && (
        <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", flex: "2 1 320px", alignItems: "flex-start" }}>
          <TextField
            select
            size="small"
            label="How many?"
            value={entry.mode}
            onChange={(ev) => onChange({ mode: ev.target.value as EntryMode })}
            slotProps={{ select: { native: true }, htmlInput: { "aria-label": `How many? ${l}` } }}
            sx={{ width: { xs: "100%", sm: 290 } }}
          >
            <option value="fraction">A share of the neighbourhood (range)</option>
            <option value="count">An exact number of buildings</option>
          </TextField>
          {entry.mode === "count" ? (
            <TextField
              size="small"
              label="Number of buildings"
              value={entry.count}
              onChange={(ev) => onChange({ count: ev.target.value })}
              error={!!errors?.count}
              helperText={errors?.count}
              slotProps={{ htmlInput: { inputMode: "numeric", "aria-label": `Number of buildings: ${l}` } }}
              sx={FIELD_WIDTH}
            />
          ) : (
            <>
              <TextField
                size="small"
                label="Smallest share (0 to 1)"
                value={entry.min}
                onChange={(ev) => onChange({ min: ev.target.value })}
                error={!!errors?.min}
                helperText={errors?.min}
                slotProps={{ htmlInput: { inputMode: "decimal", "aria-label": `Smallest share (0 to 1): ${l}` } }}
                sx={FIELD_WIDTH}
              />
              <TextField
                size="small"
                label="Largest share (0 to 1)"
                value={entry.max}
                onChange={(ev) => onChange({ max: ev.target.value })}
                error={!!errors?.max}
                helperText={errors?.max}
                slotProps={{ htmlInput: { inputMode: "decimal", "aria-label": `Largest share (0 to 1): ${l}` } }}
                sx={FIELD_WIDTH}
              />
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
  const labelOf = (id: string) => types.data?.find((t) => t.id === id)?.label ?? id;
  const errors = validateComposition(composition, limits, labelOf);
  const problems = problemList(errors).length;
  const invalid = problems > 0;

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
        number are placed first, and the buildings left over are shared among the share-range types, each between its smallest
        and largest share of that remainder (0.25 = a quarter).
      </Typography>
      {SECTORS.map(({ sector, title }) => {
        const list = (types.data ?? []).filter((t) => t.sector === sector);
        if (list.length === 0) return null;
        return (
          <Box key={sector} component="fieldset" sx={{ border: 0, p: 0, m: 0, mb: 2, minWidth: 0 }}>
            <Typography component="legend" sx={{ fontWeight: 600, mb: 1 }}>{title}</Typography>
            {list.map((t) => (
              <TypeRow key={t.id} type={t} entry={entryOf(t.id)} errors={errors.byType?.[t.id]} onChange={(p) => setEntry(t.id, p)} />
            ))}
          </Box>
        );
      })}
      {errors.entries && <FormHelperText error role="alert" sx={{ mb: 2, fontSize: "0.9rem" }}>{errors.entries}</FormHelperText>}

      <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1, mt: 2 }}>Run settings</Typography>
      <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", alignItems: "flex-start", mb: 3 }}>
        <TextField
          label="Number of runs (at most)"
          value={maxRuns}
          onChange={(e) => setMaxRuns(e.target.value)}
          error={!!errors.maxRuns}
          helperText={errors.maxRuns ?? `How many sampled neighbourhoods at most${limits ? ` (up to ${limits.maxEnsembleRuns} on this server)` : ""}.`}
          slotProps={{ htmlInput: { inputMode: "numeric" } }}
        />
        <Box>
          <Box sx={{ display: "flex", alignItems: "center", flexWrap: "wrap" }}>
            <FormControlLabel
              control={<Switch checked={convergence} onChange={(e) => setConvergence(e.target.checked)} />}
              label="Stop early when the results settle (convergence)"
              sx={{ mr: 1 }}
            />
            {/* The state in words, so it never depends on the track colour. */}
            <Typography data-testid="convergence-state" sx={{ fontWeight: 600 }} aria-hidden="true">{convergence ? "On" : "Off"}</Typography>
          </Box>
          <FormHelperText>
            {convergence
              ? `On: the server's standard convergence rule decides when to stop${limits ? `; it needs at least ${limits.defaultMinRuns} runs` : ""}.`
              : "Off: exactly the number of runs above is made."}
          </FormHelperText>
        </Box>
      </Box>

      {can.setSeeds && (
        <>
          <Typography variant="h3" sx={{ fontSize: "1.1rem", mb: 1 }}>Random seeds (optional)</Typography>
          <Typography variant="body2" sx={{ mb: 1 }}>
            Only needed to repeat an earlier estimate exactly. Leave a seed blank for a fresh random one; the run screen shows every
            seed used.
          </Typography>
          <Box sx={{ display: "grid", gap: 2, gridTemplateColumns: { sm: "1fr 1fr 1fr" }, mb: 3 }}>
            {SEED_KEYS.map((k) => (
              <TextField
                key={k}
                label={SEED_INFO[k].label}
                value={seedText[k]}
                onChange={(e) => setSeedText((s) => ({ ...s, [k]: e.target.value }))}
                error={!!errors[k]}
                helperText={errors[k] ?? `${SEED_INFO[k].explanation} Blank = random. Technical name: ${k}.`}
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
      {invalid && (
        <Alert id="form-problems" severity="warning" role="status" sx={{ mb: 2 }}>
          {problems === 1
            ? "There is 1 problem to fix before the run can start. It is marked above."
            : `There are ${problems} problems to fix before the run can start. They are marked above.`}
        </Alert>
      )}
      <Button
        type="submit"
        variant="contained"
        disabled={invalid || start.isPending || types.isPending}
        sx={{ minHeight: 44 }}
        {...(invalid ? { "aria-describedby": "form-problems" } : {})}
      >
        Start run
      </Button>
    </Paper>
  );
}

export function CompositionPage() {
  const { can } = useAuth();
  return (
    <Box>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 2 }}>Define a community composition</Typography>
      {!can.composeCustom && can.runPredefined && <Alert severity="info" sx={{ mb: 2 }}>{PREDEFINED_ONLY_NOTICE}</Alert>}
      {!can.composeCustom && !can.runPredefined && (
        <Alert severity="info" sx={{ mb: 2 }}>This account cannot start estimates. Ask the administrator if you need to.</Alert>
      )}
      {can.runPredefined && <PredefinedSection />}
      {can.composeCustom && <CustomForm />}
    </Box>
  );
}
