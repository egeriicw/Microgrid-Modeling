import { useState, type FormEvent } from "react";
import { Alert, Box, Button, FormControl, FormControlLabel, FormLabel, Paper, Radio, RadioGroup, Typography } from "@mui/material";
import { useNavigate } from "react-router-dom";
import { DEMO_RUN_ID } from "../api/types";
import type { Role } from "../api/types";
import { useAuth } from "../auth/AuthContext";

const ROLES: { role: Role; help: string }[] = [
  { role: "admin", help: "Everything, including building filters" },
  { role: "client", help: "Compose and run, view results" },
  { role: "demo", help: "View the predefined demo results only" },
];

export function LoginPage() {
  const { signIn } = useAuth();
  const navigate = useNavigate();
  const [role, setRole] = useState<Role>("client");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await signIn(role);
      navigate(role === "demo" ? `/results/${DEMO_RUN_ID}` : "/composition");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sign-in failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Paper component="form" onSubmit={submit} sx={{ p: 3, maxWidth: 480, mx: "auto", mt: 6 }}>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 1 }}>
        Sign in
      </Typography>
      <Alert severity="info" sx={{ mb: 2 }}>
        Mock sign-in for development: pick a role. The real service checks credentials and enforces what each role may do.
      </Alert>
      <FormControl>
        <FormLabel id="role-label">Role</FormLabel>
        <RadioGroup aria-labelledby="role-label" value={role} onChange={(e) => setRole(e.target.value as Role)}>
          {ROLES.map((r) => (
            <FormControlLabel
              key={r.role}
              value={r.role}
              control={<Radio />}
              label={
                <Box>
                  <b style={{ textTransform: "capitalize" }}>{r.role}</b> <Typography component="span" variant="body2">{r.help}</Typography>
                </Box>
              }
            />
          ))}
        </RadioGroup>
      </FormControl>
      {error && <Alert severity="error" sx={{ mt: 2 }}>{error}</Alert>}
      <Button type="submit" variant="contained" disabled={busy} sx={{ mt: 2 }}>
        Sign in
      </Button>
    </Paper>
  );
}
