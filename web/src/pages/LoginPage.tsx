import { useState, type FormEvent } from "react";
import { Alert, Button, LinearProgress, Paper, TextField, Typography } from "@mui/material";
import { useNavigate } from "react-router-dom";
import { useApiClient } from "../api/context";
import { useAuth } from "../auth/AuthContext";

export function LoginPage() {
  const { signIn } = useAuth();
  const client = useApiClient();
  const navigate = useNavigate();
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await signIn(key);
      setKey("");
      navigate("/composition");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Sign-in failed.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <Paper component="form" onSubmit={submit} sx={{ p: 3, maxWidth: 480, mx: "auto", mt: 6 }} noValidate>
      <Typography variant="h1" sx={{ fontSize: "1.75rem", mb: 1 }}>
        Sign in
      </Typography>
      <Typography variant="body2" sx={{ mb: 2 }}>
        Paste the API key you were given. The server decides what your key may do (admin, client or demo). The key is
        kept only in this browser tab and is forgotten when you sign out or close the tab.
      </Typography>
      {client.kind === "mock" && (
        <Alert severity="info" sx={{ mb: 2 }}>
          Mock mode (no server): type admin, client or demo as the key.
        </Alert>
      )}
      <TextField
        label="API key"
        type="password"
        value={key}
        onChange={(e) => setKey(e.target.value)}
        fullWidth
        required
        slotProps={{ htmlInput: { autoComplete: "off", spellCheck: false } }}
      />
      {busy && <LinearProgress aria-label="Checking the key" sx={{ mt: 2 }} />}
      {error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}
      <Button type="submit" variant="contained" disabled={busy || key.trim() === ""} sx={{ mt: 2 }}>
        Sign in
      </Button>
    </Paper>
  );
}
