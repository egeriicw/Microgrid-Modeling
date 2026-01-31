import { useRouter } from 'next/router';
import { useEffect, useState } from 'react';

import { Button, Container, Stack, TextField, Typography } from '@mui/material';

import { AppShell } from '@/components/AppShell';

type ConfigOut = {
  id: number;
  name: string;
  yaml_text: string;
  created_at: string;
  updated_at: string;
};

async function fetchConfig(id: number): Promise<ConfigOut> {
  const res = await fetch(`/api/configs/${id}`);
  if (!res.ok) throw new Error('Not found');
  return (await res.json()) as ConfigOut;
}

async function updateConfig(id: number, payload: { name?: string; yaml_text?: string }) {
  const res = await fetch(`/api/configs/${id}`, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error('Failed to update');
  return (await res.json()) as ConfigOut;
}

export default function ConfigDetailPage() {
  const router = useRouter();
  const idNum = typeof router.query.id === 'string' ? Number(router.query.id) : NaN;

  const [cfg, setCfg] = useState<ConfigOut | null>(null);
  const [name, setName] = useState('');
  const [yamlText, setYamlText] = useState('');

  useEffect(() => {
    if (!Number.isFinite(idNum)) return;
    fetchConfig(idNum).then((c) => {
      setCfg(c);
      setName(c.name);
      setYamlText(c.yaml_text);
    });
  }, [idNum]);

  return (
    <AppShell>
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Stack spacing={2}>
          <Typography variant="h4">Config</Typography>

          {cfg ? (
            <Typography color="text.secondary">id={cfg.id}</Typography>
          ) : (
            <Typography color="text.secondary">Loading…</Typography>
          )}

          <TextField label="Name" value={name} onChange={(e) => setName(e.target.value)} />
          <TextField
            label="config.yaml"
            value={yamlText}
            onChange={(e) => setYamlText(e.target.value)}
            multiline
            minRows={12}
            spellCheck={false}
            inputProps={{ style: { fontFamily: 'monospace' } }}
          />

          <Stack direction="row" spacing={2}>
            <Button variant="outlined" href="/configs">
              Back
            </Button>
            <Button
              variant="contained"
              disabled={!cfg}
              onClick={async () => {
                if (!cfg) return;
                const updated = await updateConfig(cfg.id, { name, yaml_text: yamlText });
                setCfg(updated);
              }}
            >
              Save
            </Button>
          </Stack>
        </Stack>
      </Container>
    </AppShell>
  );
}
