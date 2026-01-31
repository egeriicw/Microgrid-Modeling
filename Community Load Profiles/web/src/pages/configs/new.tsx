import { useState } from 'react';

import { Button, Container, Stack, TextField, Typography } from '@mui/material';

import { AppShell } from '@/components/AppShell';

async function createConfig(payload: { name: string; yaml_text: string }) {
  const res = await fetch('/api/configs', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error('Failed to create config');
  return (await res.json()) as { id: number };
}

export default function NewConfigPage() {
  const [name, setName] = useState('');
  const [yamlText, setYamlText] = useState('state: DC\nupgrade_num: 0\n');
  const [busy, setBusy] = useState(false);

  return (
    <AppShell>
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Stack spacing={2}>
          <Typography variant="h4">New config</Typography>

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
              Cancel
            </Button>
            <Button
              variant="contained"
              disabled={busy || name.trim().length === 0 || yamlText.trim().length === 0}
              onClick={async () => {
                setBusy(true);
                try {
                  const out = await createConfig({ name, yaml_text: yamlText });
                  window.location.href = `/configs/${out.id}`;
                } finally {
                  setBusy(false);
                }
              }}
            >
              Create
            </Button>
          </Stack>
        </Stack>
      </Container>
    </AppShell>
  );
}
