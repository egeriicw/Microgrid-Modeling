import { Container, Stack, Typography } from '@mui/material';

import { AppShell } from '@/components/AppShell';

export default function HomePage() {
  return (
    <AppShell>
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Stack spacing={2}>
          <Typography variant="h4">Microgrid Run Browser</Typography>
          <Typography color="text.secondary">
            This is the Next.js (TypeScript, Pages Router) foundation. Next PRs will add config editing,
            async run launch/status, and D3 charts.
          </Typography>
        </Stack>
      </Container>
    </AppShell>
  );
}
