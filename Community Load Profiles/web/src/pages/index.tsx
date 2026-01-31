import { Button, Container, Stack, Typography } from '@mui/material';

import { AppShell } from '@/components/AppShell';

export default function HomePage() {
  return (
    <AppShell>
      <Container maxWidth="lg" sx={{ py: 4 }}>
        <Stack spacing={2}>
          <Typography variant="h4">Microgrid Modeling</Typography>
          <Typography color="text.secondary">
            Start here:
          </Typography>
          <Stack direction="row" spacing={2}>
            <Button variant="contained" href="/configs">
              Configs
            </Button>
          </Stack>
        </Stack>
      </Container>
    </AppShell>
  );
}
