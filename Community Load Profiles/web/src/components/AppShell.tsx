import { PropsWithChildren } from 'react';

import { AppBar, Box, Toolbar, Typography } from '@mui/material';

import { JobsWidget } from '@/components/JobsWidget';

export function AppShell({ children }: PropsWithChildren) {
  return (
    <Box sx={{ minHeight: '100vh', bgcolor: 'background.default', color: 'text.primary' }}>
      <AppBar position="static" color="default" elevation={1}>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            Microgrid Modeling
          </Typography>
          <JobsWidget />
        </Toolbar>
      </AppBar>

      <Box component="main">{children}</Box>
    </Box>
  );
}
