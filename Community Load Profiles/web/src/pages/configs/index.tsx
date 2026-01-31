import { useEffect, useState } from 'react';

import {
  Box,
  Button,
  Container,
  Divider,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  Stack,
  Typography,
} from '@mui/material';

import { AppShell } from '@/components/AppShell';

type ConfigOut = {
  id: number;
  name: string;
  yaml_text: string;
  created_at: string;
  updated_at: string;
};

async function fetchConfigs(): Promise<ConfigOut[]> {
  const res = await fetch('/api/configs');
  if (!res.ok) return [];
  return (await res.json()) as ConfigOut[];
}

export default function ConfigListPage() {
  const [items, setItems] = useState<ConfigOut[]>([]);

  useEffect(() => {
    let alive = true;
    fetchConfigs().then((data) => {
      if (alive) setItems(data);
    });
    return () => {
      alive = false;
    };
  }, []);

  return (
    <AppShell>
      <Container maxWidth="md" sx={{ py: 4 }}>
        <Stack spacing={2}>
          <Typography variant="h4">Configs</Typography>
          <Typography color="text.secondary">
            Stored scenario configurations. (Backend endpoint provided by PR #6.)
          </Typography>

          <Box>
            <Button variant="contained" href="/configs/new">
              New config
            </Button>
          </Box>

          <Divider />

          <List disablePadding>
            {items.map((c) => (
              <ListItem key={c.id} disablePadding>
                <ListItemButton component="a" href={`/configs/${c.id}`}
                  >
                  <ListItemText primary={c.name} secondary={`id=${c.id}`} />
                </ListItemButton>
              </ListItem>
            ))}
            {items.length === 0 ? (
              <ListItem>
                <ListItemText primary="No configs yet." />
              </ListItem>
            ) : null}
          </List>
        </Stack>
      </Container>
    </AppShell>
  );
}
