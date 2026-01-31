import { useEffect, useMemo, useState } from 'react';

import { Box, Chip, LinearProgress, Stack, Tooltip, Typography } from '@mui/material';

import { fetchActiveRuns, RunSummary } from '@/lib/api';

function pct(current?: number | null, total?: number | null): number | null {
  if (current == null || total == null || total <= 0) return null;
  const p = Math.round((current / total) * 100);
  return Math.max(0, Math.min(100, p));
}

/**
 * Global jobs indicator.
 *
 * Best-practice behavior:
 * - Poll only while there are active jobs.
 * - Show a compact progress indicator in the header.
 */
export function JobsWidget() {
  const [runs, setRuns] = useState<RunSummary[]>([]);

  useEffect(() => {
    let alive = true;

    async function tick() {
      const data = await fetchActiveRuns();
      if (!alive) return;
      setRuns(data);
    }

    tick();

    const id = window.setInterval(tick, 4000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, []);

  const active = runs.filter((r) => r.status === 'queued' || r.status === 'running');

  const label = useMemo(() => {
    if (active.length === 0) return 'No active runs';
    if (active.length === 1) return '1 active run';
    return `${active.length} active runs`;
  }, [active.length]);

  if (active.length === 0) {
    return <Chip label={label} size="small" variant="outlined" />;
  }

  const primary = active[0];
  const percent = pct(primary.progressCurrent, primary.progressTotal);

  return (
    <Tooltip
      title={
        <Stack spacing={1} sx={{ p: 1 }}>
          <Typography variant="subtitle2">Active runs</Typography>
          {active.slice(0, 5).map((r) => (
            <Box key={r.id}>
              <Typography variant="body2">{r.id}</Typography>
              <Typography variant="caption" color="text.secondary">
                {r.status}
                {r.progressMessage ? ` — ${r.progressMessage}` : ''}
              </Typography>
            </Box>
          ))}
          {active.length > 5 ? <Typography variant="caption">…and more</Typography> : null}
        </Stack>
      }
    >
      <Stack spacing={0.5} sx={{ minWidth: 220 }}>
        <Chip label={label} size="small" color="primary" />
        {percent == null ? (
          <LinearProgress />
        ) : (
          <LinearProgress variant="determinate" value={percent} />
        )}
      </Stack>
    </Tooltip>
  );
}
