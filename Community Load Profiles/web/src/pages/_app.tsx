import type { AppProps } from 'next/app';

import { CssBaseline } from '@mui/material';

import { AppThemeProvider } from '@/styles/theme';

export default function App({ Component, pageProps }: AppProps) {
  return (
    <AppThemeProvider>
      <CssBaseline />
      <Component {...pageProps} />
    </AppThemeProvider>
  );
}
