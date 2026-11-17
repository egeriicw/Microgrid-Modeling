import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { AppRoutes } from "./App";
import { ApiClientProvider } from "./api/context";
import { ApiError } from "./api/types";
import { createClientFromEnv } from "./api/select";
import { AuthProvider } from "./auth/AuthContext";

// Colour-blind-safe primary (Okabe-Ito blue).
const theme = createTheme({ palette: { primary: { main: "#0072B2" } } });
// Retry once, but never on a client error (401/403/404/422): asking again will not change the answer.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (n, err) => n < 1 && !(err instanceof ApiError && err.status >= 400 && err.status < 500),
      refetchOnWindowFocus: false,
    },
  },
});
// VITE_API_MODE=mock selects the fixture client; otherwise the real API at VITE_API_BASE_URL (default /api/v1).
const client = createClientFromEnv(import.meta.env);

void client.getSession().then((initialSession) =>
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <QueryClientProvider client={queryClient}>
          <ApiClientProvider client={client}>
            <AuthProvider initialSession={initialSession}>
              <BrowserRouter>
                <AppRoutes />
              </BrowserRouter>
            </AuthProvider>
          </ApiClientProvider>
        </QueryClientProvider>
      </ThemeProvider>
    </StrictMode>,
  ),
);
