import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { AppRoutes } from "./App";
import { ApiClientProvider } from "./api/context";
import { createMockClient } from "./api/mockClient";
import { AuthProvider } from "./auth/AuthContext";

// Colour-blind-safe primary (Okabe-Ito blue). Swap createMockClient for the generated client when the API exists.
const theme = createTheme({ palette: { primary: { main: "#0072B2" } } });
const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } } });
const client = createMockClient();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={queryClient}>
        <ApiClientProvider client={client}>
          <AuthProvider>
            <BrowserRouter>
              <AppRoutes />
            </BrowserRouter>
          </AuthProvider>
        </ApiClientProvider>
      </QueryClientProvider>
    </ThemeProvider>
  </StrictMode>,
);
