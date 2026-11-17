import { lazy, Suspense, type ReactNode } from "react";
import { AppBar, Box, Button, Container, LinearProgress, Link as MuiLink, Toolbar, Typography } from "@mui/material";
import { Link as RouterLink, Navigate, Outlet, Route, Routes, useNavigate } from "react-router-dom";
import { DEMO_RUN_ID } from "./api/types";
import { useAuth } from "./auth/AuthContext";
import { CompositionPage } from "./features/composition/CompositionPage";
import { RunPage } from "./features/run/RunPage";
import { LoginPage } from "./pages/LoginPage";

// ECharts lives in this chunk so the sign-in and form screens load without it.
const ResultsPage = lazy(() => import("./features/results/ResultsPage").then((m) => ({ default: m.ResultsPage })));

function RequireAuth({ children }: { children: ReactNode }) {
  const { session } = useAuth();
  return session ? <>{children}</> : <Navigate to="/login" replace />;
}

function Layout() {
  const { session, signOut, can } = useAuth();
  const navigate = useNavigate();
  return (
    <>
      <MuiLink
        href="#main"
        sx={{ position: "absolute", left: -9999, "&:focus": { left: 8, top: 8, zIndex: 2000, bgcolor: "background.paper", p: 1 } }}
      >
        Skip to main content
      </MuiLink>
      <AppBar position="static" color="primary" enableColorOnDark>
        <Toolbar sx={{ gap: 2, flexWrap: "wrap" }}>
          <Typography variant="h6" component="p" sx={{ mr: 2 }}>
            Community Energy Profiler
          </Typography>
          {session && (
            <>
              <Box component="nav" aria-label="Main" sx={{ display: "flex", gap: 1, flexGrow: 1 }}>
                {can.startRuns && (
                  <Button color="inherit" component={RouterLink} to="/composition">
                    Compose
                  </Button>
                )}
                <Button color="inherit" component={RouterLink} to={`/results/${DEMO_RUN_ID}`}>
                  Demo results
                </Button>
              </Box>
              <Typography variant="body2">Signed in as {session.role}</Typography>
              <Button
                color="inherit"
                onClick={async () => {
                  await signOut();
                  navigate("/login");
                }}
              >
                Sign out
              </Button>
            </>
          )}
        </Toolbar>
      </AppBar>
      <Container component="main" id="main" tabIndex={-1} maxWidth="lg" sx={{ py: 3 }}>
        <Outlet />
      </Container>
    </>
  );
}

/** `pollMs` is how often the run screen asks the API for progress (tests use a tiny value). */
export function AppRoutes({ pollMs = 1000 }: { pollMs?: number }) {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/composition" element={<RequireAuth><CompositionPage /></RequireAuth>} />
        <Route path="/runs/:runId" element={<RequireAuth><RunPage pollMs={pollMs} /></RequireAuth>} />
        <Route
          path="/results/:runId"
          element={
            <RequireAuth>
              <Suspense fallback={<LinearProgress aria-label="Loading results view" />}>
                <ResultsPage />
              </Suspense>
            </RequireAuth>
          }
        />
        <Route path="*" element={<Navigate to="/composition" replace />} />
      </Route>
    </Routes>
  );
}
