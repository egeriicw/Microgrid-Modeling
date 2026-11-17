import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useApiClient } from "../api/context";
import type { Session } from "../api/types";
import { canStartRuns, canUseFilters } from "./roles";

interface AuthValue {
  session: Session | null;
  /** Validates the API key with the server and keeps it for this browser tab. */
  signIn(apiKey: string): Promise<Session>;
  signOut(): Promise<void>;
  /** UI affordances only. The server enforces every permission. */
  can: { startRuns: boolean; useFilters: boolean };
}

const Ctx = createContext<AuthValue | null>(null);

export function AuthProvider({ initialSession = null, children }: { initialSession?: Session | null; children: ReactNode }) {
  const client = useApiClient();
  const queryClient = useQueryClient();
  const [session, setSession] = useState<Session | null>(initialSession);

  // The server rejected the stored key (401): drop the session and cached data; RequireAuth sends the user to sign-in.
  useEffect(
    () =>
      client.onSignedOut(() => {
        queryClient.clear();
        setSession(null);
      }),
    [client, queryClient],
  );

  const signIn = useCallback(
    async (apiKey: string) => {
      const s = await client.login(apiKey);
      queryClient.clear();
      setSession(s);
      return s;
    },
    [client, queryClient],
  );
  const signOut = useCallback(async () => {
    await client.logout();
    queryClient.clear();
    setSession(null);
  }, [client, queryClient]);

  const value = useMemo<AuthValue>(
    () => ({
      session,
      signIn,
      signOut,
      can: { startRuns: session ? canStartRuns(session.role) : false, useFilters: session ? canUseFilters(session.role) : false },
    }),
    [session, signIn, signOut],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth(): AuthValue {
  const v = useContext(Ctx);
  if (!v) throw new Error("useAuth: no AuthProvider above this component");
  return v;
}
