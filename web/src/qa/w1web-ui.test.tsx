// QA review of W1-WEB: the screens wired to the REAL HttpApiClient (fake fetch shaped like the FastAPI router).
// `it.fails` = documented defect; flips when fixed.
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AppRoutes } from "../App";
import { ApiClientProvider } from "../api/context";
import { HttpApiClient, KEY_STORAGE } from "../api/httpClient";
import { createMockClient } from "../api/mockClient";
import { ApiError, type ApiClient } from "../api/types";
import { AuthProvider } from "../auth/AuthContext";
import { renderApp } from "../test/utils";

// ECharts needs a canvas (absent in jsdom): stand in a plain element; the real charts are covered by e2e.
vi.mock("../charts/EChart", () => ({
  EChart: (props: { ariaLabel: string }) => <div role="img" aria-label={props.ariaLabel} />,
}));

const KEY = "client-key-alice-0123456789";
const RUN_ID = "7c0f2a2e-1111-4222-8333-444455556666";
const SEEDS = { composition_seed: 11, archetype_seed: 22, perturbation_seed: 33 };
const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
const ME = {
  subject: "alice",
  role: "client",
  permissions: { "filters.use": false },
  run_inputs: ["composition", "composition_id", "config"],
  may_set_seeds: true,
  limits: { max_total_buildings: 5000, max_ensemble_runs: 1000, default_min_runs: 20, demo_caps: null },
};
const detail = (over: Record<string, unknown> = {}) => ({
  id: RUN_ID,
  status: "running",
  owner: "alice",
  seeds: SEEDS,
  engine_version: "0.3.0",
  progress: {},
  cancel_requested: false,
  error_message: null,
  created_at: "2026-11-17T00:00:00Z",
  started_at: null,
  finished_at: null,
  composition_snapshot: { scenario: { name: "t" }, ensemble: { mode: "adaptive", max_runs: 3 } },
  result: null,
  ...over,
});
const sseBody = (text: string, open = false) =>
  new Response(
    new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode(text));
        if (!open) c.close();
      },
    }),
    { status: 200, headers: { "content-type": "text/event-stream" } },
  );
const progress = (id: number, payload: Record<string, unknown>, message: string) =>
  `event: progress\nid: ${id}\ndata: ${JSON.stringify({ stage: payload.stage, message, payload, ts: "2026-11-17T00:00:00" })}\n\n`;

type Route = (url: string, init: RequestInit) => Response | Promise<Response>;

async function realClient(route: Route) {
  const fetchFn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    if (url.endsWith("/me")) return json(200, ME);
    return url.endsWith("/demo-compositions") ? json(200, { items: ["small-mixed"] }) : route(url, init);
  });
  const client = new HttpApiClient({ fetch: fetchFn as unknown as typeof fetch, storage: sessionStorage, retryDelayMs: 1, maxStreamRetries: 1 });
  await client.login(KEY);
  return { client, fetchFn };
}

/** Render the app on the real client with the session it restores from sessionStorage (as main.tsx does). */
async function renderReal(client: ApiClient, route: string) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  const session = await client.getSession();
  return render(
    <QueryClientProvider client={qc}>
      <ApiClientProvider client={client}>
        <AuthProvider initialSession={session}>
          <MemoryRouter initialEntries={[route]}>
            <AppRoutes pollMs={5} />
          </MemoryRouter>
        </AuthProvider>
      </ApiClientProvider>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: errors are shown as text, never as HTML", () => {
  const evil = '<img src=x onerror="window.__pwned=1"><b>bold</b>';

  it("a server message containing HTML is rendered as literal text on the run screen", async () => {
    const { client } = await realClient(() => json(500, { detail: evil }));
    const { container } = await renderReal(client, `/runs/${RUN_ID}`);
    expect(await screen.findByText(evil)).toBeInTheDocument();
    expect(container.querySelector("main img, main b")).toBeNull();
  });

  it("422 field errors with HTML in loc and msg are rendered as text list items", async () => {
    const { client } = await realClient((url) =>
      url.endsWith("/building-types") ? json(200, { items: [] }) : json(422, { detail: [{ loc: ["body", "<i>composition_id</i>"], msg: evil, type: "value_error" }] }),
    );
    const { container } = await renderReal(client, "/composition");
    await userEvent.click(await screen.findByRole("button", { name: /run this neighbourhood: small mixed/i }));
    expect(await screen.findByText(/the server did not accept the request/i)).toBeInTheDocument();
    expect(screen.getByText(`<i>composition_id</i>: ${evil}`)).toBeInTheDocument();
    expect(container.querySelector("main img, main i, main b")).toBeNull();
  });

  it("a very long server message is shown in full and as text", async () => {
    const long = "x".repeat(5000) + "<script>bad()</script>";
    const base = createMockClient({ pointsPerYear: 96 * 4 });
    const client: ApiClient = { ...base, getResults: async () => { throw new ApiError(500, long); } };
    const { container } = await renderApp({ route: `/results/${RUN_ID}`, role: "client", client });
    expect(await screen.findByText(long)).toBeInTheDocument();
    expect(container.querySelector("script")).toBeNull();
  });

  it("network down on sign-in shows the plain 'cannot reach the API' message", async () => {
    const client = new HttpApiClient({ fetch: (() => Promise.reject(new TypeError("Failed to fetch"))) as unknown as typeof fetch, storage: sessionStorage });
    await renderReal(client, "/login");
    await userEvent.type(screen.getByLabelText(/api key/i), KEY);
    await userEvent.click(screen.getByRole("button", { name: /sign in/i }));
    expect(await screen.findByText(/cannot reach the api/i)).toBeInTheDocument();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: the run screen on the real client", () => {
  const stream =
    progress(1, { stage: "building_pool", run_index: -1, n_attempted: 0, n_feasible: 0, max_runs: 3, converged: null }, "archetype pool ready") +
    progress(2, { stage: "running", run_index: 0, n_attempted: 1, n_feasible: 1, max_runs: 3, converged: null }, "run 1/3 feasible") +
    progress(3, { stage: "running", run_index: 1, n_attempted: 2, n_feasible: 1, max_runs: 3, converged: null }, "run 2/3 rejected");

  it("shows stage, runs completed, feasible/rejected from the SSE stream, and never puts the key in the DOM", async () => {
    const { client } = await realClient((url) => (url.endsWith("/events") ? sseBody(stream, true) : json(200, detail())));
    const { container } = await renderReal(client, `/runs/${RUN_ID}`);
    expect(await screen.findByText(/run 2\/3 rejected/)).toBeInTheDocument();
    expect(screen.getByText(/neighbourhoods simulated \(runs\): 2 of 3/i)).toBeInTheDocument();
    expect(screen.getByText(/^Valid runs: 1 /)).toBeInTheDocument();
    expect(screen.getByText(/^Rejected runs: 1 /)).toBeInTheDocument();
    expect(document.body.innerHTML).not.toContain(KEY);
    void container;
  });

  it("a 401 while the run is open returns to sign-in and removes the key", async () => {
    let n = 0;
    const { client } = await realClient((url) => {
      if (url.endsWith("/events")) return json(401, { detail: "authentication required" });
      n += 1;
      return json(200, detail());
    });
    await renderReal(client, `/runs/${RUN_ID}`);
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    expect(n).toBeGreaterThan(0);
  });

  it("sign-out from the run screen removes the key and stops the stream request", async () => {
    let aborted = false;
    const { client } = await realClient((url, init) => {
      if (url.endsWith("/events")) {
        init.signal?.addEventListener("abort", () => (aborted = true));
        return sseBody(stream, true);
      }
      return json(200, detail());
    });
    await renderReal(client, `/runs/${RUN_ID}`);
    await screen.findByText(/run 2\/3 rejected/);
    await userEvent.click(screen.getByRole("button", { name: /sign out/i }));
    expect(await screen.findByRole("heading", { name: /sign in/i })).toBeInTheDocument();
    expect(sessionStorage.getItem(KEY_STORAGE)).toBeNull();
    await waitFor(() => expect(aborted).toBe(true));
  });

  it("the replay button sends the stored config and the SAME seeds; a 403 from the server is shown", async () => {
    const posts: unknown[] = [];
    const { client } = await realClient((url, init) => {
      if (url.endsWith("/runs") && init.method === "POST") {
        posts.push(JSON.parse(String(init.body)));
        return json(403, { detail: "the demo role may only run a predefined composition with its fixed seed" });
      }
      return json(200, detail({ status: "succeeded" }));
    });
    await renderReal(client, `/runs/${RUN_ID}`);
    await userEvent.click(await screen.findByRole("button", { name: /replay with same seeds/i }));
    expect(await screen.findByText(/demo role may only run a predefined composition/i)).toBeInTheDocument();
    expect(posts).toEqual([{ config: { scenario: { name: "t" }, ensemble: { mode: "adaptive", max_runs: 3 } }, seeds: SEEDS }]);
  });

  it("DEFECT (wording): a finished run that stopped without converging is not described as 'not yet converged'", async () => {
    const { client } = await realClient(() =>
      json(200, detail({ status: "succeeded", progress: { stage: "finished", run_index: 2, n_attempted: 3, n_feasible: 3, max_runs: 3, converged: false, message: "finished: 3 feasible of 3 attempted" } })),
    );
    await renderReal(client, `/runs/${RUN_ID}`);
    await screen.findByText(/neighbourhoods simulated \(runs\): 3 of 3/i);
    expect(screen.queryByText(/not yet converged/i)).toBeNull();
  });

  it("DEFECT (honesty): before any convergence check (converged: null) the screen does not assert 'not yet converged'", async () => {
    const { client } = await realClient((url) => (url.endsWith("/events") ? sseBody(stream, true) : json(200, detail())));
    await renderReal(client, `/runs/${RUN_ID}`);
    await screen.findByText(/run 2\/3 rejected/);
    expect(screen.getByText(/convergence: not (checked|reported)/i)).toBeInTheDocument();
  });
});

// ------------------------------------------------------------------------------------------------------------------
describe("QA W1-WEB: results view on the real client (FR-WEB-8 and honesty)", () => {
  const env = (b: number) => ({ mean_kw: [b, b + 1, b], p10_kw: [b - 1, b, b - 1], p50_kw: [b, b + 1, b], p90_kw: [b + 1, b + 2, b + 1] });
  const doc = {
    schema_version: "1.0",
    seeds: SEEDS,
    datasets: {},
    results: {
      n_runs: 3,
      time: { hourly: { start: "2018-01-01T00:00:00", step_minutes: 60, count: 3 }, "15min": null },
      envelopes: { hourly: { total: env(100), residential: env(60), commercial: env(40) }, "15min": null },
    },
  };

  it("shows the accuracy caveat and says weather year, calibration and temperature are not reported", async () => {
    const { client } = await realClient(() => json(200, doc));
    await renderReal(client, `/results/${RUN_ID}`);
    expect(await screen.findByText(/not validated for engineering or investment decisions/i)).toBeInTheDocument();
    expect(screen.getByText(/weather year: not reported/i)).toBeInTheDocument();
    expect(screen.getByText(/calibration status: not checked against measured data \(no site data was uploaded\)/i)).toBeInTheDocument();
    expect(screen.getByText(/do not include temperature/i)).toBeInTheDocument();
    expect(screen.queryByText(/AMY2018/)).toBeNull();
  });

  it("a run without results (404 'results not available') shows the server's message and no charts", async () => {
    const { client } = await realClient(() => json(404, { detail: "results not available" }));
    await renderReal(client, `/results/${RUN_ID}`);
    expect(await screen.findByText("results not available")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: /load-duration curve/i })).toBeNull();
  });
});
