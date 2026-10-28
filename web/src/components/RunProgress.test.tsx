/**
 * FR-F6 — RunProgress: SSE-driven progress bar, n_feasible, stage, log tail,
 * cancel, and onComplete on the terminal event.
 * RED placeholder (docs/frontend-spec.md §3, docs/test-plan.md §4 Phase 4).
 */
import { describe, it, expect, jest } from "@jest/globals";
import { render, screen, waitFor } from "@testing-library/react";

// @ts-expect-error — component does not exist yet (RED)
import { RunProgress } from "./RunProgress";

type FakeStream = { emit: (event: string, data: unknown) => void };

function mockRunEvents(): { handle: unknown; stream: FakeStream } {
  const listeners: Record<string, ((d: unknown) => void)[]> = {};
  const handle = {
    on: (ev: string, cb: (d: unknown) => void) => {
      (listeners[ev] ??= []).push(cb);
    },
    close: jest.fn(),
  };
  return {
    handle,
    stream: { emit: (ev, data) => (listeners[ev] ?? []).forEach((cb) => cb(data)) },
  };
}

describe.skip("RunProgress (FR-F6)", () => {
  it("advances to 100% and calls onComplete on `done`", async () => {
    const { handle, stream } = mockRunEvents();
    const onComplete = jest.fn();
    render(<RunProgress runId="r1" _events={handle} onComplete={onComplete} />);

    stream.emit("progress", { stage: "running", run_index: 3, n_attempted: 3, n_feasible: 2, total: 6 });
    await waitFor(() => expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "50"));

    stream.emit("progress", { stage: "done", n_attempted: 6, n_feasible: 5, converged: true });
    await waitFor(() => expect(onComplete).toHaveBeenCalled());
    expect(screen.getByText(/5/)).toBeInTheDocument(); // n_feasible
  });

  it("renders the error message on `failed`", async () => {
    const { handle, stream } = mockRunEvents();
    render(<RunProgress runId="r2" _events={handle} onComplete={jest.fn()} />);
    stream.emit("progress", { stage: "failed", message: "engine blew up" });
    await waitFor(() => expect(screen.getByText(/engine blew up/)).toBeInTheDocument());
  });

  it("shows a cancel button only for non-terminal runs", async () => {
    const { handle, stream } = mockRunEvents();
    render(<RunProgress runId="r3" _events={handle} onComplete={jest.fn()} />);
    stream.emit("progress", { stage: "running", run_index: 1, n_attempted: 1, total: 6 });
    expect(screen.getByRole("button", { name: /cancel/i })).toBeEnabled();
    stream.emit("progress", { stage: "cancelled" });
    await waitFor(() => expect(screen.queryByRole("button", { name: /cancel/i })).toBeDisabled());
  });
});
