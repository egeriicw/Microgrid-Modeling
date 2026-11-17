import { describe, expect, it } from "vitest";
import { SseParser } from "./sse";

describe("SseParser", () => {
  it("joins multi-line data, keeps the id, and defaults the event name", () => {
    const p = new SseParser();
    expect(p.push("id: 5\ndata: a\ndata: b\n\ndata: c\n\n")).toEqual([
      { event: "message", data: "a\nb", id: "5" },
      { event: "message", data: "c" },
    ]);
  });

  it("handles a CRLF split between chunks and bare CR line ends", () => {
    const p = new SseParser();
    expect(p.push("event: x\r")).toEqual([]);
    expect(p.push("\ndata: 1\r\r")).toEqual([]); // trailing CR held back: it could start a CRLF
    expect(p.push("event: y\rdata: 2\r\r\n")).toEqual([
      { event: "x", data: "1" },
      { event: "y", data: "2" },
    ]);
  });

  it("ignores comments and blank events, and does not emit an unterminated message", () => {
    const p = new SseParser();
    expect(p.push(": ping\n\n\nevent: progress\ndata: {}")).toEqual([]);
    expect(p.push("\n\n")).toEqual([{ event: "progress", data: "{}" }]);
  });
});
