// Minimal text/event-stream parser (WHATWG "server-sent events" framing) for streams read with fetch, because the
// browser's EventSource cannot send an Authorization header. Handles CRLF/LF/CR line ends, comments (pings),
// multi-line data, and chunks that split lines anywhere.

export interface SseMessage {
  event: string;
  data: string;
  /** The `id:` field of this message, if any. */
  id?: string;
}

export class SseParser {
  private buffer = "";
  private event = "";
  private data: string[] = [];
  private id: string | undefined;

  /** Feed decoded text; returns every message completed by it. */
  push(text: string): SseMessage[] {
    this.buffer += text;
    const out: SseMessage[] = [];
    // Keep a trailing "\r" in the buffer: it may be the first half of a "\r\n" split across chunks.
    for (;;) {
      const m = /\r\n|\n|\r(?!$)/.exec(this.buffer);
      if (!m) break;
      const line = this.buffer.slice(0, m.index);
      this.buffer = this.buffer.slice(m.index + m[0].length);
      const msg = this.line(line);
      if (msg) out.push(msg);
    }
    return out;
  }

  private line(line: string): SseMessage | null {
    if (line === "") {
      if (this.data.length === 0) {
        this.event = "";
        this.id = undefined;
        return null;
      }
      const msg: SseMessage = { event: this.event || "message", data: this.data.join("\n"), ...(this.id !== undefined ? { id: this.id } : {}) };
      this.event = "";
      this.data = [];
      this.id = undefined;
      return msg;
    }
    if (line.startsWith(":")) return null; // comment / keep-alive ping
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") this.event = value;
    else if (field === "data") this.data.push(value);
    else if (field === "id" && !value.includes("\0")) this.id = value;
    return null;
  }
}
