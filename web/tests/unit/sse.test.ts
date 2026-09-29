import { describe, expect, it } from "vitest";
import { createSseParser, readSse, type SseEvent } from "@/lib/sse";

function parseAll(chunks: string[]): SseEvent[] {
  const out: SseEvent[] = [];
  const p = createSseParser((e) => out.push(e));
  chunks.forEach((c) => p.feed(c));
  p.end();
  return out;
}

describe("createSseParser", () => {
  it("parses frames split across arbitrary chunks", () => {
    const text = 'event: status\ndata: {"text":"Reading the question…"}\n\nevent: done\ndata: {"cached":false}\n\n';
    const whole = parseAll([text]);
    for (let size = 1; size < 12; size++) {
      const chunks = text.match(new RegExp(`[\\s\\S]{1,${size}}`, "g")) ?? [];
      expect(parseAll(chunks)).toEqual(whole);
    }
    expect(whole).toEqual([
      { event: "status", data: '{"text":"Reading the question…"}' },
      { event: "done", data: '{"cached":false}' },
    ]);
  });

  it("joins multi-line data with newlines", () => {
    expect(parseAll(["event: sql\ndata: line one\ndata: line two\n\n"])).toEqual([
      { event: "sql", data: "line one\nline two" },
    ]);
  });

  it("dispatches a final frame without a trailing newline", () => {
    expect(parseAll(['event: answer\ndata: {"text":"Manhattan"}'])).toEqual([
      { event: "answer", data: '{"text":"Manhattan"}' },
    ]);
  });

  it("handles CRLF, comments and a missing event name", () => {
    expect(parseAll([": keep-alive\r\ndata: x\r\n", "\r\n"])).toEqual([{ event: "message", data: "x" }]);
  });
});

describe("readSse", () => {
  it("reads events from a byte stream", async () => {
    const bytes = new TextEncoder().encode('event: status\ndata: {"text":"a"}\n\nevent: done\ndata: {}\n\n');
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(bytes.slice(0, 10));
        c.enqueue(bytes.slice(10));
        c.close();
      },
    });
    const events: SseEvent[] = [];
    for await (const e of readSse(body)) events.push(e);
    expect(events.map((e) => e.event)).toEqual(["status", "done"]);
  });

  it("reassembles a multi-byte character split across chunks", async () => {
    // "…" is E2 80 A6 in UTF-8: split right after the lead byte.
    const bytes = new TextEncoder().encode('event: status\ndata: {"text":"Reading the question…"}\n\n');
    const cut = bytes.indexOf(0xe2) + 1;
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(bytes.slice(0, cut));
        c.enqueue(bytes.slice(cut));
        c.close();
      },
    });
    const events: SseEvent[] = [];
    for await (const e of readSse(body)) events.push(e);
    expect(events).toEqual([{ event: "status", data: '{"text":"Reading the question…"}' }]);
  });

  it("flushes a dangling UTF-8 lead byte at stream end as a replacement character", async () => {
    const head = new TextEncoder().encode('event: status\ndata: {"text":"a');
    const bytes = new Uint8Array([...head, 0xe2]); // lead byte of "…", never completed
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(bytes);
        c.close();
      },
    });
    const events: SseEvent[] = [];
    for await (const e of readSse(body)) events.push(e);
    expect(events).toEqual([{ event: "status", data: '{"text":"a�' }]);
  });
});
