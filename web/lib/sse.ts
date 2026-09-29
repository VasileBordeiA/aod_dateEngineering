/** SSE over POST (spec 08): EventSource can't POST, so fetch + this parser. It follows the WHATWG rules we need. */
export type SseEvent = { event: string; data: string };

export function createSseParser(onEvent: (e: SseEvent) => void) {
  let buffer = "";
  let event = "";
  let data: string[] = [];

  const dispatch = () => {
    if (data.length > 0) onEvent({ event: event || "message", data: data.join("\n") });
    event = "";
    data = [];
  };

  const line = (raw: string) => {
    if (raw === "") return dispatch();
    if (raw.startsWith(":")) return;
    const colon = raw.indexOf(":");
    const field = colon === -1 ? raw : raw.slice(0, colon);
    let value = colon === -1 ? "" : raw.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") event = value;
    else if (field === "data") data.push(value);
  };

  return {
    feed(chunk: string) {
      buffer += chunk;
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() ?? "";
      lines.forEach(line);
    },
    end() {
      if (buffer) line(buffer.replace(/\r$/, ""));
      buffer = "";
      dispatch();
    },
  };
}

export async function* readSse(body: ReadableStream<Uint8Array>): AsyncGenerator<SseEvent> {
  const queue: SseEvent[] = [];
  const parser = createSseParser((e) => queue.push(e));
  // Decode manually instead of body.pipeThrough(new TextDecoderStream()): with TS 5.9's generic
  // Uint8Array<ArrayBufferLike>, TextDecoderStream's WritableStream<BufferSource> no longer type-checks
  // as a ReadableWritablePair for pipeThrough (see task-10-report.md). Behaviour is identical: streaming
  // UTF-8 decode across arbitrary chunk boundaries.
  const decoder = new TextDecoder();
  const reader = body.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      parser.feed(decoder.decode(value, { stream: true }));
      while (queue.length > 0) yield queue.shift() as SseEvent;
    }
    parser.end();
    while (queue.length > 0) yield queue.shift() as SseEvent;
  } finally {
    reader.releaseLock();
  }
}
