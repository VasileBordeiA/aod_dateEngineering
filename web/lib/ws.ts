/** WebSocket with reconnect + backoff (spec 08 §/trace/[id]: 0.5, 1, 2, 5 s). Messages are parsed JSON. */
export const BACKOFF_MS = [500, 1000, 2000, 5000] as const;
export const backoffDelay = (attempt: number): number => BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)]!;

/** Same origin on the stand (Caddy serves everything). In dev, NEXT_PUBLIC_API_WS_ORIGIN points at the api. */
export function wsUrl(path: string, loc: { protocol: string; host: string }, origin?: string): string {
  if (origin) return `${origin.replace(/\/$/, "")}${path}`;
  return `${loc.protocol === "https:" ? "wss:" : "ws:"}//${loc.host}${path}`;
}

export type SocketHandlers = { open: () => void; close: (code: number) => void; message: (data: string) => void };
export type SocketLike = { close: () => void };
export type SocketFactory = (url: string, handlers: SocketHandlers) => SocketLike;
export type SocketStatus = "connecting" | "open" | "retrying";

export const browserSocket: SocketFactory = (url, h) => {
  const ws = new WebSocket(url);
  ws.onopen = () => h.open();
  ws.onclose = (e) => h.close(e.code);
  ws.onmessage = (e) => h.message(String(e.data));
  return ws;
};

export class ReconnectingSocket {
  private attempt = 0;
  private socket: SocketLike | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;

  constructor(
    private readonly url: string,
    private readonly onMessage: (data: unknown) => void,
    private readonly onStatus: (s: SocketStatus) => void = () => {},
    private readonly factory: SocketFactory = browserSocket,
  ) {
    this.connect();
  }

  private connect(): void {
    if (this.stopped) return;
    this.onStatus("connecting");
    this.socket = this.factory(this.url, {
      open: () => {
        this.attempt = 0;
        this.onStatus("open");
      },
      message: (data) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(data);
        } catch {
          return; // a malformed frame is skipped, never fatal
        }
        this.onMessage(parsed);
      },
      close: () => {
        if (this.stopped) return;
        this.onStatus("retrying");
        this.timer = setTimeout(() => this.connect(), backoffDelay(this.attempt++));
      },
    });
  }

  stop(): void {
    this.stopped = true;
    if (this.timer !== null) clearTimeout(this.timer);
    this.socket?.close();
  }
}
