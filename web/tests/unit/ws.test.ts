import { afterEach, describe, expect, it, vi } from "vitest";
import { backoffDelay, ReconnectingSocket, wsUrl, type SocketFactory, type SocketHandlers } from "@/lib/ws";

afterEach(() => vi.useRealTimers());

describe("wsUrl", () => {
  it("same origin, wss behind https, dev override", () => {
    expect(wsUrl("/api/trace/live", { protocol: "http:", host: "192.168.8.10" })).toBe("ws://192.168.8.10/api/trace/live");
    expect(wsUrl("/api/trace/live", { protocol: "https:", host: "stand.local" })).toBe("wss://stand.local/api/trace/live");
    expect(wsUrl("/api/trace/live", { protocol: "http:", host: "localhost:3000" }, "ws://localhost:8000/")).toBe("ws://localhost:8000/api/trace/live");
  });
});

it("backoff is 0.5, 1, 2, 5 s and then stays at 5 s", () => {
  expect([0, 1, 2, 3, 4, 9].map(backoffDelay)).toEqual([500, 1000, 2000, 5000, 5000, 5000]);
});

it("reconnects with backoff, resets after a successful open, and stops for good", () => {
  vi.useFakeTimers();
  const made: { h: SocketHandlers; closed: boolean }[] = [];
  const factory: SocketFactory = (_url, h) => {
    const s = { h, closed: false, close() { this.closed = true; } };
    made.push(s);
    return s;
  };
  const got: unknown[] = [];
  const statuses: string[] = [];
  const sock = new ReconnectingSocket("ws://x/api/trace/1", (m) => got.push(m), (s) => statuses.push(s), factory);
  made[0]!.h.open();
  made[0]!.h.message('{"type":"history"}');
  made[0]!.h.message("not json");
  expect(got).toEqual([{ type: "history" }]);

  made[0]!.h.close(1006);
  vi.advanceTimersByTime(499);
  expect(made).toHaveLength(1);
  vi.advanceTimersByTime(1);
  expect(made).toHaveLength(2);
  made[1]!.h.close(1006);
  vi.advanceTimersByTime(999);
  expect(made).toHaveLength(2);
  vi.advanceTimersByTime(1);
  expect(made).toHaveLength(3);
  made[2]!.h.open();
  made[2]!.h.close(1000);
  vi.advanceTimersByTime(500);
  expect(made).toHaveLength(4); // back to 0.5 s after a good connection

  sock.stop();
  expect(made[3]!.closed).toBe(true);
  made[3]!.h.close(1000);
  vi.advanceTimersByTime(10_000);
  expect(made).toHaveLength(4);
  expect(statuses.slice(0, 3)).toEqual(["connecting", "open", "retrying"]);
});
