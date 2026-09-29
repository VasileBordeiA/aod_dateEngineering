import { describe, expect, it } from "vitest";
import type { AgentEvent } from "@/lib/agent-events";
import { initialState, reduce, type AgentState } from "@/lib/agent-state";

const run = (events: AgentEvent[]): AgentState =>
  events.reduce((s, event) => reduce(s, { type: "event", event }), reduce(initialState, { type: "start", question: "q" }));

describe("reduce", () => {
  it("builds steps, attempts, rows, chart, answer and done", () => {
    const s = run([
      { type: "status", data: { text: "Reading the question…" } },
      { type: "tool_call", data: { id: "c1", name: "describe_table", arguments: { table: "trips" } } },
      { type: "tool_result", data: { id: "c1", name: "describe_table", ok: true, summary: "38 columns" } },
      { type: "tool_call", data: { id: "c2", name: "run_query", arguments: {} } },
      { type: "sql", data: { attempt: 1, sql: "SELECT tip FROM trips", purpose: "x" } },
      { type: "tool_result", data: { id: "c2", name: "run_query", ok: false, summary: "Unknown expression identifier `tip`" } },
      { type: "tool_call", data: { id: "c3", name: "run_query", arguments: {} } },
      { type: "sql", data: { attempt: 2, sql: "SELECT tip_pct FROM trips", purpose: "x" } },
      { type: "rows", data: { columns: [{ name: "tip_pct", type: "Float64" }], rows: [[23.1]], row_count: 1, truncated: false, elapsed_ms: 12 } },
      { type: "tool_result", data: { id: "c3", name: "run_query", ok: true, summary: "1 rows in 12 ms" } },
      { type: "chart", data: { type: "number", x: "tip_pct", y: ["tip_pct"], title: "Tip", unit: "pct" } },
      { type: "answer", data: { text: "23.1%" } },
      { type: "done", data: { request_id: "r", provider: "local", model: "m", mode: "tools", cached: false, latency_ms: 6200, tool_calls: 3, invalid_tool_calls: 0, repairs: 1 } },
    ]);
    expect(s.phase).toBe("done");
    expect(s.steps[0]).toEqual({ kind: "status", text: "Reading the question…" });
    expect(s.steps[1]).toMatchObject({ kind: "tool", label: "describe_table(trips)", state: "ok", summary: "38 columns" });
    expect(s.steps[2]).toMatchObject({ kind: "tool", state: "failed" });
    expect(s.attempts).toEqual([
      { attempt: 1, sql: "SELECT tip FROM trips", purpose: "x", failed: "Unknown expression identifier `tip`" },
      { attempt: 2, sql: "SELECT tip_pct FROM trips", purpose: "x" },
    ]);
    expect(s.rows?.row_count).toBe(1);
    expect(s.chart?.type).toBe("number");
    expect(s.answer).toBe("23.1%");
    expect(s.done?.latency_ms).toBe(6200);
  });

  it("marks a cut stream as an error, but keeps an answer that already arrived", () => {
    const cut = reduce(run([{ type: "status", data: { text: "x" } }]), { type: "stream_cut" });
    expect(cut.phase).toBe("done");
    expect(cut.error?.code).toBe("INTERNAL");
    const answered = reduce(run([{ type: "answer", data: { text: "ok" } }]), { type: "stream_cut" });
    expect(answered.error).toBeUndefined();
  });

  it("start and reset clear everything", () => {
    const s = run([{ type: "answer", data: { text: "old" } }]);
    expect(reduce(s, { type: "start", question: "new" })).toEqual({ ...initialState, phase: "running", question: "new" });
    expect(reduce(s, { type: "reset" })).toEqual(initialState);
  });

  it("drops the old run's SQL, rows and chart when the backup model or the cache takes over", () => {
    const firstRun: AgentEvent[] = [
      { type: "tool_call", data: { id: "oneshot_1", name: "run_query", arguments: {} } },
      { type: "sql", data: { attempt: 1, sql: "SELECT 1", purpose: "x" } },
      { type: "rows", data: { columns: [{ name: "x", type: "UInt8" }], rows: [[1]], row_count: 1, truncated: false, elapsed_ms: 1 } },
      { type: "tool_result", data: { id: "oneshot_1", name: "run_query", ok: true, summary: "1 rows in 1 ms" } },
      { type: "tool_call", data: { id: "oneshot_2", name: "render_chart", arguments: {} } },
      { type: "chart", data: { type: "number", x: "x", y: ["x"], title: "t", unit: "none" } },
      { type: "tool_result", data: { id: "oneshot_2", name: "render_chart", ok: true, summary: "number chart" } },
    ];
    for (const text of [
      "Switching to the backup model…",
      "AI is offline, so I'm using a pre-written query on live data",
      "That took too long, so I'm using a pre-written query on live data",
    ]) {
      const s = run([...firstRun, { type: "status", data: { text } }]);
      expect(s.attempts).toEqual([]);
      expect(s.rows).toBeUndefined();
      expect(s.chart).toBeUndefined();
      expect(s.question).toBe("q");
      expect(s.steps).toHaveLength(3);
      expect(s.steps.at(-1)).toEqual({ kind: "status", text });
    }
    const other = run([...firstRun, { type: "status", data: { text: "Fixing its own query…" } }]);
    expect(other.attempts).toHaveLength(1);
    expect(other.rows).toBeDefined();
  });

  it("a retried run that reuses a step id only finishes its own step", () => {
    const s = run([
      { type: "tool_call", data: { id: "oneshot_1", name: "run_query", arguments: {} } },
      { type: "sql", data: { attempt: 1, sql: "SELECT bad", purpose: "x" } },
      { type: "tool_result", data: { id: "oneshot_1", name: "run_query", ok: false, summary: "boom" } },
      { type: "status", data: { text: "Switching to the backup model…" } },
      { type: "tool_call", data: { id: "oneshot_1", name: "run_query", arguments: {} } },
      { type: "sql", data: { attempt: 1, sql: "SELECT good", purpose: "x" } },
      { type: "tool_result", data: { id: "oneshot_1", name: "run_query", ok: true, summary: "1 rows in 1 ms" } },
    ]);
    expect(s.steps[0]).toMatchObject({ kind: "tool", state: "failed", summary: "boom" });
    expect(s.steps[2]).toMatchObject({ kind: "tool", state: "ok", summary: "1 rows in 1 ms" });
    expect(s.attempts).toEqual([{ attempt: 1, sql: "SELECT good", purpose: "x" }]);
  });
});
