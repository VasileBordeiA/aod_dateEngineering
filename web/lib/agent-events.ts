/** The spec 06 SSE event contract, as TypeScript. */
import type { SseEvent } from "@/lib/sse";

export type Unit = "usd" | "pct" | "minutes" | "miles" | "trips" | "mph" | "ms" | "none";
export type ErrorCode = "TOO_LONG" | "RATE_LIMITED" | "LLM_UNAVAILABLE" | "TIMEOUT" | "NO_WORKING_QUERY" | "INTERNAL";
export type Column = { name: string; type: string };
export type ChartSpec = { type: "bar" | "line" | "number" | "table"; x: string; y: string[]; series?: string; title: string; unit?: Unit };
export type RowsData = { columns: Column[]; rows: unknown[][]; row_count: number; truncated: boolean; elapsed_ms: number };
export type ErrorData = { code: ErrorCode; message: string; retry_after_s?: number };
export type DoneData = {
  request_id: string; provider: string; model: string; mode: string; cached: boolean; latency_ms: number;
  tool_calls: number; invalid_tool_calls: number; repairs: number;
};

export type AgentEvent =
  | { type: "status"; data: { text: string } }
  | { type: "tool_call"; data: { id: string; name: string; arguments: Record<string, unknown> } }
  | { type: "tool_result"; data: { id: string; name: string; ok: boolean; summary: string } }
  | { type: "sql"; data: { attempt: number; sql: string; purpose: string } }
  | { type: "rows"; data: RowsData }
  | { type: "chart"; data: ChartSpec }
  | { type: "answer"; data: { text: string } }
  | { type: "error"; data: ErrorData }
  | { type: "done"; data: DoneData };

const TYPES = new Set(["status", "tool_call", "tool_result", "sql", "rows", "chart", "answer", "error", "done"]);

export function toAgentEvent(e: SseEvent): AgentEvent | null {
  if (!TYPES.has(e.event)) return null;
  try {
    return { type: e.event, data: JSON.parse(e.data) } as AgentEvent;
  } catch {
    return null;
  }
}
