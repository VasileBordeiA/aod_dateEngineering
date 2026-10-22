import { toAgentEvent, type AgentEvent } from "@/lib/agent-events";
import { readSse } from "@/lib/sse";
import type { Checkpoint } from "@/lib/trace";

export type Suggestion = { id: string; question: string };
export type LlmStatus = {
  providers: { name: string; model: string; healthy: boolean; last_ok_at: string | null; p50_ms: number | null }[];
  order: string[];
  active: string;
  mode: string;
};
export type SimClock = { sim_ts: string; speed: number | "max"; paused: boolean; mode: string };
export type AskBody = { question: string; origin: "typed" | "suggested" | "ride"; suggestion_id?: string; trace_id?: string };

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return (await r.json()) as T;
}

export const getSuggestions = () => getJson<Suggestion[]>("/api/suggestions");
export const getLlmStatus = () => getJson<LlmStatus>("/api/llm/status");
export const getSimClock = () => getJson<SimClock>("/api/sim/clock");

export async function* askAgent(body: AskBody, signal: AbortSignal): AsyncGenerator<AgentEvent> {
  const r = await fetch("/api/agent/ask", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!r.ok || !r.body) throw new Error(`ask: HTTP ${r.status}`);
  for await (const e of readSse(r.body)) {
    const event = toAgentEvent(e);
    if (event) yield event;
  }
}

export type Zone = { location_id: number; zone: string; borough: string };
export type RideEstimate = { trip_distance: number; duration_min: number; fare_amount: number; method: string };
export type RideRequest = { pickup_location_id: number; dropoff_location_id: number; payment: "card" | "cash"; tip_amount: number; passenger_count: number };
export type RideCreated = { trace_id: string; trip_id: string; trace_url: string; pickup_ts: string; estimate: RideEstimate & { total_amount: number }; notes: string[] };
export type BookResult = { ok: true; ride: RideCreated } | { ok: false; status: number; message: string; retryAfterS?: number };
export type TraceHistory = { trace_id: string; checkpoints: Checkpoint[] };

export const getZones = () => getJson<Zone[]>("/api/zones");
export const getEstimate = (pickup: number, dropoff: number) =>
  getJson<RideEstimate>(`/api/rides/estimate?pickup_location_id=${pickup}&dropoff_location_id=${dropoff}`);
export const getTrace = (id: string) => getJson<TraceHistory>(`/api/trace/${encodeURIComponent(id)}`);

export async function bookRide(body: RideRequest): Promise<BookResult> {
  let r: Response;
  try {
    r = await fetch("/api/rides", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  } catch {
    return { ok: false, status: 0, message: "Can't reach the server. Check your connection and try again." };
  }
  if (r.status === 201) return { ok: true, ride: (await r.json()) as RideCreated };
  const data = (await r.json().catch(() => ({}))) as { detail?: unknown; retry_after_s?: number };
  const message = typeof data.detail === "string" ? data.detail : "Something went wrong. Try again in a moment.";
  return { ok: false, status: r.status, message, retryAfterS: data.retry_after_s };
}
