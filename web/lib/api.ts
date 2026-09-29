import { toAgentEvent, type AgentEvent } from "@/lib/agent-events";
import { readSse } from "@/lib/sse";

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
