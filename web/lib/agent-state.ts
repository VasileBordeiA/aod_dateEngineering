/** Everything /agent shows is a pure fold of the SSE events (spec 08: no business logic in the frontend). */
import type { AgentEvent, ChartSpec, DoneData, ErrorData, RowsData } from "@/lib/agent-events";

export type Step =
  | { kind: "status"; text: string }
  | { kind: "tool"; id: string; name: string; label: string; state: "running" | "ok" | "failed"; summary?: string };
export type Attempt = { attempt: number; sql: string; purpose: string; failed?: string };
export type AgentState = {
  phase: "idle" | "running" | "done";
  question?: string;
  steps: Step[];
  attempts: Attempt[];
  rows?: RowsData;
  chart?: ChartSpec;
  answer?: string;
  error?: ErrorData;
  done?: DoneData;
};
export type Action =
  | { type: "start"; question: string }
  | { type: "event"; event: AgentEvent }
  | { type: "stream_cut" }
  | { type: "reset" };

export const initialState: AgentState = { phase: "idle", steps: [], attempts: [] };

function toolLabel(name: string, args: Record<string, unknown>): string {
  if (name === "describe_table" && typeof args.table === "string") return `describe_table(${args.table})`;
  if (name === "render_chart" && typeof args.type === "string") return `render_chart(${args.type})`;
  return name;
}

function apply(state: AgentState, event: AgentEvent): AgentState {
  switch (event.type) {
    case "status":
      return { ...state, steps: [...state.steps, { kind: "status", text: event.data.text }] };
    case "tool_call": {
      const { id, name, arguments: args } = event.data;
      return { ...state, steps: [...state.steps, { kind: "tool", id, name, label: toolLabel(name, args), state: "running" }] };
    }
    case "tool_result": {
      const { id, name, ok, summary } = event.data;
      const steps = state.steps.map((s) => (s.kind === "tool" && s.id === id ? { ...s, state: ok ? "ok" : "failed", summary } as Step : s));
      let attempts = state.attempts;
      const last = attempts.at(-1);
      if (name === "run_query" && !ok && last && !last.failed) attempts = [...attempts.slice(0, -1), { ...last, failed: summary }];
      return { ...state, steps, attempts };
    }
    case "sql":
      return { ...state, attempts: [...state.attempts, { attempt: event.data.attempt, sql: event.data.sql, purpose: event.data.purpose }] };
    case "rows":
      return { ...state, rows: event.data };
    case "chart":
      return { ...state, chart: event.data };
    case "answer":
      return { ...state, answer: event.data.text };
    case "error":
      return { ...state, error: event.data };
    case "done":
      return { ...state, phase: "done", done: event.data };
  }
}

export function reduce(state: AgentState, action: Action): AgentState {
  switch (action.type) {
    case "start":
      return { ...initialState, phase: "running", question: action.question };
    case "event":
      return apply(state, action.event);
    case "stream_cut":
      if (state.phase !== "running") return state;
      return state.answer
        ? { ...state, phase: "done" }
        : { ...state, phase: "done", error: { code: "INTERNAL", message: "The connection dropped. Try again!" } };
    case "reset":
      return initialState;
  }
}
