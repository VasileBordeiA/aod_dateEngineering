"use client";
import { useEffect, useState } from "react";
import type { AgentState } from "@/lib/agent-state";

function Countdown({ from }: { from: number }) {
  const [left, setLeft] = useState(from);
  useEffect(() => {
    const id = setInterval(() => setLeft((s) => Math.max(0, s - 1)), 1000);
    return () => clearInterval(id);
  }, []);
  return <>{left > 0 ? `Try again in ${left} s.` : "You can ask again now."}</>;
}

const FRIENDLY: Record<string, string> = {
  TOO_LONG: "That question is a bit long. Try a shorter one!",
  LLM_UNAVAILABLE: "The AI is taking a short break. Try one of the suggested questions!",
  TIMEOUT: "That took too long. Try a suggested question!",
  NO_WORKING_QUERY: "The AI couldn't write a working query for that. Try rephrasing it.",
  INTERNAL: "Something went wrong on our side. Try again!",
};

export function AnswerPanel({ state, onRetry }: { state: AgentState; onRetry: () => void }) {
  const { answer, error, done, phase } = state;
  if (phase === "idle") return null;
  const footer = done && !error && (done.cached
    ? `pre-written query on live data · ${(done.latency_ms / 1000).toFixed(1)} s`
    : `answered by ${done.model} (${done.provider}) · ${(done.latency_ms / 1000).toFixed(1)} s`);
  return (
    <div data-testid="answer" aria-live="polite"
      className={`shrink-0 rounded-xl border-2 p-6 ${error && !answer ? "border-amber bg-[#4a3300]/40" : "border-visitor bg-visitor-fill/50"}`}>
      {answer && <p className="max-w-[60ch] text-[1.75rem] leading-snug font-medium text-visitor-ink">{answer}</p>}
      {error && (
        <div className={`flex flex-wrap items-center gap-x-5 gap-y-3 text-[1.5rem] leading-snug text-amber ${answer ? "mt-4" : ""}`}>
          <p>
            {error.code === "RATE_LIMITED"
              ? <>Lots of questions right now! <Countdown key={`${state.question}-${error.retry_after_s}`} from={error.retry_after_s ?? 10} /></>
              : error.code === "INTERNAL" ? error.message : FRIENDLY[error.code] ?? error.message}
          </p>
          {(error.code === "INTERNAL" || error.code === "TIMEOUT") && (
            <button type="button" onClick={onRetry}
              className="min-h-12 rounded-xl border-2 border-amber px-5 text-lg font-semibold text-amber outline-offset-2 hover:bg-amber hover:text-black focus-visible:outline-2 focus-visible:outline-amber">
              Try again
            </button>
          )}
        </div>
      )}
      {phase === "running" && !answer && !error && <p className="text-[1.5rem] text-muted">Thinking…</p>}
      {footer && <p className="mt-4 text-base text-muted">{footer}</p>}
    </div>
  );
}
