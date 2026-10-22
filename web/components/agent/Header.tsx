"use client";
import { getLlmStatus, getSimClock, type LlmStatus } from "@/lib/api";
import { formatSim } from "@/lib/format";
import { usePoll } from "@/lib/use-poll";

const BADGE: Record<string, { label: string; className: string; dot: string }> = {
  local: { label: "local model", className: "bg-visitor-fill text-visitor-ink border-visitor", dot: "bg-visitor" },
  internal: { label: "company AI", className: "bg-[#0d3a66] text-[#dcecff] border-[#3987e5]", dot: "bg-[#3987e5]" },
  gemini: { label: "Google Gemini", className: "bg-[#0d3a66] text-[#dcecff] border-[#3987e5]", dot: "bg-[#3987e5]" },
  cache: { label: "cached answers only", className: "bg-[#4a3300] text-[#ffe7b0] border-amber", dot: "bg-amber" },
};

export function Header() {
  const clock = usePoll(getSimClock, 5000);
  const llm = usePoll<LlmStatus>(getLlmStatus, 10000);
  // No badge until the first status arrives, so the page never flashes "cached answers only" on load.
  const badge = llm ? (BADGE[llm.active] ?? BADGE.cache!) : null;
  return (
    <header className="flex flex-wrap items-center justify-between gap-4 border-b border-line px-6 py-4">
      <h1 className="text-[1.5rem] font-semibold tracking-tight">
        NYC Taxi Live <span className="text-muted">·</span> <span className="text-pipeline-ink">Ask the data</span>
      </h1>
      <div className="flex items-center gap-6 text-lg">
        <span className="text-muted">
          sim clock <span className="font-medium text-ink tabular-nums">{clock ? formatSim(clock.sim_ts) : "…"}</span>
        </span>
        {badge && (
          <span className={`inline-flex items-center gap-2 rounded-full border px-4 py-1 ${badge.className}`}>
            <span aria-hidden className={`size-3 rounded-full ${badge.dot}`} />
            {badge.label}
          </span>
        )}
      </div>
    </header>
  );
}
