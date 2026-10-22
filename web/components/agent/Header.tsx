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
    <header className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-line px-4 py-3 lg:px-6 lg:py-4">
      <h1 className="text-xl font-semibold tracking-tight lg:text-[1.5rem]">
        NYC Taxi Live <span className="text-muted">·</span> <span className="text-pipeline-ink">Ask the data</span>
      </h1>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm lg:gap-6 lg:text-lg">
        <span className="whitespace-nowrap text-muted">
          sim clock <span className="font-medium text-ink tabular-nums">{clock ? formatSim(clock.sim_ts) : "…"}</span>
        </span>
        {badge && (
          <span className={`inline-flex items-center gap-2 rounded-full border px-3 py-0.5 whitespace-nowrap lg:px-4 lg:py-1 ${badge.className}`}>
            <span aria-hidden className={`size-2.5 rounded-full lg:size-3 ${badge.dot}`} />
            {badge.label}
          </span>
        )}
      </div>
    </header>
  );
}
