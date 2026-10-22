"use client";
import { useEffect, useState } from "react";
import { getTrace } from "@/lib/api";
import { rideChip, rideSummary, type RideSummary } from "@/lib/trace";

export const RIDE_RANK_QUESTION = "Where does my ride rank for tips today?"; // = evals/agent_golden_questions.yaml ride_rank

export function RidePrompt({ traceId, onAsk }: { traceId: string; onAsk: () => void }) {
  const [summary, setSummary] = useState<RideSummary | null>(null);
  useEffect(() => {
    let alive = true;
    getTrace(traceId).then((t) => { if (alive) setSummary(rideSummary(t.checkpoints)); }).catch(() => undefined);
    return () => { alive = false; };
  }, [traceId]);
  return (
    <div data-testid="ride-prompt" className="flex flex-col gap-3 rounded-2xl border-2 border-visitor bg-visitor-fill p-4 text-visitor-ink">
      {summary && <p data-testid="ride-chip" className="self-start rounded-full bg-black/30 px-4 py-1 text-base lg:text-lg">Your ride: {rideChip(summary)}</p>}
      <button type="button" onClick={onAsk}
        className="min-h-12 rounded-xl bg-visitor px-4 py-2 text-left text-lg font-semibold lg:min-h-14 lg:px-5 lg:py-0 lg:text-xl text-black outline-offset-2 focus-visible:outline-2 focus-visible:outline-visitor-ink">
        {RIDE_RANK_QUESTION}
      </button>
    </div>
  );
}
