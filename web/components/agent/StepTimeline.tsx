"use client";
import { useEffect, useState } from "react";
import type { Step } from "@/lib/agent-state";

/** Whole seconds since `key` started while `running`; 0 otherwise. */
function useElapsed(running: boolean, key: unknown): number {
  const [tick, setTick] = useState<{ key: unknown; seconds: number }>({ key, seconds: 0 });
  useEffect(() => {
    if (!running) return;
    const started = Date.now();
    const id = setInterval(() => setTick({ key, seconds: Math.floor((Date.now() - started) / 1000) }), 500);
    return () => clearInterval(id);
  }, [running, key]);
  return running && tick.key === key ? tick.seconds : 0;
}

const ICON = { running: "…", ok: "✓", failed: "✗" } as const;
const DOT = {
  running: "border-pipeline bg-surface text-pipeline-ink animate-pulse",
  ok: "border-pipeline bg-pipeline-fill text-pipeline-ink",
  failed: "border-amber bg-[#4a3300] text-amber",
} as const;

export function StepTimeline({ steps, running, startedKey }: { steps: Step[]; running: boolean; startedKey?: string }) {
  const elapsed = useElapsed(running, startedKey);
  if (steps.length === 0) return null;
  return (
    <div data-testid="steps" className="rounded-xl border border-line bg-panel p-5">
      <div className="mb-3 flex items-baseline justify-between">
        <h2 className="text-lg font-semibold text-pipeline-ink">Steps</h2>
        {running && elapsed >= 5 && <span className="text-base text-muted tabular-nums">{elapsed} s</span>}
      </div>
      <ol className="flex flex-col">
        {steps.map((s, i) =>
          s.kind === "status" ? (
            <li key={i} className="step-in py-1 pl-10 text-base text-muted italic">{s.text}</li>
          ) : (
            <li key={i} className="step-in grid grid-cols-[1.75rem_minmax(0,1fr)] items-baseline gap-x-3 py-1">
              <span aria-label={s.state} className={`inline-flex size-7 items-center justify-center self-center rounded-full border-2 text-sm font-bold ${DOT[s.state]}`}>
                {ICON[s.state]}
              </span>
              <span className="flex min-w-0 items-baseline gap-3">
                <span className="max-w-full shrink-0 font-mono text-lg break-all text-pipeline-ink">{s.label}</span>
                {s.summary && (
                  <span className={`truncate text-base ${s.state === "failed" ? "text-amber" : "text-muted"}`} title={s.summary}>
                    {s.summary}
                  </span>
                )}
              </span>
            </li>
          ),
        )}
      </ol>
    </div>
  );
}
