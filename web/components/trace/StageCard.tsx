import { STAGE_TITLE, stageCopy, type Checkpoint, type Stage } from "@/lib/trace";

export type CardState = "done" | "rejected" | "next" | "future";

const STYLE: Record<CardState, string> = {
  done: "border-visitor bg-visitor-fill text-visitor-ink",
  rejected: "border-amber bg-[#4a3300] text-[#ffe7b0]",
  next: "border-pipeline text-ink",
  future: "border-line text-muted opacity-60",
};

export function StageCard({ stage, state, checkpoint, hopMs, waiting, waitingS, connector }: {
  stage: Stage; state: CardState; checkpoint?: Checkpoint; hopMs?: number; waiting: string | null; waitingS: number | null; connector: boolean;
}) {
  return (
    <li data-testid={`stage-${stage}`} data-state={state} className="flex flex-col">
      {connector && (
        <div aria-hidden className="relative mx-auto h-6 w-0.5 bg-line">
          {state === "next" && <span className="packet absolute -left-1 top-1.5 size-2.5 rounded-full bg-visitor" />}
        </div>
      )}
      <div className={`rounded-xl border-2 px-4 py-3 transition-colors duration-300 ${STYLE[state]}`}>
        <div className="flex items-baseline justify-between gap-3">
          <h2 className="font-semibold">{STAGE_TITLE[stage]}</h2>
          {(state === "done" || state === "rejected") && hopMs !== undefined && (
            <span className="shrink-0 text-sm tabular-nums">+{hopMs} ms</span>
          )}
        </div>
        {checkpoint && <p className="mt-1 text-sm leading-snug">{stageCopy(checkpoint)}</p>}
        {state === "next" && waiting && (
          <p className="mt-1 flex items-center gap-2 text-sm text-muted">
            <span className="spinner" aria-hidden />
            {waiting}{waitingS !== null && waitingS >= 5 ? ` (${waitingS} s)` : ""}
          </p>
        )}
      </div>
    </li>
  );
}
