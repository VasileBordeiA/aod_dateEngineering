"use client";
import { useEffect, useReducer } from "react";
import { QrCode } from "@/components/QrCode";
import { formatSim } from "@/lib/format";
import { HAPPY_PATH, STAGE_TITLE, type Stage } from "@/lib/trace";
import { applyWall, EMPTY_WALL, pruneWall, type Dot, type Wall } from "@/lib/wall";
import { ReconnectingSocket, wsUrl } from "@/lib/ws";

type Action = { kind: "message"; raw: unknown; now: number } | { kind: "tick"; now: number };
const reduce = (w: Wall, a: Action): Wall => (a.kind === "message" ? applyWall(w, a.raw, a.now) : pruneWall(w, a.now));
// Architecture image colours: the phone and the agent are visitor-facing (green), the rest is the core pipeline (purple).
const VISITOR_STAGES = new Set<Stage>(["submitted", "queryable"]);

function Box({ stage, dots, medianMs }: { stage: Stage; dots: Dot[]; medianMs?: number }) {
  const tone = stage === "rejected" ? "border-amber bg-[#4a3300] text-[#ffe7b0]"
    : VISITOR_STAGES.has(stage) ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-pipeline bg-pipeline-fill text-pipeline-ink";
  return (
    <section data-testid={`box-${stage}`} aria-label={STAGE_TITLE[stage]} className={`flex min-h-48 flex-col gap-3 rounded-2xl border-2 p-4 ${tone}`}>
      <h2 className="text-xl font-semibold">{STAGE_TITLE[stage]}</h2>
      {medianMs !== undefined && <p className="text-sm opacity-80">median hop {medianMs} ms</p>}
      <div className="flex flex-wrap content-start gap-2">
        {dots.map((d) => d.ambient
          ? <span key={d.traceId} data-testid="dot" data-ambient="true" aria-hidden className="size-3 rounded-full bg-current opacity-40" />
          : <span key={d.traceId} data-testid="dot" data-ambient="false" className="rounded-full bg-visitor px-4 py-1.5 text-lg font-semibold text-black">{d.label}</span>)}
      </div>
    </section>
  );
}

export function LiveWall({ standUrl }: { standUrl: string }) {
  const [wall, dispatch] = useReducer(reduce, EMPTY_WALL);
  useEffect(() => {
    const s = new ReconnectingSocket(wsUrl("/api/trace/live", window.location, process.env.NEXT_PUBLIC_API_WS_ORIGIN),
      (raw) => dispatch({ kind: "message", raw, now: Date.now() }));
    const tick = setInterval(() => dispatch({ kind: "tick", now: Date.now() }), 1000);
    return () => { s.stop(); clearInterval(tick); };
  }, []);
  const dots = Object.values(wall.dots);
  const at = (stage: Stage) => dots.filter((d) => d.stage === stage);
  const median = wall.stats?.medianMs ?? {};
  return (
    <div className="grid min-h-dvh grid-cols-[minmax(0,1fr)_20rem] gap-6 p-6">
      <main className="flex flex-col gap-4">
        <header className="flex items-baseline justify-between">
          <h1 className="text-4xl font-semibold">Rides crossing the pipeline, live</h1>
          <p className="text-xl text-muted">sim clock <span className="tabular-nums text-ink">{wall.simTs ? formatSim(wall.simTs) : "…"}</span></p>
        </header>
        <div className="grid grid-cols-7 gap-3">
          {HAPPY_PATH.map((s) => <Box key={s} stage={s} dots={at(s)} medianMs={median[s]} />)}
          <div className="col-start-3"><Box stage="rejected" dots={at("rejected")} /></div>
        </div>
      </main>
      <aside className="flex flex-col gap-6 rounded-2xl border border-line bg-panel p-6 text-xl">
        <div><p className="text-muted">Rides today</p><p data-testid="rides-today" className="text-5xl font-semibold tabular-nums">{wall.stats?.ridesToday ?? "–"}</p></div>
        <div><p className="text-muted">Rejected today</p><p data-testid="rejected-today" className="text-5xl font-semibold tabular-nums text-amber">{wall.stats?.rejectedToday ?? "–"}</p></div>
        <figure data-testid="ride-qr" className="mt-auto flex flex-col items-center gap-3">
          <QrCode value={`${standUrl}/ride`} size={220} title="Open the ride page" />
          <figcaption className="text-center">Book your own ride:<br /><span className="font-semibold">{standUrl}/ride</span></figcaption>
        </figure>
      </aside>
    </div>
  );
}
