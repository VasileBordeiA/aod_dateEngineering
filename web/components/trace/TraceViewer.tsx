"use client";
import Link from "next/link";
import { useEffect, useReducer, useRef, useState } from "react";
import { getSimClock } from "@/lib/api";
import { formatSim } from "@/lib/format";
import {
  applyMessage, EMPTY_VIEW, HAPPY_PATH, hopMs, isTraceId, money, rideSummary, TERMINAL, toTraceMessage, totalMs, waitingText,
  type Stage,
} from "@/lib/trace";
import { usePoll } from "@/lib/use-poll";
import { ReconnectingSocket, wsUrl } from "@/lib/ws";
import { StageCard, type CardState } from "./StageCard";

const NOT_FOUND_MS = 15_000;
const WITH_REJECTED: Stage[] = ["submitted", "ingested", "rejected", "enriched", "aggregated", "stored", "queryable"];

function NotFound() {
  return (
    <p role="status" className="rounded-xl border border-line px-4 py-3">
      We can&apos;t find this ride. <Link href="/ride" className="font-semibold text-visitor-ink underline">Book a new one</Link>
    </p>
  );
}

export function TraceViewer({ traceId }: { traceId: string }) {
  const valid = isTraceId(traceId);
  const [view, dispatch] = useReducer(applyMessage, EMPTY_VIEW);
  const [now, setNow] = useState(() => Date.now());
  const [lost, setLost] = useState(false);
  const socket = useRef<ReconnectingSocket | null>(null);
  const clock = usePoll(getSimClock, 5000);

  useEffect(() => {
    if (!valid) return;
    const s = new ReconnectingSocket(wsUrl(`/api/trace/${traceId}`, window.location, process.env.NEXT_PUBLIC_API_WS_ORIGIN), (raw) => {
      const msg = toTraceMessage(raw);
      if (msg) dispatch(msg);
    });
    socket.current = s;
    return () => s.stop();
  }, [traceId, valid]);

  const cps = view.checkpoints;
  const terminal = cps.some((c) => TERMINAL.has(c.stage));
  useEffect(() => {
    if (terminal) socket.current?.stop(); // nothing more will come: don't reconnect when the server closes
  }, [terminal]);
  useEffect(() => {
    if (terminal) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [terminal]);
  useEffect(() => {
    if (!valid || cps.length > 0) return;
    const id = setTimeout(() => setLost(true), NOT_FOUND_MS);
    return () => clearTimeout(id);
  }, [valid, cps.length]);

  if (!valid) return <NotFound />;

  const rejected = cps.some((c) => c.stage === "rejected");
  const stages = rejected ? WITH_REJECTED : [...HAPPY_PATH];
  const byStage = new Map(cps.map((c) => [c.stage, c]));
  const hops = hopMs(cps);
  const next = rejected ? null : (stages.find((s) => !byStage.has(s)) ?? null);
  const last = cps.at(-1);
  const waitingS = last ? Math.max(0, Math.floor((now - Date.parse(last.at)) / 1000)) : null;
  const summary = rideSummary(cps);
  const total = totalMs(cps);
  const state = (s: Stage): CardState =>
    byStage.has(s) ? (s === "rejected" ? "rejected" : "done") : s === next ? "next" : "future";
  const sim = clock?.sim_ts ?? view.simTs;

  return (
    <div className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <h1 className="text-xl font-semibold leading-snug">{summary ? `${summary.pickupZone} → ${summary.dropoffZone}` : "Your ride"}</h1>
        {summary && (
          <p className="text-sm text-muted">
            {summary.fare !== null && `fare ~${money(summary.fare)} · `}
            {summary.payment === "cash" ? "cash (tip not recorded)" : `${money(summary.tip)} tip`}
          </p>
        )}
        <p className="text-sm text-muted">sim clock <span className="tabular-nums text-ink">{sim ? formatSim(sim) : "…"}</span></p>
      </header>
      {cps.length === 0 && (lost ? <NotFound /> : <p role="status" className="text-muted">Looking for your ride…</p>)}
      <ol data-testid="stages" className="flex flex-col">
        {stages.map((s, i) => (
          <StageCard key={s} stage={s} state={state(s)} checkpoint={byStage.get(s)} hopMs={hops[s]}
            waiting={s === next ? waitingText(s) : null} waitingS={s === next ? waitingS : null} connector={i > 0} />
        ))}
      </ol>
      {total !== null && (
        <div data-testid="total" role="status" className="rounded-xl bg-visitor-fill px-4 py-3 text-center text-lg font-semibold text-visitor-ink">
          From your phone to queryable in {(total / 1000).toFixed(1)} s
        </div>
      )}
      {byStage.has("queryable") && (
        <Link href={`/agent?trace_id=${traceId}&suggestion=ride_rank`}
          className="flex min-h-14 items-center justify-center rounded-xl bg-visitor px-4 text-center text-lg font-semibold text-black">
          Where does my ride rank for tips today?
        </Link>
      )}
      {rejected && (
        <Link href="/ride" className="flex min-h-14 items-center justify-center rounded-xl border-2 border-amber text-lg font-semibold">
          Book another ride
        </Link>
      )}
    </div>
  );
}
