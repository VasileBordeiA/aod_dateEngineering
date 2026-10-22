"use client";
import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { askAgent, getSuggestions, type Suggestion } from "@/lib/api";
import { initialState, reduce } from "@/lib/agent-state";
import { STAND_SCREEN } from "@/lib/use-media-query";
import { AnswerPanel } from "./AnswerPanel";
import { AskForm } from "./AskForm";
import { AttractScreen } from "./AttractScreen";
import { Header } from "./Header";
import { AgentChart } from "./AgentChart";
import { ResultTable } from "./ResultTable";
import { RidePrompt, RIDE_RANK_QUESTION } from "./RidePrompt";
import { SqlPanel } from "./SqlPanel";
import { StepTimeline } from "./StepTimeline";
import { SuggestionList } from "./SuggestionList";
import { useIdle } from "./useIdle";

export const IDLE_MS = 90_000;

type AskOpts = { suggestionId?: string; rideTraceId?: string };

export function AgentKiosk({ rideTraceId }: { rideTraceId?: string } = {}) {
  const [state, dispatch] = useReducer(reduce, initialState);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [offline, setOffline] = useState(false);
  const abort = useRef<AbortController | null>(null);
  const last = useRef<{ question: string; opts: AskOpts } | null>(null);

  useEffect(() => {
    let alive = true;
    let retry: ReturnType<typeof setTimeout>;
    const load = () => {
      getSuggestions()
        .then((s) => {
          if (!alive) return;
          setSuggestions(s);
          setOffline(false);
        })
        .catch(() => {
          if (!alive) return;
          setOffline(true);
          retry = setTimeout(load, 3000);
        });
    };
    load();
    return () => {
      alive = false;
      clearTimeout(retry);
    };
  }, []);

  const ask = useCallback(async (question: string, opts: AskOpts = {}) => {
    abort.current?.abort();
    const controller = new AbortController();
    abort.current = controller;
    last.current = { question, opts };
    dispatch({ type: "start", question });
    // Phones: the answer sits right under the form, and the visitor may have tapped a suggestion further down.
    if (!window.matchMedia(STAND_SCREEN).matches) {
      const still = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      window.scrollTo({ top: 0, behavior: still ? "instant" : "smooth" });
    }
    try {
      const origin = opts.rideTraceId ? "ride" : opts.suggestionId ? "suggested" : "typed";
      for await (const event of askAgent({ question, origin, suggestion_id: opts.suggestionId, trace_id: opts.rideTraceId }, controller.signal)) {
        // A newer question (or an idle reset) replaced this stream. Frames it already parsed must not
        // land in the new question's state, so stop before dispatching.
        if (controller !== abort.current || controller.signal.aborted) return;
        dispatch({ type: "event", event });
      }
      setOffline(false);
    } catch {
      if (controller !== abort.current || controller.signal.aborted) return;
      setOffline(true);
    }
    if (controller !== abort.current || controller.signal.aborted) return;
    dispatch({ type: "stream_cut" });
  }, []);

  const reset = useCallback(() => {
    abort.current?.abort();
    dispatch({ type: "reset" });
  }, []);
  const [idle, wake] = useIdle(IDLE_MS, reset, state);
  const retry = () => last.current && ask(last.current.question, last.current.opts);

  if (idle) {
    return <AttractScreen suggestions={suggestions} onPick={(s) => { wake(); void ask(s.question, { suggestionId: s.id }); }} />;
  }
  const running = state.phase === "running";
  const showChart = state.rows && state.chart && state.chart.type !== "table" && state.rows.rows.length > 0;
  // A wide result (3+ columns) gets half the row next to the chart instead of two fifths.
  const split = (state.rows?.columns.length ?? 0) > 2 ? "xl:grid-cols-2" : "xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]";
  // On the stand (lg+) the page is exactly one screen. The chart and the answer keep their height;
  // the SQL panel gives up height first and scrolls inside.
  // Below lg both sections dissolve (display: contents) into one scrolling column, reordered for a phone:
  // form, answer, steps, SQL, chart and table. The suggestions go last once a question is asked.
  return (
    <div className="flex min-h-dvh flex-col lg:h-dvh lg:overflow-hidden">
      <Header />
      {offline && (
        <div data-testid="offline" role="status" className="bg-amber px-4 py-2 text-center text-base font-medium text-black lg:px-6 lg:text-lg">
          Reconnecting to the pipeline…
        </div>
      )}
      <main className="grid flex-1 grid-cols-1 content-start gap-5 p-4 lg:min-h-0 lg:grid-cols-[minmax(0,5fr)_minmax(0,8fr)] lg:content-normal lg:gap-6 lg:p-6">
        <section aria-label="Ask" className="contents min-w-0 flex-col gap-5 lg:flex lg:min-h-0 lg:overflow-y-auto">
          <div className="order-first flex min-w-0 flex-col gap-5 lg:order-none">
            {rideTraceId && <RidePrompt traceId={rideTraceId} onAsk={() => void ask(RIDE_RANK_QUESTION, { rideTraceId })} />}
            <AskForm busy={running} onAsk={(q) => void ask(q)} />
          </div>
          <SuggestionList suggestions={suggestions} onPick={(s) => void ask(s.question, { suggestionId: s.id })}
            className={state.phase === "idle" ? "" : "order-last lg:order-none"} />
          <StepTimeline steps={state.steps} running={running} startedKey={state.question} />
        </section>
        <section aria-label="What the AI did" className="contents min-w-0 flex-col gap-5 lg:flex lg:min-h-0 lg:overflow-y-auto">
          <SqlPanel attempts={state.attempts} />
          {state.rows && (
            <div className={`grid min-w-0 shrink-0 gap-5 ${showChart ? split : ""}`}>
              {showChart && state.chart && <AgentChart spec={state.chart} rows={state.rows} />}
              <ResultTable rows={state.rows} unit={state.chart?.unit} valueColumns={state.chart?.y ?? []} />
            </div>
          )}
          <AnswerPanel state={state} onRetry={retry} className="order-first lg:order-none" />
        </section>
      </main>
    </div>
  );
}
