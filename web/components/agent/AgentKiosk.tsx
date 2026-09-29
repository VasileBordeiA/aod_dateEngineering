"use client";
import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { askAgent, getSuggestions, type Suggestion } from "@/lib/api";
import { initialState, reduce } from "@/lib/agent-state";
import { AnswerPanel } from "./AnswerPanel";
import { AskForm } from "./AskForm";
import { AttractScreen } from "./AttractScreen";
import { Header } from "./Header";
import { AgentChart } from "./AgentChart";
import { ResultTable } from "./ResultTable";
import { SqlPanel } from "./SqlPanel";
import { StepTimeline } from "./StepTimeline";
import { SuggestionList } from "./SuggestionList";
import { useIdle } from "./useIdle";

export const IDLE_MS = 90_000;

export function AgentKiosk() {
  const [state, dispatch] = useReducer(reduce, initialState);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [offline, setOffline] = useState(false);
  const abort = useRef<AbortController | null>(null);
  const last = useRef<{ question: string; suggestionId?: string } | null>(null);

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

  const ask = useCallback(async (question: string, suggestionId?: string) => {
    abort.current?.abort();
    const controller = new AbortController();
    abort.current = controller;
    last.current = { question, suggestionId };
    dispatch({ type: "start", question });
    try {
      const origin = suggestionId ? "suggested" : "typed";
      for await (const event of askAgent({ question, origin, suggestion_id: suggestionId }, controller.signal)) {
        dispatch({ type: "event", event });
      }
      setOffline(false);
    } catch {
      if (!controller.signal.aborted) setOffline(true);
    }
    if (!controller.signal.aborted) dispatch({ type: "stream_cut" });
  }, []);

  const reset = useCallback(() => {
    abort.current?.abort();
    dispatch({ type: "reset" });
  }, []);
  const [idle, wake] = useIdle(IDLE_MS, reset, state);
  const retry = () => last.current && ask(last.current.question, last.current.suggestionId);

  if (idle) {
    return <AttractScreen suggestions={suggestions} onPick={(s) => { wake(); void ask(s.question, s.id); }} />;
  }
  const running = state.phase === "running";
  const showChart = state.rows && state.chart && state.chart.type !== "table" && state.rows.rows.length > 0;
  // A wide result (3+ columns) gets half the row next to the chart instead of two fifths.
  const split = (state.rows?.columns.length ?? 0) > 2 ? "xl:grid-cols-2" : "xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]";
  // On the stand (lg+) the page is exactly one screen. The chart and the answer keep their height;
  // the SQL panel gives up height first and scrolls inside. Phones get a normal scrolling column.
  return (
    <div className="flex min-h-dvh flex-col lg:h-dvh lg:overflow-hidden">
      <Header />
      {offline && (
        <div data-testid="offline" role="status" className="bg-amber px-6 py-2 text-center text-lg font-medium text-black">
          Reconnecting to the pipeline…
        </div>
      )}
      <main className="grid flex-1 grid-cols-1 gap-6 p-6 lg:min-h-0 lg:grid-cols-[minmax(0,5fr)_minmax(0,8fr)]">
        <section aria-label="Ask" className="flex min-w-0 flex-col gap-5 lg:min-h-0 lg:overflow-y-auto">
          <AskForm busy={running} onAsk={(q) => void ask(q)} />
          <SuggestionList suggestions={suggestions} onPick={(s) => void ask(s.question, s.id)} />
          <StepTimeline steps={state.steps} running={running} startedKey={state.question} />
        </section>
        <section aria-label="What the AI did" className="flex min-w-0 flex-col gap-5 lg:min-h-0 lg:overflow-y-auto">
          <SqlPanel attempts={state.attempts} />
          {state.rows && (
            <div className={`grid min-w-0 shrink-0 gap-5 ${showChart ? split : ""}`}>
              {showChart && state.chart && <AgentChart spec={state.chart} rows={state.rows} />}
              <ResultTable rows={state.rows} unit={state.chart?.unit} valueColumns={state.chart?.y ?? []} />
            </div>
          )}
          <AnswerPanel state={state} onRetry={retry} />
        </section>
      </main>
    </div>
  );
}
