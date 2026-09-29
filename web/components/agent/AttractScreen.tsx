"use client";
import { useEffect, useState } from "react";
import type { Suggestion } from "@/lib/api";

export function AttractScreen({ suggestions, onPick }: { suggestions: Suggestion[]; onPick: (s: Suggestion) => void }) {
  const [i, setI] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setI((n) => n + 1), 4000);
    return () => clearInterval(id);
  }, []);
  const current = suggestions.length ? suggestions[i % suggestions.length] : undefined;
  // The window-level idle listener would wake the kiosk (and unmount this button) on pointerdown,
  // before the click lands. Stop it here; onPick wakes the kiosk and asks in one go.
  const keep = (e: React.SyntheticEvent) => e.stopPropagation();
  return (
    <main data-testid="attract" className="flex min-h-dvh flex-col items-center justify-center gap-12 p-10 text-center">
      <h1 className="text-[2.25rem] font-semibold text-ink">
        Ask the data. The AI shows you its SQL.
      </h1>
      {current && (
        <button key={current.id} type="button" onClick={() => onPick(current)} onPointerDown={keep} onTouchStart={keep} onKeyDown={keep}
          className="step-in max-w-5xl rounded-2xl border-2 border-visitor bg-visitor-fill px-12 py-10 text-[2.75rem] leading-tight font-semibold text-visitor-ink outline-offset-4 focus-visible:outline-2 focus-visible:outline-visitor-ink">
          {current.question}
        </button>
      )}
      <p className="text-xl text-muted">Tap the question, or anywhere else to type your own.</p>
    </main>
  );
}
