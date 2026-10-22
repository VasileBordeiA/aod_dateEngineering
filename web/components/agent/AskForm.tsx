"use client";
import { useRef, useState } from "react";
import { STAND_SCREEN } from "@/lib/use-media-query";

const MAX = 300; // mirrors AGENT_MAX_QUESTION_CHARS; the api is the real limit

export function AskForm({ busy, onAsk }: { busy: boolean; onAsk: (q: string) => void }) {
  const [text, setText] = useState("");
  const input = useRef<HTMLInputElement>(null);
  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    const question = text.trim();
    if (!question) return;
    // Clear at once: the kiosk is shared, and one visitor's free text is never shown to the next (invariant 8).
    setText("");
    // Phones: close the on-screen keyboard so it doesn't cover the answer. The kiosk keeps focus for the next question.
    if (!window.matchMedia(STAND_SCREEN).matches) input.current?.blur();
    onAsk(question);
  };
  return (
    <form onSubmit={submit} className="flex gap-2 lg:gap-3">
      <input ref={input} value={text} onChange={(e) => setText(e.target.value)} maxLength={MAX} aria-label="Your question"
        placeholder="Ask anything about NYC taxis…" autoComplete="off" spellCheck={false} enterKeyHint="send"
        className="min-h-12 min-w-0 flex-1 rounded-xl border-2 border-visitor bg-panel px-3 text-base text-ink outline-none placeholder:text-muted focus-visible:border-visitor-ink lg:min-h-14 lg:px-4 lg:text-xl" />
      {text.length > MAX - 50 && <span className="self-center text-muted tabular-nums">{MAX - text.length}</span>}
      <button type="submit" disabled={!text.trim()}
        className="min-h-12 rounded-xl bg-visitor px-4 text-lg font-semibold text-black outline-offset-2 focus-visible:outline-2 focus-visible:outline-visitor-ink disabled:opacity-40 lg:min-h-14 lg:px-6 lg:text-xl">
        {busy ? "Ask again" : "Ask"}
      </button>
    </form>
  );
}
