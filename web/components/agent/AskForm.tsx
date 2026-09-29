"use client";
import { useState } from "react";

const MAX = 300; // mirrors AGENT_MAX_QUESTION_CHARS; the api is the real limit

export function AskForm({ busy, onAsk }: { busy: boolean; onAsk: (q: string) => void }) {
  const [text, setText] = useState("");
  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    const question = text.trim();
    if (!question) return;
    // Clear at once: the kiosk is shared, and one visitor's free text is never shown to the next (invariant 8).
    setText("");
    onAsk(question);
  };
  return (
    <form onSubmit={submit} className="flex gap-3">
      <input value={text} onChange={(e) => setText(e.target.value)} maxLength={MAX} aria-label="Your question"
        placeholder="Ask anything about NYC taxis…" autoComplete="off" spellCheck={false}
        className="min-h-14 min-w-0 flex-1 rounded-xl border-2 border-visitor bg-panel px-4 text-xl text-ink outline-none placeholder:text-muted focus-visible:border-visitor-ink" />
      {text.length > MAX - 50 && <span className="self-center text-muted tabular-nums">{MAX - text.length}</span>}
      <button type="submit" disabled={!text.trim()}
        className="min-h-14 rounded-xl bg-visitor px-6 text-xl font-semibold text-black outline-offset-2 focus-visible:outline-2 focus-visible:outline-visitor-ink disabled:opacity-40">
        {busy ? "Ask again" : "Ask"}
      </button>
    </form>
  );
}
