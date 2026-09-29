import type { Suggestion } from "@/lib/api";

export function SuggestionList({ suggestions, onPick }: { suggestions: Suggestion[]; onPick: (s: Suggestion) => void }) {
  if (suggestions.length === 0) return null;
  return (
    <nav aria-label="Suggested questions">
      <p className="mb-2 text-base text-muted">Or tap a question</p>
      <ul className="flex flex-col gap-2">
        {suggestions.map((s) => (
          <li key={s.id}>
            <button type="button" onClick={() => onPick(s)}
              className="min-h-12 w-full rounded-xl border border-visitor/60 bg-visitor-fill/30 px-4 py-2 text-left text-lg text-visitor-ink outline-offset-2 transition-colors hover:border-visitor hover:bg-visitor-fill focus-visible:outline-2 focus-visible:outline-visitor-ink active:bg-visitor-fill">
              {s.question}
            </button>
          </li>
        ))}
      </ul>
    </nav>
  );
}
