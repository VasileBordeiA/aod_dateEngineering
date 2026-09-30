"use client";
import { useState } from "react";
import { money } from "@/lib/trace";
import { tipAmount, type TipChoice } from "@/lib/ride";

const PRESETS: { label: string; choice: TipChoice }[] = [
  { label: "$0", choice: { kind: "none" } },
  { label: "15%", choice: { kind: "pct", pct: 15 } },
  { label: "20%", choice: { kind: "pct", pct: 20 } },
  { label: "25%", choice: { kind: "pct", pct: 25 } },
];
const same = (a: TipChoice, b: TipChoice) => a.kind === b.kind && (a.kind !== "pct" || (b.kind === "pct" && a.pct === b.pct));

export function TipChips({ value, onChange, fare }: { value: TipChoice; onChange: (c: TipChoice) => void; fare: number | null }) {
  const [custom, setCustom] = useState("");
  const chip = (on: boolean) => `min-h-11 flex-1 rounded-full border-2 px-3 font-medium ${on ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-line"} disabled:opacity-40`;
  return (
    <fieldset className="flex flex-col gap-2">
      <legend className="flex w-full items-baseline justify-between pb-2 font-medium">
        <span>Tip</span>
        <span data-testid="tip-amount" className="tabular-nums text-muted">{money(tipAmount(value, fare))}</span>
      </legend>
      <div className="flex gap-2">
        {PRESETS.map((p) => (
          <button key={p.label} type="button" aria-pressed={same(value, p.choice)} disabled={p.choice.kind === "pct" && fare === null}
            onClick={() => onChange(p.choice)} className={chip(same(value, p.choice))}>{p.label}</button>
        ))}
        <button type="button" aria-pressed={value.kind === "custom"} onClick={() => onChange({ kind: "custom", amount: Number(custom) || 0 })}
          className={chip(value.kind === "custom")}>Custom</button>
      </div>
      {value.kind === "custom" && (
        <label className="flex items-center gap-2">
          <span aria-hidden>$</span>
          <input type="number" inputMode="decimal" min={0} max={999} step={0.01} aria-label="Custom tip in dollars" value={custom} autoFocus
            onChange={(e) => { setCustom(e.target.value); onChange({ kind: "custom", amount: Number(e.target.value) }); }}
            className="min-h-11 w-32 rounded-lg border border-line bg-panel px-3 text-base text-ink" />
        </label>
      )}
    </fieldset>
  );
}
