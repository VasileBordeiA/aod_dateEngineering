"use client";

export function PassengerStepper({ value, onChange }: { value: number; onChange: (n: number) => void }) {
  const btn = "size-11 rounded-full border-2 border-line text-xl disabled:opacity-40";
  return (
    <div className="flex items-center justify-between">
      <span className="font-medium">Passengers</span>
      <div className="flex items-center gap-4">
        <button type="button" aria-label="Fewer passengers" disabled={value <= 1} onClick={() => onChange(value - 1)} className={btn}>−</button>
        <span data-testid="passengers" className="w-6 text-center text-xl tabular-nums">{value}</span>
        <button type="button" aria-label="More passengers" disabled={value >= 6} onClick={() => onChange(value + 1)} className={btn}>+</button>
      </div>
    </div>
  );
}
