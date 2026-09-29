import type { Unit } from "@/lib/agent-events";

const n = (v: number, digits: number) =>
  v.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });

export function formatValue(v: unknown, unit: Unit = "none"): string {
  if (v === null || v === undefined) return "–";
  if (typeof v !== "number") return String(v);
  switch (unit) {
    case "usd": return `$${n(v, 2)}`;
    case "pct": return `${n(v, 1)}%`;
    case "minutes": return `${n(v, 1)} min`;
    case "miles": return `${n(v, 2)} mi`;
    case "mph": return `${n(v, 1)} mph`;
    case "ms": return `${n(v, 0)} ms`;
    case "trips": return n(v, 0);
    default: return v.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
}

export function compactNumber(v: number): string {
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(v);
}
