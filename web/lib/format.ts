import type { RowsData, Unit } from "@/lib/agent-events";

/** Sim time is naive NYC local: format it as-is, never convert it (ARCHITECTURE §4). */
export function formatSim(ts: string): string {
  const d = new Date(`${ts.slice(0, 19)}Z`);
  const f = new Intl.DateTimeFormat("en-GB", { weekday: "short", day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" });
  const p = Object.fromEntries(f.formatToParts(d).map((x) => [x.type, x.value]));
  return `${p.weekday} ${p.day} ${p.month} ${p.hour}:${p.minute}`;
}

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

/** The UI gets at most 50 rows of up to 200 (the safety LIMIT); say so whenever fewer are shown than came back. */
export function rowCountLabel(rows: Pick<RowsData, "rows" | "row_count" | "truncated">): string {
  if (rows.rows.length >= rows.row_count) return `${rows.row_count} rows`;
  return `Showing ${rows.rows.length} of ${rows.row_count} rows${rows.truncated ? " (safety LIMIT 200)" : ""}`;
}
