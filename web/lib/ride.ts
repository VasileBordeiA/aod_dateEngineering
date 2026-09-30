import type { RideEstimate, Zone } from "@/lib/api";
import { isTraceId } from "@/lib/trace";

/** spec 08 §/ride "Popular": JFK, LaGuardia, Times Sq/Theatre District, Midtown Center, East Village, Williamsburg (N). */
export const POPULAR_ZONE_IDS = [132, 138, 230, 161, 79, 255] as const;

export type TipChoice = { kind: "none" } | { kind: "pct"; pct: 15 | 20 | 25 } | { kind: "custom"; amount: number };

export function tipAmount(choice: TipChoice, fare: number | null): number {
  switch (choice.kind) {
    case "none":
      return 0;
    case "pct":
      return fare === null ? 0 : Math.round(fare * choice.pct) / 100; // cents first: 19.50 × 15 % = $2.93
    case "custom": {
      const cents = Number.isFinite(choice.amount) ? Math.round(choice.amount * 100) / 100 : 0;
      return Math.min(999, Math.max(0, cents)); // no warning for absurd tips: the processor rejects them (spec 08)
    }
  }
}

export function groupByBorough(zones: readonly Zone[]): [string, Zone[]][] {
  const groups = new Map<string, Zone[]>();
  for (const z of zones) groups.set(z.borough, [...(groups.get(z.borough) ?? []), z]);
  return [...groups.entries()];
}

export function filterZones(zones: readonly Zone[], query: string): Zone[] {
  const q = query.trim().toLowerCase();
  return q ? zones.filter((z) => z.zone.toLowerCase().includes(q) || z.borough.toLowerCase().includes(q)) : [...zones];
}

export const estimateLine = (e: RideEstimate): string =>
  `~${e.trip_distance.toFixed(1)} mi · ~${Math.round(e.duration_min)} min · fare ~$${e.fare_amount.toFixed(2)} (${e.method})`;

export const LAST_TRACE_KEY = "nyc-taxi-live:last-trace";
type KeyValue = Pick<Storage, "getItem" | "setItem">;

function browserStorage(): KeyValue | null {
  try {
    return window.localStorage;
  } catch {
    return null; // no window (tests, server) or blocked storage (private mode)
  }
}

export function saveLastTrace(id: string, storage: KeyValue | null = browserStorage()): void {
  try {
    storage?.setItem(LAST_TRACE_KEY, id);
  } catch {
    /* storage full or blocked: "continue watching" is a convenience */
  }
}

export function loadLastTrace(storage: KeyValue | null = browserStorage()): string | null {
  try {
    const v = storage?.getItem(LAST_TRACE_KEY) ?? null;
    return v && isTraceId(v) ? v : null;
  } catch {
    return null;
  }
}
