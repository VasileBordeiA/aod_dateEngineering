import type { TraceCheckpoint } from "@/lib/contracts/trace-checkpoint";

export type Checkpoint = TraceCheckpoint;
export type Stage = Checkpoint["stage"];

export const HAPPY_PATH: readonly Stage[] = ["submitted", "ingested", "validated", "enriched", "aggregated", "stored", "queryable"];
export const TERMINAL: ReadonlySet<Stage> = new Set<Stage>(["queryable", "rejected"]);
const STAGES: ReadonlySet<string> = new Set<string>([...HAPPY_PATH, "rejected"]);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const isTraceId = (s: string): boolean => UUID.test(s);

export const STAGE_TITLE: Record<Stage, string> = {
  submitted: "Your phone", ingested: "Redpanda", validated: "Data-quality check", rejected: "Rejected",
  enriched: "Enrichment", aggregated: "Live dashboard", stored: "ClickHouse", queryable: "AI agent",
};

const WAITING: Record<Stage, string> = {
  submitted: "waiting for the API…", ingested: "waiting for Redpanda…", validated: "waiting for the stream processor…",
  rejected: "waiting for the stream processor…", enriched: "waiting for the stream processor…",
  aggregated: "waiting for the stream processor…", stored: "waiting for ClickHouse…",
  queryable: "waiting for the AI agent to see it…",
};
/** The line under the next card while it hasn't arrived yet (spec 07 §Failure modes: "waiting for the stream processor…"). */
export const waitingText = (next: Stage): string => WAITING[next];

/** Plain English for the processor's reason codes (spec 03 §Validation rules). */
export const REASON_TEXT: Record<string, string> = {
  TIP_UNREALISTIC: "the tip is unrealistically high", TIP_NEGATIVE: "the tip is negative",
  FARE_NON_POSITIVE: "the fare is zero or negative", TOTAL_NEGATIVE: "the total is negative",
  DISTANCE_NON_POSITIVE: "the distance is zero", DISTANCE_TOO_LONG: "the distance is over 200 miles",
  DROPOFF_NOT_AFTER_PICKUP: "the drop-off isn't after the pickup", DURATION_TOO_LONG: "the ride took over 6 hours",
  LOCATION_INVALID: "a zone id doesn't exist", PASSENGER_COUNT_INVALID: "the passenger count is impossible",
  SCHEMA_INVALID: "the message didn't match the contract",
};

type Detail = Checkpoint["detail"];
const num = (d: Detail, k: string): number | null => { const v = d[k]; return typeof v === "number" ? v : null; };
const text = (d: Detail, k: string): string | null => { const v = d[k]; return typeof v === "string" ? v : null; };
const strings = (d: Detail, k: string): string[] => { const v = d[k]; return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : []; };

export function stageCopy(cp: Checkpoint): string {
  const d = cp.detail;
  switch (cp.stage) {
    case "submitted":
      return "Your phone sent the ride to our API.";
    case "ingested":
      return `Safely stored in Redpanda: topic ${text(d, "topic") ?? "trips.raw"}, partition ${num(d, "partition") ?? "?"}, offset ${num(d, "offset") ?? "?"}. It can't get lost now.`;
    case "validated":
      return `The stream processor checked ${num(d, "rules_checked") ?? 11} data-quality rules. All passed ✓`;
    case "rejected": {
      const reasons = strings(d, "reasons").map((r) => REASON_TEXT[r] ?? r);
      return `Rejected: ${reasons.length ? reasons.join(" and ") : "a data-quality rule failed"}. Our pipeline rejects about 1 in 10 real NYC trips for problems like this. Try a normal tip!`;
    }
    case "enriched": {
      const tip = num(d, "tip_pct");
      return tip === null
        ? "Added zone and borough names, and worked out your duration. Cash tips aren't recorded, so there's no tip %."
        : `Added zone and borough names, and worked out your tip (${tip}%) and duration.`;
    }
    case "aggregated":
      return `Counted in the ${text(d, "borough") ?? "?"} · ${(text(d, "window_start") ?? "").slice(11, 16)} bucket on the live dashboard.`;
    case "stored":
      return `Written to ClickHouse (${text(d, "table") ?? "taxi.trips"}) in a batch of ${num(d, "batch_rows") ?? "?"} trips.`;
    case "queryable":
      return "The AI agent can now see your ride. Ask it something!";
  }
}

/** Dedup by stage (the first one wins) and order by seq, never by arrival (spec 07 §Ordering). */
export function mergeCheckpoints(have: readonly Checkpoint[], incoming: readonly Checkpoint[]): Checkpoint[] {
  const byStage = new Map<Stage, Checkpoint>();
  for (const cp of [...have, ...incoming]) if (!byStage.has(cp.stage)) byStage.set(cp.stage, cp);
  return [...byStage.values()].sort((a, b) => a.seq - b.seq || a.at.localeCompare(b.at));
}

/** Milliseconds since the previous checkpoint. Wall time `at` only (CLAUDE.md invariant 3). */
export function hopMs(cps: readonly Checkpoint[]): Partial<Record<Stage, number>> {
  const sorted = mergeCheckpoints(cps, []);
  const out: Partial<Record<Stage, number>> = {};
  for (let i = 1; i < sorted.length; i++) {
    out[sorted[i]!.stage] = Math.max(0, Date.parse(sorted[i]!.at) - Date.parse(sorted[i - 1]!.at));
  }
  return out;
}

export function totalMs(cps: readonly Checkpoint[]): number | null {
  const s = cps.find((c) => c.stage === "submitted");
  const q = cps.find((c) => c.stage === "queryable");
  return s && q ? Date.parse(q.at) - Date.parse(s.at) : null;
}

export type TraceMessage =
  | { type: "history"; checkpoints: Checkpoint[]; sim_ts: string | null }
  | { type: "checkpoint"; checkpoint: Checkpoint };

export function isCheckpoint(x: unknown): x is Checkpoint {
  if (typeof x !== "object" || x === null) return false;
  const c = x as Record<string, unknown>;
  return typeof c.trace_id === "string" && typeof c.stage === "string" && STAGES.has(c.stage)
    && typeof c.seq === "number" && typeof c.at === "string" && typeof c.detail === "object" && c.detail !== null;
}

export function toTraceMessage(raw: unknown): TraceMessage | null {
  if (typeof raw !== "object" || raw === null) return null;
  const m = raw as Record<string, unknown>;
  if (m.type === "history" && Array.isArray(m.checkpoints)) {
    return { type: "history", checkpoints: m.checkpoints.filter(isCheckpoint), sim_ts: typeof m.sim_ts === "string" ? m.sim_ts : null };
  }
  if (m.type === "checkpoint") {
    const rest: Record<string, unknown> = { ...m };
    delete rest.type;
    delete rest.ambient;
    return isCheckpoint(rest) ? { type: "checkpoint", checkpoint: rest } : null;
  }
  return null;
}

export type TraceView = { checkpoints: Checkpoint[]; simTs: string | null };
export const EMPTY_VIEW: TraceView = { checkpoints: [], simTs: null };

export function applyMessage(view: TraceView, msg: TraceMessage): TraceView {
  if (msg.type === "history") return { checkpoints: mergeCheckpoints(view.checkpoints, msg.checkpoints), simTs: msg.sim_ts ?? view.simTs };
  return { ...view, checkpoints: mergeCheckpoints(view.checkpoints, [msg.checkpoint]) };
}

export type RideSummary = { pickupZone: string; dropoffZone: string; payment: "card" | "cash"; tip: number; fare: number | null };

export function rideSummary(cps: readonly Checkpoint[]): RideSummary | null {
  const s = cps.find((c) => c.stage === "submitted");
  if (!s) return null;
  const pu = text(s.detail, "pickup_zone");
  const dz = text(s.detail, "dropoff_zone");
  if (!pu || !dz) return null;
  return { pickupZone: pu, dropoffZone: dz, payment: text(s.detail, "payment") === "cash" ? "cash" : "card",
    tip: num(s.detail, "tip_amount") ?? 0, fare: num(s.detail, "fare_amount") };
}

export const money = (x: number): string => (Number.isInteger(x) ? `$${x}` : `$${x.toFixed(2)}`);

/** "Midtown Center → JFK Airport, $5 tip" (spec 08 §/agent). */
export const rideChip = (s: RideSummary): string =>
  `${s.pickupZone} → ${s.dropoffZone}, ${s.payment === "cash" ? "cash" : `${money(s.tip)} tip`}`;
