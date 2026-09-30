import { isCheckpoint, mergeCheckpoints, TERMINAL, type Checkpoint, type Stage } from "@/lib/trace";

export const FADE_MS = 20_000; // spec 08: a dot fades out 20 s after its terminal stage
export const STALE_MS = 60_000; // a ride that stopped moving (processor down) leaves the wall after a minute

export type Dot = { traceId: string; label: string; stage: Stage; seq: number; ambient: boolean; terminal: boolean; updatedAt: number };
export type WallStats = { ridesToday: number; rejectedToday: number; medianMs: Partial<Record<Stage, number>> };
export type Wall = { dots: Record<string, Dot>; simTs: string | null; stats: WallStats | null };
export const EMPTY_WALL: Wall = { dots: {}, simTs: null, stats: null };

/** The first 4 characters of the trace id, plus the pickup borough once it's known (spec 08 §/trace/live). */
export const dotLabel = (traceId: string, borough?: string | null): string => `${traceId.slice(0, 4)}${borough ? ` · ${borough}` : ""}`;

function advance(dot: Dot | undefined, cp: Checkpoint, ambient: boolean, now: number): Dot {
  const b = cp.detail.pu_borough;
  const label = cp.stage === "enriched" && typeof b === "string" ? dotLabel(cp.trace_id, b) : (dot?.label ?? dotLabel(cp.trace_id));
  if (dot && cp.seq < dot.seq) return { ...dot, label }; // arrived late: never move a dot backwards
  return { traceId: cp.trace_id, label, stage: cp.stage, seq: cp.seq, ambient, terminal: TERMINAL.has(cp.stage), updatedAt: now };
}

function stripped(m: Record<string, unknown>): unknown {
  const rest: Record<string, unknown> = { ...m };
  delete rest.type;
  delete rest.ambient;
  return rest;
}

export function applyWall(wall: Wall, raw: unknown, now: number): Wall {
  if (typeof raw !== "object" || raw === null) return wall;
  const m = raw as Record<string, unknown>;
  switch (m.type) {
    case "snapshot": {
      const dots = { ...wall.dots };
      for (const t of Array.isArray(m.traces) ? (m.traces as { checkpoints?: unknown }[]) : []) {
        const cps = mergeCheckpoints([], Array.isArray(t?.checkpoints) ? t.checkpoints.filter(isCheckpoint) : []);
        let dot: Dot | undefined;
        for (const c of cps) dot = advance(dot, c, false, Date.parse(c.at));
        if (dot && !(dot.terminal && now - dot.updatedAt > FADE_MS)) dots[dot.traceId] = dot;
      }
      return { ...wall, dots, simTs: typeof m.sim_ts === "string" ? m.sim_ts : wall.simTs };
    }
    case "checkpoint": {
      const cp = stripped(m);
      if (!isCheckpoint(cp)) return wall;
      return { ...wall, dots: { ...wall.dots, [cp.trace_id]: advance(wall.dots[cp.trace_id], cp, m.ambient === true, now) } };
    }
    case "clock":
      return typeof m.sim_ts === "string" ? { ...wall, simTs: m.sim_ts } : wall;
    case "stats": {
      const median = typeof m.median_ms === "object" && m.median_ms !== null ? (m.median_ms as Partial<Record<Stage, number>>) : {};
      return { ...wall, stats: { ridesToday: Number(m.rides_today) || 0, rejectedToday: Number(m.rejected_today) || 0, medianMs: median } };
    }
    default:
      return wall;
  }
}

export function pruneWall(wall: Wall, now: number): Wall {
  const keep = Object.entries(wall.dots).filter(([, d]) => now - d.updatedAt <= (d.terminal ? FADE_MS : STALE_MS));
  return keep.length === Object.keys(wall.dots).length ? wall : { ...wall, dots: Object.fromEntries(keep) };
}
