import { describe, expect, it } from "vitest";
import { applyWall, EMPTY_WALL, FADE_MS, pruneWall, STALE_MS } from "@/lib/wall";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const cp = (stage: string, seq: number, at = "2026-10-16T10:31:07.000Z", detail: Record<string, unknown> = {}, source = "student") =>
  ({ trace_id: TID, trip_id: `s-${TID}`, source, stage, seq, service: "processor", at, detail });
const NOW = Date.parse("2026-10-16T10:31:10.000Z");

describe("live wall", () => {
  it("a checkpoint moves the dot, and enriched adds the pickup borough to the label", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("submitted", 1), ambient: false }, NOW);
    expect(w.dots[TID]).toMatchObject({ stage: "submitted", label: "3f2c", ambient: false, terminal: false });
    w = applyWall(w, { type: "checkpoint", ...cp("enriched", 4, undefined, { pu_borough: "Manhattan" }), ambient: false }, NOW);
    expect(w.dots[TID]).toMatchObject({ stage: "enriched", label: "3f2c · Manhattan" });
  });
  it("a late checkpoint never moves a dot backwards", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("stored", 6), ambient: false }, NOW);
    w = applyWall(w, { type: "checkpoint", ...cp("aggregated", 5), ambient: false }, NOW);
    expect(w.dots[TID]!.stage).toBe("stored");
  });
  it("terminal dots fade 20 s after they finish; stalled ones after 60 s", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("queryable", 7), ambient: false }, NOW);
    w = applyWall(w, { type: "checkpoint", ...cp("ingested", 2), trace_id: "r", ambient: true }, NOW);
    expect(Object.keys(pruneWall(w, NOW + FADE_MS - 1).dots)).toHaveLength(2);
    expect(Object.keys(pruneWall(w, NOW + FADE_MS + 1).dots)).toEqual(["r"]);
    expect(Object.keys(pruneWall(w, NOW + STALE_MS + 1).dots)).toEqual([]);
  });
  it("the snapshot skips rides that finished long ago", () => {
    const old = { trace_id: "old", checkpoints: [{ ...cp("submitted", 1, "2026-10-16T10:00:00.000Z"), trace_id: "old" }, { ...cp("queryable", 7, "2026-10-16T10:00:02.000Z"), trace_id: "old" }] };
    const fresh = { trace_id: TID, checkpoints: [cp("submitted", 1, "2026-10-16T10:31:09.000Z")] };
    const w = applyWall(EMPTY_WALL, { type: "snapshot", traces: [old, fresh], sim_ts: "2025-10-09T17:42:00" }, NOW);
    expect(Object.keys(w.dots)).toEqual([TID]);
    expect(w.simTs).toBe("2025-10-09T17:42:00");
  });
  it("clock and stats frames, and garbage is ignored", () => {
    let w = applyWall(EMPTY_WALL, { type: "clock", sim_ts: "2025-10-09T17:43:00" }, NOW);
    w = applyWall(w, { type: "stats", rides_today: 57, rejected_today: 6, median_ms: { ingested: 40 } }, NOW);
    expect(w.simTs).toBe("2025-10-09T17:43:00");
    expect(w.stats).toEqual({ ridesToday: 57, rejectedToday: 6, medianMs: { ingested: 40 } });
    expect(applyWall(w, "nope", NOW)).toBe(w);
    expect(applyWall(w, { type: "checkpoint", stage: "teleported" }, NOW)).toBe(w);
  });
});
