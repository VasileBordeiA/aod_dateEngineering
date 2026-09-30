import { describe, expect, it } from "vitest";
import {
  applyMessage, EMPTY_VIEW, hopMs, isTraceId, mergeCheckpoints, rideChip, rideSummary, stageCopy, toTraceMessage,
  totalMs, waitingText, type Checkpoint,
} from "@/lib/trace";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const cp = (stage: Checkpoint["stage"], seq: number, at: string, detail: Record<string, unknown> = {}): Checkpoint => ({
  trace_id: TID, trip_id: `s-${TID}`, source: "student", stage, seq, service: "processor", at, detail,
});
const S = cp("submitted", 1, "2026-10-16T10:31:07.000Z", { pickup_zone: "Midtown Center", dropoff_zone: "JFK Airport", payment: "card", tip_amount: 5, fare_amount: 70.5 });
const I = cp("ingested", 2, "2026-10-16T10:31:07.041Z", { topic: "trips.raw", partition: 0, offset: 482113 });
const Q = cp("queryable", 7, "2026-10-16T10:31:08.800Z", { table: "taxi.trips", visible_after_ms: 590 });

describe("merge and latency", () => {
  it("dedups by stage (first wins) and orders by seq, not arrival", () => {
    const later = { ...I, at: "2026-10-16T10:31:09.000Z" };
    expect(mergeCheckpoints([Q, I], [S, later]).map((c) => c.stage)).toEqual(["submitted", "ingested", "queryable"]);
    expect(mergeCheckpoints([I], [later])[0]!.at).toBe(I.at);
  });
  it("hop latency comes from wall time between consecutive stages", () => {
    expect(hopMs([Q, S, I])).toEqual({ ingested: 41, queryable: 1759 });
    expect(totalMs([S, I, Q])).toBe(1800);
    expect(totalMs([S, I])).toBeNull();
  });
});

describe("stage copy (spec 07)", () => {
  it("fills the detail values in", () => {
    expect(stageCopy(I)).toBe("Safely stored in Redpanda: topic trips.raw, partition 0, offset 482113. It can't get lost now.");
    expect(stageCopy(cp("validated", 3, S.at, { rules_checked: 11 }))).toBe("The stream processor checked 11 data-quality rules. All passed ✓");
    expect(stageCopy(cp("aggregated", 5, S.at, { window_start: "2025-10-09T17:42:00", borough: "Manhattan" })))
      .toBe("Counted in the Manhattan · 17:42 bucket on the live dashboard.");
    expect(stageCopy(cp("stored", 6, S.at, { table: "taxi.trips", batch_rows: 142 }))).toBe("Written to ClickHouse (taxi.trips) in a batch of 142 trips.");
    expect(stageCopy(Q)).toBe("The AI agent can now see your ride. Ask it something!");
  });
  it("rejected names the reason in plain English", () => {
    expect(stageCopy(cp("rejected", 3, S.at, { reasons: ["TIP_UNREALISTIC"] })))
      .toBe("Rejected: the tip is unrealistically high. Our pipeline rejects about 1 in 10 real NYC trips for problems like this. Try a normal tip!");
  });
  it("enriched copy for a card ride shows the tip %", () => {
    expect(stageCopy(cp("enriched", 4, S.at, { tip_pct: 20, duration_min: 16.5 }))).toBe("Added zone and borough names, and worked out your tip (20%) and duration.");
  });
  it("enriched copy for a cash ride never shows null%", () => {
    const text = stageCopy(cp("enriched", 4, S.at, { tip_pct: null, duration_min: 16.5 }));
    expect(text).not.toContain("null");
    expect(text).toBe("Added zone and borough names, and worked out your duration. Cash tips aren't recorded, so there's no tip %.");
  });
  it("waiting text names who is next", () => {
    expect(waitingText("validated")).toBe("waiting for the stream processor…");
    expect(waitingText("stored")).toBe("waiting for ClickHouse…");
  });
});

describe("messages", () => {
  it("parses history and checkpoint frames and drops garbage", () => {
    expect(toTraceMessage({ type: "history", checkpoints: [S, { nope: 1 }], sim_ts: "2025-10-09T17:42:00" }))
      .toEqual({ type: "history", checkpoints: [S], sim_ts: "2025-10-09T17:42:00" });
    expect(toTraceMessage({ type: "checkpoint", ...I })).toEqual({ type: "checkpoint", checkpoint: I });
    expect(toTraceMessage({ type: "checkpoint", stage: "teleported" })).toBeNull();
    expect(toTraceMessage("x")).toBeNull();
  });
  it("a reconnect's history merges without duplicates", () => {
    let v = applyMessage(EMPTY_VIEW, { type: "history", checkpoints: [S], sim_ts: null });
    v = applyMessage(v, { type: "checkpoint", checkpoint: I });
    v = applyMessage(v, { type: "history", checkpoints: [S, I, Q], sim_ts: "2025-10-09T17:42:00" });
    expect(v.checkpoints.map((c) => c.stage)).toEqual(["submitted", "ingested", "queryable"]);
    expect(v.simTs).toBe("2025-10-09T17:42:00");
  });
});

describe("ride summary", () => {
  it("rideChip for card", () => {
    expect(rideChip(rideSummary([S])!)).toBe("Midtown Center → JFK Airport, $5 tip");
    expect(rideChip({ ...rideSummary([S])!, tip: 3.9 })).toBe("Midtown Center → JFK Airport, $3.90 tip");
  });
  it("rideChip for cash", () => {
    const cash = cp("submitted", 1, S.at, { ...S.detail, payment: "cash" });
    expect(rideChip(rideSummary([cash])!)).toBe("Midtown Center → JFK Airport, cash");
  });
  it("no submitted checkpoint, no summary", () => expect(rideSummary([I])).toBeNull());
  it("trace id check", () => {
    expect(isTraceId(TID)).toBe(true);
    expect(isTraceId("live")).toBe(false);
  });
});
