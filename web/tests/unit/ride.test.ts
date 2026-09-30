import { describe, expect, it } from "vitest";
import type { Zone } from "@/lib/api";
import { estimateLine, filterZones, groupByBorough, loadLastTrace, saveLastTrace, tipAmount } from "@/lib/ride";

const ZONES: Zone[] = [
  { location_id: 255, zone: "Williamsburg (North Side)", borough: "Brooklyn" },
  { location_id: 79, zone: "East Village", borough: "Manhattan" },
  { location_id: 161, zone: "Midtown Center", borough: "Manhattan" },
  { location_id: 132, zone: "JFK Airport", borough: "Queens" },
];

describe("tips", () => {
  it("percentages of the estimated fare, rounded to cents", () => {
    expect(tipAmount({ kind: "pct", pct: 20 }, 19.5)).toBe(3.9);
    expect(tipAmount({ kind: "pct", pct: 15 }, 19.5)).toBe(2.93);
    expect(tipAmount({ kind: "pct", pct: 25 }, null)).toBe(0);
    expect(tipAmount({ kind: "none" }, 19.5)).toBe(0);
  });
  it("custom is clamped to 0-999 with no other warning (the processor decides)", () => {
    expect(tipAmount({ kind: "custom", amount: 999 }, 19.5)).toBe(999);
    expect(tipAmount({ kind: "custom", amount: 5000 }, 19.5)).toBe(999);
    expect(tipAmount({ kind: "custom", amount: -3 }, 19.5)).toBe(0);
    expect(tipAmount({ kind: "custom", amount: Number.NaN }, 19.5)).toBe(0);
    expect(tipAmount({ kind: "custom", amount: 4.567 }, 19.5)).toBe(4.57);
  });
});

describe("zones", () => {
  it("groups by borough in api order", () => {
    expect(groupByBorough(ZONES).map(([b, zs]) => [b, zs.length])).toEqual([["Brooklyn", 1], ["Manhattan", 2], ["Queens", 1]]);
  });
  it("filters by zone or borough, case-insensitively", () => {
    expect(filterZones(ZONES, "jfk").map((z) => z.location_id)).toEqual([132]);
    expect(filterZones(ZONES, "manhattan")).toHaveLength(2);
    expect(filterZones(ZONES, "  ")).toHaveLength(4);
  });
});

it("estimate line", () => {
  expect(estimateLine({ trip_distance: 3.1, duration_min: 16.5, fare_amount: 19.5, method: "median of 214 real trips between these zones" }))
    .toBe("~3.1 mi · ~17 min · fare ~$19.50 (median of 214 real trips between these zones)");
});

it("last trace survives in storage, and a broken storage never throws", () => {
  const data = new Map<string, string>();
  const storage = { getItem: (k: string) => data.get(k) ?? null, setItem: (k: string, v: string) => void data.set(k, v) };
  saveLastTrace("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f", storage);
  expect(loadLastTrace(storage)).toBe("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f");
  data.set("nyc-taxi-live:last-trace", "garbage");
  expect(loadLastTrace(storage)).toBeNull();
  const broken = { getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } };
  expect(() => saveLastTrace("x", broken)).not.toThrow();
  expect(loadLastTrace(broken)).toBeNull();
  expect(loadLastTrace()).toBeNull(); // no window in the test environment
});
