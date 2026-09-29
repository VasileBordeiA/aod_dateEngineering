import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { BOROUGH_COLORS } from "@/lib/palette";

describe("palette", () => {
  it("matches the spec 05 borough table used by Grafana", () => {
    const spec = readFileSync(new URL("../../../docs/specs/05-grafana-dashboards.md", import.meta.url), "utf8");
    const rows = [...spec.matchAll(/^\| (Manhattan|Brooklyn|Queens|Bronx|Staten Island|EWR) \| `(#[0-9a-f]{6})` \|/gm)];
    expect(Object.fromEntries(rows.map((m) => [m[1], m[2]]))).toEqual(BOROUGH_COLORS);
  });
});
