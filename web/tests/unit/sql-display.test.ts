import { describe, expect, it } from "vitest";
import { flowSql } from "@/lib/sql-display";

describe("flowSql", () => {
  it("joins a pretty-printed query into one flowing line", () => {
    const sql = "SELECT pu_borough AS borough,\n       count() AS trips\nFROM taxi.trips\nWHERE is_card\n  AND tip_pct IS NOT NULL\nGROUP BY borough";
    expect(flowSql(sql)).toBe("SELECT pu_borough AS borough, count() AS trips FROM taxi.trips WHERE is_card AND tip_pct IS NOT NULL GROUP BY borough");
  });
  it("keeps whitespace inside string literals", () => {
    expect(flowSql("SELECT 1\nWHERE zone = 'Times  Sq'\n  AND x = 'a\nb'")).toBe("SELECT 1 WHERE zone = 'Times  Sq' AND x = 'a\nb'");
  });
  it("keeps escaped quotes inside literals", () => {
    expect(flowSql("SELECT 'it''s   ok'\nFROM t")).toBe("SELECT 'it''s   ok' FROM t");
  });
  it("leaves SQL with line comments untouched, because joining lines would comment out the rest", () => {
    const sql = "SELECT 1 -- one\nFROM t";
    expect(flowSql(sql)).toBe(sql);
  });
  it("trims", () => {
    expect(flowSql("  SELECT 1  \n")).toBe("SELECT 1");
  });
});
