import { describe, expect, it } from "vitest";
import { compactNumber, formatValue, rowCountLabel } from "@/lib/format";

describe("formatValue", () => {
  it("formats by unit", () => {
    expect(formatValue(1234.5, "usd")).toBe("$1,234.50");
    expect(formatValue(23.14, "pct")).toBe("23.1%");
    expect(formatValue(12.345, "minutes")).toBe("12.3 min");
    expect(formatValue(3.456, "miles")).toBe("3.46 mi");
    expect(formatValue(987654, "trips")).toBe("987,654");
    expect(formatValue(1834.4, "ms")).toBe("1,834 ms");
    expect(formatValue(11.04, "mph")).toBe("11.0 mph");
    expect(formatValue(0.5)).toBe("0.5");
  });
  it("handles null and text", () => {
    expect(formatValue(null)).toBe("–");
    expect(formatValue("Manhattan")).toBe("Manhattan");
  });
  it("compacts axis ticks", () => {
    expect(compactNumber(12300)).toBe("12.3K");
    expect(compactNumber(950)).toBe("950");
  });
});

describe("rowCountLabel", () => {
  const rows = (shown: number) => Array.from({ length: shown }, (_, i) => [i]);
  it("says how many of the rows are shown when the UI got fewer than came back", () => {
    expect(rowCountLabel({ rows: rows(5), row_count: 5, truncated: false })).toBe("5 rows");
    expect(rowCountLabel({ rows: rows(50), row_count: 120, truncated: false })).toBe("Showing 50 of 120 rows");
    expect(rowCountLabel({ rows: rows(50), row_count: 200, truncated: true })).toBe("Showing 50 of 200 rows (safety LIMIT 200)");
  });
});
