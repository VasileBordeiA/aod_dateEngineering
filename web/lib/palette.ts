/** One fixed colour per borough, shared with Grafana (spec 05 §Borough palette). tests/unit/palette.test.ts keeps them in sync. */
export const BOROUGH_COLORS: Record<string, string> = {
  Manhattan: "#3987e5",
  Brooklyn: "#d95926",
  Queens: "#199e70",
  Bronx: "#c98500",
  "Staten Island": "#d55181",
  EWR: "#008300",
};

/** The architecture image's colours (spec 08 §Visual language). */
export const PIPELINE = { fill: "#3C3489", stroke: "#534AB7", text: "#EEEDFE" } as const;
export const VISITOR = { fill: "#0B4D3F", stroke: "#1D9E75", text: "#E1F5EE" } as const;
export const SOURCE = "#46463F";
export const AMBER = "#fab219";

/** Series colours for non-borough charts: the first 3 slots of the validated palette. */
export const SERIES = ["#3987e5", "#d95926", "#199e70"] as const;
