"use client";
import { Bar, BarChart, CartesianGrid, Cell, LabelList, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { ChartSpec, RowsData } from "@/lib/agent-events";
import { compactNumber, formatValue } from "@/lib/format";
import { BOROUGH_COLORS, SERIES } from "@/lib/palette";
import { STAND_SCREEN, useMediaQuery } from "@/lib/use-media-query";

// Recessive chart chrome: text tokens for text, one-step-off-surface hairlines (dataviz marks spec).
const INK = "#f2f2f0";
const MUTED = "#a3a29b";
const GRID = "#2c2f36";
const PANEL = "#181b1f";
const MAX_LABELLED_BARS = 12;

/** Chart text and axis sizes in px: the stand screen is read from a step away, a phone at arm's length. */
const STAND = { font: 17, yAxis: 64, perChar: 10, maxCategory: 280, row: 32, maxHeight: 400 } as const;
const PHONE = { font: 13, yAxis: 44, perChar: 7.5, maxCategory: 136, row: 40, maxHeight: 500 } as const;

function chrome(font: number) {
  return {
    tick: { fill: MUTED, fontSize: font },
    tooltip: {
      contentStyle: { background: PANEL, border: `1px solid ${GRID}`, borderRadius: 8, fontSize: font },
      labelStyle: { color: MUTED },
      itemStyle: { color: INK },
    },
    legend: (value: string) => <span style={{ color: INK, fontSize: font }}>{value}</span>,
  };
}

type Datum = Record<string, string | number | null>;

/** Colour follows the entity: boroughs keep their fixed colour, anything else takes the series slot. */
const colorFor = (key: string, slot: number) => BOROUGH_COLORS[key] ?? SERIES[slot % SERIES.length]!;

function toData(spec: ChartSpec, rows: RowsData): { data: Datum[]; keys: string[] } {
  const col = (name: string) => rows.columns.findIndex((c) => c.name === name);
  const xi = col(spec.x);
  const num = (v: unknown) => (typeof v === "number" ? v : v == null ? null : Number(v));
  if (spec.series) {
    const si = col(spec.series);
    const yi = col(spec.y[0] ?? "");
    const keys = [...new Set(rows.rows.map((r) => String(r[si])))].slice(0, 3); // the first 3 slots validate all-pairs
    const byX = new Map<string, Datum>();
    for (const r of rows.rows) {
      const x = String(r[xi]);
      const key = String(r[si]);
      if (!keys.includes(key)) continue;
      byX.set(x, { ...(byX.get(x) ?? { x }), [key]: num(r[yi]) });
    }
    return { data: [...byX.values()], keys };
  }
  const data = rows.rows.map((r) => ({ x: String(r[xi] ?? "–"), ...Object.fromEntries(spec.y.map((y) => [y, num(r[col(y)])])) }));
  return { data, keys: spec.y };
}

/** "2025-10-09 17:42:00" → "17:42" on a time axis; anything else unchanged. */
function timeTick(v: unknown): string {
  const s = String(v);
  const m = /^\d{4}-\d{2}-\d{2}[ T](\d{2}:\d{2})/.exec(s);
  return m ? m[1]! : s;
}

export function AgentChart({ spec, rows }: { spec: ChartSpec; rows: RowsData }) {
  const stand = useMediaQuery(STAND_SCREEN);
  const size = stand ? STAND : PHONE;
  const { tick, tooltip, legend: legendText } = chrome(size.font);
  const unit = spec.unit ?? "none";
  if (spec.type === "table" || rows.rows.length === 0) return null;
  if (spec.type === "number") {
    const yi = rows.columns.findIndex((c) => c.name === spec.y[0]);
    return (
      <figure className="flex flex-col justify-center self-start rounded-xl border border-line bg-panel p-4 lg:p-6">
        <figcaption className="text-base text-muted lg:text-lg">{spec.title}</figcaption>
        <p className="mt-2 text-[2.75rem] leading-none font-semibold text-ink lg:text-[3.5rem]">{formatValue(rows.rows[0]?.[yi], unit)}</p>
      </figure>
    );
  }
  const { data, keys } = toData(spec, rows);
  const tip = (v: unknown) => formatValue(v, unit);
  const multi = keys.length > 1;

  if (spec.type === "line") {
    return (
      <figure className="min-w-0 self-start rounded-xl border border-line bg-panel p-4 lg:p-5">
        <figcaption className="mb-3 text-base font-medium text-ink lg:text-lg">{spec.title}</figcaption>
        <div className="h-[13rem]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 8, right: stand ? 24 : 8, bottom: 4, left: 4 }}>
              <CartesianGrid stroke={GRID} vertical={false} />
              <XAxis dataKey="x" tick={tick} tickFormatter={timeTick} minTickGap={32} tickLine={false} axisLine={{ stroke: GRID }} />
              <YAxis tick={tick} tickFormatter={compactNumber} tickLine={false} axisLine={false} width={size.yAxis} niceTicks="snap125" />
              <Tooltip formatter={tip} {...tooltip} />
              {multi && <Legend formatter={legendText} />}
              {keys.map((k, i) => (
                <Line key={k} dataKey={k} name={k} stroke={colorFor(multi ? k : "", i)} strokeWidth={2} dot={false}
                  activeDot={{ r: 5, stroke: PANEL, strokeWidth: 2 }} isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      </figure>
    );
  }

  // Bars: horizontal when category labels are longer than 12 characters (spec 08), and always on a phone,
  // where a row per category is the only way every label stays readable.
  const horizontal = !stand || data.some((d) => String(d.x).length > 12);
  const labelled = !multi && data.length <= MAX_LABELLED_BARS;
  const longest = Math.max(...data.map((d) => String(d.x).length));
  const categoryWidth = Math.min(size.maxCategory, longest * size.perChar + 16);
  // One readable row per category (every label shown) up to 12 bars; beyond that the height is capped
  // and Recharts thins the labels out. The table next to the chart always has every value.
  const height = horizontal ? Math.min(size.maxHeight, Math.max(160, data.length * (multi ? 48 : size.row) + 16)) : 240;
  const valueLabel = (v: unknown) => formatValue(v, unit);
  return (
    <figure className="min-w-0 self-start rounded-xl border border-line bg-panel p-4 lg:p-5">
      <figcaption className="mb-3 text-base font-medium text-ink lg:text-lg">{spec.title}</figcaption>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout={horizontal ? "vertical" : "horizontal"} barGap={2}
            margin={{ top: labelled && !horizontal ? 28 : 4, right: labelled && horizontal ? (stand ? 72 : 56) : 16, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={GRID} horizontal={!horizontal} vertical={horizontal && !labelled} />
            {horizontal ? (
              <>
                <XAxis type="number" hide={labelled} niceTicks="snap125" tick={tick} tickFormatter={compactNumber} tickLine={false} axisLine={false} />
                <YAxis type="category" dataKey="x" tick={{ ...tick, fill: INK }} width={categoryWidth} tickLine={false} axisLine={{ stroke: GRID }}
                  interval={labelled ? 0 : "preserveStartEnd"} />
              </>
            ) : (
              <>
                <XAxis dataKey="x" tick={{ ...tick, fill: INK }} tickLine={false} axisLine={{ stroke: GRID }} interval={labelled ? 0 : "preserveStartEnd"} />
                <YAxis hide={labelled} niceTicks="snap125" tick={tick} tickFormatter={compactNumber} tickLine={false} axisLine={false} width={size.yAxis} />
              </>
            )}
            <Tooltip formatter={tip} cursor={{ fill: "#ffffff10" }} {...tooltip} />
            {multi && <Legend formatter={legendText} />}
            {keys.map((k, i) => (
              <Bar key={k} dataKey={k} name={multi ? k : spec.title} fill={colorFor(multi ? k : "", i)} maxBarSize={28}
                radius={horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0]} isAnimationActive={false}>
                {!multi && data.map((d) => <Cell key={String(d.x)} fill={colorFor(String(d.x), 0)} />)}
                {labelled && (
                  <LabelList dataKey={k} position={horizontal ? "right" : "top"} formatter={valueLabel}
                    style={{ fill: INK, fontSize: size.font + 1, fontWeight: 600 }} />
                )}
              </Bar>
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </figure>
  );
}
