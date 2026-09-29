"use client";
import { Bar, BarChart, CartesianGrid, Cell, LabelList, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { ChartSpec, RowsData } from "@/lib/agent-events";
import { compactNumber, formatValue } from "@/lib/format";
import { BOROUGH_COLORS, SERIES } from "@/lib/palette";

// Recessive chart chrome: text tokens for text, one-step-off-surface hairlines (dataviz marks spec).
const INK = "#f2f2f0";
const MUTED = "#a3a29b";
const GRID = "#2c2f36";
const PANEL = "#181b1f";
const TICK = { fill: MUTED, fontSize: 17 };
const TOOLTIP = {
  contentStyle: { background: PANEL, border: `1px solid ${GRID}`, borderRadius: 8, fontSize: 17 },
  labelStyle: { color: MUTED },
  itemStyle: { color: INK },
};
const MAX_LABELLED_BARS = 12;

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

function legendText(value: string) {
  return <span style={{ color: INK, fontSize: 17 }}>{value}</span>;
}

export function AgentChart({ spec, rows }: { spec: ChartSpec; rows: RowsData }) {
  const unit = spec.unit ?? "none";
  if (spec.type === "table" || rows.rows.length === 0) return null;
  if (spec.type === "number") {
    const yi = rows.columns.findIndex((c) => c.name === spec.y[0]);
    return (
      <figure className="flex flex-col justify-center self-start rounded-xl border border-line bg-panel p-6">
        <figcaption className="text-lg text-muted">{spec.title}</figcaption>
        <p className="mt-2 text-[3.5rem] leading-none font-semibold text-ink">{formatValue(rows.rows[0]?.[yi], unit)}</p>
      </figure>
    );
  }
  const { data, keys } = toData(spec, rows);
  const tip = (v: unknown) => formatValue(v, unit);
  const multi = keys.length > 1;

  if (spec.type === "line") {
    return (
      <figure className="min-w-0 self-start rounded-xl border border-line bg-panel p-5">
        <figcaption className="mb-3 text-lg font-medium text-ink">{spec.title}</figcaption>
        <div className="h-[13rem]">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 8, right: 24, bottom: 4, left: 4 }}>
              <CartesianGrid stroke={GRID} vertical={false} />
              <XAxis dataKey="x" tick={TICK} tickFormatter={timeTick} minTickGap={32} tickLine={false} axisLine={{ stroke: GRID }} />
              <YAxis tick={TICK} tickFormatter={compactNumber} tickLine={false} axisLine={false} width={64} niceTicks="snap125" />
              <Tooltip formatter={tip} {...TOOLTIP} />
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

  // Bars: horizontal when category labels are longer than 12 characters (spec 08).
  const horizontal = data.some((d) => String(d.x).length > 12);
  const labelled = !multi && data.length <= MAX_LABELLED_BARS;
  const longest = Math.max(...data.map((d) => String(d.x).length));
  const categoryWidth = Math.min(280, longest * 10 + 16);
  // One readable row per category (every label shown) up to 12 bars; beyond that the height is capped
  // and Recharts thins the labels out. The table next to the chart always has every value.
  const height = horizontal ? Math.min(400, Math.max(160, data.length * (multi ? 48 : 32) + 16)) : 240;
  const valueLabel = (v: unknown) => formatValue(v, unit);
  return (
    <figure className="min-w-0 self-start rounded-xl border border-line bg-panel p-5">
      <figcaption className="mb-3 text-lg font-medium text-ink">{spec.title}</figcaption>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout={horizontal ? "vertical" : "horizontal"} barGap={2}
            margin={{ top: labelled && !horizontal ? 28 : 4, right: labelled && horizontal ? 72 : 16, bottom: 4, left: 4 }}>
            <CartesianGrid stroke={GRID} horizontal={!horizontal} vertical={horizontal && !labelled} />
            {horizontal ? (
              <>
                <XAxis type="number" hide={labelled} niceTicks="snap125" tick={TICK} tickFormatter={compactNumber} tickLine={false} axisLine={false} />
                <YAxis type="category" dataKey="x" tick={{ ...TICK, fill: INK }} width={categoryWidth} tickLine={false} axisLine={{ stroke: GRID }}
                  interval={labelled ? 0 : "preserveStartEnd"} />
              </>
            ) : (
              <>
                <XAxis dataKey="x" tick={{ ...TICK, fill: INK }} tickLine={false} axisLine={{ stroke: GRID }} interval={labelled ? 0 : "preserveStartEnd"} />
                <YAxis hide={labelled} niceTicks="snap125" tick={TICK} tickFormatter={compactNumber} tickLine={false} axisLine={false} width={64} />
              </>
            )}
            <Tooltip formatter={tip} cursor={{ fill: "#ffffff10" }} {...TOOLTIP} />
            {multi && <Legend formatter={legendText} />}
            {keys.map((k, i) => (
              <Bar key={k} dataKey={k} name={multi ? k : spec.title} fill={colorFor(multi ? k : "", i)} maxBarSize={28}
                radius={horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0]} isAnimationActive={false}>
                {!multi && data.map((d) => <Cell key={String(d.x)} fill={colorFor(String(d.x), 0)} />)}
                {labelled && (
                  <LabelList dataKey={k} position={horizontal ? "right" : "top"} formatter={valueLabel}
                    style={{ fill: INK, fontSize: 18, fontWeight: 600 }} />
                )}
              </Bar>
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>
    </figure>
  );
}
