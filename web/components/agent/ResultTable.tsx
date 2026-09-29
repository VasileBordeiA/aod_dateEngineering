import type { RowsData, Unit } from "@/lib/agent-events";
import { formatValue } from "@/lib/format";

const NUMERIC = /^(Nullable\()?(U?Int|Float|Decimal)/;

export function ResultTable({ rows, unit, valueColumns }: { rows: RowsData; unit?: Unit; valueColumns: string[] }) {
  const numeric = rows.columns.map((c) => NUMERIC.test(c.type));
  const temporal = rows.columns.map((c) => /Date/.test(c.type));
  return (
    <div className="flex max-h-[20rem] min-w-0 flex-col self-start rounded-xl border border-line bg-panel">
      <div className="min-h-0 overflow-auto">
        <table className="w-full text-left text-base">
          <thead className="sticky top-0 bg-panel text-muted">
            <tr>
              {rows.columns.map((c, j) => (
                <th key={c.name} scope="col" className={`px-3 py-2 font-medium whitespace-nowrap ${numeric[j] ? "text-right" : ""}`}>{c.name}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.rows.map((r, i) => (
              <tr key={i} className="border-t border-line">
                {r.map((v, j) => {
                  const name = rows.columns[j]?.name ?? "";
                  // ClickHouse sends 64-bit integers as JSON strings; show them as numbers.
                  const value = numeric[j] && typeof v === "string" && v.trim() !== "" && Number.isFinite(Number(v)) ? Number(v) : v;
                  const isNum = numeric[j] || typeof value === "number";
                  return (
                    <td key={j} className={`px-3 py-1.5 whitespace-nowrap ${isNum || temporal[j] ? "tabular-nums" : ""} ${isNum ? "text-right" : ""}`}>
                      {formatValue(value, valueColumns.includes(name) ? unit : "none")}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="border-t border-line px-3 py-2 text-sm text-muted">
        {rows.truncated ? `Showing ${rows.rows.length} of ${rows.row_count} rows (safety LIMIT 200)` : `${rows.row_count} rows`} · {rows.elapsed_ms} ms
      </p>
    </div>
  );
}
