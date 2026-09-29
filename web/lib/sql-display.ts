/**
 * Reflow SQL for the kiosk SQL panel: runs of whitespace outside string literals become one space,
 * so a pretty-printed query wraps to the panel width instead of taking one screen line per clause.
 * The SQL itself is unchanged. SQL with a `--` comment is returned as-is, because joining its lines
 * would turn the rest of the query into comment.
 */
export function flowSql(sql: string): string {
  if (sql.includes("--")) return sql;
  // Split into string literals (odd indexes, '' is an escaped quote) and code (even indexes).
  return sql
    .split(/('(?:[^']|'')*')/)
    .map((part, i) => (i % 2 === 1 ? part : part.replace(/\s+/g, " ")))
    .join("")
    .trim();
}
