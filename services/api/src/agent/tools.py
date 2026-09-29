"""The agent's four tools (spec 06). Every run_query goes through the SQL guard, then runs as agent_ro."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from agent.catalog import AGENT_TABLES, Catalog
from agent.sql_guard import SAFETY_LIMIT, check_sql
from api.db import Db, DbError, Result

Emit = Callable[[str, dict[str, Any]], Awaitable[None]]

UI_ROWS = 50
LLM_ROWS = 50
CHART_TYPES = ("bar", "line", "number", "table")
UNITS = ("usd", "pct", "minutes", "miles", "trips", "mph", "ms", "none")

TOOL_DEFS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "List the tables you can query, with a one-line description each.",
            "parameters": {"type": "object", "properties": {}, "additionalProperties": False},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_table",
            "description": "Get columns, types and comments of one table, plus 3 sample rows. "
            "Column comments contain important caveats.",
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["table"],
                "properties": {"table": {"type": "string", "enum": list(AGENT_TABLES)}},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_query",
            "description": "Run ONE read-only ClickHouse SELECT query against database taxi. Results are limited to "
            "200 rows; aggregate instead of listing rows.",
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["sql", "purpose"],
                "properties": {
                    "sql": {"type": "string", "description": "ClickHouse SQL. SELECT or WITH only."},
                    "purpose": {
                        "type": "string",
                        "description": "One short sentence for the student: what this query computes.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "render_chart",
            "description": "Show a small chart of the LAST successful query result. "
            "Call at most once, after run_query.",
            "parameters": {
                "type": "object",
                "additionalProperties": False,
                "required": ["type", "x", "y", "title"],
                "properties": {
                    "type": {"type": "string", "enum": list(CHART_TYPES)},
                    "x": {"type": "string", "description": "Column name for the x axis / category."},
                    "y": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3},
                    "series": {"type": "string", "description": "Optional column to split into series."},
                    "title": {"type": "string", "maxLength": 60},
                    "unit": {"type": "string", "enum": list(UNITS)},
                },
            },
        },
    },
]


@dataclass(frozen=True)
class ToolOutcome:
    ok: bool
    summary: str  # short text for the tool_result event
    content: str  # JSON sent back to the model as the tool message
    invalid: bool = False  # unknown tool or unusable arguments: counts toward failover
    query_failed: bool = False  # run_query rejected by the guard or ClickHouse: counts as a repair


def _error(summary: str, message: str, **flags: bool) -> ToolOutcome:
    return ToolOutcome(False, summary[:120], json.dumps({"error": message}), **flags)


class Toolbox:
    """Per-request tool state: the last successful result (for render_chart) and the attempt counter."""

    def __init__(self, db: Db, catalog: Catalog) -> None:
        self._db = db
        self._catalog = catalog
        self.last_result: Result | None = None
        self.attempts = 0
        self.chart_done = False

    async def call(self, name: str, raw_arguments: str | None, emit: Emit) -> ToolOutcome:
        try:
            args = json.loads(raw_arguments or "{}")
        except ValueError as exc:
            return _error(
                "invalid arguments", f"Arguments were not valid JSON ({exc}). Send a JSON object.", invalid=True
            )
        if not isinstance(args, dict):
            return _error("invalid arguments", "Arguments must be a JSON object.", invalid=True)
        if name == "list_tables":
            return await self._list_tables()
        if name == "describe_table":
            return await self._describe_table(args)
        if name == "run_query":
            return await self._run_query(args, emit)
        if name == "render_chart":
            return await self._render_chart(args, emit)
        return _error(
            "unknown tool",
            f"Unknown tool {name!r}. Use list_tables, describe_table, run_query or render_chart.",
            invalid=True,
        )

    async def _list_tables(self) -> ToolOutcome:
        tables = await self._catalog.tables()
        return ToolOutcome(True, f"{len(tables)} tables", json.dumps({"tables": tables}))

    async def _describe_table(self, args: dict[str, Any]) -> ToolOutcome:
        table = str(args.get("table", "")).strip().removeprefix("taxi.")
        if table not in AGENT_TABLES:
            return _error(
                "unknown table", f"Unknown table {table!r}. Use one of: {', '.join(AGENT_TABLES)}.", invalid=True
            )
        info = await self._catalog.describe(table)
        return ToolOutcome(True, f"{len(info['columns'])} columns", json.dumps(info, default=str))

    async def _run_query(self, args: dict[str, Any], emit: Emit) -> ToolOutcome:
        sql = args.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            return _error("invalid arguments", "run_query needs a non-empty 'sql' string.", invalid=True)
        purpose = str(args.get("purpose") or "")[:200]
        self.attempts += 1
        await emit("sql", {"attempt": self.attempts, "sql": sql.strip(), "purpose": purpose})
        verdict = check_sql(sql)
        if not verdict.ok:
            return _error(verdict.reason, verdict.reason, query_failed=True)
        try:
            result = await self._db.select(verdict.safe_sql)
        except DbError as exc:
            return _error(str(exc), str(exc), query_failed=True)
        self.last_result = result
        truncated = len(result.rows) >= SAFETY_LIMIT
        await emit(
            "rows",
            {
                "columns": [{"name": n, "type": t} for n, t in result.columns],
                "rows": result.rows[:UI_ROWS],
                "row_count": len(result.rows),
                "truncated": truncated,
                "elapsed_ms": result.elapsed_ms,
            },
        )
        content = json.dumps(
            {
                "columns": result.names,
                "rows": result.rows[:LLM_ROWS],
                "row_count": len(result.rows),
                "truncated": truncated,
            },
            default=str,
        )
        return ToolOutcome(True, f"{len(result.rows)} rows in {result.elapsed_ms} ms", content)

    async def _render_chart(self, args: dict[str, Any], emit: Emit) -> ToolOutcome:
        if self.last_result is None:
            return _error("no result yet", "Run a successful run_query first.")
        if self.chart_done:
            return _error("already rendered", "The chart is already on screen. Now write the answer.")
        chart_type, x, series = args.get("type"), args.get("x"), args.get("series") or None
        y = args.get("y")
        y_list = [y] if isinstance(y, str) else list(y) if isinstance(y, list) else []
        unit = args.get("unit") if args.get("unit") in UNITS else "none"
        columns = self.last_result.names
        problems = []
        if chart_type not in CHART_TYPES:
            problems.append(f"type must be one of {', '.join(CHART_TYPES)}")
        if not 1 <= len(y_list) <= 3:
            problems.append("y must list 1 to 3 columns")
        for column in [x, *y_list, *([series] if series else [])]:
            if column not in columns:
                problems.append(f"column {column!r} is not in the last result ({', '.join(columns)})")
        if problems:
            return _error("bad chart spec", "; ".join(problems))
        spec: dict[str, Any] = {
            "type": chart_type,
            "x": x,
            "y": y_list,
            "title": str(args.get("title") or "")[:60],
            "unit": unit,
        }
        if series:
            spec["series"] = series
        self.chart_done = True
        await emit("chart", spec)
        return ToolOutcome(True, f"{chart_type} chart", json.dumps({"ok": True}))
