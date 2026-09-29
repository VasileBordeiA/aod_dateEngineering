"""What the agent may know about the schema: the whitelisted tables, their comments and 3 sample rows (spec 06).

These are fixed queries written here, not model SQL, and they run as agent_ro. That user sees only its granted tables
in system.tables, so agent_log never appears (spec 04).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

from api.db import Db

AGENT_TABLES = ("trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events")
ONESHOT_TABLES = ("trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now")


class Catalog:
    def __init__(self, db: Db, ttl_s: float = 300.0, clock: Callable[[], float] = time.monotonic) -> None:
        self._db = db
        self._ttl = ttl_s
        self._clock = clock
        self._tables: list[dict[str, str]] | None = None
        self._described: dict[str, tuple[float, dict[str, Any]]] = {}

    async def tables(self) -> list[dict[str, str]]:
        if self._tables is None:
            names = ", ".join(f"'{t}'" for t in AGENT_TABLES)
            r = await self._db.select(
                f"SELECT name, comment FROM system.tables WHERE database = 'taxi' AND name IN ({names})"
            )
            comments = {row[0]: row[1] for row in r.rows}
            self._tables = [{"name": t, "description": comments.get(t, "")} for t in AGENT_TABLES]
        return self._tables

    async def describe(self, table: str) -> dict[str, Any]:
        if table not in AGENT_TABLES:
            raise ValueError(f"unknown table {table!r}")
        hit = self._described.get(table)
        if hit is not None and self._clock() - hit[0] < self._ttl:
            return hit[1]
        columns = await self._db.select(f"DESCRIBE TABLE taxi.{table}")
        idx = {name: i for i, name in enumerate(columns.names)}
        sample = await self._db.select(f"SELECT * FROM taxi.{table} LIMIT 3")
        description = next(t["description"] for t in await self.tables() if t["name"] == table)
        info = {
            "table": table,
            "description": description,
            "columns": [
                {"name": r[idx["name"]], "type": r[idx["type"]], "comment": r[idx["comment"]]} for r in columns.rows
            ],
            "sample_rows": {"columns": sample.names, "rows": sample.rows},
        }
        self._described[table] = (self._clock(), info)
        return info

    async def schema_text(self, tables: Sequence[str] = ONESHOT_TABLES) -> str:
        blocks = []
        for table in tables:
            info = await self.describe(table)
            cols = "\n".join(
                f"  {c['name']} {c['type']}" + (f" -- {c['comment']}" if c["comment"] else "") for c in info["columns"]
            )
            blocks.append(f"taxi.{table}: {info['description']}\n{cols}")
        return "\n\n".join(blocks)
