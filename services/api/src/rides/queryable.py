"""Queryable detector (spec 07): after `stored`, poll as trace_ro until the row is visible, then emit `queryable`."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from api import log
from api.db import DbError, DbUnavailable, TraceDb
from rides.checkpoints import checkpoint, parse_wall, wall_str

# One query per tick for every pending trace (ADR-019), so a burst of 50 rides costs 4 queries/s, not 200.
VISIBLE_SQL = "SELECT DISTINCT trace_id FROM taxi.trips WHERE trace_id IN {ids:Array(UUID)}"


@dataclass
class _Pending:
    stored: dict[str, Any]
    started: float


class QueryableDetector:
    def __init__(self, db: TraceDb, emit: Callable[[dict[str, Any]], None], poll_ms: int, *,
                 timeout_s: float = 10.0, max_pending: int = 20, clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._db, self._emit, self._poll_s = db, emit, poll_ms / 1000
        self._timeout, self._max, self._clock, self._wall = timeout_s, max_pending, clock, wall
        self._pending: dict[str, _Pending] = {}
        self._waiting: deque[dict[str, Any]] = deque(maxlen=500)
        self._wake = asyncio.Event()

    @property
    def pending(self) -> int:
        return len(self._pending)

    def on_stored(self, cp: dict[str, Any]) -> None:
        if cp["trace_id"] in self._pending:
            return
        if len(self._pending) < self._max:
            self._pending[cp["trace_id"]] = _Pending(cp, self._clock())
        else:
            self._waiting.append(cp)
        self._wake.set()

    def _queryable(self, stored: dict[str, Any]) -> dict[str, Any]:
        now = self._wall()
        ms = max(0, int((now - parse_wall(stored["at"])).total_seconds() * 1000))
        return checkpoint(stored["trace_id"], stored["trip_id"], stored["source"], "queryable",
                          {"table": "taxi.trips", "visible_after_ms": ms}, wall_str(now))

    async def tick(self) -> None:
        if not self._pending:
            return
        ids = list(self._pending)
        r = await self._db.query(VISIBLE_SQL, {"ids": ids})
        visible = {str(row[0]) for row in r.rows}
        now = self._clock()
        for tid in ids:
            p = self._pending[tid]
            if tid in visible:
                del self._pending[tid]
                self._emit(self._queryable(p.stored))
            elif now - p.started >= self._timeout:
                del self._pending[tid]
                log.event("queryable_timeout", trace_id=tid, source=p.stored["source"])
        while self._waiting and len(self._pending) < self._max:
            self.on_stored(self._waiting.popleft())

    async def safe_tick(self) -> None:
        try:
            await self.tick()
        except (DbError, DbUnavailable) as exc:
            log.event("queryable_poll_failed", error=str(exc)[:200])

    async def run(self) -> None:
        while True:
            if not self._pending:
                self._wake.clear()
                await self._wake.wait()
            await self.safe_tick()
            await asyncio.sleep(self._poll_s)
