"""History + live WebSocket streams for the trace viewers (spec 07 §Trace relay)."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Protocol

from starlette.websockets import WebSocket, WebSocketDisconnect

from api import log
from api.db import DbError, DbUnavailable, TraceDb
from rides.checkpoints import TERMINAL, wall_str
from rides.hub import Hub

HISTORY_SQL = ("SELECT trace_id, trip_id, source, stage, seq, service, at, detail FROM taxi.trace_events "
               "WHERE trace_id = {id:UUID} ORDER BY seq, at")
# "Today" is the wall-clock event day, like Grafana panel 11: these are visitors' rides, not replayed trips.
TODAY_SQL = ("SELECT countIf(stage = 'submitted') AS rides, countIf(stage = 'rejected') AS rejected "
             "FROM taxi.trace_events WHERE source = 'student' AND at >= toStartOfDay(now64(3))")
HOPS_SQL = """
SELECT stage, round(quantile(0.5)(ms)) AS p50_ms
FROM
(
    SELECT stage, seq,
           dateDiff('millisecond',
                    lagInFrame(at) OVER (PARTITION BY trace_id ORDER BY seq ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                    at) AS ms,
           row_number() OVER (PARTITION BY trace_id ORDER BY seq) AS rn
    FROM taxi.trace_events
    WHERE at >= now64(3) - INTERVAL 15 MINUTE
)
WHERE rn > 1
GROUP BY stage
ORDER BY any(seq)
"""
# A follower that hears nothing for REFRESH_AFTER_S re-reads the history, in case the relay missed checkpoints while
# it (re)joined. Each refresh that finds nothing doubles the wait, up to REFRESH_MAX_S, so a stuck trace stays cheap.
REFRESH_AFTER_S = 5.0
REFRESH_MAX_S = 60.0


class Sender(Protocol):
    async def send_json(self, data: Any) -> None: ...

    async def close(self, code: int = 1000) -> None: ...


class LatestClock(Protocol):
    latest: dict[str, Any] | None


def normalise_trace_id(s: str) -> str | None:
    try:
        return str(uuid.UUID(s))
    except ValueError:
        return None


def _json_object(text: Any) -> dict[str, Any]:
    try:
        value = json.loads(text) if isinstance(text, str) else text
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def _row_checkpoint(row: dict[str, Any]) -> dict[str, Any]:
    return {"trace_id": str(row["trace_id"]), "trip_id": row["trip_id"], "source": row["source"],
            "stage": row["stage"], "seq": int(row["seq"]), "service": row["service"],
            "at": wall_str(datetime.fromisoformat(str(row["at"]))), "detail": _json_object(row["detail"])}


class TraceService:
    def __init__(self, hub: Hub, db: TraceDb, sim_clock: LatestClock, *, follow_max_s: float = 600,
                 after_terminal_s: float = 60, clock_every_s: float = 1.0, stats_every_s: float = 5.0,
                 refresh_after_s: float = REFRESH_AFTER_S) -> None:
        self._hub, self._db, self._sim = hub, db, sim_clock
        self._follow_max, self._after_terminal = follow_max_s, after_terminal_s
        self._refresh_after = refresh_after_s
        self._clock_every, self._stats_every = clock_every_s, stats_every_s
        self._stats: dict[str, Any] | None = None
        self._stats_at = float("-inf")

    def sim_ts(self) -> str | None:
        latest = self._sim.latest
        return str(latest["sim_ts"]) if latest and "sim_ts" in latest else None

    async def history(self, trace_id: str, rearm: bool = True) -> list[dict[str, Any]]:
        """Memory ∪ taxi.trace_events, dedup by stage. After an api restart, memory only has what came since."""
        try:
            r = await self._db.query(HISTORY_SQL, {"id": trace_id})
            stored = [_row_checkpoint(dict(zip(r.names, row, strict=True))) for row in r.rows]
        except (DbError, DbUnavailable):
            stored = []
        cps = self._hub.history.merge(trace_id, stored) if stored else self._hub.history.get(trace_id)
        stages = {c["stage"] for c in cps}
        if rearm and "stored" in stages and not stages & TERMINAL and self._hub.on_stored is not None:
            # restarted between `stored` and `queryable`: poll again so the viewer still reaches the end
            self._hub.on_stored(next(c for c in cps if c["stage"] == "stored"))
        return cps

    async def follow(self, ws: Sender, trace_id: str) -> None:
        q = self._hub.follow(trace_id)  # subscribe first, so nothing published during the history read is lost
        try:
            cps = await self.history(trace_id)
            seen = {c["stage"] for c in cps}
            await ws.send_json({"type": "history", "checkpoints": cps, "sim_ts": self.sim_ts()})
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._follow_max
            if seen & TERMINAL:
                deadline = min(deadline, loop.time() + self._after_terminal)
            refresh_in = self._refresh_after
            while (left := deadline - loop.time()) > 0:
                wait = left if seen & TERMINAL else min(left, refresh_in)
                try:
                    cp = await asyncio.wait_for(q.get(), wait)
                except TimeoutError:
                    if wait >= left:  # the deadline itself
                        break
                    found = await self._catch_up(ws, trace_id, seen)
                    refresh_in = self._refresh_after if found else min(refresh_in * 2, REFRESH_MAX_S)
                    if seen & TERMINAL:
                        deadline = min(deadline, loop.time() + self._after_terminal)
                    continue
                refresh_in = self._refresh_after
                if cp["stage"] in seen:
                    continue
                seen.add(cp["stage"])
                await ws.send_json({"type": "checkpoint", **cp})
                if cp["stage"] in TERMINAL:
                    deadline = min(deadline, loop.time() + self._after_terminal)
            await ws.close(1000)
        finally:
            self._hub.unfollow(trace_id, q)

    async def _catch_up(self, ws: Sender, trace_id: str, seen: set[str]) -> bool:
        """Send the stages this follower hasn't seen yet from a fresh history read. True if there were any."""
        new = [c for c in await self.history(trace_id, rearm=False) if c["stage"] not in seen]
        for c in new:
            seen.add(c["stage"])
            await ws.send_json({"type": "checkpoint", **c})
        stored = next((c for c in new if c["stage"] == "stored"), None)
        if stored is not None and not seen & TERMINAL and self._hub.on_stored is not None:
            self._hub.on_stored(stored)  # the hub never saw this `stored`, so the queryable poll never started
        return bool(new)

    async def stats(self) -> dict[str, Any] | None:
        now = asyncio.get_running_loop().time()
        if now - self._stats_at < self._stats_every:
            return self._stats
        try:
            today = await self._db.query(TODAY_SQL, {})
            hops = await self._db.query(HOPS_SQL, {})
        except (DbError, DbUnavailable):
            return self._stats
        row = dict(zip(today.names, today.rows[0], strict=True)) if today.rows else {}
        self._stats = {"type": "stats", "rides_today": int(row.get("rides") or 0),
                       "rejected_today": int(row.get("rejected") or 0),
                       "median_ms": {str(r[0]): int(r[1]) for r in hops.rows if len(r) > 1 and r[1] is not None}}
        self._stats_at = now
        return self._stats

    async def live(self, ws: Sender) -> None:
        q = self._hub.watch_live()
        try:
            traces = [{"trace_id": t, "checkpoints": c} for t, c in self._hub.history.recent_student(20)]
            await ws.send_json({"type": "snapshot", "traces": traces, "sim_ts": self.sim_ts()})
            loop = asyncio.get_running_loop()
            next_clock = next_stats = loop.time()
            while True:
                now = loop.time()
                if now >= next_clock:
                    await ws.send_json({"type": "clock", "sim_ts": self.sim_ts()})
                    next_clock = now + self._clock_every
                if now >= next_stats:
                    stats = await self.stats()
                    if stats is not None:
                        await ws.send_json(stats)
                    next_stats = now + self._stats_every
                try:
                    cp = await asyncio.wait_for(q.get(), max(0.0, min(next_clock, next_stats) - loop.time()))
                except TimeoutError:
                    continue
                await ws.send_json({"type": "checkpoint", **cp, "ambient": cp.get("source") != "student"})
        finally:
            self._hub.unwatch_live(q)


async def serve(ws: WebSocket, work: Coroutine[Any, Any, None]) -> None:
    """Run `work` until it ends or the client goes away (a phone locking its screen), whichever comes first."""

    async def disconnected() -> None:
        while (await ws.receive())["type"] != "websocket.disconnect":
            pass

    tasks = [asyncio.ensure_future(work), asyncio.ensure_future(disconnected())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        # asyncio.wait, not gather: gather re-raises a child's CancelledError when the server cancels this task, and
        # anyio (uvicorn shutdown, TestClient) does not recognise that one as its own, so it would escape the handler
        await asyncio.wait(tasks)
    for t in tasks:
        exc = None if t.cancelled() else t.exception()
        if isinstance(exc, Exception) and not isinstance(exc, WebSocketDisconnect | RuntimeError):
            log.event("ws_error", error=log.error_label(exc), where=log.error_where(exc))
