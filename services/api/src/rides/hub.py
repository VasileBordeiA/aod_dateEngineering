"""Checkpoint fan-out to WebSocket clients. Event-loop only: the relay thread hands over with call_soon_threadsafe."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from rides.history import TraceHistory

Checkpoint = dict[str, Any]


def _offer(q: asyncio.Queue[Checkpoint], cp: Checkpoint) -> None:
    try:
        q.put_nowait(cp)
    except asyncio.QueueFull:
        pass  # a stuck client misses live updates; it gets the full history when it reconnects


class Hub:
    def __init__(self, history: TraceHistory, on_stored: Callable[[Checkpoint], None] | None = None,
                 queue_size: int = 200) -> None:
        self.history = history
        self.on_stored = on_stored
        self._size = queue_size
        self._followers: dict[str, set[asyncio.Queue[Checkpoint]]] = {}
        self._live: set[asyncio.Queue[Checkpoint]] = set()

    def publish(self, cp: Checkpoint) -> None:
        if not self.history.add(cp):
            return  # already seen: every stage reaches each client once
        for q in [*self._followers.get(cp["trace_id"], ()), *self._live]:
            _offer(q, cp)
        if cp["stage"] == "stored" and self.on_stored is not None:
            self.on_stored(cp)

    def follow(self, trace_id: str) -> asyncio.Queue[Checkpoint]:
        q: asyncio.Queue[Checkpoint] = asyncio.Queue(self._size)
        self._followers.setdefault(trace_id, set()).add(q)
        return q

    def unfollow(self, trace_id: str, q: asyncio.Queue[Checkpoint]) -> None:
        qs = self._followers.get(trace_id)
        if qs is not None:
            qs.discard(q)
            if not qs:
                del self._followers[trace_id]

    def watch_live(self) -> asyncio.Queue[Checkpoint]:
        q: asyncio.Queue[Checkpoint] = asyncio.Queue(self._size * 5)
        self._live.add(q)
        return q

    def unwatch_live(self, q: asyncio.Queue[Checkpoint]) -> None:
        self._live.discard(q)

    def follower_count(self, trace_id: str) -> int:
        return len(self._followers.get(trace_id, ()))

    def live_count(self) -> int:
        return len(self._live)
