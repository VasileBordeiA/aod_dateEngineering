"""Short-term trace history: trace_id → {stage: checkpoint} (spec 07 §Trace relay, History)."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any

from rides.checkpoints import ordered

Checkpoint = dict[str, Any]


class TraceHistory:
    """In-memory, least recently touched first. TTL and size bound so a long stand day can't grow it forever."""

    def __init__(self, ttl_s: float, max_traces: int = 20_000, clock: Callable[[], float] = time.monotonic) -> None:
        self._ttl, self._max, self._clock = ttl_s, max_traces, clock
        self._traces: OrderedDict[str, tuple[float, dict[str, Checkpoint]]] = OrderedDict()

    def _expire(self, now: float) -> None:
        while self._traces:
            tid, (touched, _) = next(iter(self._traces.items()))
            if now - touched <= self._ttl:
                return
            del self._traces[tid]

    def add(self, cp: Checkpoint) -> bool:
        """Store a checkpoint. False if this trace already has that stage (the first one wins)."""
        now = self._clock()
        self._expire(now)
        tid = cp["trace_id"]
        _, stages = self._traces.pop(tid, (now, {}))
        is_new = cp["stage"] not in stages
        if is_new:
            stages[cp["stage"]] = cp
        self._traces[tid] = (now, stages)
        while len(self._traces) > self._max:
            self._traces.popitem(last=False)
        return is_new

    def get(self, trace_id: str) -> list[Checkpoint]:
        self._expire(self._clock())
        entry = self._traces.get(trace_id)
        return ordered(entry[1].values()) if entry else []

    def merge(self, trace_id: str, cps: Iterable[Checkpoint]) -> list[Checkpoint]:
        for cp in cps:
            self.add(cp)
        return self.get(trace_id)

    def has_stage(self, trace_id: str, stage: str) -> bool:
        entry = self._traces.get(trace_id)
        return entry is not None and stage in entry[1]

    def recent_student(self, n: int = 20) -> list[tuple[str, list[Checkpoint]]]:
        self._expire(self._clock())
        out: list[tuple[str, list[Checkpoint]]] = []
        for tid, (_, stages) in reversed(self._traces.items()):
            if any(c.get("source") == "student" for c in stages.values()):
                out.append((tid, ordered(stages.values())))
                if len(out) == n:
                    break
        return out
