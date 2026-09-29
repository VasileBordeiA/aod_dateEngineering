"""Token bucket per client IP (spec 06 §Limits). The IP is only an in-memory key: it's never logged or stored."""

from __future__ import annotations

import time
from collections.abc import Callable

IDLE_FORGET_S = 600.0


class TokenBucket:
    def __init__(self, per_min: int, clock: Callable[[], float] = time.monotonic) -> None:
        self._capacity = float(per_min)
        self._rate = per_min / 60.0
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}  # key -> (tokens, last_seen)

    def allow(self, key: str) -> tuple[bool, float]:
        now = self._clock()
        self._forget_idle(now)
        tokens, last = self._buckets.get(key, (self._capacity, now))
        tokens = min(self._capacity, tokens + (now - last) * self._rate)
        if tokens >= 1.0:
            self._buckets[key] = (tokens - 1.0, now)
            return True, 0.0
        self._buckets[key] = (tokens, now)
        return False, (1.0 - tokens) / self._rate

    def _forget_idle(self, now: float) -> None:
        stale = [k for k, (_, last) in self._buckets.items() if now - last > IDLE_FORGET_S]
        for k in stale:
            del self._buckets[k]
