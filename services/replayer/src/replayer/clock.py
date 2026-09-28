"""Simulated clocks (spec 01 §Replayer pacing).

SimClock:    sim_now = sim_anchor + (monotonic - wall_anchor) * speed. It re-anchors on pause, resume and
             speed change, so none of them jump. A wall gap > max_gap_s is not replayed as a burst.
ManualClock: backfill mode. "now" is the pickup time of the last row produced.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timedelta


class SimClock:
    def __init__(self, sim_start: datetime, speed: float, monotonic: Callable[[], float] = time.monotonic,
                 max_gap_s: float = 10.0) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        self._mono = monotonic
        self._speed = float(speed)
        self._sim_anchor = sim_start
        self._wall_anchor = monotonic()
        self._last_tick = self._wall_anchor
        self._paused = False
        self._max_gap = max_gap_s

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    def now(self) -> datetime:
        if self._paused:
            return self._sim_anchor
        return self._sim_anchor + timedelta(seconds=(self._mono() - self._wall_anchor) * self._speed)

    def _reanchor(self) -> None:
        self._sim_anchor = self.now()
        self._wall_anchor = self._mono()

    def pause(self) -> None:
        if not self._paused:
            self._reanchor()
            self._paused = True

    def resume(self) -> None:
        if self._paused:
            self._wall_anchor = self._last_tick = self._mono()
            self._paused = False

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        if not self._paused:
            self._reanchor()
        self._speed = float(speed)

    def tick(self) -> datetime:
        """Call once per loop iteration."""
        mono = self._mono()
        if not self._paused and mono - self._last_tick > self._max_gap:
            # Freeze sim time at the last tick instead of replaying the gap.
            self._sim_anchor += timedelta(seconds=(self._last_tick - self._wall_anchor) * self._speed)
            self._wall_anchor = mono
        self._last_tick = mono
        return self.now()


class ManualClock:
    paused = False

    def __init__(self, sim_start: datetime) -> None:
        self._now = sim_start

    def now(self) -> datetime:
        return self._now

    def tick(self) -> datetime:
        return self._now

    def advance_to(self, ts: datetime) -> None:
        if ts > self._now:
            self._now = ts
