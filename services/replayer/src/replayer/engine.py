"""Replay engine: paces rows by the sim clock, loops at EOF, backfills, survives backpressure (spec 01)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from replayer.clock import ManualClock, SimClock
from replayer.config import Config
from replayer.log import event
from replayer.messages import build_clock, build_ingested_checkpoint, build_trip, format_sim
from replayer.source import ReplaySource
from replayer.state import ReplayState, load_state, save_state

TRIPS_TOPIC = "trips.raw"
CLOCK_TOPIC = "sim.clock"
TRACE_TOPIC = "trace.checkpoints"
STATE_SAVE_INTERVAL_S = 5.0
PROGRESS_INTERVAL_S = 10.0
RESUME_BELOW_PENDING = 50_000
IDLE_SLEEP_S = 0.02


class Out(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any],
             on_delivery: Callable[[int, int], None] | None = None) -> None: ...
    def poll(self, timeout: float = 0.0) -> None: ...
    def pending(self) -> int: ...
    def flush(self, timeout: float = 30.0) -> int: ...


def utc_now() -> datetime:
    return datetime.now(UTC)


class Replayer:
    def __init__(self, cfg: Config, source: ReplaySource, out: Out, *,
                 monotonic: Callable[[], float] = time.monotonic, wall: Callable[[], datetime] = utc_now,
                 heartbeat_path: Path | None = None) -> None:
        self.cfg, self.source, self.out = cfg, source, out
        self._mono, self._wall, self._heartbeat = monotonic, wall, heartbeat_path
        state = load_state(cfg.state_path)
        if state is None:
            state = ReplayState(loop=0, row_idx=source.first_row_at_or_after(cfg.start_ts), sim_ts=cfg.start_ts)
        self.state = state
        self.clock: SimClock | ManualClock = (
            ManualClock(state.sim_ts) if cfg.speed is None else SimClock(state.sim_ts, cfg.speed, monotonic))
        self._rows: Iterator[dict[str, Any]] = source.rows_from(state.row_idx)
        self._next: dict[str, Any] | None = next(self._rows, None)
        self._lock = threading.RLock()
        self._user_paused = False
        self._backpressure = False
        self.finished = False
        self.sent_total = 0
        self._sent_since_progress = 0
        now = monotonic()
        self._last_clock = float("-inf")
        self._last_save = now
        self._last_progress = now

    # ── pacing ────────────────────────────────────────────────────────────
    def step(self, max_rows: int = 5_000) -> int:
        """Produce every row whose (loop-shifted) pickup_ts is due. Returns the number produced."""
        with self._lock:
            if self.finished or self._backpressure:
                return 0
            now = self.clock.tick()
            produced = 0
            while produced < max_rows:
                row = self._current_row()
                if row is None:
                    break
                due = row["pickup_ts"] + timedelta(days=self.state.loop * self.cfg.loop_offset_days)
                now = max(now, self.clock.now())  # the clock may have jumped at a loop wrap
                if self.cfg.speed is None:
                    if self.cfg.until_ts is not None and due > self.cfg.until_ts:
                        self._finish()
                        break
                elif self.clock.paused or due > now:
                    break
                try:
                    self._produce(row)
                except BufferError:
                    self._enter_backpressure()
                    break
                if isinstance(self.clock, ManualClock):
                    self.clock.advance_to(due)
                produced += 1
            self.sent_total += produced
            self._sent_since_progress += produced
            return produced

    def _current_row(self) -> dict[str, Any] | None:
        if self._next is None:
            if self.source.num_rows == 0:
                return None
            self.state.loop += 1
            self.state.row_idx = 0
            self._rows = self.source.rows_from(0)
            self._next = next(self._rows, None)
            if self._next is not None and isinstance(self.clock, SimClock):
                # Skip the empty days between the end of the month and the shifted start of the next loop.
                offset = timedelta(days=self.state.loop * self.cfg.loop_offset_days)
                self.clock.jump_to(self._next["pickup_ts"] + offset)
            event("loop", loop=self.state.loop, sim_ts=self.clock.now())
        return self._next

    def _produce(self, row: dict[str, Any]) -> None:
        msg = build_trip(row, loop=self.state.loop, offset_days=self.cfg.loop_offset_days,
                         trace_sample_rate=self.cfg.trace_sample_rate, produced_at=self._wall())
        on_delivery = self._checkpoint_on_delivery(msg) if msg["trace_id"] else None
        self.out.send(TRIPS_TOPIC, msg["trip_id"], msg, on_delivery)  # may raise BufferError: row not consumed
        self.state.row_idx = int(row["row_idx"]) + 1
        self._next = next(self._rows, None)

    def _checkpoint_on_delivery(self, msg: dict[str, Any]) -> Callable[[int, int], None]:
        def on_delivery(partition: int, offset: int) -> None:
            cp = build_ingested_checkpoint(msg, topic=TRIPS_TOPIC, partition=partition, offset=offset,
                                           at=self._wall())
            try:
                self.out.send(TRACE_TOPIC, msg["trace_id"], cp)
            except BufferError:
                event("checkpoint_dropped", trace_id=msg["trace_id"])
        return on_delivery

    def _finish(self) -> None:
        self.finished = True
        if self.cfg.until_ts is not None and isinstance(self.clock, ManualClock):
            self.clock.advance_to(self.cfg.until_ts)
        event("backfill_done", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx)

    # ── backpressure ──────────────────────────────────────────────────────
    def _enter_backpressure(self) -> None:
        self._backpressure = True
        if isinstance(self.clock, SimClock):
            self.clock.pause()
        event("broker_unavailable", pending=self.out.pending())

    def relieve_backpressure(self) -> None:
        with self._lock:
            if not self._backpressure:
                return
            self.out.poll(0.5)
            if self.out.pending() < RESUME_BELOW_PENDING:
                self._backpressure = False
                if isinstance(self.clock, SimClock) and not self._user_paused:
                    self.clock.resume()
                event("broker_available")

    # ── control ───────────────────────────────────────────────────────────
    @property
    def paused(self) -> bool:
        return self._user_paused or self._backpressure

    @property
    def speed(self) -> float | None:
        return self.clock.speed if isinstance(self.clock, SimClock) else None

    def pause(self) -> None:
        with self._lock:
            self._user_paused = True
            if isinstance(self.clock, SimClock):
                self.clock.pause()

    def resume(self) -> None:
        with self._lock:
            self._user_paused = False
            if isinstance(self.clock, SimClock) and not self._backpressure:
                self.clock.resume()

    def set_speed(self, speed: float) -> None:
        with self._lock:
            if not isinstance(self.clock, SimClock):
                raise ValueError("speed can't change in backfill mode")
            self.clock.set_speed(speed)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"mode": self.cfg.mode, "sim_ts": format_sim(self.clock.now(), millis=True),
                    "speed": "max" if self.speed is None else self.speed, "paused": self.paused,
                    "backpressure": self._backpressure, "loop": self.state.loop, "row_idx": self.state.row_idx,
                    "sent_total": self.sent_total, "producer_queue": self.out.pending(),
                    "finished": self.finished}

    # ── housekeeping & main loop ──────────────────────────────────────────
    def housekeeping(self, force: bool = False) -> None:
        with self._lock:
            mono = self._mono()
            if force or mono - self._last_clock >= self.cfg.clock_interval_s:
                self._last_clock = mono
                clock = build_clock(sim_ts=self.clock.now(), at=self._wall(), speed=self.speed,
                                    loop=self.state.loop, row_idx=self.state.row_idx, paused=self.paused)
                try:
                    self.out.send(CLOCK_TOPIC, "clock", clock)
                except BufferError:
                    pass
                if self._heartbeat is not None:
                    self._heartbeat.touch()
            if force or mono - self._last_save >= STATE_SAVE_INTERVAL_S:
                self._last_save = mono
                self.state.sim_ts = self.clock.now()
                save_state(self.cfg.state_path, self.state)
            if mono - self._last_progress >= PROGRESS_INTERVAL_S:
                elapsed = mono - self._last_progress
                event("progress", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx,
                      rows_sent_10s=self._sent_since_progress,
                      rate_per_s=round(self._sent_since_progress / elapsed, 1),
                      producer_queue=self.out.pending(), paused=self.paused)
                self._last_progress, self._sent_since_progress = mono, 0

    def run(self, stop: threading.Event) -> None:
        event("start", mode=self.cfg.mode, sim_ts=self.clock.now(), loop=self.state.loop,
              row_idx=self.state.row_idx, speed=self.speed or "max")
        while not stop.is_set() and not self.finished:
            produced = self.step()
            self.out.poll(0)
            self.housekeeping()
            if self._backpressure:
                self.relieve_backpressure()
            elif produced == 0:
                time.sleep(IDLE_SLEEP_S)
        self.housekeeping(force=True)  # final heartbeat + state, then flush so nothing is left queued
        self.out.flush(30)
        event("stop", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx,
              sent_total=self.sent_total)
