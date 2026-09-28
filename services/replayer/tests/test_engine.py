import json
import threading
from datetime import datetime, timezone

import pytest
from contract_validator import assert_valid

from replayer.config import Config
from replayer.engine import Replayer
from replayer.messages import trip_id
from replayer.source import ReplaySource
from replayer.state import load_state


def wall():
    return datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc)


class FakeOut:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.sent: list[tuple[str, str, dict]] = []
        self.fail_on = set(fail_on or ())
        self.pending_n = 0
        self._offset = 0

    def send(self, topic, key, value, on_delivery=None):
        if len(self.sent) in self.fail_on:
            self.fail_on.discard(len(self.sent))
            raise BufferError("Local: Queue full")
        self.sent.append((topic, key, value))
        if on_delivery:
            self._offset += 1
            on_delivery(0, self._offset)

    def poll(self, timeout=0.0):
        pass

    def pending(self):
        return self.pending_n

    def flush(self, timeout=30.0):
        return 0

    def on(self, topic):
        return [v for t, _, v in self.sent if t == topic]


def make(tmp_path, parquet, mono, **env):
    base = {"REPLAY_START_TS": "2025-10-07T07:00:00", "REPLAY_STATE_PATH": str(tmp_path / "state.json"),
            "REPLAY_PARQUET_PATH": str(parquet), "REPLAY_TRACE_SAMPLE_RATE": "0"}
    out = FakeOut()
    rep = Replayer(Config.from_env(base | env), ReplaySource(parquet), out, monotonic=mono, wall=wall)
    return rep, out


def ids(out):
    return [t["trip_id"] for t in out.on("trips.raw")]


def test_only_due_rows_are_produced(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    assert rep.step() == 1  # sim 07:00:00 → only the 07:00:00 trip
    mono.t = 1.0            # sim 07:01:00 → 07:00:30 and 07:01:00 are due
    assert rep.step() == 2
    assert ids(out) == [trip_id(0, i) for i in range(3)]
    for t in out.on("trips.raw"):
        assert_valid("trip", t)


def test_loops_with_offset_after_end_of_file(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="100000")
    mono.t = 1.0
    assert rep.step(max_rows=100) == 20
    assert rep.state.loop == 1 and rep.state.row_idx == 0
    for _ in range(8):  # small wall steps: a jump > 10 s would be treated as a freeze (see test_clock)
        mono.t += 5.0
        rep.step(max_rows=100)
    loop1 = out.on("trips.raw")[20]
    assert loop1["trip_id"] == "r1-000000000" and loop1["pickup_ts"] == "2025-11-11T07:00:00"


def test_backfill_stops_at_until_and_live_resumes_there(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="max", REPLAY_UNTIL_TS="2025-10-07T07:04:00")
    rep.run(threading.Event())
    assert rep.finished and ids(out) == [trip_id(0, i) for i in range(9)]  # 07:00:00 … 07:04:00
    saved = load_state(tmp_path / "state.json")
    assert saved.row_idx == 9 and saved.sim_ts == datetime(2025, 10, 7, 7, 4)
    clocks = out.on("sim.clock")
    assert clocks and clocks[-1]["mode"] == "backfill"
    assert_valid("sim_clock", clocks[-1])

    live, out2 = make(tmp_path, replay_parquet, mono)
    assert live.state.row_idx == 9 and live.clock.now() == datetime(2025, 10, 7, 7, 4)
    assert live.step() == 0
    mono.t += 0.5  # +30 sim seconds
    assert live.step() == 1 and ids(out2) == [trip_id(0, 9)]


def test_traced_trips_emit_ingested_checkpoint(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_TRACE_SAMPLE_RATE="1")
    rep.step()
    (t1, k1, trip), (t2, k2, cp) = out.sent
    assert (t1, t2) == ("trips.raw", "trace.checkpoints") and k1 == trip["trip_id"] and k2 == trip["trace_id"]
    assert_valid("trace_checkpoint", cp)
    assert cp["trip_id"] == trip["trip_id"] and cp["detail"]["offset"] == 1


def test_pause_and_resume(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    rep.step()
    rep.pause()
    mono.t = 10.0
    assert rep.step() == 0 and rep.status()["paused"] is True
    rep.resume()
    mono.t = 11.0  # only 1 wall second since resume → sim 07:01:00
    rep.step()
    assert ids(out) == [trip_id(0, i) for i in range(3)]


def test_speed_cannot_change_in_backfill(tmp_path, replay_parquet, mono):
    rep, _ = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="max", REPLAY_UNTIL_TS="2025-10-07T07:04:00")
    with pytest.raises(ValueError):
        rep.set_speed(120)


def test_backpressure_keeps_the_row(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    out.fail_on = {1}
    mono.t = 1.0
    assert rep.step() == 1 and rep.paused
    assert rep.step() == 0
    out.pending_n = 0
    rep.relieve_backpressure()
    assert not rep.paused
    mono.t = 2.0  # clock resumed at t=1 (sim 07:01) → now 07:02
    rep.step()
    assert ids(out) == [trip_id(0, i) for i in range(5)]


def test_housekeeping_heartbeat_state_and_clock(tmp_path, replay_parquet, mono):
    heartbeat = tmp_path / "healthy"
    rep, out = make(tmp_path, replay_parquet, mono)
    rep._heartbeat = heartbeat
    rep.housekeeping(force=True)
    (clock,) = out.on("sim.clock")
    assert_valid("sim_clock", clock)
    assert [k for t, k, _ in out.sent] == ["clock"] and heartbeat.exists()
    assert json.loads((tmp_path / "state.json").read_text())["row_idx"] == 0


def test_fresh_start_seeks_to_start_ts(tmp_path, replay_parquet, mono):
    rep, _ = make(tmp_path, replay_parquet, mono, REPLAY_START_TS="2025-10-07T07:05:00")
    assert rep.state.row_idx == 10
