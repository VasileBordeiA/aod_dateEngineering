import json
from datetime import UTC, datetime

from contract_validator import assert_valid

from api.simclock import SimClockFollower

MSG = {"sim_ts": "2025-10-07T08:00:00", "at": "2026-09-28T12:00:00.000Z", "speed": 60, "loop": 0,
       "row_idx": 1234, "paused": False, "mode": "live"}


def test_keeps_the_latest_valid_message():
    f = SimClockFollower("unused:9092")
    assert f.latest is None
    f.handle(json.dumps(MSG).encode())
    assert f.latest == MSG
    assert_valid("sim_clock", f.latest)


def test_ignores_garbage():
    f = SimClockFollower("unused:9092")
    f.handle(json.dumps(MSG).encode())
    f.handle(b"not json")
    f.handle(b"[1, 2]")
    assert f.latest == MSG


def test_fresh_sim_ts_only_while_heartbeats_are_recent():
    now = datetime(2026, 9, 28, 12, 0, 5, tzinfo=UTC)
    f = SimClockFollower("unused:9092", now=lambda: now)
    assert f.fresh_sim_ts() is None
    f.handle(json.dumps(MSG).encode())  # at = 12:00:00.000Z, 5 s old
    assert f.fresh_sim_ts() == "2025-10-07T08:00:00"
    now = datetime(2026, 9, 28, 12, 0, 11, tzinfo=UTC)  # 11 s old
    assert f.fresh_sim_ts() is None


def test_fresh_sim_ts_ignores_a_heartbeat_without_a_valid_at():
    f = SimClockFollower("unused:9092", now=lambda: datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC))
    f.handle(json.dumps({**MSG, "at": "yesterday"}).encode())
    assert f.fresh_sim_ts() is None
