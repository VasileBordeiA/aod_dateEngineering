import json

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
