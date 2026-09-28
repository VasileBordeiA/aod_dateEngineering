from datetime import datetime

import pytest

from replayer.clock import ManualClock, SimClock

T0 = datetime(2025, 10, 7, 7, 0, 0)


def test_advances_at_speed(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 1.0
    assert c.now() == datetime(2025, 10, 7, 7, 1, 0)


def test_pause_freezes_and_resume_does_not_jump(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 10.0
    c.pause()
    mono.t = 100.0
    assert c.paused and c.now() == datetime(2025, 10, 7, 7, 10)
    c.resume()
    mono.t = 101.0
    assert c.now() == datetime(2025, 10, 7, 7, 11)


def test_speed_change_does_not_jump(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 5.0
    c.set_speed(120)
    assert c.now() == datetime(2025, 10, 7, 7, 5)
    mono.t = 6.0
    assert c.now() == datetime(2025, 10, 7, 7, 7) and c.speed == 120


def test_wall_gap_is_not_replayed(mono):
    c = SimClock(T0, 60, mono, max_gap_s=10)
    mono.t = 1.0
    assert c.tick() == datetime(2025, 10, 7, 7, 1)
    mono.t = 61.0  # process frozen for 60 s
    assert c.tick() == datetime(2025, 10, 7, 7, 1)
    mono.t = 62.0
    assert c.tick() == datetime(2025, 10, 7, 7, 2)


@pytest.mark.parametrize("speed", [0, -1])
def test_invalid_speed(mono, speed):
    with pytest.raises(ValueError):
        SimClock(T0, speed, mono)
    with pytest.raises(ValueError):
        SimClock(T0, 60, mono).set_speed(speed)


def test_manual_clock_only_moves_forward():
    c = ManualClock(T0)
    c.advance_to(datetime(2025, 10, 7, 8))
    c.advance_to(datetime(2025, 10, 7, 7, 30))
    assert c.now() == c.tick() == datetime(2025, 10, 7, 8) and c.paused is False


def test_jump_to_moves_forward_only_and_keeps_running(mono):
    c = SimClock(T0, 60, mono)
    c.jump_to(datetime(2025, 11, 11, 7, 0))
    assert c.now() == datetime(2025, 11, 11, 7, 0)
    mono.t = 1.0
    assert c.now() == datetime(2025, 11, 11, 7, 1)
    c.jump_to(datetime(2025, 10, 1))  # never backwards
    assert c.now() == datetime(2025, 11, 11, 7, 1)
