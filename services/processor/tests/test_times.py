from datetime import datetime, timezone

from processor.times import format_sim, format_wall, parse_sim, parse_wall, sim_epoch_ms, sim_from_epoch_ms


def test_parse_sim_variants():
    assert parse_sim("2025-10-07T08:14:03") == datetime(2025, 10, 7, 8, 14, 3)
    assert parse_sim("2025-10-07T08:14:03.250") == datetime(2025, 10, 7, 8, 14, 3, 250_000)
    assert parse_sim(datetime(2025, 10, 7, tzinfo=timezone.utc)) == datetime(2025, 10, 7)
    assert parse_sim("yesterday") is None and parse_sim(None) is None and parse_sim(12) is None


def test_format_sim():
    assert format_sim(datetime(2025, 10, 7, 8, 14, 3)) == "2025-10-07T08:14:03"
    assert format_sim(datetime(2025, 10, 7, 8, 14, 3, 250_000)) == "2025-10-07T08:14:03.250"


def test_wall_roundtrip():
    dt = datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=timezone.utc)
    assert format_wall(dt) == "2026-10-16T08:02:11.482Z"
    assert parse_wall("2026-10-16T08:02:11.482Z") == dt


def test_sim_epoch_is_naive_as_utc():
    ms = sim_epoch_ms(datetime(2025, 10, 7, 8, 14))
    assert ms == 1759824840000
    assert sim_from_epoch_ms(ms) == datetime(2025, 10, 7, 8, 14)
