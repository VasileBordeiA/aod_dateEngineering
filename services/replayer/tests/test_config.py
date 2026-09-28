from datetime import datetime
from pathlib import Path

import pytest

from replayer.config import Config


def test_defaults():
    c = Config.from_env({})
    assert c.speed == 60.0 and c.mode == "live"
    assert c.parquet_path == Path("/data/replay/trips_2025-10.parquet")
    assert c.start_ts == datetime(2025, 10, 7, 7, 0) and c.until_ts is None
    assert (c.loop_offset_days, c.trace_sample_rate, c.control_port) == (35, 0.001, 8001)
    assert c.state_path == Path("/data/state/replayer.json") and c.brokers == "redpanda:9092"


def test_backfill_mode_requires_until():
    with pytest.raises(ValueError, match="REPLAY_UNTIL_TS"):
        Config.from_env({"REPLAY_SPEED": "max"})
    c = Config.from_env({"REPLAY_SPEED": "MAX", "REPLAY_UNTIL_TS": "2025-10-14T07:00:00"})
    assert c.speed is None and c.mode == "backfill" and c.until_ts == datetime(2025, 10, 14, 7)


@pytest.mark.parametrize("bad", ["0", "-5", "fast"])
def test_invalid_speed(bad):
    with pytest.raises(ValueError):
        Config.from_env({"REPLAY_SPEED": bad})


def test_parquet_path_follows_month_and_data_dir():
    c = Config.from_env({"TLC_MONTH": "2026-06", "DATA_DIR": "/x"})
    assert c.parquet_path == Path("/x/replay/trips_2026-06.parquet")
    assert Config.from_env({"REPLAY_PARQUET_PATH": "/y/t.parquet"}).parquet_path == Path("/y/t.parquet")
