from datetime import datetime

from replayer.state import ReplayState, load_state, save_state


def test_missing_state_is_none(tmp_path):
    assert load_state(tmp_path / "nope.json") is None


def test_roundtrip(tmp_path):
    path = tmp_path / "state" / "replayer.json"
    save_state(path, ReplayState(loop=2, row_idx=812344, sim_ts=datetime(2025, 10, 14, 7, 0, 0, 250_000)))
    assert load_state(path) == ReplayState(loop=2, row_idx=812344, sim_ts=datetime(2025, 10, 14, 7, 0, 0, 250_000))


def test_save_is_atomic(tmp_path):
    path = tmp_path / "replayer.json"
    save_state(path, ReplayState(0, 1, datetime(2025, 10, 7)))
    save_state(path, ReplayState(0, 2, datetime(2025, 10, 7)))
    assert [p.name for p in tmp_path.iterdir()] == ["replayer.json"]  # no temp files left behind
    assert load_state(path).row_idx == 2
