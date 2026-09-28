from datetime import datetime

import pytest

from replayer.source import ReplaySource


def test_num_rows_and_seek(replay_parquet):
    src = ReplaySource(replay_parquet)
    assert src.num_rows == 20
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 6, 0)) == 0
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 7, 2, 0)) == 4
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 7, 2, 1)) == 5
    assert src.first_row_at_or_after(datetime(2025, 10, 8)) == 20


def test_rows_from_crosses_row_groups(replay_parquet):
    rows = list(ReplaySource(replay_parquet).rows_from(4))
    assert [r["row_idx"] for r in rows] == list(range(4, 20))
    assert rows[0]["pickup_ts"] == datetime(2025, 10, 7, 7, 2, 0)


def test_rows_from_end_is_empty(replay_parquet):
    assert list(ReplaySource(replay_parquet).rows_from(20)) == []


def test_missing_file_explains_what_to_do(tmp_path):
    with pytest.raises(FileNotFoundError, match="make data"):
        ReplaySource(tmp_path / "missing.parquet")
