import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "contracts"))

START = datetime(2025, 10, 7, 7, 0, 0)


def make_replay_parquet(path: Path, n: int = 20, start: datetime = START, step_s: int = 30,
                        row_group_size: int = 7) -> Path:
    """A tiny prepared replay file (same columns as scripts/prepare_data.py output): one trip every step_s."""
    pickups = [start + timedelta(seconds=i * step_s) for i in range(n)]
    table = pa.table({
        "row_idx": pa.array(range(n), pa.int64()),
        "vendor_id": pa.array([2] * n, pa.int32()),
        "pickup_ts": pa.array(pickups, pa.timestamp("us")),
        "dropoff_ts": pa.array([p + timedelta(minutes=12) for p in pickups], pa.timestamp("us")),
        "passenger_count": pa.array([1] * n, pa.int32()),
        "trip_distance": pa.array([2.5] * n),
        "ratecode_id": pa.array([1] * n, pa.int32()),
        "store_and_fwd_flag": pa.array(["N"] * n),
        "pu_location_id": pa.array([161] * n, pa.int32()),
        "do_location_id": pa.array([237] * n, pa.int32()),
        "payment_type": pa.array([1] * n, pa.int32()),
        "fare_amount": pa.array([15.0] * n),
        "extra": pa.array([0.0] * n),
        "mta_tax": pa.array([0.5] * n),
        "tip_amount": pa.array([3.0] * n),
        "tolls_amount": pa.array([0.0] * n),
        "improvement_surcharge": pa.array([1.0] * n),
        "congestion_surcharge": pa.array([2.5] * n),
        "airport_fee": pa.array([None] * n, pa.float64()),
        "cbd_congestion_fee": pa.array([0.75] * n),
        "total_amount": pa.array([22.75] * n),
    })
    pq.write_table(table, path, row_group_size=row_group_size)
    return path


@pytest.fixture
def replay_parquet(tmp_path: Path) -> Path:
    return make_replay_parquet(tmp_path / "trips.parquet")


class FakeMono:
    """A controllable time.monotonic()."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def mono() -> FakeMono:
    return FakeMono()
