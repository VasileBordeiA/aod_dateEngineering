import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare_data as prep


def tlc_table(pickups, *, with_cbd: bool = False) -> pa.Table:
    n = len(pickups)
    cols = {
        "VendorID": pa.array([2] * n, pa.int32()),
        "tpep_pickup_datetime": pa.array(pickups, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([p.replace(minute=59) for p in pickups], pa.timestamp("us")),
        "passenger_count": pa.array([1.0] * n),
        "trip_distance": pa.array([2.5] * n),
        "RatecodeID": pa.array([1.0] * n),
        "store_and_fwd_flag": pa.array(["N"] * n),
        "PULocationID": pa.array([161] * n, pa.int32()),
        "DOLocationID": pa.array([237] * n, pa.int32()),
        "payment_type": pa.array([1] * n, pa.int64()),
        "fare_amount": pa.array([15.0] * n),
        "extra": pa.array([0.0] * n),
        "mta_tax": pa.array([0.5] * n),
        "tip_amount": pa.array([3.0] * n),
        "tolls_amount": pa.array([0.0] * n),
        "improvement_surcharge": pa.array([1.0] * n),
        "total_amount": pa.array([22.0] * n),
        "congestion_surcharge": pa.array([2.5] * n),
        "Airport_fee": pa.array([1.75] * n),
    }
    if with_cbd:
        cols["cbd_congestion_fee"] = pa.array([0.75] * n)
    return pa.table(cols)


def test_month_bounds_handles_december():
    assert prep.month_bounds("2025-10") == ("2025-10-01", "2025-11-01")
    assert prep.month_bounds("2025-12") == ("2025-12-01", "2026-01-01")


def test_prepare_replay_filters_sorts_and_maps_columns(tmp_path):
    pickups = [
        datetime(2025, 10, 9, 8, 0), datetime(2009, 1, 1, 0, 0),  # out of month
        datetime(2025, 10, 1, 0, 5), datetime(2025, 11, 1, 0, 0),  # next month
        datetime(2025, 10, 31, 23, 59),
    ]
    raw = tmp_path / "raw.parquet"
    pq.write_table(tlc_table(pickups), raw)
    out = tmp_path / "replay" / "trips.parquet"

    assert prep.prepare_replay(raw, out, "2025-10") == 3

    t = pq.read_table(out).to_pylist()
    assert [r["row_idx"] for r in t] == [0, 1, 2]
    assert [r["pickup_ts"] for r in t] == [datetime(2025, 10, 1, 0, 5), datetime(2025, 10, 9, 8, 0),
                                          datetime(2025, 10, 31, 23, 59)]
    assert t[0]["airport_fee"] == 1.75 and t[0]["cbd_congestion_fee"] is None
    assert t[0]["passenger_count"] == 1 and isinstance(t[0]["passenger_count"], int)
    assert pq.read_schema(out).field("pickup_ts").type == pa.timestamp("us")


def test_missing_required_column_raises():
    with pytest.raises(ValueError, match="pickup_ts"):
        prep.select_list(["VendorID", "PULocationID"])


def test_count_reasons_uses_processor_rules():
    good = {"pickup_ts": datetime(2025, 10, 7, 8, 0), "dropoff_ts": datetime(2025, 10, 7, 8, 10),
            "trip_distance": 1.0, "pu_location_id": 1, "do_location_id": 2, "payment_type": 1,
            "fare_amount": 10.0, "tip_amount": 1.0, "total_amount": 12.0, "passenger_count": 1}
    refund = dict(good, fare_amount=-10.0, total_amount=-12.0)
    zero = dict(good, trip_distance=0.0, pickup_ts=datetime(2025, 10, 8, 9, 0), dropoff_ts=datetime(2025, 10, 8, 9, 5))
    total, per_day = prep.count_reasons([good, refund, zero])
    assert total["rows"] == 3 and total["rejected_rows"] == 2
    assert total["FARE_NON_POSITIVE"] == 1 and total["TOTAL_NEGATIVE"] == 1 and total["DISTANCE_NON_POSITIVE"] == 1
    assert per_day["2025-10-07"]["rows"] == 2 and per_day["2025-10-08"]["DISTANCE_NON_POSITIVE"] == 1


def test_zones_frames_dissolves_duplicates_and_keeps_unmatched_ids():
    shapes = gpd.GeoDataFrame(
        {"LocationID": [1, 1, 2]},
        geometry=[box(1_000_000, 200_000, 1_001_000, 201_000), box(1_001_000, 200_000, 1_002_000, 201_000),
                  box(1_010_000, 210_000, 1_011_000, 211_000)],
        crs=2263,
    )
    lookup = pd.DataFrame({"LocationID": [1, 2, 264], "Borough": ["EWR", "Queens", "Unknown"],
                           "Zone": ["Newark Airport", "Jamaica Bay", "NV"], "service_zone": ["EWR", "Boro Zone", None]})
    zones, geo = prep.zones_frames(lookup, shapes)
    assert list(zones["location_id"]) == [1, 2, 264]
    assert zones.loc[zones.location_id == 1, "lat"].notna().all()
    assert zones.loc[zones.location_id == 264, "lat"].isna().all()
    assert zones.loc[zones.location_id == 264, "service_zone"].item() == "Unknown"
    assert len(geo) == 2 and geo.crs.to_epsg() == 4326
    assert set(geo.columns) >= {"location_id", "zone", "borough", "geometry"}
    lat = zones.loc[zones.location_id == 1, "lat"].item()
    assert 40.0 < lat < 41.5  # a NY State Plane box lands near NYC
