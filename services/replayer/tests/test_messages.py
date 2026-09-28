import math
from datetime import UTC, datetime

from contract_validator import assert_valid

from replayer.messages import build_clock, build_ingested_checkpoint, build_trip, format_wall, sampled_trace_id, trip_id

WALL = datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=UTC)
ROW = {
    "row_idx": 123456, "vendor_id": 2, "pickup_ts": datetime(2025, 10, 7, 8, 14, 3),
    "dropoff_ts": datetime(2025, 10, 7, 8, 29, 41), "passenger_count": 1, "trip_distance": 2.87,
    "ratecode_id": 1, "store_and_fwd_flag": "N", "pu_location_id": 161, "do_location_id": 237,
    "payment_type": 1, "fare_amount": 17.0, "extra": 0.0, "mta_tax": 0.5, "tip_amount": 4.4,
    "tolls_amount": 0.0, "improvement_surcharge": 1.0, "congestion_surcharge": 2.5, "airport_fee": None,
    "cbd_congestion_fee": 0.75, "total_amount": 26.15,
}


def build(row=ROW, loop=0, rate=0.0):
    return build_trip(row, loop=loop, offset_days=35, trace_sample_rate=rate, produced_at=WALL)


def test_trip_id_and_wall_format():
    assert trip_id(3, 42) == "r3-000000042"
    assert format_wall(WALL) == "2026-10-16T08:02:11.482Z"


def test_trip_matches_contract():
    msg = build()
    assert_valid("trip", msg)
    assert msg["trip_id"] == "r0-000123456" and msg["pickup_ts"] == "2025-10-07T08:14:03"
    assert msg["produced_at"] == "2026-10-16T08:02:11.482Z" and msg["airport_fee"] is None


def test_loop_offset_shifts_timestamps():
    msg = build(loop=1)
    assert msg["trip_id"] == "r1-000123456" and msg["loop"] == 1
    assert msg["pickup_ts"] == "2025-11-11T08:14:03" and msg["dropoff_ts"] == "2025-11-11T08:29:41"


def test_nan_and_missing_values():
    row = dict(ROW, extra=math.nan, passenger_count=None, trip_distance=None, pu_location_id=None,
               payment_type=None, store_and_fwd_flag="?", dropoff_ts=None)
    msg = build(row)
    assert_valid("trip", msg)
    assert msg["extra"] is None and msg["passenger_count"] is None and msg["store_and_fwd_flag"] is None
    # required numbers fall back to values the processor will reject with a clear reason
    assert msg["trip_distance"] == 0.0 and msg["pu_location_id"] == 0 and msg["payment_type"] == 5
    assert msg["dropoff_ts"] == msg["pickup_ts"]


def test_sampling_is_deterministic_and_close_to_rate():
    assert sampled_trace_id("r0-000000001", 0.0) is None
    assert sampled_trace_id("r0-000000001", 1.0) == sampled_trace_id("r0-000000001", 1.0) is not None
    hits = sum(sampled_trace_id(trip_id(0, i), 0.001) is not None for i in range(100_000))
    assert 50 <= hits <= 150


def test_clock_messages_match_contract():
    live = build_clock(sim_ts=datetime(2025, 10, 9, 17, 42, 0, 250_000), at=WALL, speed=60.0, loop=0,
                       row_idx=812344, paused=False)
    assert_valid("sim_clock", live)
    assert live["sim_ts"] == "2025-10-09T17:42:00.250" and live["mode"] == "live"
    backfill = build_clock(sim_ts=datetime(2025, 10, 9), at=WALL, speed=None, loop=0, row_idx=1, paused=False)
    assert_valid("sim_clock", backfill)
    assert backfill["speed"] == "max" and backfill["mode"] == "backfill"


def test_ingested_checkpoint_matches_contract():
    trip = build(rate=1.0)
    cp = build_ingested_checkpoint(trip, topic="trips.raw", partition=1, offset=482113, at=WALL)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "ingested" and cp["seq"] == 2 and cp["service"] == "replayer"
    assert cp["detail"] == {"topic": "trips.raw", "partition": 1, "offset": 482113}
