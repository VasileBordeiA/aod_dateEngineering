from datetime import datetime

from contract_validator import assert_valid

from processor.aggregate import add_trip, init_bucket, to_agg_record
from processor.times import sim_epoch_ms

T1 = {"is_card": True, "tip_pct": 20.0, "fare_amount": 10.0, "total_amount": 15.0, "trip_distance": 1.0, "source": "replay"}
T2 = {"is_card": True, "tip_pct": 30.0, "fare_amount": 20.0, "total_amount": 30.0, "trip_distance": 3.0, "source": "student"}
T3 = {"is_card": False, "tip_pct": None, "fare_amount": 30.0, "total_amount": 35.0, "trip_distance": 5.0, "source": "replay"}
START = sim_epoch_ms(datetime(2025, 10, 7, 8, 14))
AT = "2026-10-16T08:02:15.004Z"


def test_reduce_and_record():
    bucket = add_trip(add_trip(init_bucket(T1), T2), T3)
    record = to_agg_record({"start": START, "end": START + 60_000, "value": bucket}, "Manhattan", AT)
    assert record == {
        "window_start": "2025-10-07T08:14:00", "window_end": "2025-10-07T08:15:00", "borough": "Manhattan",
        "trips": 3, "revenue": 80.0, "avg_fare": 20.0, "avg_distance": 3.0, "card_trips": 2,
        "avg_card_tip_pct": 25.0, "student_trips": 1, "emitted_at": AT,
    }
    assert_valid("borough_minute_agg", record)


def test_no_card_trips_means_null_tip():
    record = to_agg_record({"start": START, "end": START + 60_000, "value": init_bucket(T3)}, "Bronx", AT)
    assert record["avg_card_tip_pct"] is None and record["card_trips"] == 0
    assert_valid("borough_minute_agg", record)


def test_add_trip_does_not_mutate():
    bucket = init_bucket(T1)
    add_trip(bucket, T2)
    assert bucket["trips"] == 1
