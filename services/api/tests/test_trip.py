from datetime import datetime

from contract_validator import assert_valid

from rides.estimate import Estimate, Zone, amounts
from rides.trip import build_trip

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
PU = Zone(230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985)
DO = Zone(132, "JFK Airport", "Queens", "Airports", 40.641, -73.779)


def trip(duration: float = 16.5) -> dict:
    est = Estimate(3.1, duration, 19.5, "x")
    return build_trip(trace_id=TID, pickup_ts=datetime(2025, 10, 9, 17, 42, 0), est=est,
                      amt=amounts(est, PU, DO, "card", 5.0), pu_location_id=230, do_location_id=132,
                      passenger_count=2, produced_at="2026-10-16T10:31:07.041Z")


def test_trip_follows_the_contract():
    t = trip()
    assert_valid("trip", t)
    assert t["trip_id"] == f"s-{TID}" and t["source"] == "student" and t["trace_id"] == TID and t["loop"] == 0
    assert t["pickup_ts"] == "2025-10-09T17:42:00" and t["passenger_count"] == 2 and t["ratecode_id"] == 1
    assert t["payment_type"] == 1 and t["tip_amount"] == 5.0 and t["total_amount"] == 29.25


def test_dropoff_after_pickup():
    assert trip(16.5)["dropoff_ts"] == "2025-10-09T17:58:30"
    assert trip(1.0)["dropoff_ts"] == "2025-10-09T17:43:00"
