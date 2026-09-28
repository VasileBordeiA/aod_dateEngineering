import json

import pytest
from contract_validator import assert_valid

from processor.rules import REASON_CODES, annotate, check_trip, is_traced, to_rejected

BASE = {"trip_id": "r0-000000001", "source": "replay", "trace_id": None, "loop": 0,
        "pickup_ts": "2025-10-07T08:14:03", "dropoff_ts": "2025-10-07T08:29:41", "passenger_count": 1,
        "trip_distance": 2.87, "pu_location_id": 161, "do_location_id": 237, "payment_type": 1,
        "fare_amount": 17.0, "tip_amount": 4.4, "total_amount": 26.15}
AT = "2026-10-16T08:03:01.200Z"

CASES = [  # (code, makes it fail, boundary that still passes)
    ("FARE_NON_POSITIVE", {"fare_amount": 0.0}, {"fare_amount": 0.01}),
    ("TOTAL_NEGATIVE", {"total_amount": -0.01}, {"total_amount": 0.0}),
    ("DISTANCE_NON_POSITIVE", {"trip_distance": 0.0}, {"trip_distance": 0.01}),
    ("DISTANCE_TOO_LONG", {"trip_distance": 200.01}, {"trip_distance": 200.0}),
    ("DROPOFF_NOT_AFTER_PICKUP", {"dropoff_ts": "2025-10-07T08:14:03"}, {"dropoff_ts": "2025-10-07T08:14:04"}),
    ("DURATION_TOO_LONG", {"dropoff_ts": "2025-10-07T14:14:04"}, {"dropoff_ts": "2025-10-07T14:14:03"}),
    ("LOCATION_INVALID", {"pu_location_id": 266}, {"pu_location_id": 265}),
    ("LOCATION_INVALID", {"do_location_id": 0}, {"do_location_id": 1}),
    ("PASSENGER_COUNT_INVALID", {"passenger_count": 10}, {"passenger_count": 0}),
    ("TIP_NEGATIVE", {"tip_amount": -1.0}, {"tip_amount": 0.0}),
    ("TIP_UNREALISTIC", {"tip_amount": 100.01}, {"tip_amount": 100.0}),
    ("TIP_UNREALISTIC", {"fare_amount": 50.0, "tip_amount": 150.01}, {"fare_amount": 50.0, "tip_amount": 150.0}),
]


def test_base_trip_is_valid():
    assert check_trip(BASE) == []
    assert check_trip(dict(BASE, passenger_count=None)) == []


@pytest.mark.parametrize("code,bad,ok", CASES)
def test_each_rule(code, bad, ok):
    assert code in check_trip(dict(BASE, **bad))
    assert code not in check_trip(dict(BASE, **ok))


def test_all_failing_rules_are_reported():
    assert check_trip(dict(BASE, fare_amount=-12.5, total_amount=-16.0)) == ["FARE_NON_POSITIVE", "TOTAL_NEGATIVE"]


@pytest.mark.parametrize("bad", [
    {"pickup_ts": None}, {"pickup_ts": "noon"}, {"fare_amount": "17"}, {"pu_location_id": True},
    {"source": "taxi"}, {"trip_id": 5}, {"payment_type": 1.5}, {"total_amount": float("nan")},
])
def test_schema_invalid(bad):
    assert check_trip(dict(BASE, **bad)) == ["SCHEMA_INVALID"]


def test_reason_codes_match_contract():
    import contract_validator as cv
    assert list(REASON_CODES) == cv.schema("rejected_trip")["$defs"]["reason_code"]["enum"]


def test_annotate_valid_json():
    v = annotate(json.dumps(BASE).encode())
    assert v["_reasons"] == [] and v["trip_id"] == BASE["trip_id"]


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b"\xff\xfe", "{broken"])
def test_annotate_garbage(raw):
    v = annotate(raw)
    assert v["_reasons"] == ["SCHEMA_INVALID"] and isinstance(v["_raw"], str)


def test_to_rejected_keeps_original_payload():
    r = to_rejected(annotate(json.dumps(dict(BASE, fare_amount=-12.5)).encode()), AT)
    assert_valid("rejected_trip", r)
    assert r["trip_id"] == "r0-000000001" and r["reasons"] == ["FARE_NON_POSITIVE"]
    assert json.loads(r["payload"])["fare_amount"] == -12.5 and "_reasons" not in r["payload"]


def test_to_rejected_garbage_gets_a_stable_id():
    r1, r2 = (to_rejected(annotate(b"not json"), AT) for _ in range(2))
    assert_valid("rejected_trip", r1)
    assert r1["trip_id"] == r2["trip_id"] and r1["trip_id"].startswith("invalid-")
    assert r1["payload"] == "not json" and r1["pickup_ts"] is None and r1["trace_id"] is None


def test_is_traced():
    assert is_traced(dict(BASE, trace_id="3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"))
    assert not is_traced(BASE) and not is_traced(dict(BASE, trace_id="nope"))
    assert not is_traced({"_reasons": ["SCHEMA_INVALID"], "_raw": "x"})
