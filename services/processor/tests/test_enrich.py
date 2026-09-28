from contract_validator import assert_valid

from processor.enrich import enrich

PROCESSED_AT = "2026-10-16T08:02:11.731Z"


def test_example_is_enriched_exactly(trip_example, enriched_example, zones):
    assert enrich(trip_example, zones, PROCESSED_AT) == enriched_example


def test_output_is_contract_valid_and_drops_internal_and_unknown_keys(trip_example, zones):
    out = enrich(dict(trip_example, _reasons=[], surprise="x"), zones, PROCESSED_AT)
    assert_valid("enriched_trip", out)
    assert "_reasons" not in out and "surprise" not in out


def test_cash_trip_has_no_tip_pct(trip_example, zones):
    out = enrich(dict(trip_example, payment_type=2, tip_amount=0.0), zones, PROCESSED_AT)
    assert out["is_card"] is False and out["tip_pct"] is None and out["payment_type_name"] == "Cash"


def test_unknown_zone_short_trip_and_airport(trip_example, zones):
    out = enrich(dict(trip_example, pu_location_id=264, do_location_id=132,
                      dropoff_ts="2025-10-07T08:14:40"), zones, PROCESSED_AT)
    assert (out["pu_zone"], out["pu_borough"], out["pu_service_zone"]) == ("Unknown", "Unknown", "Unknown")
    assert out["do_zone"] == "JFK Airport" and out["is_airport"] is True
    assert out["duration_min"] == 0.62 and out["avg_speed_mph"] is None
    assert_valid("enriched_trip", out)


def test_numbers_are_normalised(trip_example, zones):
    out = enrich(dict(trip_example, pu_location_id=161.0, fare_amount=17, produced_at="bad"), zones, PROCESSED_AT)
    assert out["pu_location_id"] == 161 and isinstance(out["pu_location_id"], int)
    assert out["fare_amount"] == 17.0 and isinstance(out["fare_amount"], float)
    assert out["produced_at"] == PROCESSED_AT
