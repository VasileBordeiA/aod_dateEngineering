from contract_validator import assert_valid

from processor.checkpoints import enriched_and_aggregated, stored_checkpoint, validated_or_rejected
from processor.enrich import enrich
from processor.rules import annotate

TRACE = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
AT = "2026-10-16T10:31:07.120Z"


def test_validated(trip_example):
    cp = validated_or_rejected(annotate(dict(trip_example, trace_id=TRACE)), AT)
    assert_valid("trace_checkpoint", cp)
    assert (cp["stage"], cp["seq"], cp["service"], cp["detail"]) == ("validated", 3, "processor", {"rules_checked": 11})


def test_rejected(trip_example):
    cp = validated_or_rejected(annotate(dict(trip_example, trace_id=TRACE, tip_amount=999.0)), AT)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "rejected" and cp["seq"] == 3 and cp["detail"] == {"reasons": ["TIP_UNREALISTIC"]}


def test_enriched_and_aggregated(trip_example, zones):
    enriched = enrich(dict(trip_example, trace_id=TRACE), zones, AT)
    e, a = enriched_and_aggregated(enriched, AT)
    for cp in (e, a):
        assert_valid("trace_checkpoint", cp)
    assert (e["stage"], e["seq"], a["stage"], a["seq"]) == ("enriched", 4, "aggregated", 5)
    assert e["detail"]["pu_zone"] == "Midtown Center" and e["detail"]["tip_pct"] == 25.88
    assert a["detail"] == {"window_start": "2025-10-07T08:14:00", "borough": "Manhattan"}


def test_stored(trip_example, zones):
    cp = stored_checkpoint(enrich(dict(trip_example, trace_id=TRACE), zones, AT), "taxi.trips", 143, AT)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "stored" and cp["seq"] == 6 and cp["detail"] == {"table": "taxi.trips", "batch_rows": 143}
