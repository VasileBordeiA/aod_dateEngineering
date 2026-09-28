"""Enrichment: zone names and derived fields (spec 03 §Enrichment)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from processor.times import format_sim, parse_sim
from processor.zones import UNKNOWN, Zone, zone_for

TRIP_FIELDS = (
    "trip_id", "source", "trace_id", "loop", "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count",
    "trip_distance", "ratecode_id", "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount", "produced_at",
)
INT_FIELDS = ("loop", "vendor_id", "passenger_count", "ratecode_id", "pu_location_id", "do_location_id", "payment_type")
FLOAT_FIELDS = ("trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount")
PAYMENT_NAMES = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown",
                 6: "Voided trip"}
AIRPORT_IDS = frozenset({1, 132, 138})
_WALL = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def enrich(trip: Mapping[str, Any], zones: Mapping[int, Zone], processed_at: str) -> dict[str, Any]:
    """A validated trip → enriched_trip contract. Unknown and internal (`_`) keys are dropped."""
    out: dict[str, Any] = {f: trip.get(f) for f in TRIP_FIELDS}
    for f in INT_FIELDS:
        if out[f] is not None:
            out[f] = int(out[f])
    for f in FLOAT_FIELDS:
        if out[f] is not None:
            out[f] = float(out[f])
    out["loop"] = out["loop"] or 0
    if not (isinstance(out["produced_at"], str) and _WALL.fullmatch(out["produced_at"])):
        out["produced_at"] = processed_at
    pickup, dropoff = parse_sim(trip["pickup_ts"]), parse_sim(trip["dropoff_ts"])
    assert pickup is not None and dropoff is not None  # guaranteed by check_trip
    out["pickup_ts"], out["dropoff_ts"] = format_sim(pickup), format_sim(dropoff)

    pu = zone_for(zones, out["pu_location_id"]) if out["pu_location_id"] < 264 else UNKNOWN
    do = zone_for(zones, out["do_location_id"]) if out["do_location_id"] < 264 else UNKNOWN
    seconds = (dropoff - pickup).total_seconds()
    is_card = out["payment_type"] == 1
    fare = out["fare_amount"]
    out.update(
        pu_zone=pu.zone, pu_borough=pu.borough, pu_service_zone=pu.service_zone,
        do_zone=do.zone, do_borough=do.borough, do_service_zone=do.service_zone,
        payment_type_name=PAYMENT_NAMES.get(out["payment_type"], "Unknown"),
        is_card=is_card,
        is_airport=out["pu_location_id"] in AIRPORT_IDS or out["do_location_id"] in AIRPORT_IDS,
        duration_min=round(seconds / 60, 2),
        avg_speed_mph=round(out["trip_distance"] / (seconds / 3600), 2) if seconds >= 60 else None,
        tip_pct=round(100 * out["tip_amount"] / fare, 2) if is_card and fare > 0 else None,
        pickup_hour=pickup.hour,
        pickup_dow=pickup.isoweekday(),
        processed_at=processed_at,
    )
    return out
