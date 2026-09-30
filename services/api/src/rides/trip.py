"""A student ride as a trips.raw message (contracts/events/trip.schema.json, source=student)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from rides.estimate import Amounts, Estimate


def _sim(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat(timespec="seconds")


def build_trip(*, trace_id: str, pickup_ts: datetime, est: Estimate, amt: Amounts, pu_location_id: int,
               do_location_id: int, passenger_count: int, produced_at: str) -> dict[str, Any]:
    dropoff = pickup_ts + timedelta(seconds=int(est.duration_min * 60))
    return {
        "trip_id": f"s-{trace_id}", "source": "student", "trace_id": trace_id, "loop": 0, "vendor_id": None,
        "pickup_ts": _sim(pickup_ts), "dropoff_ts": _sim(dropoff), "passenger_count": passenger_count,
        "trip_distance": est.trip_distance, "ratecode_id": 1, "store_and_fwd_flag": None,
        "pu_location_id": pu_location_id, "do_location_id": do_location_id, "payment_type": amt.payment_type,
        "fare_amount": amt.fare_amount, "extra": amt.extra, "mta_tax": amt.mta_tax, "tip_amount": amt.tip_amount,
        "tolls_amount": amt.tolls_amount, "improvement_surcharge": amt.improvement_surcharge,
        "congestion_surcharge": amt.congestion_surcharge, "airport_fee": amt.airport_fee,
        "cbd_congestion_fee": amt.cbd_congestion_fee, "total_amount": amt.total_amount, "produced_at": produced_at,
    }
