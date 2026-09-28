"""Build messages that follow contracts/events/{trip,sim_clock,trace_checkpoint}.schema.json."""

from __future__ import annotations

import hashlib
import math
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

TRACE_NAMESPACE = uuid.UUID("6f1c2d3e-4b5a-4c6d-8e7f-9a0b1c2d3e4f")


def format_sim(dt: datetime, *, millis: bool = False) -> str:
    return dt.replace(tzinfo=None).isoformat(timespec="milliseconds" if millis else "seconds")


def format_wall(dt: datetime) -> str:
    utc = dt.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def trip_id(loop: int, row_idx: int) -> str:
    return f"r{loop}-{row_idx:09d}"


def sampled_trace_id(tid: str, rate: float) -> str | None:
    """Deterministic: the same trip_id always gets the same decision and the same trace_id."""
    bucket = int(hashlib.sha1(tid.encode()).hexdigest()[:8], 16) % 10_000
    return str(uuid.uuid5(TRACE_NAMESPACE, tid)) if bucket < round(rate * 10_000) else None


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return round(float(v), 2)


def _int(v: Any) -> int | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return int(v)


def build_trip(row: Mapping[str, Any], *, loop: int, offset_days: int, trace_sample_rate: float,
               produced_at: datetime) -> dict[str, Any]:
    shift = timedelta(days=loop * offset_days)
    tid = trip_id(loop, int(row["row_idx"]))
    pickup: datetime = row["pickup_ts"] + shift
    dropoff = row["dropoff_ts"] + shift if row.get("dropoff_ts") is not None else pickup
    flag = row.get("store_and_fwd_flag")
    payment = _int(row.get("payment_type"))
    return {
        "trip_id": tid,
        "source": "replay",
        "trace_id": sampled_trace_id(tid, trace_sample_rate),
        "loop": loop,
        "vendor_id": _int(row.get("vendor_id")),
        "pickup_ts": format_sim(pickup),
        "dropoff_ts": format_sim(dropoff),
        "passenger_count": _int(row.get("passenger_count")),
        "trip_distance": _num(row.get("trip_distance")) or 0.0,
        "ratecode_id": _int(row.get("ratecode_id")),
        "store_and_fwd_flag": flag if flag in ("Y", "N") else None,
        "pu_location_id": _int(row.get("pu_location_id")) or 0,
        "do_location_id": _int(row.get("do_location_id")) or 0,
        "payment_type": 5 if payment is None else payment,
        "fare_amount": _num(row.get("fare_amount")) or 0.0,
        "extra": _num(row.get("extra")),
        "mta_tax": _num(row.get("mta_tax")),
        "tip_amount": _num(row.get("tip_amount")) or 0.0,
        "tolls_amount": _num(row.get("tolls_amount")),
        "improvement_surcharge": _num(row.get("improvement_surcharge")),
        "congestion_surcharge": _num(row.get("congestion_surcharge")),
        "airport_fee": _num(row.get("airport_fee")),
        "cbd_congestion_fee": _num(row.get("cbd_congestion_fee")),
        "total_amount": _num(row.get("total_amount")) or 0.0,
        "produced_at": format_wall(produced_at),
    }


def build_clock(*, sim_ts: datetime, at: datetime, speed: float | None, loop: int, row_idx: int,
                paused: bool) -> dict[str, Any]:
    return {
        "sim_ts": format_sim(sim_ts, millis=True),
        "at": format_wall(at),
        "speed": "max" if speed is None else speed,
        "loop": loop,
        "row_idx": row_idx,
        "paused": paused,
        "mode": "backfill" if speed is None else "live",
    }


def build_ingested_checkpoint(trip: Mapping[str, Any], *, topic: str, partition: int, offset: int,
                              at: datetime) -> dict[str, Any]:
    return {
        "trace_id": trip["trace_id"],
        "trip_id": trip["trip_id"],
        "source": trip["source"],
        "stage": "ingested",
        "seq": 2,
        "service": "replayer",
        "at": format_wall(at),
        "detail": {"topic": topic, "partition": partition, "offset": offset},
    }
