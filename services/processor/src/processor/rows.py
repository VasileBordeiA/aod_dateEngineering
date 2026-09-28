"""Message → ClickHouse row mapping. Column order equals infra/clickhouse/init DDL (tested)."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from processor.times import parse_sim, parse_wall

TRIPS_COLUMNS = (
    "trip_id", "source", "trace_id", "loop", "vendor_id", "pickup_ts", "dropoff_ts", "pickup_hour", "pickup_dow",
    "passenger_count", "trip_distance", "duration_min", "avg_speed_mph", "ratecode_id", "store_and_fwd_flag",
    "pu_location_id", "pu_zone", "pu_borough", "pu_service_zone", "do_location_id", "do_zone", "do_borough",
    "do_service_zone", "payment_type", "payment_type_name", "is_card", "is_airport", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tip_pct", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee", "total_amount", "produced_at", "processed_at",
)  # stored_at is filled by ClickHouse (DEFAULT now64(3))
REJECTED_COLUMNS = ("trip_id", "source", "trace_id", "pickup_ts", "reasons", "payload", "rejected_at")
AGG_COLUMNS = ("window_start", "window_end", "borough", "trips", "revenue", "avg_fare", "avg_distance",
               "card_trips", "avg_card_tip_pct", "student_trips", "emitted_at")

ZERO_IF_NULL = {"extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
                "airport_fee", "cbd_congestion_fee"}
SIM_COLUMNS = {"pickup_ts", "dropoff_ts", "window_start", "window_end"}
WALL_COLUMNS = {"produced_at", "processed_at", "rejected_at", "emitted_at"}


def _sim(v: Any) -> datetime | None:
    """Naive sim time → aware UTC with the same wall values, so the host timezone can never shift it."""
    dt = parse_sim(v)
    return dt.replace(tzinfo=UTC) if dt is not None else None


def _row(v: Mapping[str, Any], columns: tuple[str, ...]) -> list[Any]:
    row: list[Any] = []
    for c in columns:
        x = v.get(c)
        if c in SIM_COLUMNS:
            x = _sim(x)
        elif c in WALL_COLUMNS and isinstance(x, str):
            x = parse_wall(x)
        elif c == "trace_id":
            x = uuid.UUID(x) if x else None
        elif c in ZERO_IF_NULL and x is None:
            x = 0.0
        row.append(x)
    return row


def trip_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, TRIPS_COLUMNS)


def rejected_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, REJECTED_COLUMNS)


def agg_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, AGG_COLUMNS)
