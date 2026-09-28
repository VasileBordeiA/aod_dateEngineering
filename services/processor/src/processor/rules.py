"""Validation rules (spec 03 §Validation rules).

Stdlib-only on purpose: scripts/prepare_data.py imports this module to compute the data-quality report with
exactly the rules the stream processor applies.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from processor.times import format_sim, parse_sim

REASON_CODES = (
    "SCHEMA_INVALID", "FARE_NON_POSITIVE", "TOTAL_NEGATIVE", "DISTANCE_NON_POSITIVE", "DISTANCE_TOO_LONG",
    "DROPOFF_NOT_AFTER_PICKUP", "DURATION_TOO_LONG", "LOCATION_INVALID", "PASSENGER_COUNT_INVALID",
    "TIP_NEGATIVE", "TIP_UNREALISTIC",
)
RULES_CHECKED = len(REASON_CODES)
MAX_DISTANCE_MILES = 200.0
MAX_DURATION = timedelta(hours=6)
REQUIRED_INT = ("pu_location_id", "do_location_id", "payment_type")
REQUIRED_NUM = ("trip_distance", "fare_amount", "tip_amount", "total_amount")
OPTIONAL_NUM = ("extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
                "airport_fee", "cbd_congestion_fee")
# Integer fields and the ClickHouse column range they must fit (UInt8 / UInt16).
INT_RANGES = {"loop": (0, 65_535), "payment_type": (0, 255), "vendor_id": (0, 255), "ratecode_id": (0, 255)}
SOURCES = ("replay", "student")


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _is_int(v: Any) -> bool:
    return (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, float) and v.is_integer())


def is_uuid(v: Any) -> bool:
    if not isinstance(v, str):
        return False
    try:
        uuid.UUID(v)
    except ValueError:
        return False
    return True


def _schema_ok(t: Mapping[str, Any]) -> bool:
    """Every field downstream code or a ClickHouse column type relies on (trip contract)."""
    if not isinstance(t.get("trip_id"), str) or t.get("source") not in SOURCES:
        return False
    if parse_sim(t.get("pickup_ts")) is None or parse_sim(t.get("dropoff_ts")) is None:
        return False
    if not all(_is_int(t.get(k)) for k in REQUIRED_INT) or not all(_is_num(t.get(k)) for k in REQUIRED_NUM):
        return False
    if not all(t.get(k) is None or _is_num(t.get(k)) for k in OPTIONAL_NUM):
        return False
    for k, (lo, hi) in INT_RANGES.items():
        v = t.get(k)
        if v is not None and not (_is_int(v) and lo <= int(v) <= hi):
            return False
    if t.get("trace_id") is not None and not is_uuid(t.get("trace_id")):
        return False
    return t.get("store_and_fwd_flag") in ("Y", "N", None)


def check_trip(t: Mapping[str, Any], tip_max_abs: float = 100.0) -> list[str]:
    """Every reason code the trip fails, in REASON_CODES order. Empty list = valid."""
    if not _schema_ok(t):
        return ["SCHEMA_INVALID"]
    pickup, dropoff = parse_sim(t["pickup_ts"]), parse_sim(t["dropoff_ts"])
    assert pickup is not None and dropoff is not None
    fare, total = float(t["fare_amount"]), float(t["total_amount"])
    dist, tip = float(t["trip_distance"]), float(t["tip_amount"])
    reasons = []
    if fare <= 0:
        reasons.append("FARE_NON_POSITIVE")
    if total < 0:
        reasons.append("TOTAL_NEGATIVE")
    if dist <= 0:
        reasons.append("DISTANCE_NON_POSITIVE")
    if dist > MAX_DISTANCE_MILES:
        reasons.append("DISTANCE_TOO_LONG")
    if dropoff <= pickup:
        reasons.append("DROPOFF_NOT_AFTER_PICKUP")
    elif dropoff - pickup > MAX_DURATION:
        reasons.append("DURATION_TOO_LONG")
    if not all(1 <= int(t[k]) <= 265 for k in ("pu_location_id", "do_location_id")):
        reasons.append("LOCATION_INVALID")
    pc = t.get("passenger_count")
    if pc is not None and (not _is_int(pc) or not 0 <= int(pc) <= 9):
        reasons.append("PASSENGER_COUNT_INVALID")
    if tip < 0:
        reasons.append("TIP_NEGATIVE")
    elif tip > max(tip_max_abs, 3 * fare):
        reasons.append("TIP_UNREALISTIC")
    return reasons


def annotate(raw: bytes | str | Mapping[str, Any], tip_max_abs: float = 100.0) -> dict[str, Any]:
    """Parse a trips.raw message and attach `_reasons`. Never raises on bad input."""
    if isinstance(raw, Mapping):
        value: Any = dict(raw)
    else:
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        try:
            value = json.loads(text)
        except (ValueError, RecursionError):  # RecursionError: absurdly nested input
            return {"_reasons": ["SCHEMA_INVALID"], "_raw": text}
        if not isinstance(value, dict):
            return {"_reasons": ["SCHEMA_INVALID"], "_raw": text}
    value["_reasons"] = check_trip(value, tip_max_abs)
    return dict(value)


def is_traced(v: Mapping[str, Any]) -> bool:
    return is_uuid(v.get("trace_id")) and isinstance(v.get("trip_id"), str) and v.get("source") in SOURCES


def to_rejected(v: Mapping[str, Any], rejected_at: str) -> dict[str, Any]:
    """Dead-letter record (rejected_trip contract). The original message is kept verbatim in `payload`."""
    if "_raw" in v:
        payload = str(v["_raw"])
    else:
        payload = json.dumps({k: x for k, x in v.items() if not k.startswith("_")}, default=str,
                             separators=(",", ":"))
    tid = v.get("trip_id")
    if not isinstance(tid, str) or not tid:
        tid = "invalid-" + hashlib.sha1(payload.encode()).hexdigest()[:12]
    pickup = parse_sim(v.get("pickup_ts"))
    return {
        "trip_id": tid,
        "source": v.get("source") if v.get("source") in SOURCES else "replay",
        "trace_id": v.get("trace_id") if is_uuid(v.get("trace_id")) else None,
        "pickup_ts": format_sim(pickup) if pickup else None,
        "reasons": list(v["_reasons"]),
        "payload": payload,
        "rejected_at": rejected_at,
    }
