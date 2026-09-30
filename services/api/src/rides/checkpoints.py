"""Trace checkpoints (contracts/events/trace_checkpoint.schema.json, spec 07 §Checkpoint catalogue)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

TRACE_TOPIC = "trace.checkpoints"
SEQ = {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
       "stored": 6, "queryable": 7}
TERMINAL = frozenset({"queryable", "rejected"})


def wall_str(dt: datetime) -> str:
    utc = dt.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def parse_wall(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def checkpoint(trace_id: str, trip_id: str, source: str, stage: str, detail: dict[str, Any], at: str,
               service: str = "api") -> dict[str, Any]:
    return {"trace_id": trace_id, "trip_id": trip_id, "source": source, "stage": stage, "seq": SEQ[stage],
            "service": service, "at": at, "detail": detail}


def ordered(cps: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Checkpoints can arrive out of order on the topic, so everything sorts by seq (spec 07 §Ordering)."""
    return sorted(cps, key=lambda c: (c["seq"], c["at"]))


def parse_checkpoint(value: bytes | None) -> dict[str, Any] | None:
    """A checkpoint from the topic, or None for anything malformed (the relay must never die on bad input)."""
    try:
        cp = json.loads(value or b"")
    except ValueError:
        return None
    if not isinstance(cp, dict):
        return None
    if not isinstance(cp.get("trace_id"), str) or cp.get("stage") not in SEQ:
        return None
    if not isinstance(cp.get("seq"), int) or not isinstance(cp.get("at"), str):
        return None
    return cp
