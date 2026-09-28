"""Trace checkpoints emitted by the processor (spec 03 §Trace checkpoints, spec 07)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from processor.rules import RULES_CHECKED
from processor.times import format_sim, parse_sim

SEQ = {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
       "stored": 6, "queryable": 7}


def checkpoint(trip: Mapping[str, Any], stage: str, detail: dict[str, Any], at: str) -> dict[str, Any]:
    return {"trace_id": trip["trace_id"], "trip_id": trip["trip_id"], "source": trip["source"], "stage": stage,
            "seq": SEQ[stage], "service": "processor", "at": at, "detail": detail}


def validated_or_rejected(v: Mapping[str, Any], at: str) -> dict[str, Any]:
    if v["_reasons"]:
        return checkpoint(v, "rejected", {"reasons": list(v["_reasons"])}, at)
    return checkpoint(v, "validated", {"rules_checked": RULES_CHECKED}, at)


def enriched_and_aggregated(v: Mapping[str, Any], at: str) -> list[dict[str, Any]]:
    pickup = parse_sim(v["pickup_ts"])
    assert pickup is not None
    enriched = {k: v[k] for k in ("pu_zone", "pu_borough", "do_zone", "do_borough", "tip_pct", "duration_min")}
    window = {"window_start": format_sim(pickup.replace(second=0, microsecond=0)), "borough": v["pu_borough"]}
    return [checkpoint(v, "enriched", enriched, at), checkpoint(v, "aggregated", window, at)]


def stored_checkpoint(v: Mapping[str, Any], table: str, batch_rows: int, at: str) -> dict[str, Any]:
    return checkpoint(v, "stored", {"table": table, "batch_rows": batch_rows}, at)
