"""Tumbling-window aggregation per pickup borough (spec 03 §Aggregation)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from processor.times import format_sim, sim_from_epoch_ms


def _empty() -> dict[str, Any]:
    return {"trips": 0, "revenue": 0.0, "fare_sum": 0.0, "distance_sum": 0.0, "card_trips": 0,
            "card_tip_pct_sum": 0.0, "card_tip_pct_n": 0, "student_trips": 0}


def add_trip(bucket: Mapping[str, Any], trip: Mapping[str, Any]) -> dict[str, Any]:
    b = dict(bucket)
    b["trips"] += 1
    b["revenue"] += float(trip["total_amount"])
    b["fare_sum"] += float(trip["fare_amount"])
    b["distance_sum"] += float(trip["trip_distance"])
    if trip.get("is_card"):
        b["card_trips"] += 1
        if trip.get("tip_pct") is not None:
            b["card_tip_pct_sum"] += float(trip["tip_pct"])
            b["card_tip_pct_n"] += 1
    if trip.get("source") == "student":
        b["student_trips"] += 1
    return b


def init_bucket(trip: Mapping[str, Any]) -> dict[str, Any]:
    """Quix calls the initializer with the window's first value, so it must count it."""
    return add_trip(_empty(), trip)


def to_agg_record(window: Mapping[str, Any], borough: str, emitted_at: str) -> dict[str, Any]:
    b = window["value"]
    n = b["trips"]
    return {
        "window_start": format_sim(sim_from_epoch_ms(window["start"])),
        "window_end": format_sim(sim_from_epoch_ms(window["end"])),
        "borough": borough,
        "trips": n,
        "revenue": round(b["revenue"], 2),
        "avg_fare": round(b["fare_sum"] / n, 2),
        "avg_distance": round(b["distance_sum"] / n, 3),
        "card_trips": b["card_trips"],
        "avg_card_tip_pct": round(b["card_tip_pct_sum"] / b["card_tip_pct_n"], 2) if b["card_tip_pct_n"] else None,
        "student_trips": b["student_trips"],
        "emitted_at": emitted_at,
    }
