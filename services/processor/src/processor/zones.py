"""Taxi zone lookup loaded once at startup from data/zones.csv (spec 01 output)."""

from __future__ import annotations

import csv
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple

BOROUGHS = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown", "N/A"}
SERVICE_ZONES = {"Yellow Zone", "Boro Zone", "Airports", "EWR", "N/A", "Unknown"}


class Zone(NamedTuple):
    zone: str
    borough: str
    service_zone: str


UNKNOWN = Zone("Unknown", "Unknown", "Unknown")


def load_zones(path: Path) -> dict[int, Zone]:
    zones: dict[int, Zone] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            location_id = int(r["location_id"])
            if location_id >= 264:  # 264 "Unknown" and 265 "Outside of NYC" enrich as UNKNOWN (spec 03)
                continue
            borough = r["borough"] if r["borough"] in BOROUGHS else "Unknown"
            service = r["service_zone"] if r["service_zone"] in SERVICE_ZONES else "Unknown"
            zones[location_id] = Zone(r["zone"] or "Unknown", borough, service)
    if not zones:
        raise ValueError(f"{path}: no zones loaded")
    return zones


def zone_for(zones: Mapping[int, Zone], location_id: int) -> Zone:
    return zones.get(location_id, UNKNOWN)
