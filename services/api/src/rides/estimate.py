"""Fare estimate and ride amounts (spec 07 §Ride creation steps 3-4). Pure functions, TDD."""

from __future__ import annotations

import math
from dataclasses import dataclass

MIN_SAMPLES = 5
AIRPORT_PICKUPS = frozenset({132, 138})
EARTH_RADIUS_MI = 3958.8
TYPICAL_DISTANCE_MI = 2.5  # for a zone without a centroid (264/265, duplicate shapefile ids)
CASH_NOTE = "NYC taxi data doesn't record cash tips, so your tip will show as $0"


@dataclass(frozen=True)
class Zone:
    location_id: int
    zone: str
    borough: str
    service_zone: str
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class Estimate:
    trip_distance: float
    duration_min: float
    fare_amount: float
    method: str


@dataclass(frozen=True)
class Amounts:
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    congestion_surcharge: float
    airport_fee: float
    cbd_congestion_fee: float
    total_amount: float
    payment_type: int


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(math.sqrt(a))


def _estimate(distance: float, duration: float, fare: float, method: str) -> Estimate:
    # Floors keep every student ride valid for the processor (DISTANCE_NON_POSITIVE, DROPOFF_NOT_AFTER_PICKUP,
    # FARE_NON_POSITIVE): only the tip decides whether it is rejected.
    return Estimate(trip_distance=round(max(distance, 0.1), 2), duration_min=round(max(duration, 1.0), 1),
                    fare_amount=round(max(fare, 3.0), 2), method=method)


def from_median(n: int, distance: float | None, duration: float | None, fare: float | None) -> Estimate | None:
    """The median of real replayed trips between the two zones, or None if there are fewer than 5."""
    if n < MIN_SAMPLES or distance is None or duration is None or fare is None:
        return None
    return _estimate(distance, duration, fare, f"median of {n} real trips between these zones")


def from_centroids(pu: Zone, do: Zone) -> Estimate:
    if pu.lat is None or pu.lon is None or do.lat is None or do.lon is None:
        distance = TYPICAL_DISTANCE_MI
        method = "typical NYC trip (no map point for this zone)"
    else:
        distance = max(0.5, 1.3 * haversine_miles(pu.lat, pu.lon, do.lat, do.lon))
        method = "straight-line distance × 1.3 (too few real trips between these zones)"
    duration = distance / 12 * 60 + 3
    return _estimate(distance, duration, 3.00 + 3.50 * distance + 0.30 * duration, method)


def amounts(est: Estimate, pu: Zone, do: Zone, payment: str, tip: float) -> Amounts:
    yellow = "Yellow Zone" in (pu.service_zone, do.service_zone)
    tip_amount = round(tip, 2) if payment == "card" else 0.0  # NYC TLC data never records cash tips
    congestion = 2.50 if yellow else 0.0
    cbd = 0.75 if yellow else 0.0
    airport = 1.75 if pu.location_id in AIRPORT_PICKUPS else 0.0
    total = est.fare_amount + 0.50 + tip_amount + 1.00 + congestion + airport + cbd
    return Amounts(fare_amount=est.fare_amount, extra=0.0, mta_tax=0.50, tip_amount=tip_amount, tolls_amount=0.0,
                   improvement_surcharge=1.00, congestion_surcharge=congestion, airport_fee=airport,
                   cbd_congestion_fee=cbd, total_amount=round(total, 2), payment_type=1 if payment == "card" else 2)


def ride_notes(payment: str) -> list[str]:
    return [CASH_NOTE] if payment == "cash" else []
