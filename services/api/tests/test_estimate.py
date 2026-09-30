import pytest

from rides.estimate import Estimate, Zone, amounts, from_centroids, from_median, haversine_miles, ride_notes

TIMES_SQ = Zone(230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985)
JFK = Zone(132, "JFK Airport", "Queens", "Airports", 40.641, -73.779)
ASTORIA = Zone(7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920)
NOWHERE = Zone(103, "Governor's Island/Ellis Island/Liberty Island", "Manhattan", "Yellow Zone", None, None)


def test_median_needs_five_trips():
    assert from_median(4, 3.1, 16.5, 19.5) is None
    assert from_median(5, None, 16.5, 19.5) is None  # quantile over zero rows is NaN → None
    est = from_median(214, 3.1, 16.54, 19.5)
    assert est == Estimate(3.1, 16.5, 19.5, "median of 214 real trips between these zones")


def test_haversine_one_degree_of_latitude():
    assert haversine_miles(0.0, 0.0, 1.0, 0.0) == pytest.approx(69.09, rel=1e-3)


def test_centroid_fallback_minimum_distance():
    same = Zone(7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920)
    est = from_centroids(ASTORIA, same)
    assert est.trip_distance == 0.5  # max(0.5, 1.3 × 0)
    assert est.duration_min == 5.5   # 0.5 / 12 × 60 + 3
    assert est.fare_amount == 6.4    # 3.00 + 3.50 × 0.5 + 0.30 × 5.5
    assert est.method.startswith("straight-line distance × 1.3")


def test_centroid_fallback_scales_with_distance():
    est = from_centroids(TIMES_SQ, JFK)
    d = 1.3 * haversine_miles(TIMES_SQ.lat, TIMES_SQ.lon, JFK.lat, JFK.lon)  # type: ignore[arg-type]
    assert est.trip_distance == round(d, 2)
    assert est.duration_min == round(d / 12 * 60 + 3, 1)
    assert est.fare_amount == round(3.0 + 3.5 * d + 0.3 * (d / 12 * 60 + 3), 2)


def test_zone_without_centroid_uses_a_typical_trip():
    est = from_centroids(NOWHERE, JFK)
    assert est.trip_distance == 2.5 and est.method == "typical NYC trip (no map point for this zone)"


def test_estimate_is_always_a_valid_trip():
    tiny = from_median(10, 0.0, 0.2, 0.0)
    assert tiny is not None
    assert tiny.trip_distance == 0.1 and tiny.duration_min == 1.0 and tiny.fare_amount == 3.0


def test_amounts_card_yellow_zone_airport_pickup_matches_the_spec_example():
    est = Estimate(3.1, 16.5, 19.5, "median of 214 real trips between these zones")
    a = amounts(est, JFK, TIMES_SQ, "card", 5.0)
    assert (a.mta_tax, a.improvement_surcharge, a.congestion_surcharge, a.cbd_congestion_fee) == (0.5, 1.0, 2.5, 0.75)
    assert a.airport_fee == 1.75 and a.extra == 0.0 and a.tolls_amount == 0.0
    assert a.tip_amount == 5.0 and a.payment_type == 1
    assert a.total_amount == 31.0  # 19.5 + 0.5 + 5 + 1 + 2.5 + 1.75 + 0.75


def test_amounts_spec_example_total():
    est = Estimate(3.1, 16.5, 19.5, "x")
    assert amounts(est, TIMES_SQ, JFK, "card", 5.0).total_amount == 29.25  # spec 07 response example


def test_amounts_cash_records_no_tip():
    a = amounts(Estimate(3.1, 16.5, 19.5, "x"), ASTORIA, JFK, "cash", 5.0)
    assert a.tip_amount == 0.0 and a.payment_type == 2
    assert a.congestion_surcharge == 0.0 and a.cbd_congestion_fee == 0.0 and a.airport_fee == 0.0
    assert a.total_amount == 21.0  # 19.5 + 0.5 + 1.0
    assert ride_notes("cash") == ["NYC taxi data doesn't record cash tips, so your tip will show as $0"]
    assert ride_notes("card") == []
