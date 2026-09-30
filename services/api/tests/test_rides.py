import json

import pytest
from conftest import FakeDb, FakeOut, FakeRideClock, FakeTraceDb, make_config
from contract_validator import assert_valid
from fastapi.testclient import TestClient

from api.db import DbError, DbUnavailable, Result
from api.main import Deps, create_app
from rides.service import RideRejected, RideRequest, RideService

ZONES = Result(
    [("location_id", "UInt16"), ("zone", "String"), ("borough", "String"), ("service_zone", "String"),
     ("lat", "Nullable(Float64)"), ("lon", "Nullable(Float64)")],
    [[230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985],
     [132, "JFK Airport", "Queens", "Airports", 40.641, -73.779],
     [161, "Midtown Center", "Manhattan", "Yellow Zone", 40.758, -73.978],
     [7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920]], 1)
MEDIAN = Result([("n", "UInt64"), ("distance", "Float64"), ("duration", "Float64"), ("fare", "Float64")],
                [[214, 3.1, 16.5, 19.5]], 3)
FEW = Result(MEDIAN.columns, [[2, 3.0, 12.0, 15.0]], 3)
SIM_NOW = Result([("now_ts", "DateTime64(3)")], [["2025-10-09T08:00:00.500"]], 1)
CARD = RideRequest(230, 132, "card", 5.0, 1)


def service(db: FakeTraceDb | None = None, out: FakeOut | None = None, clock: FakeRideClock | None = None,
            **env: str) -> tuple[RideService, FakeTraceDb, FakeOut]:
    db = db or FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": MEDIAN, "taxi.sim_now": SIM_NOW})
    out = out or FakeOut()
    t = [0.0]
    svc = RideService(make_config(**env), db, out, clock or FakeRideClock(), clock=lambda: t[0])
    return svc, db, out


@pytest.mark.anyio
async def test_create_produces_the_trip_then_submitted_and_ingested():
    svc, db, out = service()
    r = await svc.create(CARD, "10.0.0.1")
    tid = r["trace_id"]
    assert r["trip_id"] == f"s-{tid}" and r["trace_url"] == f"/trace/{tid}" and r["pickup_ts"] == "2025-10-09T17:42:00"
    assert r["estimate"] == {"trip_distance": 3.1, "duration_min": 16.5, "fare_amount": 19.5, "total_amount": 29.25,
                             "method": "median of 214 real trips between these zones"}
    assert r["notes"] == []
    assert [(t, k) for t, k, _ in out.sent] == [("trips.raw", f"s-{tid}"), ("trace.checkpoints", tid),
                                                ("trace.checkpoints", tid)]
    trip, submitted, ingested = (v for _, _, v in out.sent)
    assert_valid("trip", trip)
    for cp in (submitted, ingested):
        assert_valid("trace_checkpoint", cp)
    assert (submitted["stage"], submitted["seq"], ingested["stage"], ingested["seq"]) == ("submitted", 1, "ingested", 2)
    assert submitted["detail"] == {"pickup_zone": "Times Sq/Theatre District", "dropoff_zone": "JFK Airport",
                                   "payment": "card", "tip_amount": 5.0, "fare_amount": 19.5,
                                   "estimate_method": "median of 214 real trips between these zones"}
    assert ingested["detail"] == {"topic": "trips.raw", "partition": 0, "offset": 482113}
    assert submitted["at"] <= ingested["at"]
    median_sql, params = next(c for c in db.calls if "quantile(0.5)" in c[0])
    assert params == {"pickup": 230, "dropoff": 132} and "{pickup:UInt16}" in median_sql


@pytest.mark.anyio
async def test_cash_ride_records_no_tip_and_explains_why():
    svc, _, out = service()
    r = await svc.create(RideRequest(230, 132, "cash", 5.0, 1), "10.0.0.1")
    assert r["notes"] == ["NYC taxi data doesn't record cash tips, so your tip will show as $0"]
    trip = out.sent[0][2]
    assert trip["tip_amount"] == 0.0 and trip["payment_type"] == 2
    assert out.sent[1][2]["detail"]["tip_amount"] == 5.0  # what the student sent, shown on the ride chip


@pytest.mark.anyio
async def test_few_real_trips_falls_back_to_centroids():
    db = FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": FEW})
    svc, _, _ = service(db=db)
    assert (await svc.estimate(230, 7)).method.startswith("straight-line distance × 1.3")


@pytest.mark.anyio
async def test_estimate_is_cached_per_zone_pair():
    svc, db, _ = service()
    await svc.estimate(230, 132)
    await svc.estimate(230, 132)
    assert sum("quantile(0.5)" in sql for sql, _ in db.calls) == 1


@pytest.mark.anyio
async def test_estimate_survives_clickhouse_down_once_zones_are_loaded():
    svc, db, _ = service()
    await svc.zones()
    db.fail = DbUnavailable("down")
    est = await svc.estimate(230, 132)
    assert est.method.startswith("straight-line distance")
    db.fail = None
    await svc.estimate(230, 132)  # the fallback was not cached: the median is tried again
    assert sum("quantile(0.5)" in sql for sql, _ in db.calls) == 2


@pytest.mark.anyio
async def test_stale_sim_clock_falls_back_to_sim_now():
    svc, db, _out = service(clock=FakeRideClock(sim_ts=None))
    r = await svc.create(CARD, "10.0.0.1")
    assert r["pickup_ts"] == "2025-10-09T08:00:00"
    assert any("taxi.sim_now" in sql for sql, _ in db.calls)


@pytest.mark.anyio
async def test_broker_failure_is_503_and_emits_nothing():
    out = FakeOut()
    out.fail = True
    svc, _, _ = service(out=out)
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.0.1")
    assert e.value.status == 503 and e.value.message == "The pipeline is catching its breath, try again in a moment"
    assert out.sent == []


@pytest.mark.anyio
async def test_fourth_ride_from_one_ip_within_a_minute_is_429():
    svc, _, _ = service()
    for _ in range(3):
        await svc.create(CARD, "10.0.0.1")
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.0.1")
    assert e.value.status == 429 and e.value.retry_after_s is not None and e.value.retry_after_s > 0
    await svc.create(CARD, "10.0.0.2")  # another phone is not affected


@pytest.mark.anyio
async def test_global_cap_across_phones():
    svc, _, _ = service(RIDES_GLOBAL_PER_MIN="5")
    for i in range(5):
        await svc.create(CARD, f"10.0.1.{i}")
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.1.99")
    assert e.value.status == 429 and e.value.message == "The dispatcher is busy, try again in a few seconds"


@pytest.mark.anyio
async def test_published_checkpoints_reach_the_hub_immediately():
    seen: list[str] = []
    db = FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": MEDIAN})
    svc = RideService(make_config(), db, FakeOut(), FakeRideClock(), publish=lambda cp: seen.append(cp["stage"]))
    await svc.create(CARD, "10.0.0.1")
    assert seen == ["submitted", "ingested"]


@pytest.mark.anyio
async def test_cold_zone_cache_and_a_clickhouse_error_is_503_not_500():
    svc, db, out = service()
    db.fail = DbError("Code: 201. DB::Exception: Quota for user `trace_ro` has been exceeded")
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.0.1")
    assert e.value.status == 503 and e.value.message == "The zone list is still loading, try again in a moment"
    assert out.sent == []
    await svc.warm_up()  # logged, never raised: the lifespan task must not die
    r = app(svc).post("/api/rides", json=BODY)
    assert r.status_code == 503 and r.json()["detail"] == "The zone list is still loading, try again in a moment"


def app(svc: RideService) -> TestClient:
    return TestClient(create_app(Deps(cfg=make_config(), db=FakeDb(), sim_clock=FakeRideClock(), rides=svc)))


BODY = {"pickup_location_id": 230, "dropoff_location_id": 132, "payment": "card", "tip_amount": 5.0, "passenger_count": 1}


def test_post_rides_returns_201():
    svc, _, _ = service()
    r = app(svc).post("/api/rides", json=BODY)
    assert r.status_code == 201
    assert set(r.json()) == {"trace_id", "trip_id", "trace_url", "pickup_ts", "estimate", "notes"}


@pytest.mark.parametrize(("field", "value"), [
    ("pickup_location_id", 0), ("pickup_location_id", 264), ("pickup_location_id", "230"), ("pickup_location_id", 23.5),
    ("dropoff_location_id", -1), ("payment", "bitcoin"), ("tip_amount", -0.01), ("tip_amount", 999.01),
    ("tip_amount", 1.234), ("tip_amount", "5"), ("tip_amount", True), ("tip_amount", 10**400), ("passenger_count", 0), ("passenger_count", 7),
    ("passenger_count", True),
])
def test_invalid_field_is_422_naming_it(field, value):
    svc, _, out = service()
    r = app(svc).post("/api/rides", json={**BODY, field: value})
    assert r.status_code == 422
    assert r.json()["detail"][0]["loc"][-1] == field
    assert out.sent == []


def test_missing_field_is_422_naming_it():
    svc, _, _ = service()
    r = app(svc).post("/api/rides", json={k: v for k, v in BODY.items() if k != "payment"})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "payment"


def test_nan_tip_is_422():
    svc, _, _ = service()
    raw = json.dumps(BODY).replace('"tip_amount": 5.0', '"tip_amount": NaN')
    r = app(svc).post("/api/rides", content=raw, headers={"Content-Type": "application/json"})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "tip_amount"


def test_absurd_tip_is_accepted_on_purpose():
    svc, _, _ = service()
    assert app(svc).post("/api/rides", json={**BODY, "tip_amount": 999}).status_code == 201


def test_rate_limit_is_429_with_retry_after():
    svc, _, _ = service(RIDES_RATE_LIMIT_PER_MIN="1")
    c = app(svc)
    assert c.post("/api/rides", json=BODY).status_code == 201
    r = c.post("/api/rides", json=BODY)
    assert r.status_code == 429 and int(r.headers["Retry-After"]) > 0
    assert r.json()["retry_after_s"] == int(r.headers["Retry-After"])


def test_broker_down_is_503():
    out = FakeOut()
    out.fail = True
    svc, _, _ = service(out=out)
    r = app(svc).post("/api/rides", json=BODY)
    assert r.status_code == 503 and r.json()["detail"] == "The pipeline is catching its breath, try again in a moment"


def test_estimate_endpoint():
    svc, _, _ = service()
    c = app(svc)
    r = c.get("/api/rides/estimate", params={"pickup_location_id": 230, "dropoff_location_id": 132})
    assert r.json() == {"trip_distance": 3.1, "duration_min": 16.5, "fare_amount": 19.5,
                        "method": "median of 214 real trips between these zones"}
    assert c.get("/api/rides/estimate", params={"pickup_location_id": 0, "dropoff_location_id": 132}).status_code == 422
