from conftest import FakeDb, make_config
from fastapi.testclient import TestClient

from api.db import DbUnavailable, Result
from api.main import Deps, create_app


class Clock:
    latest = None


def client(db: FakeDb | None = None, clock: Clock | None = None) -> TestClient:
    return TestClient(create_app(Deps(cfg=make_config(), db=db or FakeDb(), sim_clock=clock or Clock())))


def test_healthz_reports_dependencies():
    db = FakeDb()
    assert client(db).get("/api/healthz").json() == {"ok": True, "clickhouse": True, "sim_clock": False}
    db.fail = DbUnavailable("down")
    assert client(db).get("/api/healthz").json()["clickhouse"] is False


def test_zones_are_sorted_and_cached():
    zones = Result([("location_id", "UInt16"), ("zone", "String"), ("borough", "LowCardinality(String)")],
                   [[4, "Alphabet City", "Manhattan"], [132, "JFK Airport", "Queens"]], 2)
    db = FakeDb({"FROM taxi.zones": zones})
    c = client(db)
    first = c.get("/api/zones").json()
    assert first == [{"location_id": 4, "zone": "Alphabet City", "borough": "Manhattan"},
                     {"location_id": 132, "zone": "JFK Airport", "borough": "Queens"}]
    assert c.get("/api/zones").json() == first
    assert len(db.queries) == 1
    assert "ORDER BY borough, zone" in db.queries[0] and "BETWEEN 1 AND 263" in db.queries[0]


def test_zones_503_when_database_down():
    db = FakeDb()
    db.fail = DbUnavailable("down")
    r = client(db).get("/api/zones")
    assert r.status_code == 503


def test_sim_clock_503_until_first_heartbeat():
    clock = Clock()
    c = client(clock=clock)
    assert c.get("/api/sim/clock").status_code == 503
    clock.latest = {"sim_ts": "2025-10-07T08:00:00"}
    assert c.get("/api/sim/clock").json() == {"sim_ts": "2025-10-07T08:00:00"}
