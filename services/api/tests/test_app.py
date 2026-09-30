from conftest import GOLDEN, FakeDb, make_config
from fake_llm import FakeLLM
from fastapi.testclient import TestClient

from agent.golden import load_golden
from agent.providers import ProviderPool
from api.db import DbUnavailable, Result
from api.envconfig import ProviderConfig
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


class Thread:
    def __init__(self, alive: bool) -> None:
        self.alive = alive


def test_healthz_reports_the_background_threads():
    deps = Deps(cfg=make_config(), db=FakeDb(), sim_clock=Clock(), out=Thread(True), relay=Thread(False))
    body = TestClient(create_app(deps)).get("/api/healthz").json()
    assert body["kafka_out"] is True and body["trace_relay"] is False


def test_zones_are_sorted_and_cached():
    zones = Result(
        [("location_id", "UInt16"), ("zone", "String"), ("borough", "LowCardinality(String)")],
        [[4, "Alphabet City", "Manhattan"], [132, "JFK Airport", "Queens"]],
        2,
    )
    db = FakeDb({"FROM taxi.zones": zones})
    c = client(db)
    first = c.get("/api/zones").json()
    assert first == [
        {"location_id": 4, "zone": "Alphabet City", "borough": "Manhattan"},
        {"location_id": 132, "zone": "JFK Airport", "borough": "Queens"},
    ]
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


def app_with_pool() -> tuple[TestClient, ProviderPool]:
    cfg = make_config()
    fakes = {"local": FakeLLM()}
    p = ProviderPool(
        [ProviderConfig("local", "http://x/v1", "k", "fake-local")], cfg.llm_order, lambda pc: fakes[pc.name].client()
    )
    return TestClient(create_app(Deps(cfg=cfg, db=FakeDb(), sim_clock=Clock(), pool=p))), p


def test_llm_status():
    c, _ = app_with_pool()
    body = c.get("/api/llm/status").json()
    assert body["active"] == "local" and body["providers"][0]["model"] == "fake-local"


def test_admin_order_needs_the_token():
    c, p = app_with_pool()
    assert c.post("/api/admin/llm/order", json={"order": []}).status_code == 401
    assert c.post("/api/admin/llm/order", json={"order": []}, headers={"X-Admin-Token": "nope"}).status_code == 401
    ok = c.post("/api/admin/llm/order", json={"order": []}, headers={"X-Admin-Token": "t0ken"})
    assert ok.status_code == 200 and ok.json()["active"] == "cache" and p.order == []
    bad = c.post("/api/admin/llm/order", json={"order": ["cloud"]}, headers={"X-Admin-Token": "t0ken"})
    assert bad.status_code == 400


def test_admin_disabled_when_token_empty():
    cfg = make_config(ADMIN_TOKEN="")
    c = TestClient(
        create_app(Deps(cfg=cfg, db=FakeDb(), sim_clock=Clock(), pool=ProviderPool([], (), lambda pc: None)))
    )
    assert c.post("/api/admin/llm/order", json={"order": []}, headers={"X-Admin-Token": ""}).status_code == 401


def test_suggestions_route():
    golden = tuple(load_golden(GOLDEN))
    c = TestClient(create_app(Deps(cfg=make_config(), db=FakeDb(), sim_clock=Clock(), golden=golden)))
    body = c.get("/api/suggestions").json()
    assert [s["id"] for s in body] == ["best_tippers", "rush_hour_distance", "busiest_zones_last_hour",
                                       "airport_vs_other", "rejected_why"]
    assert set(body[0]) == {"id", "question"}


def test_ask_validates_the_body():
    c = TestClient(create_app(Deps(cfg=make_config(), db=FakeDb(), sim_clock=Clock())))
    assert c.post("/api/agent/ask", json={"question": "   "}).status_code == 422
    assert c.post("/api/agent/ask", json={"question": "hi", "origin": "admin"}).status_code == 422
