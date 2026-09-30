import asyncio
import json
import time

import pytest
from conftest import FakeDb, FakeRideClock, FakeTraceDb, make_config
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_history import cp

from api.db import DbUnavailable, Result
from api.main import Deps, create_app
from rides.history import TraceHistory
from rides.hub import Hub
from rides.trace_service import TraceService, normalise_trace_id

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
EVENTS_COLUMNS = [("trace_id", "UUID"), ("trip_id", "String"), ("source", "String"), ("stage", "String"),
                  ("seq", "UInt8"), ("service", "String"), ("at", "DateTime64(3, 'UTC')"), ("detail", "String")]


def ch_rows(*stages: tuple[str, int, str]) -> Result:
    return Result(EVENTS_COLUMNS, [[TID, f"s-{TID}", "student", s, seq, "api", at, json.dumps({"k": s})]
                                   for s, seq, at in stages], 2)


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.closed: int | None = None

    async def send_json(self, data: dict) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000) -> None:
        self.closed = code


def svc(db: FakeTraceDb | None = None, **kw) -> tuple[TraceService, Hub]:
    hub = Hub(TraceHistory(3600))
    return TraceService(hub, db or FakeTraceDb(), FakeRideClock(), **kw), hub


@pytest.mark.anyio
async def test_history_merges_clickhouse_and_memory():
    # after an api restart, memory only has what arrived since; ClickHouse has the start
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, "2026-10-16T10:31:07.000+00:00"),
                                                   ("ingested", 2, "2026-10-16T10:31:07.041+00:00"))})
    s, hub = svc(db)
    hub.publish(cp(TID, "ingested", 2, "2026-10-16T10:31:07.041Z"))
    hub.publish(cp(TID, "validated", 3, "2026-10-16T10:31:07.103Z"))
    got = await s.history(TID)
    assert [c["stage"] for c in got] == ["submitted", "ingested", "validated"]
    assert got[0]["at"] == "2026-10-16T10:31:07.000Z" and got[0]["detail"] == {"k": "submitted"}
    sql, params = db.calls[-1]
    assert "{id:UUID}" in sql and params == {"id": TID}


@pytest.mark.anyio
async def test_history_uses_memory_when_clickhouse_is_down():
    db = FakeTraceDb()
    db.fail = DbUnavailable("down")
    s, hub = svc(db)
    hub.publish(cp(TID, "submitted", 1))
    assert [c["stage"] for c in await s.history(TID)] == ["submitted"]


@pytest.mark.anyio
async def test_history_rearms_queryable_after_restart():
    rearmed: list[str] = []
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, "2026-10-16T10:31:07.000+00:00"),
                                                   ("stored", 6, "2026-10-16T10:31:08.000+00:00"))})
    s, hub = svc(db)
    hub.on_stored = lambda c: rearmed.append(c["stage"])
    await s.history(TID)
    assert rearmed == ["stored"]


@pytest.mark.anyio
async def test_follow_sends_history_then_new_stages_once_then_closes_after_terminal():
    s, hub = svc(after_terminal_s=0.05)
    hub.publish(cp(TID, "submitted", 1))
    ws = FakeWs()
    task = asyncio.create_task(s.follow(ws, TID))
    for _ in range(20):
        await asyncio.sleep(0)
    hub.publish(cp(TID, "ingested", 2))
    hub.publish(cp(TID, "ingested", 2))
    hub.publish(cp(TID, "queryable", 7))
    await asyncio.wait_for(task, 1)
    assert ws.sent[0]["type"] == "history" and [c["stage"] for c in ws.sent[0]["checkpoints"]] == ["submitted"]
    assert [(m["type"], m["stage"]) for m in ws.sent[1:]] == [("checkpoint", "ingested"), ("checkpoint", "queryable")]
    assert ws.closed == 1000 and hub.follower_count(TID) == 0


@pytest.mark.anyio
async def test_follow_closes_at_the_max_duration():
    s, _ = svc(follow_max_s=0.05)
    ws = FakeWs()
    await asyncio.wait_for(s.follow(ws, TID), 1)
    assert ws.sent == [{"type": "history", "checkpoints": [], "sim_ts": "2025-10-09T17:42:00.250"}]
    assert ws.closed == 1000


@pytest.mark.anyio
async def test_follow_catches_up_on_stages_the_relay_missed():
    # the relay was rejoining while `ingested` and `stored` went by: only ClickHouse has them
    t0, t1, t2, t3 = (f"2026-10-16T10:31:0{i}.000+00:00" for i in range(6, 10))
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, t0))})
    s, hub = svc(db, refresh_after_s=0.05, after_terminal_s=0.05)
    rearmed: list[str] = []
    hub.on_stored = lambda c: rearmed.append(c["stage"])
    ws = FakeWs()
    task = asyncio.create_task(s.follow(ws, TID))
    await asyncio.sleep(0.01)
    db.results["taxi.trace_events"] = ch_rows(("submitted", 1, t0), ("ingested", 2, t1), ("stored", 6, t2))
    await asyncio.sleep(0.1)
    db.results["taxi.trace_events"] = ch_rows(("submitted", 1, t0), ("ingested", 2, t1), ("stored", 6, t2),
                                              ("queryable", 7, t3))
    await asyncio.wait_for(task, 1)
    assert [c["stage"] for c in ws.sent[0]["checkpoints"]] == ["submitted"]
    assert [(m["type"], m["stage"]) for m in ws.sent[1:]] == [("checkpoint", "ingested"), ("checkpoint", "stored"),
                                                              ("checkpoint", "queryable")]
    assert rearmed == ["stored"]  # the missed `stored` still starts the queryable poll, once
    assert ws.closed == 1000


@pytest.mark.anyio
async def test_follow_does_not_refresh_after_a_terminal_stage():
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, "2026-10-16T10:31:07.000+00:00"),
                                                   ("rejected", 3, "2026-10-16T10:31:08.000+00:00"))})
    s, _ = svc(db, refresh_after_s=0.01, after_terminal_s=0.1)
    await asyncio.wait_for(s.follow(FakeWs(), TID), 1)
    assert len(db.calls) == 1  # only the initial history read


@pytest.mark.anyio
async def test_live_sends_snapshot_clock_stats_and_flags_ambient():
    db = FakeTraceDb({"countIf(stage = 'submitted')": Result([("rides", "UInt64"), ("rejected", "UInt64")], [[57, 6]], 1),
                      "lagInFrame": Result([("stage", "String"), ("p50_ms", "Float64")], [["ingested", 40.0]], 1)})
    s, hub = svc(db, clock_every_s=0.01, stats_every_s=10)
    hub.publish(cp(TID, "submitted", 1))
    ws = FakeWs()
    task = asyncio.create_task(s.live(ws))
    await asyncio.sleep(0.03)
    hub.publish(cp("r", "ingested", 2, source="replay"))
    await asyncio.sleep(0.03)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)  # let its finally run (unwatch_live)
    types = [m["type"] for m in ws.sent]
    assert types[0] == "snapshot" and ws.sent[0]["traces"][0]["trace_id"] == TID
    assert "clock" in types
    assert {"type": "stats", "rides_today": 57, "rejected_today": 6, "median_ms": {"ingested": 40}} in ws.sent
    (ambient,) = [m for m in ws.sent if m["type"] == "checkpoint"]
    assert ambient["ambient"] is True and ambient["trace_id"] == "r"
    assert hub.live_count() == 0


@pytest.mark.anyio
async def test_stats_are_cached_between_clients():
    db = FakeTraceDb()
    s, _ = svc(db, stats_every_s=60)
    await s.stats()
    await s.stats()
    assert len(db.calls) == 2  # one "today" query + one hops query, once


def test_normalise_trace_id():
    assert normalise_trace_id(TID.upper()) == TID
    assert normalise_trace_id("live") is None and normalise_trace_id("'; DROP TABLE x; --") is None


def client(s: TraceService, **env: str) -> TestClient:
    return TestClient(create_app(Deps(cfg=make_config(**env), db=FakeDb(), sim_clock=FakeRideClock(), trace=s)))


def test_ws_route_sends_history_and_rejects_bad_ids():
    s, hub = svc(follow_max_s=0.2)
    hub.publish(cp(TID, "submitted", 1))
    c = client(s)
    with c.websocket_connect(f"/api/trace/{TID}") as ws:
        first = ws.receive_json()
        assert first["type"] == "history" and first["checkpoints"][0]["stage"] == "submitted"
    with pytest.raises(WebSocketDisconnect) as e:
        with c.websocket_connect("/api/trace/not-a-uuid") as ws:
            ws.receive_json()
    assert e.value.code == 1008


def test_disconnect_releases_the_subscription():
    s, hub = svc()  # follow_max_s = 10 min: only the disconnect can end it
    with client(s).websocket_connect(f"/api/trace/{TID}") as ws:
        ws.receive_json()
        assert hub.follower_count(TID) == 1
    for _ in range(50):  # the app task finishes on the TestClient's portal thread
        if hub.follower_count(TID) == 0:
            break
        time.sleep(0.02)
    assert hub.follower_count(TID) == 0


def test_live_route_is_not_parsed_as_a_trace_id():
    s, _ = svc()
    with client(s).websocket_connect("/api/trace/live") as ws:
        assert ws.receive_json()["type"] == "snapshot"


def test_get_trace_history():
    s, hub = svc()
    hub.publish(cp(TID, "submitted", 1))
    c = client(s)
    assert c.get(f"/api/trace/{TID}").json()["checkpoints"][0]["stage"] == "submitted"
    assert c.get("/api/trace/00000000-0000-0000-0000-000000000000").status_code == 404
    assert c.get("/api/trace/nope").status_code == 404


def test_uvicorn_can_serve_websockets():
    import importlib.util

    assert importlib.util.find_spec("websockets") is not None  # without it uvicorn answers every WS with 404


def test_public_reads_share_one_per_ip_limit_and_get_429():
    from test_rides import service

    s, hub = svc()
    hub.publish(cp(TID, "submitted", 1))
    rides, _, _ = service()
    c = TestClient(create_app(Deps(cfg=make_config(TRACE_READ_RATE_LIMIT_PER_MIN="2"), db=FakeDb(),
                                   sim_clock=FakeRideClock(), rides=rides, trace=s)))
    est = c.get("/api/rides/estimate", params={"pickup_location_id": 230, "dropoff_location_id": 132})
    assert est.status_code == 200
    assert c.get(f"/api/trace/{TID}").status_code == 200
    r = c.get(f"/api/trace/{TID}")  # the third read from this IP within the minute
    assert r.status_code == 429 and int(r.headers["Retry-After"]) > 0
    assert r.json() == {"detail": "Too many requests, try again in a few seconds",
                        "retry_after_s": int(r.headers["Retry-After"])}
    assert c.get("/api/rides/estimate", params={"pickup_location_id": 230, "dropoff_location_id": 132}).status_code == 429


def test_websocket_over_the_read_limit_is_closed_with_1013():
    s, _ = svc(follow_max_s=0.05)
    c = client(s, TRACE_READ_RATE_LIMIT_PER_MIN="1")
    with c.websocket_connect(f"/api/trace/{TID}") as ws:
        assert ws.receive_json()["type"] == "history"
    for path in (f"/api/trace/{TID}", "/api/trace/live"):
        with pytest.raises(WebSocketDisconnect) as e:
            with c.websocket_connect(path) as ws:
                ws.receive_json()
        assert e.value.code == 1013  # "try again later": the web client reconnects with a back-off
