import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from conftest import FakeTraceDb
from contract_validator import assert_valid

from api.db import DbUnavailable, Result
from rides.queryable import QueryableDetector

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"


def stored(tid: str = TID, source: str = "student") -> dict:
    return {"trace_id": tid, "trip_id": f"s-{tid}", "source": source, "stage": "stored", "seq": 6,
            "service": "processor", "at": "2026-10-16T10:31:08.000Z", "detail": {"table": "taxi.trips", "batch_rows": 9}}


def visible(*tids: str) -> Result:
    return Result([("trace_id", "Nullable(UUID)")], [[t] for t in tids], 1)


def detector(db: FakeTraceDb, **kw) -> tuple[QueryableDetector, list[dict], list[float]]:
    emitted: list[dict] = []
    t = [0.0]
    d = QueryableDetector(db, emitted.append, 250, clock=lambda: t[0],
                          wall=lambda: datetime(2026, 10, 16, 10, 31, 8, 590_000, tzinfo=UTC), **kw)
    return d, emitted, t


@pytest.mark.anyio
async def test_emits_queryable_when_the_row_is_visible():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, emitted, _ = detector(db)
    d.on_stored(stored())
    await d.tick()
    assert emitted == [] and d.pending == 1
    db.results["FROM taxi.trips"] = visible(TID)
    await d.tick()
    assert d.pending == 0
    (q,) = emitted
    assert_valid("trace_checkpoint", q)
    assert (q["stage"], q["seq"], q["service"], q["source"]) == ("queryable", 7, "api", "student")
    assert q["detail"] == {"table": "taxi.trips", "visible_after_ms": 590}
    sql, params = db.calls[-1]
    assert "{ids:Array(UUID)}" in sql and params == {"ids": [TID]}


@pytest.mark.anyio
async def test_one_query_polls_every_pending_trace():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, _, _ = detector(db)
    for i in range(3):
        d.on_stored(stored(f"00000000-0000-0000-0000-00000000000{i}"))
    await d.tick()
    assert len(db.calls) == 1 and len(db.calls[0][1]["ids"]) == 3


@pytest.mark.anyio
async def test_times_out_after_ten_seconds():
    d, emitted, t = detector(FakeTraceDb({"FROM taxi.trips": visible()}))
    d.on_stored(stored())
    t[0] = 10.0
    await d.tick()
    assert d.pending == 0 and emitted == []


@pytest.mark.anyio
async def test_at_most_twenty_in_flight_the_rest_wait():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, _, _ = detector(db)
    ids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(25)]
    for tid in ids:
        d.on_stored(stored(tid))
    assert d.pending == 20
    db.results["FROM taxi.trips"] = visible(*ids[:20])
    await d.tick()
    assert d.pending == 5  # the waiting five were promoted


@pytest.mark.anyio
async def test_database_down_does_not_kill_the_poller():
    db = FakeTraceDb()
    d, emitted, _ = detector(db)
    d.on_stored(stored())
    db.fail = DbUnavailable("down")
    await d.safe_tick()  # logged, not raised
    assert d.pending == 1 and emitted == []


@pytest.mark.anyio
async def test_times_out_while_the_database_is_down():
    db = FakeTraceDb()
    d, emitted, t = detector(db)
    d.on_stored(stored())
    db.fail = DbUnavailable("down")
    t[0] = 10.0
    await d.safe_tick()
    assert d.pending == 0 and emitted == []


@pytest.mark.anyio
async def test_waiting_traces_are_promoted_while_the_database_is_down():
    db = FakeTraceDb()
    d, _, t = detector(db)
    for i in range(25):
        d.on_stored(stored(f"00000000-0000-0000-0000-{i:012d}"))
    db.fail = DbUnavailable("down")
    t[0] = 10.0
    await d.safe_tick()
    assert d.pending == 5


class BrokenOnce(FakeTraceDb):
    """The first query raises something nobody expected; after that it answers normally."""

    def __init__(self, results: dict[str, Result]) -> None:
        super().__init__(results)
        self.raised = 0

    async def query(self, sql: str, params: dict[str, Any]) -> Result:
        if not self.raised:
            self.raised += 1
            raise RuntimeError("surprise")
        return await super().query(sql, params)


@pytest.mark.anyio
async def test_an_unexpected_error_does_not_end_the_loop():
    db = BrokenOnce({"FROM taxi.trips": visible(TID)})
    emitted: list[dict] = []
    d = QueryableDetector(db, emitted.append, 1)
    d.on_stored(stored())
    task = asyncio.create_task(d.run())
    try:
        for _ in range(200):
            if emitted:
                break
            await asyncio.sleep(0.005)
        assert not task.done()  # still polling
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert db.raised == 1 and [c["stage"] for c in emitted] == ["queryable"]
