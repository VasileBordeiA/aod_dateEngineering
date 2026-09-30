import json
import math
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from conftest import make_config

from api.db import ClickHouseDb, ClickHouseTraceDb, DbError, DbUnavailable, Result, clickhouse_message, jsonable


def test_jsonable_scalars_and_nan():
    assert jsonable(None) is None and jsonable(True) is True and jsonable(3) == 3 and jsonable("x") == "x"
    assert jsonable(1.5) == 1.5
    assert jsonable(math.nan) is None and jsonable(math.inf) is None


def test_jsonable_clickhouse_types():
    assert jsonable(Decimal("1.50")) == 1.5
    assert jsonable(datetime(2025, 10, 7, 7, 0, 0)) == "2025-10-07T07:00:00"
    assert jsonable(datetime(2025, 10, 7, 7, 0, 0, 250000)) == "2025-10-07T07:00:00.250"
    assert jsonable(datetime(2026, 9, 28, 12, 0, tzinfo=UTC)) == "2026-09-28T12:00:00+00:00"
    assert jsonable(date(2025, 10, 1)) == "2025-10-01"
    assert jsonable(UUID("00000000-0000-0000-0000-000000000001")) == "00000000-0000-0000-0000-000000000001"
    assert jsonable(["a", Decimal("2"), (math.nan,)]) == ["a", 2.0, [None]]
    json.dumps(jsonable({"k": [math.nan, Decimal("1")]}), allow_nan=False)  # must not raise


def test_clickhouse_message_keeps_the_server_line():
    exc = DatabaseError("Received ClickHouse exception, code: 47, server response: Code: 47. DB::Exception: "
                        "Unknown expression identifier `nope` in scope SELECT nope\nStack trace: …")
    assert clickhouse_message(exc) == "Code: 47. DB::Exception: Unknown expression identifier `nope` in scope SELECT nope"
    assert len(clickhouse_message(DatabaseError("x" * 1000))) == 300


def test_result_names():
    assert Result([("a", "UInt8"), ("b", "String")], [], 0).names == ["a", "b"]


class _Boom:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def query(self, sql: str, parameters: object = None) -> None:
        raise self.exc


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def query(self, sql: str, parameters: object = None) -> SimpleNamespace:
        self.calls.append((sql, parameters))
        return SimpleNamespace(column_names=("n",), column_types=(SimpleNamespace(name="UInt64"),), result_rows=[(2,)])


@pytest.mark.anyio
async def test_database_error_becomes_db_error():
    db = ClickHouseDb(make_config())
    db._agent = _Boom(DatabaseError("server response: Code: 497. DB::Exception: agent_ro: Not enough privileges"))
    with pytest.raises(DbError, match="Not enough privileges"):
        await db.select("SELECT * FROM taxi.agent_log")


@pytest.mark.anyio
async def test_operational_error_resets_client():
    db = ClickHouseDb(make_config())
    db._agent = _Boom(OperationalError("HTTPConnectionPool: Max retries exceeded"))
    with pytest.raises(DbUnavailable):
        await db.select("SELECT 1")
    assert db._agent is None  # the next call reconnects instead of reusing a dead connection


@pytest.mark.anyio
async def test_insert_agent_log_never_raises():
    db = ClickHouseDb(make_config())
    db._ingest = _Boom(OperationalError("down"))
    await db.insert_agent_log({"question": "x"})  # logged, not raised


@pytest.mark.anyio
async def test_trace_db_sends_server_side_parameters():
    db = ClickHouseTraceDb(make_config())
    db._client = _Recorder()
    r = await db.query("SELECT count() AS n FROM taxi.trips WHERE trace_id = {id:UUID}", {"id": "00000000-0000-0000-0000-000000000001"})
    assert r.rows == [[2]] and r.names == ["n"]
    assert db._client.calls == [("SELECT count() AS n FROM taxi.trips WHERE trace_id = {id:UUID}",
                                 {"id": "00000000-0000-0000-0000-000000000001"})]


@pytest.mark.anyio
async def test_trace_db_operational_error_resets_client():
    db = ClickHouseTraceDb(make_config())
    db._client = _Boom(OperationalError("down"))
    with pytest.raises(DbUnavailable):
        await db.query("SELECT 1", {})
    assert db._client is None
