import json

import pytest
from conftest import FakeDb

from agent.catalog import Catalog
from agent.tools import TOOL_DEFS, Toolbox
from api.db import DbError, Result

pytestmark = pytest.mark.anyio

TIPS = Result([("borough", "String"), ("avg_tip_pct", "Float64")], [["Manhattan", 23.1], ["Brooklyn", 19.4]], 84)
TABLES = Result([("name", "String"), ("comment", "String")], [["trips", "Trips."]], 1)


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    async def __call__(self, event: str, data: dict) -> None:
        self.events.append((event, data))


def toolbox(db: FakeDb | None = None) -> tuple[Toolbox, Recorder]:
    db = db or FakeDb({"SELECT * FROM (": TIPS})
    return Toolbox(db, Catalog(db)), Recorder()


def test_tool_defs_are_the_spec_four():
    assert [t["function"]["name"] for t in TOOL_DEFS] == ["list_tables", "describe_table", "run_query", "render_chart"]


async def test_run_query_success_emits_sql_then_rows():
    tb, rec = toolbox()
    out = await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips", "purpose": "Tips"}), rec)
    assert out.ok and out.summary == "2 rows in 84 ms" and not out.query_failed
    assert [e for e, _ in rec.events] == ["sql", "rows"]
    assert rec.events[0][1] == {"attempt": 1, "sql": "SELECT 1 FROM trips", "purpose": "Tips"}
    rows = rec.events[1][1]
    assert rows["columns"] == [{"name": "borough", "type": "String"}, {"name": "avg_tip_pct", "type": "Float64"}]
    assert rows["row_count"] == 2 and rows["truncated"] is False and rows["elapsed_ms"] == 84
    assert json.loads(out.content)["rows"] == [["Manhattan", 23.1], ["Brooklyn", 19.4]]
    assert tb.last_result is TIPS


async def test_run_query_executes_the_guarded_sql():
    db = FakeDb({"SELECT * FROM (": TIPS})
    tb, rec = Toolbox(db, Catalog(db)), Recorder()
    await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips;", "purpose": "x"}), rec)
    assert db.queries == ["SELECT * FROM (\nSELECT 1 FROM trips\n) LIMIT 200"]


async def test_guard_rejection_is_a_failed_query_with_the_sql_shown():
    tb, rec = toolbox()
    out = await tb.call("run_query", json.dumps({"sql": "SELECT * FROM system.users", "purpose": "x"}), rec)
    assert not out.ok and out.query_failed and "system.users" in json.loads(out.content)["error"]
    assert [e for e, _ in rec.events] == ["sql"]


async def test_clickhouse_error_goes_back_to_the_model():
    db = FakeDb()
    db.fail = DbError("Code: 47. DB::Exception: Unknown expression identifier `tip`")
    tb, rec = Toolbox(db, Catalog(db)), Recorder()
    out = await tb.call("run_query", json.dumps({"sql": "SELECT tip FROM trips", "purpose": "x"}), rec)
    assert out.query_failed and "Unknown expression identifier" in json.loads(out.content)["error"]
    assert tb.attempts == 1


async def test_run_query_nan_becomes_null():
    nan_result = Result([("avg", "Float64")], [[None]], 3)  # ClickHouseDb.jsonable already mapped NaN → None
    tb, rec = toolbox(FakeDb({"SELECT * FROM (": nan_result}))
    out = await tb.call("run_query", json.dumps({"sql": "SELECT avg(x) FROM trips", "purpose": "x"}), rec)
    assert json.loads(out.content)["rows"] == [[None]]
    json.dumps(rec.events, allow_nan=False)


async def test_truncated_when_safety_limit_hit():
    big = Result([("n", "UInt32")], [[i] for i in range(200)], 5)
    tb, rec = toolbox(FakeDb({"SELECT * FROM (": big}))
    out = await tb.call("run_query", json.dumps({"sql": "SELECT n FROM trips", "purpose": "x"}), rec)
    rows = rec.events[1][1]
    assert rows["truncated"] is True and rows["row_count"] == 200 and len(rows["rows"]) == 50
    assert len(json.loads(out.content)["rows"]) == 50


async def test_table_chart_may_list_more_than_three_columns():
    # The UI draws no chart for a table (the result table shows every column), so the 3-column cap is for bar/line.
    wide = Result([(c, "Float64") for c in ("trip_type", "trips", "avg_fare", "avg_total", "avg_miles")],
                  [["Airport", 12575, 59.26, 81.8, 13.02]], 38)
    tb, rec = toolbox(FakeDb({"SELECT * FROM (": wide}))
    await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips", "purpose": "x"}), rec)
    y = ["trips", "avg_fare", "avg_total", "avg_miles"]
    bar = await tb.call("render_chart", json.dumps({"type": "bar", "x": "trip_type", "y": y, "title": "t"}), rec)
    assert not bar.ok and "1 to 3" in json.loads(bar.content)["error"]
    table = await tb.call("render_chart", json.dumps({"type": "table", "x": "trip_type", "y": y, "title": "t"}), rec)
    assert table.ok and rec.events[-1][1]["y"] == y
    tb, rec = toolbox(FakeDb({"SELECT * FROM (": wide}))
    await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips", "purpose": "x"}), rec)
    missing = await tb.call("render_chart", json.dumps({"type": "table", "x": "trip_type", "y": ["nope"], "title": "t"}),
                            rec)
    assert not missing.ok and "nope" in json.loads(missing.content)["error"]


async def test_render_chart_validates_columns_and_runs_once():
    tb, rec = toolbox()
    no_result = await tb.call(
        "render_chart", json.dumps({"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "t"}), rec
    )
    assert not no_result.ok
    await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips", "purpose": "x"}), rec)
    bad = await tb.call(
        "render_chart", json.dumps({"type": "bar", "x": "zone", "y": ["avg_tip_pct"], "title": "t"}), rec
    )
    assert not bad.ok and "zone" in json.loads(bad.content)["error"] and not bad.invalid
    good = await tb.call(
        "render_chart",
        json.dumps({"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "Average card tip", "unit": "pct"}),
        rec,
    )
    assert good.ok and rec.events[-1] == (
        "chart",
        {"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "Average card tip", "unit": "pct"},
    )
    again = await tb.call(
        "render_chart", json.dumps({"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "t"}), rec
    )
    assert not again.ok


async def test_odd_arguments_are_normalised():
    tb, rec = toolbox(
        FakeDb(
            {
                "system.tables": TABLES,
                "DESCRIBE TABLE": Result([("name", "String"), ("type", "String"), ("comment", "String")], [], 1),
                "SELECT * FROM (": TIPS,
            }
        )
    )
    out = await tb.call("describe_table", json.dumps({"table": "taxi.trips"}), rec)
    assert out.ok
    await tb.call("run_query", json.dumps({"sql": "SELECT 1 FROM trips", "purpose": "x"}), rec)
    chart = await tb.call(
        "render_chart",
        json.dumps({"type": "bar", "x": "borough", "y": "avg_tip_pct", "title": "t", "unit": "percent"}),
        rec,
    )
    assert chart.ok and rec.events[-1][1]["y"] == ["avg_tip_pct"] and rec.events[-1][1]["unit"] == "none"
    empty = await tb.call("list_tables", "", rec)
    assert empty.ok


async def test_odd_arguments_that_are_invalid():
    tb, rec = toolbox()
    for name, raw in [
        ("run_query", "{not json"),
        ("run_query", "[1, 2]"),
        ("describe_table", '{"table": "agent_log"}'),
        ("drop_everything", "{}"),
        ("run_query", '{"sql": ""}'),
    ]:
        out = await tb.call(name, raw, rec)
        assert not out.ok and out.invalid, (name, raw)
        assert "error" in json.loads(out.content)
