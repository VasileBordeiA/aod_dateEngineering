import pytest
from conftest import FakeDb

from agent.catalog import AGENT_TABLES, Catalog
from api.db import Result

pytestmark = pytest.mark.anyio

TABLES = Result(
    [("name", "String"), ("comment", "String")], [["trips", "One row per valid trip."], ["zones", "The 265 zones."]], 1
)
DESCRIBE = Result(
    [
        ("name", "String"),
        ("type", "String"),
        ("default_type", "String"),
        ("default_expression", "String"),
        ("comment", "String"),
    ],
    [["tip_pct", "Nullable(Float32)", "", "", "Only set for card trips."]],
    1,
)
SAMPLE = Result([("tip_pct", "Nullable(Float32)")], [[12.5], [None], [20.0]], 1)


def fake() -> FakeDb:
    return FakeDb({"system.tables": TABLES, "DESCRIBE TABLE": DESCRIBE, "LIMIT 3": SAMPLE})


async def test_tables_are_the_whitelist_with_comments():
    tables = await Catalog(fake()).tables()
    assert [t["name"] for t in tables] == list(AGENT_TABLES)
    assert tables[0] == {"name": "trips", "description": "One row per valid trip."}
    assert all("agent_log" not in t["name"] for t in tables)


async def test_describe_includes_comments_and_samples_and_is_cached():
    db = fake()
    cat = Catalog(db)
    info = await cat.describe("trips")
    assert info["columns"] == [{"name": "tip_pct", "type": "Nullable(Float32)", "comment": "Only set for card trips."}]
    assert info["sample_rows"] == {"columns": ["tip_pct"], "rows": [[12.5], [None], [20.0]]}
    n = len(db.queries)
    await cat.describe("trips")
    assert len(db.queries) == n


async def test_describe_rejects_other_tables():
    with pytest.raises(ValueError):
        await Catalog(fake()).describe("agent_log")


async def test_schema_text_for_oneshot():
    text = await Catalog(fake()).schema_text(("trips",))
    assert "taxi.trips" in text and "tip_pct Nullable(Float32) -- Only set for card trips." in text
