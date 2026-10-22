import json

import pytest
from conftest import FakeDb
from fake_llm import FakeLLM, say

from agent.catalog import Catalog
from agent.loop import NO_WORKING_QUERY, ProviderFailed
from agent.oneshot import ONESHOT_STATUS, run_oneshot
from agent.providers import LLM
from agent.tools import Toolbox
from api.db import Result

pytestmark = pytest.mark.anyio

TIPS = Result([("borough", "String"), ("avg_tip_pct", "Float64")], [["Manhattan", 23.1]], 40)
PLAN = {"sql": "SELECT pu_borough AS borough, avg(tip_pct) AS avg_tip_pct FROM taxi.trips GROUP BY borough",
        "purpose": "Tips per borough", "chart": {"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "Tips"}}


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    async def __call__(self, event: str, data: dict) -> None:
        self.events.append((event, data))


async def run(script, json_mode=True, ride_line=None):
    fake = FakeLLM(script)
    db = FakeDb({"system.tables": Result([("name", "String"), ("comment", "String")], [["trips", "Trips."]], 1),
                 "DESCRIBE TABLE": Result([("name", "String"), ("type", "String"), ("comment", "String")],
                                          [["tip_pct", "Float32", "Card only."]], 1),
                 "LIMIT 3": Result([("tip_pct", "Float32")], [[1.0]], 1), "SELECT * FROM (": TIPS})
    rec = Recorder()
    result = await run_oneshot(LLM("local", "fake", fake.client(), json_mode), "Which borough tips best?", ride_line,
                               Toolbox(db, Catalog(db)), Catalog(db), rec, temperature=0.0)
    return result, rec, fake


async def test_happy_path():
    result, rec, fake = await run([say(json.dumps(PLAN)), say("Manhattan tips best at 23.1%.")])
    assert [e for e, _ in rec.events] == ["status", "tool_call", "sql", "rows", "tool_result",
                                          "tool_call", "chart", "tool_result"]
    assert rec.events[0][1] == {"text": ONESHOT_STATUS}
    assert rec.events[1][1] == {"id": "oneshot_1", "name": "run_query",
                                "arguments": {"sql": PLAN["sql"], "purpose": "Tips per borough"}}
    assert rec.events[4][1] == {"id": "oneshot_1", "name": "run_query", "ok": True, "summary": "1 rows in 40 ms"}
    assert rec.events[5][1]["id"] == rec.events[7][1]["id"] == "oneshot_2"
    assert rec.events[7][1]["name"] == "render_chart" and rec.events[7][1]["ok"] is True
    assert result.answer == "Manhattan tips best at 23.1%." and result.tool_calls == 0
    first = fake.requests[0]
    assert first["response_format"] == {"type": "json_object"} and "tools" not in first
    assert "taxi.trips" in first["messages"][0]["content"] and "Card only." in first["messages"][0]["content"]
    assert "Manhattan" in fake.requests[1]["messages"][-1]["content"]


async def test_json_mode_off_sends_no_response_format():
    # Some gateways corrupt JSON mode on reasoning models ('{"{"sql": …'); the prompt alone asks for JSON.
    result, _, fake = await run([say("\n\n" + json.dumps(PLAN)), say("Manhattan tips best.")], json_mode=False)
    assert result.answer == "Manhattan tips best."
    assert "response_format" not in fake.requests[0]


async def test_ride_line_joins_the_leading_system_message():
    _, _, fake = await run([say(json.dumps(PLAN)), say("Your ride tips above average.")], ride_line="RIDE LINE")
    msgs = fake.requests[0]["messages"]
    assert [m["role"] for m in msgs] == ["system", "user"]
    assert "taxi.trips" in msgs[0]["content"] and msgs[0]["content"].endswith("\n\nRIDE LINE")


async def test_off_topic_answer_without_sql():
    result, rec, _ = await run([say(json.dumps({"answer": "I only know taxi data!"}))])
    assert result.answer == "I only know taxi data!" and [e for e, _ in rec.events] == ["status"]


async def test_one_repair_then_no_working_query():
    bad = say(json.dumps({"sql": "SELECT * FROM system.users", "purpose": "x"}))
    result, rec, fake = await run([bad, bad])
    assert result.error == NO_WORKING_QUERY and len(fake.requests) == 2
    assert "failed" in fake.requests[1]["messages"][-1]["content"]
    assert [e for e, _ in rec.events] == ["status", "tool_call", "sql", "tool_result", "status",
                                          "tool_call", "sql", "tool_result"]
    results = [d for e, d in rec.events if e == "tool_result"]
    assert [(d["id"], d["name"], d["ok"]) for d in results] == [("oneshot_1", "run_query", False),
                                                                ("oneshot_2", "run_query", False)]
    assert all("system.users" in d["summary"] for d in results) and result.tool_calls == 0


async def test_repair_succeeds():
    result, _, _ = await run([say(json.dumps({"sql": "SELECT * FROM system.users", "purpose": "x"})),
                              say(json.dumps(PLAN)), say("Manhattan.")])
    assert result.answer == "Manhattan." and result.repairs == 1


async def test_json_in_code_fences_is_accepted():
    result, _, _ = await run([say("```json\n" + json.dumps(PLAN) + "\n```"), say("ok")])
    assert result.answer == "ok"


async def test_not_json_is_a_provider_failure():
    with pytest.raises(ProviderFailed):
        await run([say("SELECT 1")])


async def test_bad_chart_is_skipped():
    plan = {**PLAN, "chart": {"type": "pie", "x": "nope", "y": ["x"], "title": "t"}}
    result, rec, _ = await run([say(json.dumps(plan)), say("ok")])
    assert "chart" not in [e for e, _ in rec.events] and result.answer == "ok"
    assert rec.events[-1] == ("tool_result", {"id": "oneshot_2", "name": "render_chart", "ok": False,
                                              "summary": "bad chart spec"})
