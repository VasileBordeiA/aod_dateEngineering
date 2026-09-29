import json

import pytest
from conftest import FakeDb
from fake_llm import FakeLLM, http, raw, say, slow, tool, tools

from agent.catalog import Catalog
from agent.loop import FORCED_ANSWER_PROMPT, NO_WORKING_QUERY, ProviderFailed, clean_answer, run_tools
from agent.prompt import build_messages
from agent.providers import LLM
from agent.tools import Toolbox
from api.db import Result

pytestmark = pytest.mark.anyio

TABLES = Result([("name", "String"), ("comment", "String")], [["trips", "Trips."]], 1)
DESCRIBE = Result([("name", "String"), ("type", "String"), ("comment", "String")],
                  [["tip_pct", "Nullable(Float32)", "Only for card trips."]], 1)
TIPS = Result([("borough", "String"), ("avg_tip_pct", "Float64")], [["Manhattan", 23.1], ["Brooklyn", 19.4]], 84)
GOOD_SQL = {"sql": "SELECT pu_borough AS borough, avg(tip_pct) AS avg_tip_pct FROM trips WHERE is_card GROUP BY borough",
            "purpose": "Average card tip per borough"}
CHART = {"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "Average card tip", "unit": "pct"}


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    async def __call__(self, event: str, data: dict) -> None:
        self.events.append((event, data))

    @property
    def names(self) -> list[str]:
        return [e for e, _ in self.events]


def db() -> FakeDb:
    return FakeDb({"system.tables": TABLES, "DESCRIBE TABLE": DESCRIBE, "LIMIT 3": Result([("tip_pct", "Float32")], [[1.0]], 1),
                   "SELECT * FROM (": TIPS})


async def run(script, *, max_iterations=6, max_repairs=2, timeout=5.0):
    fake = FakeLLM(script)
    d = db()
    rec = Recorder()
    result = await run_tools(LLM("local", "fake", fake.client(timeout)), build_messages("Which borough tips best?", None),
                             Toolbox(d, Catalog(d)), rec, max_iterations=max_iterations, max_repairs=max_repairs,
                             temperature=0.0)
    return result, rec, fake


async def test_happy_path_event_order():
    result, rec, fake = await run([tool("list_tables"), tool("describe_table", {"table": "trips"}),
                                   tool("run_query", GOOD_SQL), tool("render_chart", CHART),
                                   say("Manhattan riders tip best: 23.1%. Only card tips are recorded.")])
    assert rec.names == ["tool_call", "tool_result", "tool_call", "tool_result", "tool_call", "sql", "rows",
                         "tool_result", "tool_call", "chart", "tool_result"]
    assert result.answer == "Manhattan riders tip best: 23.1%. Only card tips are recorded."
    assert (result.iterations, result.tool_calls, result.repairs) == (5, 4, 0)
    assert rec.events[2][1] == {"id": rec.events[2][1]["id"], "name": "describe_table", "arguments": {"table": "trips"}}
    assert rec.events[1][1]["ok"] is True and rec.events[1][1]["summary"] == "6 tables"
    second = fake.requests[1]
    assert second["tools"][0]["function"]["name"] == "list_tables" and second["tool_choice"] == "auto"
    assert second["messages"][-1]["role"] == "tool" and second["messages"][-2]["role"] == "assistant"


async def test_repair_then_success():
    result, rec, _ = await run([tool("run_query", {"sql": "SELECT * FROM system.users", "purpose": "x"}),
                                tool("run_query", GOOD_SQL), say("Manhattan.")])
    assert result.answer == "Manhattan." and result.repairs == 1
    assert ("status", {"text": "Fixing its own query…"}) in rec.events
    assert [d["attempt"] for e, d in rec.events if e == "sql"] == [1, 2]


async def test_too_many_repairs_is_no_working_query():
    bad = tool("run_query", {"sql": "SELECT * FROM system.users", "purpose": "x"})
    result, _rec, _ = await run([bad, bad, bad, say("never reached")])
    assert result.error == NO_WORKING_QUERY and result.answer is None and result.repairs == 3


async def test_two_invalid_calls_fail_over():
    with pytest.raises(ProviderFailed, match="invalid"):
        await run([tool("run_query", "{broken"), tool("drop_everything", {})])


async def test_invalid_then_valid_resets_the_counter():
    result, _, _ = await run([tool("run_query", "{broken"), tool("list_tables"), tool("nope", {}), say("ok")])
    assert result.answer == "ok" and result.invalid_tool_calls == 2


async def test_think_tags_are_stripped():
    result, _, _ = await run([say("<think>They want tips.\nLet me think.</think>\nManhattan tips best.")])
    assert result.answer == "Manhattan tips best."
    assert clean_answer("<think>x</think>") == ""


@pytest.mark.parametrize("reply", ["", None, "   ", "<think>only thoughts</think>"])
async def test_empty_reply_is_a_provider_failure(reply):
    with pytest.raises(ProviderFailed):
        await run([say(reply)])


@pytest.mark.parametrize("status", [500, 503, 429, 400])
async def test_http_errors_are_provider_failures(status):
    with pytest.raises(ProviderFailed):
        await run([http(status)])


async def test_timeout_is_a_provider_failure():
    with pytest.raises(ProviderFailed):
        await run([slow(1.0, say("late"))], timeout=0.2)


async def test_non_function_tool_call_fails_over():
    custom_call_body = json.dumps({
        "id": "chatcmpl-fake", "object": "chat.completion", "created": 0, "model": "fake",
        "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
            "role": "assistant", "content": None,
            "tool_calls": [{"id": "call_1", "type": "custom", "custom": {"name": "foo", "input": "bar"}}]}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    })
    with pytest.raises(ProviderFailed, match="non-function"):
        await run([raw(custom_call_body)])


async def test_invalid_json_body_is_a_provider_failure():
    with pytest.raises(ProviderFailed):
        await run([raw("this is not json")])


async def test_iteration_cap_forces_an_answer():
    result, _, fake = await run([tool("list_tables")] * 3 + [say("Final answer.")], max_iterations=3)
    assert result.answer == "Final answer." and result.iterations == 3
    last = fake.requests[-1]
    assert last["tool_choice"] == "none" and last["messages"][-1] == {"role": "user", "content": FORCED_ANSWER_PROMPT}


async def test_refusal_without_tools():
    result, rec, _ = await run([say("I only know about NYC taxi data. Try asking which borough tips best!")])
    assert result.answer.startswith("I only know") and result.tool_calls == 0 and rec.events == []


async def test_several_tool_calls_in_one_message():
    result, rec, _ = await run([tools(tool("list_tables"), tool("describe_table", {"table": "trips"})), say("ok")])
    assert rec.names == ["tool_call", "tool_result", "tool_call", "tool_result"] and result.tool_calls == 2


async def test_unparseable_arguments_are_shown_raw():
    _, rec, _ = await run([tool("run_query", "{broken"), say("ok")])
    assert rec.events[0][1]["arguments"] == {"raw": "{broken"}
    assert json.dumps(rec.events)
