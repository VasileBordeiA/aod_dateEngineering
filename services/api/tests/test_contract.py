import json

from conftest import GOLDEN, FakeDb, make_config
from fake_llm import FakeLLM, say, tool
from fastapi.testclient import TestClient

from agent.catalog import Catalog
from agent.golden import load_golden
from agent.providers import ProviderPool
from agent.service import AgentService
from api.db import Result
from api.main import Deps, create_app

TIPS = Result([("borough", "String"), ("avg_tip_pct", "Float64")], [["Manhattan", 23.1], ["Brooklyn", 19.4]], 84)
KEYS = {
    "status": {"text"},
    "tool_call": {"id", "name", "arguments"},
    "tool_result": {"id", "name", "ok", "summary"},
    "sql": {"attempt", "sql", "purpose"},
    "rows": {"columns", "rows", "row_count", "truncated", "elapsed_ms"},
    "chart": {"type", "x", "y", "title", "unit"},
    "answer": {"text"},
    "done": {"request_id", "provider", "model", "mode", "cached", "latency_ms", "tool_calls", "invalid_tool_calls",
             "repairs"},
}


class Clock:
    latest = None


def test_sse_event_sequence_and_shapes():
    cfg = make_config()
    fake = FakeLLM([tool("list_tables"), tool("describe_table", {"table": "trips"}),
                    tool("run_query", {"sql": "SELECT pu_borough AS borough, avg(tip_pct) AS avg_tip_pct FROM trips GROUP BY borough",
                                       "purpose": "Average card tip per borough"}),
                    tool("render_chart", {"type": "bar", "x": "borough", "y": ["avg_tip_pct"], "title": "Tips", "unit": "pct"}),
                    say("Manhattan riders tip best: 23.1%. Only card tips are recorded.")])
    pool = ProviderPool(cfg.providers, cfg.llm_order, lambda pc: fake.client())
    db = FakeDb({"system.tables": Result([("name", "String"), ("comment", "String")], [["trips", "Trips."]], 1),
                 "DESCRIBE TABLE": Result([("name", "String"), ("type", "String"), ("comment", "String")],
                                          [["tip_pct", "Float32", "Card only."]], 1),
                 "LIMIT 3": Result([("tip_pct", "Float32")], [[1.0]], 1), "SELECT * FROM (": TIPS})
    golden = tuple(load_golden(GOLDEN))
    agent = AgentService(cfg, db, pool, Catalog(db), list(golden))
    client = TestClient(create_app(Deps(cfg=cfg, db=db, sim_clock=Clock(), pool=pool, agent=agent, golden=golden)))
    with client.stream("POST", "/api/agent/ask", json={"question": "Which borough tips best?", "origin": "typed"}) as r:
        assert r.headers["content-type"].startswith("text/event-stream")
        body = "".join(r.iter_text())
    events = [(f.split("\n")[0].removeprefix("event: "), json.loads(f.split("\n")[1].removeprefix("data: ")))
              for f in body.split("\n\n") if f.strip()]
    assert [e for e, _ in events] == ["status", "tool_call", "tool_result", "tool_call", "tool_result", "tool_call",
                                      "sql", "rows", "tool_result", "tool_call", "chart", "tool_result", "answer", "done"]
    for event, data in events:
        assert set(data) == KEYS[event], event
