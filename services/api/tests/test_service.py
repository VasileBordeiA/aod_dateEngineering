import asyncio
import json
from typing import Any

import pytest
from conftest import GOLDEN, FakeDb, make_config
from fake_llm import FakeLLM, http, raw, say, slow, tool

import agent.service as service_module
from agent.cache import CACHE_STATUS, TIMEOUT_CACHE_STATUS
from agent.catalog import Catalog
from agent.golden import load_golden
from agent.providers import ProviderPool
from agent.service import AgentService, AskRequest
from api.db import DbUnavailable, Result

pytestmark = pytest.mark.anyio

TIPS = Result([("borough", "String"), ("avg_tip_pct", "Float64"), ("card_trips", "UInt64")],
              [["Manhattan", 23.1, 5000], ["Queens", 20.7, 900]], 40)
GOOD = {"sql": "SELECT pu_borough AS borough, avg(tip_pct) AS avg_tip_pct FROM trips GROUP BY borough", "purpose": "Tips"}


def parse(frames: list[bytes]) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for frame in b"".join(frames).decode().split("\n\n"):
        if frame.strip():
            head, data = frame.split("\n", 1)
            out.append((head.removeprefix("event: "), json.loads(data.removeprefix("data: "))))
    return out


def make(scripts: dict[str, list] | None = None, db: FakeDb | None = None, **env: str):
    cfg = make_config(LLM_INTERNAL_BASE_URL="http://fake-gw/v1", LLM_INTERNAL_MODEL="gw-model", **env)
    fakes = {name: FakeLLM(script) for name, script in (scripts or {"local": [], "internal": []}).items()}
    for name in ("local", "internal"):
        fakes.setdefault(name, FakeLLM())
    pool = ProviderPool(cfg.providers, cfg.llm_order, lambda pc: fakes[pc.name].client(timeout=cfg.llm_timeout_s))
    db = db or FakeDb({"system.tables": Result([("name", "String"), ("comment", "String")], [], 1),
                       "trace_id": Result([("n", "UInt64")], [[1]], 1),
                       "SELECT * FROM (": TIPS})
    catalog = Catalog(db)
    return AgentService(cfg, db, pool, catalog, load_golden(GOLDEN)), pool, fakes, db


async def ask(svc: AgentService, question: str = "Which borough tips best?", ip: str = "10.0.0.1", **kw: Any):
    return parse([chunk async for chunk in svc.ask(AskRequest(question=question, **kw), ip)])


def names(events):
    return [e for e, _ in events]


async def settle() -> None:
    await asyncio.sleep(0.05)  # let the fire-and-forget agent_log insert run


async def test_answer_and_done_and_log_row():
    svc, _, _, db = make({"local": [tool("run_query", GOOD), say("Manhattan riders tip best: 23.1%.")]})
    events = await ask(svc)
    assert names(events) == ["status", "tool_call", "sql", "rows", "tool_result", "answer", "done"]
    assert events[0][1] == {"text": "Reading the question…"}
    done = events[-1][1]
    assert done["provider"] == "local" and done["model"] == "fake-local" and done["mode"] == "tools"
    assert done["cached"] is False and done["tool_calls"] == 1 and done["repairs"] == 0 and done["latency_ms"] >= 0
    await settle()
    row = db.logged[0]
    assert row["question"] == "Which borough tips best?" and row["success"] is True and row["refused"] is False
    assert row["sql_attempts"] == [GOOD["sql"]] and row["final_sql"] == GOOD["sql"] and row["result_rows"] == 2
    assert row["origin"] == "typed" and row["suggestion_id"] == "" and row["error"] == ""
    assert "10.0.0.1" not in json.dumps(row, default=str)


async def test_refusal_is_logged_as_refused():
    svc, _, _, db = make({"local": [say("I only know taxi data. Try: which borough tips best?")]})
    events = await ask(svc, "Write me a poem about taxis.")
    assert names(events) == ["status", "answer", "done"]
    await settle()
    assert db.logged[0]["refused"] is True and db.logged[0]["success"] is True


async def test_too_long():
    svc, _, _, _ = make()
    events = await ask(svc, "x" * 301)
    assert names(events) == ["error", "done"] and events[0][1]["code"] == "TOO_LONG"


async def test_rate_limited_per_ip_with_retry_after():
    svc, _, _, _ = make({"local": [say("ok")] * 11})
    for _ in range(10):
        await ask(svc, ip="10.0.0.9")
    events = await ask(svc, ip="10.0.0.9")
    assert events[0][0] == "error" and events[0][1]["code"] == "RATE_LIMITED" and events[0][1]["retry_after_s"] > 0
    assert names(await ask(svc, ip="10.0.0.10"))[-2] == "answer"


async def test_failover_to_the_backup_provider():
    svc, pool, _, _ = make({"local": [http(500)], "internal": [say("From the gateway.")]})
    events = await ask(svc)
    assert ("status", {"text": "Switching to the backup model…"}) in events
    assert events[-2] == ("answer", {"text": "From the gateway."}) and events[-1][1]["provider"] == "internal"
    assert pool.status()["providers"][0]["healthy"] is True  # one failure isn't enough to mark it down


async def test_no_provider_typed_is_llm_unavailable():
    svc, pool, _, _ = make()
    pool.set_order([])
    events = await ask(svc)
    assert names(events) == ["status", "error", "done"] and events[1][1]["code"] == "LLM_UNAVAILABLE"


async def test_no_provider_suggested_uses_the_cache_on_live_data():
    svc, pool, _, db = make()
    pool.set_order([])
    events = await ask(svc, "Which borough has the best tippers?", origin="suggested", suggestion_id="best_tippers")
    assert names(events) == ["status", "status", "sql", "rows", "chart", "answer", "done"]
    assert events[1][1] == {"text": CACHE_STATUS}
    assert events[4][1]["type"] == "bar" and events[4][1]["y"] == ["avg_tip_pct"]
    assert events[5][1]["text"].startswith("Manhattan riders tip best: 23.1% of the fare on average.")
    done = events[-1][1]
    assert done["cached"] is True and done["mode"] == "cached" and done["provider"] == "cache"
    assert any(q.startswith("SELECT * FROM (\nSELECT pu_borough AS borough") for q in db.queries)  # guarded


async def test_timeout_typed_and_suggested():
    svc, _, _, _ = make({"local": [slow(2.0, say("late"))]}, AGENT_TOTAL_TIMEOUT_S="0.3")
    events = await ask(svc)
    assert events[-2][0] == "error" and events[-2][1]["code"] == "TIMEOUT"
    svc, _, _, _ = make({"local": [slow(2.0, say("late"))]}, AGENT_TOTAL_TIMEOUT_S="0.3")
    events = await ask(svc, "Which borough has the best tippers?", origin="suggested", suggestion_id="best_tippers")
    assert ("status", {"text": TIMEOUT_CACHE_STATUS}) in events and events[-2][0] == "answer"


async def test_database_down_is_internal_error():
    db = FakeDb()
    db.fail = DbUnavailable("down")
    svc, _, _, _ = make({"local": [tool("list_tables"), say("x")]}, db=db)
    events = await ask(svc)
    assert events[-2] == ("error", {"code": "INTERNAL", "message": "The database is restarting, try again in a moment."})
    assert events[-1][0] == "done"


async def test_ride_context_only_for_a_known_uuid():
    svc, _, fakes, _ = make({"local": [say("ok"), say("ok")]})
    await ask(svc, "Where does my ride rank?", origin="ride", trace_id="00000000-0000-0000-0000-000000000001")
    msgs = fakes["local"].requests[0]["messages"]
    assert [m["role"] for m in msgs] == ["system", "user"]
    assert "00000000-0000-0000-0000-000000000001" in msgs[0]["content"]
    await ask(svc, "Where does my ride rank?", origin="ride", trace_id="'; DROP TABLE taxi.trips; --")
    msgs = fakes["local"].requests[1]["messages"]
    assert [m["role"] for m in msgs] == ["system", "user"] and "DROP" not in msgs[0]["content"]


async def test_queue_status_while_waiting(monkeypatch):
    monkeypatch.setattr(service_module, "QUEUE_STATUS_EVERY_S", 0.05)
    svc, _, _, _ = make({"local": [slow(0.4, say("first")), say("second")]}, AGENT_MAX_CONCURRENCY="1")
    first, second = await asyncio.gather(ask(svc, ip="10.0.0.1"), ask(svc, ip="10.0.0.2"))
    assert ("status", {"text": "You're next in line…"}) in second
    assert first[-2][1]["text"] == "first" and second[-2][1]["text"] == "second"


async def test_twenty_concurrent_requests(monkeypatch):
    """Spec 06 acceptance 8: nothing crashes. Beyond the running + queued slots, requests get RATE_LIMITED."""
    monkeypatch.setattr(service_module, "QUEUE_STATUS_EVERY_S", 0.05)
    svc, _, _, _ = make({"local": [slow(0.3, say("ok"))] * 20})
    results = await asyncio.gather(*(ask(svc, ip=f"10.1.0.{i}") for i in range(20)))
    finals = [r[-2] for r in results]
    assert all(r[-1][0] == "done" for r in results)
    answered = [f for f in finals if f[0] == "answer"]
    limited = [f for f in finals if f[0] == "error" and f[1]["code"] == "RATE_LIMITED"]
    assert len(answered) == 2 + 6 and len(limited) == 12


async def test_client_disconnect_releases_slot_and_logs():
    svc, _, _, db = make({"local": [slow(1.0, say("too late"))]}, AGENT_MAX_CONCURRENCY="1")
    stream = svc.ask(AskRequest(question="Which borough tips best?"), "10.0.0.1")
    await stream.__anext__()  # the first status arrived, then the visitor walks away
    await asyncio.sleep(0.1)
    await stream.aclose()
    await settle()
    assert db.logged and db.logged[0]["error"] == "CLIENT_GONE" and db.logged[0]["success"] is False
    events = await ask(svc, ip="10.0.0.2")  # the slot is free again
    assert events[-1][0] == "done"


async def test_oneshot_failed_attempt_shows_as_a_failed_run_query_step():
    bad = {"sql": "SELECT * FROM system.users", "purpose": "x"}
    db = FakeDb({"system.tables": Result([("name", "String"), ("comment", "String")], [], 1),
                 "DESCRIBE TABLE": Result([("name", "String"), ("type", "String"), ("comment", "String")],
                                          [["tip_pct", "Float32", ""]], 1),
                 "SELECT * FROM (": TIPS})
    svc, _, _, _ = make({"local": [say(json.dumps(bad)), say(json.dumps(GOOD)), say("Manhattan riders tip best.")]},
                        db=db, AGENT_MODE="oneshot")
    events = await ask(svc)
    steps = [(e, d) for e, d in events if e in ("tool_call", "tool_result")]
    assert [(e, d["id"], d["name"]) for e, d in steps] == [
        ("tool_call", "oneshot_1", "run_query"), ("tool_result", "oneshot_1", "run_query"),
        ("tool_call", "oneshot_2", "run_query"), ("tool_result", "oneshot_2", "run_query")]
    failed = steps[1][1]
    assert failed["ok"] is False and "system.users" in failed["summary"]
    assert ("status", {"text": "Fixing its own query…"}) in events and steps[3][1]["ok"] is True
    done = events[-1][1]
    assert done["mode"] == "oneshot" and done["tool_calls"] == 0 and done["repairs"] == 1
    await settle()
    row = db.logged[0]
    assert row["tool_calls"] == 0 and row["refused"] is False and row["success"] is True
    assert row["sql_attempts"] == [bad["sql"], GOOD["sql"]] and row["final_sql"] == GOOD["sql"]


async def test_provider_error_body_never_reaches_the_logs(capsys):
    body = json.dumps({"error": {"message": "Incorrect API key provided: sk-FAKE-leaked-1234567890"}})
    svc, _, _, _ = make({"local": [raw(body, status=401)], "internal": [say("From the gateway.")]})
    events = await ask(svc)
    assert events[-2] == ("answer", {"text": "From the gateway."})
    out = capsys.readouterr().out
    assert "sk-FAKE-leaked" not in out and "Incorrect API key" not in out
    failed = [json.loads(line) for line in out.splitlines() if '"provider_failed"' in line]
    assert [f["reason"] for f in failed] == ["AuthenticationError 401"]


async def test_crash_logs_only_the_exception_class_and_place(monkeypatch, capsys):
    async def explode(*_: Any, **__: Any) -> None:
        raise RuntimeError("secret sk-FAKE-crash-987 in the message")

    monkeypatch.setattr(service_module, "run_tools", explode)
    svc, _, _, _ = make()
    events = await ask(svc)
    assert events[-2][0] == "error" and events[-2][1]["code"] == "INTERNAL"
    out = capsys.readouterr().out
    assert "sk-FAKE-crash" not in out
    crash = next(json.loads(line) for line in out.splitlines() if '"agent_crash"' in line)
    assert crash["error"] == "RuntimeError" and crash["where"].startswith("test_service.py:")
