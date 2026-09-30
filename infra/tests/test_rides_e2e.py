"""Trace your ride end to end on the running stack (spec 07 acceptance 3-6): make up && make test-e2e.

POSTs go straight to the api on 127.0.0.1:8000 so each test can use its own X-Forwarded-For (uvicorn trusts it,
spec 09) and never trips the per-IP limit. WebSockets go through Caddy, the path a phone takes.
"""

import asyncio
import json
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import pytest
import websockets

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "contracts"))
from contract_validator import assert_valid  # noqa: E402

pytestmark = pytest.mark.e2e
API = "http://127.0.0.1:8000"
CADDY_WS = "ws://localhost"
HAPPY = ["submitted", "ingested", "validated", "enriched", "aggregated", "stored", "queryable"]


def env() -> dict[str, str]:
    return dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
                if "=" in line and not line.lstrip().startswith("#"))


def unique_ip() -> str:
    n = uuid.uuid4().int
    return f"10.77.{n % 250}.{(n >> 8) % 250 + 1}"


def book(**overrides: object) -> dict:
    body = {"pickup_location_id": 161, "dropoff_location_id": 230, "payment": "card", "tip_amount": 3.0,
            "passenger_count": 1, **overrides}
    req = urllib.request.Request(f"{API}/api/rides", method="POST", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "X-Forwarded-For": unique_ip()})
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 201
        return json.load(r)


async def _collect(trace_id: str, until: set[str], timeout_s: float, base: str) -> tuple[list[dict], list[dict]]:
    history: list[dict] = []
    live: list[dict] = []
    deadline = time.monotonic() + timeout_s
    async with websockets.connect(f"{base}/api/trace/{trace_id}") as ws:
        while (left := deadline - time.monotonic()) > 0:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
            except (TimeoutError, websockets.ConnectionClosed):
                break
            if msg.pop("type") == "history":
                history = msg["checkpoints"]
            else:
                live.append(msg)
            if {c["stage"] for c in history + live} & until:
                break
    return history, live


def collect(trace_id: str, until: set[str], timeout_s: float, base: str = CADDY_WS) -> tuple[list[dict], list[dict]]:
    return asyncio.run(_collect(trace_id, until, timeout_s, base))


def by_seq(cps: list[dict]) -> list[str]:
    return [c["stage"] for c in sorted(cps, key=lambda c: (c["seq"], c["at"]))]


def ms(a: str, b: str) -> float:
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    return (datetime.strptime(b, fmt) - datetime.strptime(a, fmt)).total_seconds() * 1000


def test_normal_ride_reaches_queryable_within_5s():
    started = time.monotonic()
    ride = book()
    history, live = collect(ride["trace_id"], {"queryable", "rejected"}, 10)
    elapsed = time.monotonic() - started
    cps = history + live
    assert by_seq(cps) == HAPPY
    assert len({c["stage"] for c in cps}) == len(cps), "a stage arrived twice"
    for c in cps:
        assert_valid("trace_checkpoint", c)
    assert elapsed < 5, f"phone → queryable took {elapsed:.1f} s"
    first, last = min(c["at"] for c in cps), max(c["at"] for c in cps)
    print(f"\nsubmitted → queryable {ms(first, last):.0f} ms (wall), client saw it in {elapsed:.2f} s")


def test_absurd_tip_is_rejected_and_nothing_follows():
    ride = book(tip_amount=999)
    history, live = collect(ride["trace_id"], {"queryable"}, 6)  # listens the full 6 s: nothing may follow
    cps = history + live
    assert by_seq(cps) == ["submitted", "ingested", "rejected"]
    rejected = next(c for c in cps if c["stage"] == "rejected")
    assert "TIP_UNREALISTIC" in rejected["detail"]["reasons"]


def test_reconnect_after_stored_gives_full_history_without_duplicates():
    ride = book()
    collect(ride["trace_id"], {"stored"}, 10)
    history, live = collect(ride["trace_id"], {"queryable"}, 5)
    stages = [c["stage"] for c in history + live]
    assert sorted(stages, key=HAPPY.index) == HAPPY and len(stages) == len(set(stages))


def test_api_restart_keeps_history():
    ride = book()
    collect(ride["trace_id"], {"queryable"}, 10)
    e = env()
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="agent_ro",
                                       password=e["CLICKHOUSE_AGENT_PASSWORD"], database="taxi")
    deadline = time.monotonic() + 20  # the Kafka engine flushes taxi.trace_events every few seconds
    while time.monotonic() < deadline:
        n = ch.query("SELECT uniqExact(stage) FROM taxi.trace_events WHERE trace_id = {id:UUID}",
                     parameters={"id": ride["trace_id"]}).result_rows[0][0]
        if n == 7:
            break
        time.sleep(1)
    assert n == 7, "the checkpoints never reached taxi.trace_events"
    subprocess.run(["docker", "compose", "restart", "api"], cwd=ROOT, check=True, capture_output=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{API}/api/healthz", timeout=2) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(1)
    history, _ = collect(ride["trace_id"], {"queryable"}, 5)
    assert by_seq(history) == HAPPY
