"""make burst (spec 07 acceptance 7): 50 rides in 10 s from 10 simulated phones against the running api.

Pass = no 5xx, every accepted ride reaches a terminal stage, each phone's 4th and 5th ride within the minute
get 429, and the p95 of POST /api/rides is under 300 ms. Run it with no other rides booked in the last minute:
10 phones × 3 rides is exactly the global cap of 30.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import websockets

API = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000"
PHONES = [f"10.88.0.{i}" for i in range(1, 11)]
RIDES_PER_PHONE = 5
SPAN_S = 10.0
BODY = {"pickup_location_id": 161, "dropoff_location_id": 230, "payment": "card", "tip_amount": 3.0,
        "passenger_count": 1}


def post(ip: str) -> tuple[int, float, dict[str, Any]]:
    req = urllib.request.Request(f"{API}/api/rides", method="POST", data=json.dumps(BODY).encode(),
                                 headers={"Content-Type": "application/json", "X-Forwarded-For": ip})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, time.monotonic() - started, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, time.monotonic() - started, json.loads(e.read() or b"{}")
    except OSError:
        return 0, time.monotonic() - started, {}


async def terminal(trace_id: str, timeout_s: float = 30) -> str | None:
    deadline = time.monotonic() + timeout_s
    try:
        async with websockets.connect(f"{WS}/api/trace/{trace_id}") as ws:
            while (left := deadline - time.monotonic()) > 0:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
                stages = [c["stage"] for c in msg["checkpoints"]] if msg["type"] == "history" else [msg["stage"]]
                for s in stages:
                    if s in ("queryable", "rejected"):
                        return s
    except (TimeoutError, websockets.ConnectionClosed, OSError):
        return None
    return None


async def main() -> int:
    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=16)
    # Round-robin over the phones, spread evenly over SPAN_S: every phone's 4th ride lands well inside its minute.
    schedule = [(i * SPAN_S / (len(PHONES) * RIDES_PER_PHONE), ip)
                for i, ip in enumerate(ip for _ in range(RIDES_PER_PHONE) for ip in PHONES)]
    started = time.monotonic()

    async def fire(at: float, ip: str) -> tuple[str, tuple[int, float, dict[str, Any]]]:
        await asyncio.sleep(max(0.0, at - (time.monotonic() - started)))
        return ip, await loop.run_in_executor(pool, post, ip)

    results = await asyncio.gather(*(fire(at, ip) for at, ip in schedule))
    statuses = [s for _, (s, _, _) in results]
    created = [b["trace_id"] for _, (s, _, b) in results if s == 201]
    latencies = sorted(dt for _, (s, dt, _) in results if s == 201)
    p95 = latencies[int(0.95 * (len(latencies) - 1))] if latencies else float("inf")
    per_phone: dict[str, list[int]] = {}
    for ip, (s, _, _) in results:
        per_phone.setdefault(ip, []).append(s)
    ends = await asyncio.gather(*(terminal(t) for t in created))

    checks = {
        "no 5xx or connection errors": all(0 < s < 500 for s in statuses),
        "every phone: 3 accepted, then 429": all(v == [201, 201, 201, 429, 429] for v in per_phone.values()),
        "every accepted ride reached a terminal stage": all(e is not None for e in ends),
        "p95 POST /api/rides < 300 ms": p95 < 0.3,
    }
    print(f"{len(results)} requests: {statuses.count(201)} × 201, {statuses.count(429)} × 429, "
          f"others {[s for s in statuses if s not in (201, 429)]}")
    print(f"terminal stages: {sorted((e or 'none') for e in ends)}")
    print(f"POST p95 {p95 * 1000:.0f} ms")
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
