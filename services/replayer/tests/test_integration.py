"""Backfill a tiny file into a real Redpanda and check what landed (needs Docker)."""

import json
import threading
import time

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from contract_validator import assert_valid
from testcontainers.kafka import RedpandaContainer

from conftest import make_replay_parquet
from replayer.config import Config
from replayer.engine import Replayer
from replayer.producer import KafkaOut
from replayer.source import ReplaySource

pytestmark = pytest.mark.integration


def consume_all(bootstrap: str, topic: str, expected: int, timeout_s: float = 30) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{topic}", "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    out: list[dict] = []
    deadline = time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < deadline:
        msg = c.poll(1.0)
        if msg is not None and msg.error() is None:
            out.append(json.loads(msg.value()))
    c.close()
    return out


def test_backfill_into_redpanda(tmp_path):
    with RedpandaContainer("redpandadata/redpanda:v26.2.3") as rp:
        bootstrap = rp.get_bootstrap_server()
        topics = [NewTopic(t, 3, 1) for t in ("trips.raw", "trace.checkpoints")] + [NewTopic("sim.clock", 1, 1)]
        admin = AdminClient({"bootstrap.servers": bootstrap})  # keep a reference: futures die with the client
        for f in admin.create_topics(topics).values():
            f.result(30)
        parquet = make_replay_parquet(tmp_path / "trips.parquet", n=40)
        cfg = Config.from_env({"KAFKA_BROKERS": bootstrap, "REPLAY_PARQUET_PATH": str(parquet),
                               "REPLAY_SPEED": "max", "REPLAY_UNTIL_TS": "2025-10-07T08:00:00",
                               "REPLAY_START_TS": "2025-10-07T07:00:00", "REPLAY_TRACE_SAMPLE_RATE": "0.25",
                               "REPLAY_STATE_PATH": str(tmp_path / "state.json")})
        rep = Replayer(cfg, ReplaySource(parquet), KafkaOut(bootstrap))
        rep.run(threading.Event())

        trips = consume_all(bootstrap, "trips.raw", 40)
        assert sorted(t["trip_id"] for t in trips) == [f"r0-{i:09d}" for i in range(40)]
        for t in trips:
            assert_valid("trip", t)
        traced = [t for t in trips if t["trace_id"]]
        checkpoints = consume_all(bootstrap, "trace.checkpoints", len(traced))
        assert {c["trace_id"] for c in checkpoints} == {t["trace_id"] for t in traced}
        for c in checkpoints:
            assert_valid("trace_checkpoint", c)
        clocks = consume_all(bootstrap, "sim.clock", 1)
        assert clocks and clocks[0]["mode"] == "backfill"
