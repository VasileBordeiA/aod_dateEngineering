"""Latest sim.clock heartbeat for GET /api/sim/clock (spec 07). A daemon thread consumes the compacted topic."""

from __future__ import annotations

import json
import threading
import uuid
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, KafkaException, TopicPartition

from api import log

TOPIC = "sim.clock"


class SimClockFollower:
    def __init__(self, brokers: str) -> None:
        self._brokers = brokers
        self.latest: dict[str, Any] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def handle(self, value: bytes | None) -> None:
        try:
            msg = json.loads(value or b"")
        except ValueError:
            return
        if isinstance(msg, dict) and "sim_ts" in msg:
            self.latest = msg

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="sim-clock", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._consume()
            except KafkaException as exc:
                log.event("sim_clock_consumer_error", error=str(exc)[:300])
                self._stop.wait(2)

    def _consume(self) -> None:
        # A throwaway group without commits: every api start re-reads the latest heartbeat.
        consumer = Consumer({"bootstrap.servers": self._brokers, "group.id": f"api-sim-clock-{uuid.uuid4()}",
                             "enable.auto.commit": False, "auto.offset.reset": "latest"})
        try:
            _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=5)
            consumer.assign([TopicPartition(TOPIC, 0, high - 1 if high > 0 else OFFSET_END)])
            while not self._stop.is_set():
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    self.handle(msg.value())
        finally:
            consumer.close()
