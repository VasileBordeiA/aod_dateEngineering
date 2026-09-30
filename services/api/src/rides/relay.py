"""Relay consumer: every checkpoint on trace.checkpoints, from "now", handed to the event loop (spec 07)."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaException

from api import log
from rides.checkpoints import TRACE_TOPIC, parse_checkpoint


def consumer_config(brokers: str) -> dict[str, Any]:
    # A throwaway group without commits: every api instance sees everything from "now". A short fetch wait keeps
    # each hop visible on the phone within ~50 ms instead of librdkafka's default 500 ms.
    return {"bootstrap.servers": brokers, "group.id": f"api-relay-{uuid.uuid4()}", "enable.auto.commit": False,
            "auto.offset.reset": "latest", "fetch.wait.max.ms": 50}


class TraceRelay:
    def __init__(self, brokers: str) -> None:
        self._brokers = brokers
        self._deliver: Callable[[dict[str, Any]], None] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, deliver: Callable[[dict[str, Any]], None]) -> None:
        self._deliver = deliver
        self._thread = threading.Thread(target=self._run, name="trace-relay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def handle(self, value: bytes | None) -> None:
        cp = parse_checkpoint(value)
        if cp is None or self._deliver is None:
            return
        try:
            self._deliver(cp)
        except RuntimeError:  # the event loop is shutting down
            self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._consume()
            except KafkaException as exc:
                log.event("trace_relay_error", error=str(exc)[:300])
                self._stop.wait(2)

    def _consume(self) -> None:
        consumer = Consumer(consumer_config(self._brokers))
        try:
            consumer.subscribe([TRACE_TOPIC])
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is not None and msg.error() is None:
                    self.handle(msg.value())
        finally:
            consumer.close()
