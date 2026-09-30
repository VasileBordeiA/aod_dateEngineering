"""Relay consumer: every checkpoint on trace.checkpoints, from "now", handed to the event loop (spec 07)."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaException

from api import log
from rides.checkpoints import TRACE_TOPIC, parse_checkpoint

RETRY_WAIT_S = 2.0
ERROR_LOG_EVERY_S = 30.0


def consumer_config(brokers: str) -> dict[str, Any]:
    # A throwaway group without commits: every api instance sees everything from "now". A short fetch wait keeps
    # each hop visible on the phone within ~50 ms instead of librdkafka's default 500 ms.
    return {"bootstrap.servers": brokers, "group.id": f"api-relay-{uuid.uuid4()}", "enable.auto.commit": False,
            "auto.offset.reset": "latest", "fetch.wait.max.ms": 50}


class TraceRelay:
    def __init__(self, brokers: str, clock: Callable[[], float] = time.monotonic) -> None:
        self._brokers = brokers
        self._deliver: Callable[[dict[str, Any]], None] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._clock = clock
        self._error_logged_at = float("-inf")

    def start(self, deliver: Callable[[dict[str, Any]], None]) -> None:
        self._deliver = deliver
        self._thread = threading.Thread(target=self._run, name="trace-relay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    @property
    def alive(self) -> bool:
        """The consumer thread is running (/api/healthz)."""
        return self._thread is not None and self._thread.is_alive()

    def log_error(self, error: str) -> None:
        """A consumer error (broker down, rebalance): logged at most every 30 s so an outage can't flood stdout."""
        now = self._clock()
        if now - self._error_logged_at >= ERROR_LOG_EVERY_S:
            self._error_logged_at = now
            log.event("trace_relay_error", error=error[:300])

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
                self._stop.wait(RETRY_WAIT_S)
            except Exception as exc:  # never let the thread die silently: the phones would stop moving
                log.event("trace_relay_error", error=log.error_label(exc), where=log.error_where(exc))
                self._stop.wait(RETRY_WAIT_S)

    def _consume(self) -> None:
        consumer = Consumer(consumer_config(self._brokers))
        try:
            consumer.subscribe([TRACE_TOPIC])
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error() is not None:
                    self.log_error(str(msg.error()))
                else:
                    self.handle(msg.value())
        finally:
            consumer.close()
