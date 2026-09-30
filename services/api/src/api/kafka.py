"""One Kafka producer for the api: student rides and queryable checkpoints (spec 07)."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, NamedTuple, Protocol

from confluent_kafka import KafkaException, Producer

from api import log

SEND_WAIT_S = 2.5
# librdkafka gives up (and reports it) after 1 s. That is well before POST /api/rides stops waiting, so a ride that
# got a 503 is not retried later (spec 07 §Failure modes: "no submitted is left dangling"). The 1.5 s margin covers
# the delivery-report hop to the event loop. A "possibly persisted" timeout can still land: see spec 07.
PRODUCER_CONFIG: dict[str, Any] = {"linger.ms": 2, "acks": "all", "message.timeout.ms": 1000}
POLL_ERROR_BACKOFF_S = 0.5


class Delivery(NamedTuple):
    partition: int
    offset: int


class KafkaDeliveryError(Exception):
    """The broker did not acknowledge the message."""


class Out(Protocol):
    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery: ...

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None: ...


def _resolve(fut: asyncio.Future[Delivery], result: Delivery | None, error: str | None) -> None:
    if fut.done():  # the caller already gave up
        return
    if error is not None:
        fut.set_exception(KafkaDeliveryError(error))
    else:
        assert result is not None
        fut.set_result(result)


class KafkaOut:
    def __init__(self, brokers: str, producer: Any = None) -> None:
        self._p = producer if producer is not None else Producer({"bootstrap.servers": brokers, **PRODUCER_CONFIG})
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, name="kafka-out", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._p.flush(3)

    @property
    def alive(self) -> bool:
        """The delivery thread is running (/api/healthz)."""
        return self._thread.is_alive()

    def _poll(self) -> None:
        while not self._stop.is_set():
            try:
                self._p.poll(0.1)  # runs the delivery callbacks
            except Exception as exc:  # never let the thread die: every later send would time out with a 503
                log.event("kafka_out_error", error=log.error_label(exc), where=log.error_where(exc))
                self._stop.wait(POLL_ERROR_BACKOFF_S)

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery:
        """Produce and wait for the broker's acknowledgement (at most SEND_WAIT_S)."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Delivery] = loop.create_future()

        def on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                loop.call_soon_threadsafe(_resolve, fut, None, str(err))
            else:
                loop.call_soon_threadsafe(_resolve, fut, Delivery(msg.partition(), msg.offset()), None)

        try:
            self._p.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:
            raise KafkaDeliveryError(str(exc)) from exc
        try:
            return await asyncio.wait_for(fut, SEND_WAIT_S)
        except TimeoutError as exc:
            raise KafkaDeliveryError("no broker acknowledgement") from exc

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Fire and forget (checkpoints). A failure is logged, never raised."""

        def on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                log.event("checkpoint_dropped", topic=topic, trace_id=value.get("trace_id"), error=str(err)[:200])

        try:
            self._p.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:
            log.event("checkpoint_dropped", topic=topic, trace_id=value.get("trace_id"), error=str(exc)[:200])
