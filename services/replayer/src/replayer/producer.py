"""confluent-kafka producer behind the engine's Out protocol."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from replayer.log import event

PRODUCER_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 100_000,
    "message.timeout.ms": 0,  # never expire: rows wait out a broker outage, then the queue fills → self-pause
}


class KafkaOut:
    def __init__(self, brokers: str) -> None:
        self._p = Producer({"bootstrap.servers": brokers, **PRODUCER_CONFIG})

    def send(self, topic: str, key: str, value: dict[str, Any],
             on_delivery: Callable[[int, int], None] | None = None) -> None:
        def callback(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                event("delivery_failed", topic=topic, key=key, error=str(err))
            elif on_delivery is not None:
                partition, offset = msg.partition(), msg.offset()
                if partition is not None and offset is not None:
                    on_delivery(partition, offset)

        self._p.produce(topic, key=key.encode(), value=json.dumps(value, separators=(",", ":")).encode(),
                        on_delivery=callback)

    def poll(self, timeout: float = 0.0) -> None:
        self._p.poll(timeout)

    def pending(self) -> int:
        return len(self._p)

    def flush(self, timeout: float = 30.0) -> int:
        return int(self._p.flush(timeout))
