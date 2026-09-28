"""confluent-kafka producer behind the engine's Out protocol."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from replayer.log import event


class KafkaOut:
    def __init__(self, brokers: str) -> None:
        self._p = Producer({
            "bootstrap.servers": brokers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 100_000,
        })

    def send(self, topic: str, key: str, value: dict[str, Any],
             on_delivery: Callable[[int, int], None] | None = None) -> None:
        def callback(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                event("delivery_failed", topic=topic, key=key, error=str(err))
            elif on_delivery is not None:
                on_delivery(msg.partition(), msg.offset())

        self._p.produce(topic, key=key.encode(), value=json.dumps(value, separators=(",", ":")).encode(),
                        on_delivery=callback)

    def poll(self, timeout: float = 0.0) -> None:
        self._p.poll(timeout)

    def pending(self) -> int:
        return len(self._p)

    def flush(self, timeout: float = 30.0) -> int:
        return int(self._p.flush(timeout))
