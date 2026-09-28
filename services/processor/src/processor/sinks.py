"""ClickHouse sink + checkpoint producer (spec 03 §ClickHouse sinks, ADR-003)."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

from clickhouse_connect.driver.exceptions import OperationalError
from confluent_kafka import Producer
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

from processor.log import event

TRACE_TOPIC = "trace.checkpoints"


class ClickHouseSink(BatchingSink):
    """One INSERT per Quix checkpoint (commit_interval). A connection error pauses the partition and
    retries; the batch is never dropped, and after_insert only runs for rows that were stored."""

    def __init__(self, *, table: str, columns: Sequence[str], to_row: Callable[[dict[str, Any]], list[Any]],
                 client_factory: Callable[[], Any],
                 after_insert: Callable[[list[dict[str, Any]]], None] | None = None) -> None:
        super().__init__()
        self._table, self._columns, self._to_row = table, list(columns), to_row
        self._client_factory, self._after_insert = client_factory, after_insert
        self._client: Any = None

    def setup(self) -> None:
        try:
            self._client = self._client_factory()
        except Exception as e:  # noqa: BLE001 — ClickHouse may start after us; write() retries
            event("clickhouse_unavailable", table=self._table, error=str(e)[:200])

    def write(self, batch: SinkBatch) -> None:
        self.write_values([item.value for item in batch])

    def write_values(self, values: list[dict[str, Any]]) -> None:
        if not values:
            return
        rows = [self._to_row(v) for v in values]
        try:
            if self._client is None:
                self._client = self._client_factory()
            self._client.insert(self._table, rows, column_names=self._columns)
        except (OperationalError, OSError) as e:
            self._client = None
            event("sink_backpressure", table=self._table, rows=len(rows), error=str(e)[:200])
            raise SinkBackpressureError(retry_after=5.0) from e
        if self._after_insert is not None:
            self._after_insert(values)


class CheckpointProducer:
    """Separate producer for "stored" checkpoints, which exist only after the INSERT succeeded."""

    def __init__(self, brokers: str) -> None:
        self._p = Producer({"bootstrap.servers": brokers, "linger.ms": 5})

    def emit(self, checkpoints: list[dict[str, Any]]) -> None:
        for cp in checkpoints:
            self._p.produce(TRACE_TOPIC, key=cp["trace_id"].encode(), value=json.dumps(cp).encode())
        if checkpoints:
            self._p.flush(5)
