import pytest
from clickhouse_connect.driver.exceptions import OperationalError
from quixstreams.sinks import SinkBackpressureError

from processor.rows import REJECTED_COLUMNS, rejected_row
from processor.sinks import ClickHouseSink

REJ = {"trip_id": "r0-000200017", "source": "replay", "trace_id": None, "pickup_ts": "2025-10-07T09:01:12",
       "reasons": ["FARE_NON_POSITIVE"], "payload": "{}", "rejected_at": "2026-10-16T08:03:01.200Z"}


class FakeClient:
    def __init__(self, fail: Exception | None = None):
        self.inserts = []
        self.fail = fail

    def insert(self, table, rows, column_names):
        if self.fail:
            raise self.fail
        self.inserts.append((table, rows, column_names))


def sink(client_factory, after=None):
    return ClickHouseSink(table="trips_rejected", columns=REJECTED_COLUMNS, to_row=rejected_row,
                          client_factory=client_factory, after_insert=after)


def test_inserts_rows_then_calls_after_insert():
    client, seen = FakeClient(), []
    sink(lambda: client, seen.extend).write_values([REJ])
    ((table, rows, cols),) = client.inserts
    assert table == "trips_rejected" and cols == list(REJECTED_COLUMNS) and len(rows) == 1
    assert seen == [REJ]


def test_connection_error_becomes_backpressure():
    seen = []
    s = sink(lambda: FakeClient(fail=OperationalError("connection refused")), seen.extend)
    with pytest.raises(SinkBackpressureError):
        s.write_values([REJ])
    assert seen == []  # no "stored" checkpoint for rows that were not stored


def test_unreachable_at_setup_is_retried_on_write():
    client, calls = FakeClient(), []

    def factory():
        calls.append(1)
        if len(calls) == 1:
            raise OperationalError("clickhouse not up yet")
        return client

    s = sink(factory)
    s.setup()  # must not raise
    s.write_values([REJ])
    assert len(calls) == 2 and len(client.inserts) == 1


def test_empty_batch_is_a_noop():
    client = FakeClient()
    sink(lambda: client).write_values([])
    assert client.inserts == []
