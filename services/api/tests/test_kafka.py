import pytest

from api.kafka import Delivery, KafkaDeliveryError, KafkaOut


class _Msg:
    def __init__(self, partition: int, offset: int) -> None:
        self._p, self._o = partition, offset

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeProducer:
    def __init__(self, err: str | None = None, raise_on_produce: Exception | None = None) -> None:
        self.err, self.raise_on_produce = err, raise_on_produce
        self.pending: list = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.raise_on_produce is not None:
            raise self.raise_on_produce
        self.pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        cbs, self.pending = self.pending, []
        for cb in cbs:
            if cb is not None:
                cb(self.err, None if self.err else _Msg(0, 42))
        return len(cbs)

    def flush(self, timeout: float) -> int:
        return 0


@pytest.mark.anyio
async def test_send_waits_for_the_delivery_report():
    out = KafkaOut("unused:9092", producer=FakeProducer())
    out.start()
    try:
        assert await out.send("trips.raw", "k", {"a": 1}) == Delivery(0, 42)
    finally:
        out.stop()


@pytest.mark.anyio
async def test_send_raises_on_a_failed_delivery():
    out = KafkaOut("unused:9092", producer=FakeProducer(err="Local: Message timed out"))
    out.start()
    try:
        with pytest.raises(KafkaDeliveryError, match="timed out"):
            await out.send("trips.raw", "k", {"a": 1})
    finally:
        out.stop()


@pytest.mark.anyio
async def test_send_raises_when_the_local_queue_is_full():
    out = KafkaOut("unused:9092", producer=FakeProducer(raise_on_produce=BufferError("queue full")))
    with pytest.raises(KafkaDeliveryError):
        await out.send("trips.raw", "k", {})


def test_producer_gives_up_before_the_api_stops_waiting():
    """A ride that got a 503 must never be delivered later (spec 07 §Failure modes)."""
    from api.kafka import PRODUCER_CONFIG, SEND_WAIT_S

    assert PRODUCER_CONFIG["message.timeout.ms"] <= SEND_WAIT_S * 1000 - 500
