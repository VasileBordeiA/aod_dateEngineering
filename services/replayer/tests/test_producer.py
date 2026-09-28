from replayer.producer import PRODUCER_CONFIG


def test_rows_never_expire_while_the_broker_is_down():
    """librdkafka's default message.timeout.ms (300 s) drops queued rows during a >5 min outage. With 0
    (infinite) they wait; once the 100k queue is full, BufferError makes the engine self-pause (spec 01)."""
    assert PRODUCER_CONFIG["message.timeout.ms"] == 0
    assert PRODUCER_CONFIG["queue.buffering.max.messages"] == 100_000
    assert PRODUCER_CONFIG["enable.idempotence"] is True and PRODUCER_CONFIG["acks"] == "all"
