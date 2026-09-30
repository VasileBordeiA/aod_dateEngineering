import json

from test_history import cp

from rides.relay import TraceRelay


def test_relay_delivers_valid_checkpoints_and_skips_garbage():
    got: list[dict] = []
    relay = TraceRelay("unused:9092")
    relay._deliver = got.append
    good = cp("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f", "validated", 3)
    for value in [b"garbage", None, json.dumps({"stage": "validated"}).encode(), json.dumps(good).encode()]:
        relay.handle(value)
    assert got == [good]


def test_relay_consumer_settings():
    from rides.relay import consumer_config

    c = consumer_config("redpanda:9092")
    assert c["auto.offset.reset"] == "latest" and c["enable.auto.commit"] is False
    assert c["group.id"].startswith("api-relay-") and c["fetch.wait.max.ms"] <= 50
