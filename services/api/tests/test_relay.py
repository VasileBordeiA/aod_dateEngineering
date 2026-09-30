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


def test_an_unexpected_error_does_not_kill_the_relay(monkeypatch, capsys):
    relay = TraceRelay("unused:9092")
    calls: list[int] = []

    def consume() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("a bug, not a KafkaException")
        relay.stop()

    monkeypatch.setattr(relay, "_consume", consume)
    monkeypatch.setattr(relay._stop, "wait", lambda timeout=None: False)  # skip the 2 s back-off
    relay._run()
    assert len(calls) == 2  # it retried after the error
    out = capsys.readouterr().out
    assert '"event": "trace_relay_error"' in out and '"error": "ValueError"' in out


def test_consumer_errors_are_logged_at_most_every_30_s(capsys):
    t = [0.0]
    relay = TraceRelay("unused:9092", clock=lambda: t[0])
    relay.log_error("Broker: transport failure")
    t[0] = 10.0
    relay.log_error("Broker: transport failure")
    t[0] = 31.0
    relay.log_error("Broker: transport failure")
    assert capsys.readouterr().out.count('"event": "trace_relay_error"') == 2


def test_alive_is_false_until_started():
    assert TraceRelay("unused:9092").alive is False
