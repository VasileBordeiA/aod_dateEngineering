from processor.app import pickup_ts_ms


def test_extractor_uses_pickup_ts_from_raw_bytes():
    assert pickup_ts_ms(b'{"trip_id":"x","pickup_ts":"2025-10-07T08:14:00"}', None, 42, None) == 1759824840000


def test_extractor_uses_pickup_ts_from_dict():
    assert pickup_ts_ms({"pickup_ts": "2025-10-07T08:14:00"}, None, 42, None) == 1759824840000


def test_extractor_falls_back_to_kafka_timestamp():
    assert pickup_ts_ms(b"not json", None, 42, None) == 42
    assert pickup_ts_ms({"pickup_ts": "later"}, None, 42, None) == 42


def test_error_callbacks_log_and_skip(capsys):
    from processor.app import Stats, make_error_callback
    stats = Stats()
    callback = make_error_callback("processing", stats)
    assert callback(ValueError("boom"), None, None) is True  # True = skip the record, keep the app running
    assert stats.snapshot() == {"errors_processing": 1}
    assert '"event": "record_skipped"' in capsys.readouterr().out
