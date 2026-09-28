from pathlib import Path

from processor.config import Config


def test_defaults():
    c = Config.from_env({})
    assert (c.brokers, c.consumer_group, c.commit_interval_s, c.window_grace_s) == ("redpanda:9092", "processor-v1", 1.0, 180)
    assert c.tip_max_abs == 100.0 and c.zones_path == Path("/data/zones.csv") and c.state_dir == Path("/state")
    assert (c.ch_host, c.ch_port, c.ch_db, c.ch_user) == ("clickhouse", 8123, "taxi", "ingest_writer")


def test_overrides():
    c = Config.from_env({"PROCESSOR_WINDOW_GRACE_S": "0", "CLICKHOUSE_HTTP_PORT": "18123",
                         "CLICKHOUSE_INGEST_PASSWORD": "pw", "PROCESSOR_TIP_MAX_ABS": "50"})
    assert (c.window_grace_s, c.ch_port, c.ch_password, c.tip_max_abs) == (0, 18123, "pw", 50.0)
