"""Checks against the running compose stack (make up && make test-stack)."""

import json
import time
import urllib.request
import uuid
from base64 import b64encode
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, ConfigResource

pytestmark = pytest.mark.stack
ROOT = Path(__file__).resolve().parents[2]
TOPICS = {"trips.raw", "trips.enriched", "trips.rejected", "agg.borough_minute", "trace.checkpoints", "sim.clock"}


@lru_cache(maxsize=1)
def env() -> dict[str, str]:
    """Read lazily so collecting this module never needs a .env (it is deselected by default)."""
    return dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
                if "=" in line and not line.lstrip().startswith("#"))


def client(user: str, password_key: str):
    return clickhouse_connect.get_client(host="localhost", port=8123, username=user,
                                         password=env()[password_key], database="taxi")


@pytest.fixture(scope="module")
def admin():
    return client("admin", "CLICKHOUSE_ADMIN_PASSWORD")


@pytest.fixture(scope="module")
def agent():
    return client("agent_ro", "CLICKHOUSE_AGENT_PASSWORD")


def test_topics_exist_with_configs():
    a = AdminClient({"bootstrap.servers": "localhost:19092"})
    md = a.list_topics(timeout=10)
    assert TOPICS <= set(md.topics)
    assert all(len(md.topics[t].partitions) == 3 for t in TOPICS - {"sim.clock"})
    assert len(md.topics["sim.clock"].partitions) == 1
    res = a.describe_configs([ConfigResource("topic", "trips.raw"), ConfigResource("topic", "sim.clock")])
    configs = {r.name: f.result() for r, f in res.items()}
    assert configs["trips.raw"]["retention.ms"].value == "21600000"
    assert configs["sim.clock"]["cleanup.policy"].value == "compact"


def test_tables(admin):
    names = {r[0] for r in admin.query("SHOW TABLES FROM taxi").result_rows}
    assert names == {"agent_log", "agg_borough_minute", "sim_now", "trace_events", "trace_events_mv",
                     "trace_events_queue", "trips", "trips_rejected", "zones"}


def test_zones_loaded(admin):
    total, with_centroid = admin.query("SELECT count(), countIf(lat IS NOT NULL) FROM taxi.zones").result_rows[0]
    assert total == 265 and with_centroid >= 258


@pytest.mark.parametrize("sql", [
    "INSERT INTO taxi.agent_log (question) VALUES ('x')",
    "ALTER TABLE taxi.trips DELETE WHERE 1",
    "DROP TABLE taxi.trips",
    "CREATE TABLE taxi.x (a UInt8) ENGINE = Memory",
    "SET max_execution_time = 100",
    "SELECT 1 SETTINGS max_result_rows = 10000",
    "SELECT * FROM taxi.agent_log",
    "SELECT * FROM system.users",
])
def test_agent_ro_cannot(agent, sql):
    with pytest.raises(DatabaseError):
        agent.command(sql)


def test_agent_ro_result_rows_are_capped(agent):
    # result_overflow_mode=throw: 'break' is block-granular and let 10 000 rows through.
    assert len(agent.query("SELECT number FROM numbers(500)").result_rows) == 500
    with pytest.raises(DatabaseError, match="TOO_MANY_ROWS"):
        agent.query("SELECT number FROM numbers(10000)")


def test_agent_ro_heavy_query_is_killed_fast(agent):
    start = time.monotonic()
    with pytest.raises(DatabaseError):
        agent.query("SELECT count() FROM numbers(1000000000000) AS a CROSS JOIN numbers(1000000000000) AS b")
    assert time.monotonic() - start < 6


def test_agent_ro_can_read_whitelist(agent):
    for t in ["trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"]:
        agent.query(f"SELECT * FROM taxi.{t} LIMIT 1")


def test_ingest_writer_can_insert_but_not_read(admin):
    ingest = client("ingest_writer", "CLICKHOUSE_INGEST_PASSWORD")
    ingest.insert("trips_rejected", [["stack-test", "replay", None, None, ["SCHEMA_INVALID"], "{}",
                                      datetime.now(timezone.utc)]],
                  column_names=["trip_id", "source", "trace_id", "pickup_ts", "reasons", "payload", "rejected_at"])
    with pytest.raises(DatabaseError):
        ingest.query("SELECT count() FROM taxi.trips")
    admin.command("DELETE FROM taxi.trips_rejected WHERE trip_id = 'stack-test'")


def test_trace_checkpoint_reaches_trace_events_via_kafka_engine(admin):
    trace_id = str(uuid.uuid4())
    cp = {"trace_id": trace_id, "trip_id": "r0-000000001", "source": "replay", "stage": "ingested", "seq": 2,
          "service": "replayer", "at": "2026-10-16T08:00:00.000Z", "detail": {"partition": 0, "offset": 1}}
    p = Producer({"bootstrap.servers": "localhost:19092"})
    p.produce("trace.checkpoints", key=trace_id, value=json.dumps(cp))
    p.flush(10)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        rows = admin.query(f"SELECT stage, detail FROM taxi.trace_events WHERE trace_id = '{trace_id}'").result_rows
        if rows:
            assert rows[0][0] == "ingested" and json.loads(rows[0][1]) == {"partition": 0, "offset": 1}
            return
        time.sleep(1)
    pytest.fail("checkpoint did not reach taxi.trace_events within 15 s")


def test_grafana_datasource_is_healthy_through_caddy():
    auth = b64encode(f"admin:{env()['GRAFANA_ADMIN_PASSWORD']}".encode()).decode()
    req = urllib.request.Request("http://localhost/grafana/api/datasources/uid/clickhouse/health",
                                 headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=10) as r:
        assert json.load(r)["status"] == "OK"


def test_console_is_up():
    with urllib.request.urlopen("http://127.0.0.1:8080/", timeout=10) as r:
        assert r.status == 200
