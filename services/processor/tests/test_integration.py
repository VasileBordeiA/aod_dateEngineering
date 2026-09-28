"""Processor end to end: testcontainers Redpanda + ClickHouse (infra/clickhouse init + users), the processor
running as a subprocess exactly like in Docker. Needs Docker. Run: uv run pytest -m integration."""

import json
import os
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import clickhouse_connect
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from conftest import FIXTURES, REPO
from contract_validator import assert_valid
from testcontainers.core.container import DockerContainer
from testcontainers.kafka import RedpandaContainer

pytestmark = pytest.mark.integration
RP_IMAGE = "redpandadata/redpanda:v26.2.3"
CH_IMAGE = "clickhouse/clickhouse-server:26.3.34"
PW = {"admin": "admin-pw", "ingest": "ingest-pw", "agent": "agent-pw", "grafana": "grafana-pw"}
ZONES = [161, 255, 7, 69, 5, 132]  # Manhattan, Brooklyn, Queens, Bronx, Staten Island, Queens (JFK)
START = datetime(2025, 10, 7, 8, 0, 0)
FLUSH = START + timedelta(hours=2)


def wait_for(fn, timeout=120.0, what="condition"):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Exception as e:
            last = e
        time.sleep(1)
    raise AssertionError(f"timed out waiting for {what} (last error: {last!r})")


def trip(i: int, pickup: datetime, **over) -> dict:
    card = i % 4 != 0
    return {"trip_id": f"r7-{i:09d}", "source": "replay", "trace_id": None, "loop": 7, "vendor_id": 2,
            "pickup_ts": pickup.isoformat(timespec="seconds"),
            "dropoff_ts": (pickup + timedelta(minutes=12)).isoformat(timespec="seconds"),
            "passenger_count": 1, "trip_distance": 2.5, "ratecode_id": 1, "store_and_fwd_flag": "N",
            "pu_location_id": ZONES[i % len(ZONES)], "do_location_id": 237, "payment_type": 1 if card else 2,
            "fare_amount": 15.0, "extra": 0.0, "mta_tax": 0.5, "tip_amount": 3.0 if card else 0.0,
            "tolls_amount": 0.0, "improvement_surcharge": 1.0, "congestion_surcharge": 2.5, "airport_fee": None,
            "cbd_congestion_fee": 0.75, "total_amount": 22.75, "produced_at": "2026-10-16T08:00:00.000Z"} | over


def fixture_messages(good_trace: str, bad_trace: str) -> list[tuple[str, bytes]]:
    msgs = []
    for i in range(1000):
        over: dict = {}
        if i % 20 == 19:  # 50 bad trips: 25 refunds, 25 zero-distance
            over = {"fare_amount": -5.0, "total_amount": -5.0} if (i // 20) % 2 == 0 else {"trip_distance": 0.0}
        if i == 100:
            over["trace_id"] = good_trace
        if i == 119:
            over["trace_id"] = bad_trace
        msgs.append((f"r7-{i:09d}", json.dumps(trip(i, START + timedelta(seconds=3 * i), **over)).encode()))
    msgs.append(("garbage", b"not json"))
    for j, zone in enumerate(ZONES):  # later pickups in every borough close all open windows
        msgs.append((f"r7-{2000 + j:09d}", json.dumps(trip(2000 + j, FLUSH, pu_location_id=zone)).encode()))
    return msgs


def one(client, sql):
    return client.query(sql).result_rows[0][0]


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    clickhouse = (
        DockerContainer(CH_IMAGE)
        .with_env("CLICKHOUSE_USER", "admin").with_env("CLICKHOUSE_PASSWORD", PW["admin"])
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_env("CLICKHOUSE_INGEST_PASSWORD", PW["ingest"]).with_env("CLICKHOUSE_AGENT_PASSWORD", PW["agent"])
        .with_env("CLICKHOUSE_GRAFANA_PASSWORD", PW["grafana"])
        .with_volume_mapping(str(REPO / "infra/clickhouse/init"), "/docker-entrypoint-initdb.d", "ro")
        .with_volume_mapping(str(REPO / "infra/clickhouse/config.d/stand.xml"),
                             "/etc/clickhouse-server/config.d/stand.xml", "ro")
        .with_volume_mapping(str(REPO / "infra/clickhouse/users.d/stand.xml"),
                             "/etc/clickhouse-server/users.d/stand.xml", "ro")
        .with_volume_mapping(str(FIXTURES / "zones.csv"), "/data/zones.csv", "ro")
        .with_exposed_ports(8123)
    )
    with RedpandaContainer(RP_IMAGE) as rp, clickhouse:
        bootstrap = rp.get_bootstrap_server()
        # Same partitioning as infra/redpanda/create-topics.sh: the event-time streams are single-partition.
        topics = [NewTopic(t, 1, 1) for t in ("trips.raw", "trips.enriched")] + \
                 [NewTopic(t, 3, 1) for t in ("trips.rejected", "agg.borough_minute", "trace.checkpoints")]
        kafka_admin = AdminClient({"bootstrap.servers": bootstrap})  # keep a reference: futures die with it
        for f in kafka_admin.create_topics(topics).values():
            f.result(30)
        host, port = clickhouse.get_container_host_ip(), int(clickhouse.get_exposed_port(8123))

        def admin_ready():
            c = clickhouse_connect.get_client(host=host, port=port, username="admin", password=PW["admin"],
                                              database="taxi")
            return c if one(c, "SELECT count() FROM taxi.zones") > 0 else None

        admin = wait_for(admin_ready, what="ClickHouse init scripts")
        env = {**os.environ, "TZ": "UTC", "KAFKA_BROKERS": bootstrap, "CLICKHOUSE_HOST": host,
               "CLICKHOUSE_HTTP_PORT": str(port), "CLICKHOUSE_DB": "taxi", "CLICKHOUSE_INGEST_USER": "ingest_writer",
               "CLICKHOUSE_INGEST_PASSWORD": PW["ingest"], "PROCESSOR_ZONES_PATH": str(FIXTURES / "zones.csv"),
               "PROCESSOR_STATE_DIR": str(tmp_path_factory.mktemp("state")),
               "PROCESSOR_CONSUMER_GROUP": f"it-{uuid.uuid4().hex[:8]}", "PROCESSOR_WINDOW_GRACE_S": "0"}
        log_path = tmp_path_factory.mktemp("logs") / "processor.log"
        log = log_path.open("w")
        proc = subprocess.Popen([sys.executable, "-m", "processor.app"], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            good, bad = str(uuid.uuid4()), str(uuid.uuid4())
            producer = Producer({"bootstrap.servers": bootstrap})
            for key, value in fixture_messages(good, bad):
                producer.produce("trips.raw", key=key, value=value)
            producer.flush(30)
            wait_for(lambda: one(admin, "SELECT count() FROM taxi.trips") >= 956
                     and one(admin, "SELECT count() FROM taxi.trips_rejected") >= 51
                     and (one(admin, "SELECT sum(trips) FROM taxi.agg_borough_minute "
                                     "WHERE window_start < '2025-10-07 10:00:00'") or 0) >= 950,
                     what=f"rows in ClickHouse (processor log: {log_path})")
            yield {"bootstrap": bootstrap, "admin": admin, "good": good, "bad": bad, "proc": proc}
        finally:
            proc.terminate()
            proc.wait(30)
            log.close()


def consume(bootstrap, topic, want, timeout=30.0):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"check-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    out, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline and not want(out):
        msg = c.poll(1.0)
        if msg is not None and msg.error() is None:
            out.append(json.loads(msg.value()))
    c.close()
    return out


def test_processor_is_still_running(stack):
    assert stack["proc"].poll() is None


def test_valid_and_rejected_counts(stack):
    a = stack["admin"]
    assert one(a, "SELECT count() FROM taxi.trips FINAL") == 956
    reasons = dict(a.query("SELECT reason, count() FROM taxi.trips_rejected FINAL ARRAY JOIN reasons AS reason "
                           "GROUP BY reason").result_rows)
    assert reasons == {"FARE_NON_POSITIVE": 25, "TOTAL_NEGATIVE": 25, "DISTANCE_NON_POSITIVE": 25, "SCHEMA_INVALID": 1}


def test_aggregates_sum_to_valid_trips(stack):
    assert one(stack["admin"], "SELECT sum(trips) FROM taxi.agg_borough_minute FINAL "
                               "WHERE window_start < '2025-10-07 10:00:00'") == 950


def test_sim_timestamps_are_stored_unshifted(stack):
    rows = stack["admin"].query("SELECT toString(pickup_ts), pu_zone, pu_borough, tip_pct FROM taxi.trips "
                                "WHERE trip_id = 'r7-000000001'").result_rows
    assert rows == [("2025-10-07 08:00:03.000", "Williamsburg (North Side)", "Brooklyn", 20.0)]


def test_trace_checkpoints_in_order(stack):
    good, bad = stack["good"], stack["bad"]

    def complete(msgs):
        by = defaultdict(list)
        for m in msgs:
            by[m["trace_id"]].append(m["stage"])
        return len(by[good]) >= 4 and len(by[bad]) >= 1

    msgs = consume(stack["bootstrap"], "trace.checkpoints", complete)
    for m in msgs:
        assert_valid("trace_checkpoint", m)
    # Arrival order on the topic is NOT guaranteed ("stored" comes from a second producer right after the
    # INSERT, while Quix flushes its own producer at commit). Consumers order by seq; `at` must agree.
    chain = sorted((m for m in msgs if m["trace_id"] == good), key=lambda m: m["seq"])
    assert [m["stage"] for m in chain] == ["validated", "enriched", "aggregated", "stored"]
    assert [m["at"] for m in chain] == sorted(m["at"] for m in chain)
    assert [m["stage"] for m in msgs if m["trace_id"] == bad] == ["rejected"]


@pytest.mark.parametrize("topic,schema", [("trips.enriched", "enriched_trip"), ("trips.rejected", "rejected_trip"),
                                          ("agg.borough_minute", "borough_minute_agg")])
def test_output_topics_follow_contracts(stack, topic, schema):
    msgs = consume(stack["bootstrap"], topic, lambda m: len(m) >= 20)
    assert msgs
    for m in msgs:
        assert_valid(schema, m)
