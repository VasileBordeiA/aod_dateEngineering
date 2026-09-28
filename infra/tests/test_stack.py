"""Checks against the running compose stack (make up && make test-stack)."""

import json
import time
import urllib.error
import urllib.request
import uuid
from base64 import b64encode
from datetime import UTC, datetime
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
    single = {"trips.raw", "trips.enriched", "sim.clock"}  # ordered event time (ADR-014) / compacted clock
    assert all(len(md.topics[t].partitions) == 1 for t in single)
    assert all(len(md.topics[t].partitions) == 3 for t in TOPICS - single)
    names = sorted(TOPICS)
    res = a.describe_configs([ConfigResource("topic", t) for t in names])
    configs = {r.name: f.result() for r, f in res.items()}
    for t in TOPICS - {"sim.clock"}:
        # Size-based retention: Quix writes event-time (2025) timestamps, so time-based retention would delete
        # freshly written data immediately (ADR-015).
        assert configs[t]["retention.ms"].value == "-1", t
        assert configs[t]["retention.bytes"].value == "1073741824", t
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
                                      datetime.now(UTC)]],
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


@pytest.mark.parametrize("sql", ["SELECT count() FROM taxi.agent_log", "SELECT count() FROM system.query_log",
                                 "SELECT count() FROM system.users", "SELECT count() FROM system.parts"])
def test_grafana_ro_cannot_read_visitor_text(sql):
    """Anonymous Grafana viewers can send arbitrary SQL through /grafana/api/ds/query (CLAUDE.md invariant 8)."""
    with pytest.raises(DatabaseError):
        client("grafana_ro", "CLICKHOUSE_GRAFANA_PASSWORD").query(sql)


def test_grafana_ro_reads_dashboard_tables():
    g = client("grafana_ro", "CLICKHOUSE_GRAFANA_PASSWORD")
    for t in ["taxi.trips", "taxi.trips_rejected", "taxi.agg_borough_minute", "taxi.zones", "taxi.sim_now",
              "taxi.trace_events"]:
        g.query(f"SELECT * FROM {t} LIMIT 1")


# ── Grafana (spec 05) ─────────────────────────────────────────────────────────
PUBLIC_GRAFANA = "http://localhost/grafana"  # through Caddy, as a visitor's phone sees it
OPS_GRAFANA = "http://127.0.0.1:3002"
DASHBOARDS = ROOT / "infra" / "grafana"


def http(method: str, url: str, body: dict | None = None, auth: bool = False) -> tuple[int, dict | list]:
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Basic " + b64encode(f"admin:{env()['GRAFANA_ADMIN_PASSWORD']}".encode()).decode()
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, {}


def panel_queries(path: Path) -> list[tuple[str, str]]:
    d = json.loads(path.read_text())
    return [(p["title"] or f"panel {p['id']}", t["rawSql"]) for p in d["panels"] for t in p.get("targets", [])]


def test_public_grafana_home_is_nyc_taxi_live():
    status, home = http("GET", f"{PUBLIC_GRAFANA}/api/dashboards/home")
    assert status == 200 and home["dashboard"]["uid"] == "nyc-taxi-live"
    status, found = http("GET", f"{PUBLIC_GRAFANA}/api/search")
    assert status == 200 and [d["uid"] for d in found] == ["nyc-taxi-live"]


def test_anonymous_viewers_cannot_edit():
    dashboard = {"dashboard": {"uid": "nyc-taxi-live", "title": "hacked", "panels": []}, "overwrite": True}
    assert http("POST", f"{PUBLIC_GRAFANA}/api/dashboards/db", dashboard)[0] in (401, 403)
    assert http("DELETE", f"{PUBLIC_GRAFANA}/api/dashboards/uid/nyc-taxi-live")[0] in (401, 403)


def test_public_grafana_has_no_ops_datasource():
    """ADR-017: in Grafana OSS any viewer can query any datasource by uid, so the ops one must not exist here."""
    status, settings = http("GET", f"{PUBLIC_GRAFANA}/api/frontend/settings")
    assert status == 200
    assert {ds["uid"] for ds in settings["datasources"].values() if "uid" in ds} & {"clickhouse-ops"} == set()
    query = {"queries": [{"refId": "A", "datasource": {"uid": "clickhouse-ops"}, "rawSql": "SELECT 1",
                          "format": 1, "queryType": "table"}], "from": "now-1h", "to": "now"}
    assert http("POST", f"{PUBLIC_GRAFANA}/api/ds/query", query)[0] != 200


@pytest.mark.parametrize("name, sql", panel_queries(DASHBOARDS / "provisioning" / "dashboards" / "nyc-taxi-live.json"))
def test_public_panel_queries_run_through_grafana(name, sql):
    """What a kiosk browser does every 5 s: anonymous, through Caddy, through the plugin."""
    query = {"queries": [{"refId": "A", "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"},
                          "rawSql": sql, "format": 1, "queryType": "table", "editorType": "sql"}],
             "from": "now-6h", "to": "now"}
    status, res = http("POST", f"{PUBLIC_GRAFANA}/api/ds/query", query)
    assert status == 200, name
    result = res["results"]["A"]
    assert "error" not in result, f"{name}: {result.get('error')}"


def test_zone_outlines_are_served_offline():
    status, geo = http("GET", f"{PUBLIC_GRAFANA}/public/maps/taxi_zones.geojson")
    assert status == 200 and geo["type"] == "FeatureCollection" and len(geo["features"]) >= 260


def test_ops_grafana_needs_a_login():
    assert http("GET", f"{OPS_GRAFANA}/api/search")[0] == 401
    assert http("GET", f"{OPS_GRAFANA}/api/dashboards/uid/stand-health")[0] == 401


def test_ops_grafana_serves_stand_health():
    status, home = http("GET", f"{OPS_GRAFANA}/api/dashboards/home", auth=True)
    assert status == 200 and home["dashboard"]["uid"] == "stand-health"
    status, health = http("GET", f"{OPS_GRAFANA}/api/datasources/uid/clickhouse-ops/health", auth=True)
    assert status == 200 and health["status"] == "OK"


def test_grafana_ops_is_read_only():
    ops = client("grafana_ops", "CLICKHOUSE_OPS_PASSWORD")
    for t in ["taxi.agent_log", "system.query_log", "system.parts", "taxi.trips", "taxi.sim_now"]:
        ops.query(f"SELECT * FROM {t} LIMIT 1")
    for sql in ["INSERT INTO taxi.agent_log (question) VALUES ('x')", "SELECT * FROM system.users",
                "DROP TABLE taxi.trips"]:
        with pytest.raises(DatabaseError):
            ops.query(sql)


def all_panel_queries() -> list[tuple[str, str, str, str]]:
    live = DASHBOARDS / "provisioning" / "dashboards" / "nyc-taxi-live.json"
    health = DASHBOARDS / "provisioning-ops" / "dashboards" / "stand-health.json"
    return ([("grafana_ro", "CLICKHOUSE_GRAFANA_PASSWORD", n, q) for n, q in panel_queries(live)]
            + [("grafana_ops", "CLICKHOUSE_OPS_PASSWORD", n, q) for n, q in panel_queries(health)])


@pytest.mark.parametrize("user, password_key, name, sql", all_panel_queries())
def test_panel_query_is_fast(user, password_key, name, sql):
    """Spec 05 acceptance 4: < 500 ms server-side on >= 7 backfilled sim days (median of 3, as its own user)."""
    admin_days = client("admin", "CLICKHOUSE_ADMIN_PASSWORD").query(
        "SELECT dateDiff('day', min(pickup_ts), max(pickup_ts)) FROM taxi.trips").result_rows[0][0]
    if admin_days < 7:
        pytest.skip(f"only {admin_days} sim days loaded; run make backfill first")
    c = client(user, password_key)
    elapsed = sorted(int(c.query(sql).summary["elapsed_ns"]) / 1e6 for _ in range(3))
    assert elapsed[1] < 500, f"{name}: {elapsed[1]:.0f} ms"
