"""The DDL in infra/clickhouse/init must equal spec 04 and must apply cleanly (checked in embedded chDB)."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INIT = ROOT / "infra" / "clickhouse" / "init"
SPEC = ROOT / "docs" / "specs" / "04-clickhouse-schema.md"
AGENT_TABLES = ["trips", "trips_rejected", "agg_borough_minute", "zones", "trace_events"]  # + view sim_now (table COMMENT)


def spec_files() -> dict[str, str]:
    ddl = re.search(r"### DDL\n\n```sql\n(.*?)```", SPEC.read_text(), re.S).group(1)
    return {name: body.strip() + "\n"
            for name, body in re.findall(r"-- (\d\d_\w+\.sql)\n(.*?)(?=\n-- \d\d_\w+\.sql\n|\Z)", ddl, re.S)}


def statements() -> list[str]:
    out = []
    for f in sorted(INIT.glob("*.sql")):
        for chunk in re.split(r";\s*\n", f.read_text()):
            body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
            if body:
                out.append(body)
    return out


def test_init_files_equal_spec_04():
    expected = spec_files()
    assert sorted(expected) == sorted(p.name for p in INIT.glob("*.sql"))
    for name, body in expected.items():
        assert (INIT / name).read_text() == body, f"{name} differs from spec 04 — re-extract it"


@pytest.fixture(scope="module")
def ch():
    from chdb import session
    s = session.Session()
    for stmt in statements():
        if "ENGINE = Kafka" in stmt or "trace_events_mv" in stmt:
            continue  # needs a broker; covered by infra/tests/test_stack.py
        s.query(stmt)
    yield s
    s.close()


def q(s, sql: str) -> list[list[str]]:
    return [line.split("\t") for line in s.query(sql, "TSV").bytes().decode().strip().splitlines() if line]


def test_tables_exist(ch):
    names = {r[0] for r in q(ch, "SHOW TABLES FROM taxi")}
    assert names >= {"agent_log", "agg_borough_minute", "sim_now", "trace_events", "trips", "trips_rejected", "zones"}


def test_every_agent_visible_column_has_a_comment(ch):
    tables = ",".join(f"'{t}'" for t in AGENT_TABLES)
    assert int(q(ch, f"SELECT count() FROM system.columns WHERE database='taxi' AND table IN ({tables})")[0][0]) > 50
    missing = q(ch, f"SELECT table, name FROM system.columns WHERE database='taxi' AND table IN ({tables}) AND comment=''")
    assert missing == []


def test_sim_now_ignores_student_rides(ch):
    ch.query("""INSERT INTO taxi.trips (trip_id, source, loop, pickup_ts, dropoff_ts, pickup_hour, pickup_dow,
        trip_distance, duration_min, pu_location_id, pu_zone, pu_borough, pu_service_zone, do_location_id, do_zone,
        do_borough, do_service_zone, payment_type, payment_type_name, is_card, is_airport, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
        total_amount, produced_at, processed_at) VALUES
        ('r0-000000001','replay',0,'2025-10-07 08:00:00','2025-10-07 08:10:00',8,2,1,10,161,'a','Manhattan','Yellow Zone',
         237,'b','Manhattan','Yellow Zone',1,'Credit card',true,false,10,0,0.5,2,0,1,2.5,0,0.75,16.75,now64(3),now64(3)),
        ('s-00000000-0000-0000-0000-000000000001','student',0,'2025-10-09 08:00:00','2025-10-09 08:10:00',8,4,1,10,161,
         'a','Manhattan','Yellow Zone',237,'b','Manhattan','Yellow Zone',1,'Credit card',true,false,10,0,0.5,2,0,1,2.5,0,
         0.75,16.75,now64(3),now64(3))""")
    assert q(ch, "SELECT now_ts, today, this_hour FROM taxi.sim_now") == [
        ["2025-10-07 08:00:00.000", "2025-10-07", "2025-10-07 08:00:00"]]
