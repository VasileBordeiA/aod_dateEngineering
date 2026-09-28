import pytest
import yaml
from conftest import GOLDEN

from agent.sql_guard import MAX_SQL_CHARS, PARSE_ERROR, check_sql

ATTACKS = [
    "DROP TABLE taxi.trips",
    "SELECT 1; DROP TABLE taxi.trips",
    "INSERT INTO taxi.trips SELECT * FROM taxi.trips",
    "SET readonly = 0",
    "SELECT * FROM system.users",
    "SELECT * FROM taxi.agent_log",
    "WITH x AS (SELECT * FROM system.tables) SELECT * FROM x",
    "SELECT * FROM taxi.trips UNION ALL SELECT * FROM system.users",
    "SELECT * FROM trips WHERE trip_id IN (SELECT name FROM system.tables)",
    "SELECT * FROM url('http://x/y', CSV)",
    "SELECT * FROM file('/etc/passwd')",
    "SELECT * FROM remote('localhost', system.users)",
    "SELECT * FROM numbers(1000000000000)",
    "SELECT sleep(3)",
    "SELECT getSetting('readonly')",
    "SELECT * FROM taxi.trips SETTINGS max_execution_time = 1000",
    "SELECT * FROM taxi.trips FORMAT Native",
    "SELECT * FROM taxi.trips INTO OUTFILE '/tmp/x'",
    "OPTIMIZE TABLE taxi.trips FINAL",
    "KILL QUERY WHERE 1",
    "SYSTEM SHUTDOWN",
    "SHOW TABLES",
    "EXPLAIN SELECT 1",
    # beyond the spec list
    "DESCRIBE taxi.trips",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM default.trips",
    "SELECT * FROM taxi.trips_rejected AS r JOIN system.one AS o ON 1 = 1",
    "SELECT currentUser()",
    "SELECT dictGet('d', 'x', 1)",
    "SELECT throwIf(1)",
    "SELECT * FROM s3('https://bucket/x.csv')",
    "SELECT count() FROM taxi.trips WHERE sleepEachRow(1) = 0",
    "SELECT * FROM (SELECT * FROM taxi.trips) SETTINGS max_threads = 64",
    "",
    "   ;  ",
]

MUST_PASS = [
    "WITH b AS (SELECT pu_borough, count() AS c FROM trips GROUP BY pu_borough) SELECT * FROM b ORDER BY c DESC",
    "SELECT pu_zone, row_number() OVER (PARTITION BY pu_borough ORDER BY fare_amount DESC) AS rn FROM taxi.trips",
    "SELECT quantile(0.95)(trip_distance) FROM taxi.trips",
    "SELECT arrayJoin(reasons) AS r, count() FROM taxi.trips_rejected GROUP BY r",
    "SELECT reason, count() FROM taxi.trips_rejected ARRAY JOIN reasons AS reason GROUP BY reason",
    "SELECT toStartOfHour(pickup_ts) AS h, count() FROM trips GROUP BY h ORDER BY h",
    "SELECT count() FROM taxi.trips WHERE pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 1 HOUR",
    "SELECT countIf(is_card) / count() FROM trips",
    "SELECT argMax(trips, emitted_at) FROM agg_borough_minute",
    "SELECT z.zone, count() FROM trips AS t INNER JOIN zones AS z ON z.location_id = t.pu_location_id GROUP BY z.zone",
    "SELECT stage, count() FROM trace_events GROUP BY stage",
    "select count() from TAXI.TRIPS",
    "SELECT count() FROM taxi.trips;",
    "SELECT 1 UNION ALL SELECT 2",
]


def golden_sql() -> list[tuple[str, str]]:
    out = []
    for q in yaml.safe_load(GOLDEN.read_text())["questions"]:
        sql = q["reference_sql"].replace("{trace_id_card}", "00000000-0000-0000-0000-000000000001")
        out.append((q["id"], sql.replace("{trace_id_cash}", "00000000-0000-0000-0000-000000000002")))
    return out


@pytest.mark.parametrize("sql", ATTACKS)
def test_attack_is_rejected(sql):
    r = check_sql(sql)
    assert not r.ok and r.reason and r.safe_sql == ""


@pytest.mark.parametrize("sql", MUST_PASS)
def test_valid_clickhouse_passes(sql):
    r = check_sql(sql)
    assert r.ok, r.reason


@pytest.mark.parametrize("qid, sql", golden_sql())
def test_every_reference_sql_passes(qid, sql):
    assert check_sql(sql).ok, qid


def test_safety_limit_wraps_the_original_sql():
    r = check_sql("SELECT count() FROM taxi.trips;\n")
    assert r.safe_sql == "SELECT * FROM (\nSELECT count() FROM taxi.trips\n) LIMIT 200"


def test_reasons_are_friendly():
    assert "system.users" in check_sql("SELECT * FROM system.users").reason
    assert "one statement" in check_sql("SELECT 1; SELECT 2").reason
    assert check_sql("SELEC count( FROM").reason == PARSE_ERROR
    assert "SETTINGS" in check_sql("SELECT 1 FROM trips SETTINGS max_threads = 1").reason


def test_length_limit():
    long_sql = "SELECT 1 FROM trips WHERE " + " OR ".join(["1 = 1"] * 2000)
    assert len(long_sql) > MAX_SQL_CHARS
    assert "long" in check_sql(long_sql).reason
