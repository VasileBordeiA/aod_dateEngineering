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
    # fix round 1: tokenizer errors (TokenError, not a ParseError subclass) must not escape check_sql
    "SELECT 'abc FROM trips",
    "SELECT 1 FROM trips /* unterminated",
    'SELECT "abc FROM trips',
    # fix round 1: deep nesting under MAX_SQL_CHARS blows the recursion limit while parsing
    "SELECT " + "(" * 2400 + "1" + ")" * 2400,
    # fix round 1: CTE-name shadowing must be scope-aware, not a whole-tree name collection
    "SELECT * FROM agent_log WHERE 1 IN (WITH agent_log AS (SELECT 1) SELECT * FROM agent_log)",
    "SELECT * FROM agent_log, (WITH agent_log AS (SELECT 1) SELECT * FROM agent_log) AS s",
    "SELECT * FROM agent_log UNION ALL (WITH agent_log AS (SELECT 1) SELECT * FROM agent_log)",
    "WITH agent_log AS (SELECT * FROM agent_log) SELECT * FROM agent_log",
    # fix round 2: a scalar WITH (`WITH <expr> AS name`) is a ClickHouse expression alias, never a
    # relation. Verified on the running ClickHouse: `WITH 1 AS nope SELECT * FROM nope` and
    # `WITH (SELECT 1) AS nope SELECT * FROM nope` both raise UNKNOWN_TABLE "nope" — so
    # `FROM agent_log` here can only ever resolve to the real, forbidden table.
    "WITH 1 AS agent_log SELECT * FROM agent_log",
    "WITH (SELECT 1) AS agent_log SELECT * FROM agent_log",
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
    # fix round 2: sibling CTEs in one WITH are mutually visible regardless of declaration order.
    # Verified on the running ClickHouse with a name that is not a real table:
    #   WITH a AS (SELECT * FROM nope), nope AS (SELECT 1 AS x) SELECT * FROM a  ->  returns 1
    # (a bare `SELECT * FROM nope` on its own raises UNKNOWN_TABLE, so this is not a real table
    # being reached; the forward reference to the sibling CTE resolves first). A plain backward
    # reference must of course still pass too.
    "WITH a AS (SELECT 1 AS x FROM trips), b AS (SELECT * FROM a) SELECT * FROM b",
    "WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x FROM trips) SELECT * FROM a",
    # Same mechanism with the forbidden name itself: verified directly against ClickHouse
    # (`--database taxi`) that `WITH a AS (SELECT * FROM agent_log), agent_log AS (SELECT 1 AS x)
    # SELECT * FROM a` returns column `x` with value 1 — i.e. it resolves to the sibling CTE, not
    # to the real (0-row) taxi.agent_log table, even though a real table of that name exists. The
    # query never reads agent_log; ClickHouse's own name resolution makes this safe.
    "WITH a AS (SELECT * FROM agent_log), agent_log AS (SELECT 1) SELECT * FROM a",
    # fix round 2: a WITH attached to a UNION is visible to every branch, not just the leftmost.
    # Verified on the running ClickHouse with a non-existent name:
    #   WITH nope AS (SELECT 1 AS a) SELECT * FROM nope UNION ALL SELECT * FROM nope  ->  1 \n 1
    # (both branches succeed; a lone `SELECT * FROM nope` errors UNKNOWN_TABLE, so both branches
    # really did resolve through the CTE, not a same-named real table).
    "WITH b AS (SELECT 1 AS x) SELECT * FROM b UNION ALL SELECT * FROM b",
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


def test_tokenizer_and_recursion_errors_become_parse_error():
    # TokenError (unterminated string / comment / quoted identifier) is not a ParseError subclass,
    # but a realistic input (e.g. an unescaped apostrophe in a zone name) must still surface the
    # friendly, repairable PARSE_ERROR rather than crash check_sql.
    assert check_sql("SELECT 'abc FROM trips").reason == PARSE_ERROR
    assert check_sql("SELECT 1 FROM trips /* unterminated").reason == PARSE_ERROR
    assert check_sql('SELECT "abc FROM trips').reason == PARSE_ERROR
    # Deep nesting under MAX_SQL_CHARS can blow the interpreter's recursion limit while parsing.
    deep_sql = "SELECT " + "(" * 2400 + "1" + ")" * 2400
    assert len(deep_sql) < MAX_SQL_CHARS
    assert check_sql(deep_sql).reason == PARSE_ERROR
