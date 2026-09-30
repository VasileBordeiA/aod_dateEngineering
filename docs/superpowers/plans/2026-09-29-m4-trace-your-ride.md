# M4 Trace Your Ride Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A student scans the QR code and books a pretend ride on `/ride` from their phone. They then watch it cross all 7 pipeline stages on `/trace/[id]`, with the wall-clock latency of every hop, in under 5 s. An absurd tip shows the amber `rejected` card. At `queryable`, a button hands the ride to the agent ("Where does my ride rank for tips today?"). The big screen `/trace/live` shows every student ride moving through the pipeline.

**Architecture:**
- **Already built in M1/M2. This plan only verifies it end to end.**
  - Processor checkpoints `validated|rejected`, `enriched`, `aggregated`, and `stored` (`services/processor/src/processor/checkpoints.py`, `sinks.py`).
  - The replayer's 0.1 % trace sampling and its `ingested` checkpoint (`services/replayer/src/replayer/messages.py`).
  - The `taxi.trace_events` Kafka engine + MV (`infra/clickhouse/init/50_trace_events.sql`).
  - The Grafana panels "Rides by visitors today" and "Pipeline latency per stage".
- **New in the api** (`services/api`): a third package, `rides`, for everything in spec 07.
  - Pure fare estimator + trip builder.
  - `RideService`, behind `POST /api/rides` and `GET /api/rides/estimate`.
  - An in-memory `TraceHistory` + `Hub` fan-out.
  - A `TraceRelay` consumer thread on `trace.checkpoints`.
  - A batched `QueryableDetector`.
  - `TraceService`, behind `WS /api/trace/{id}`, `WS /api/trace/live` and `GET /api/trace/{id}`.
  - One async `KafkaOut` producer for rides and `queryable` checkpoints.
- **New ClickHouse user `trace_ro`** (ADR-019). It has the same grants and profile as `agent_ro` but its own quota. It serves the api's own fixed, parameterised reads: estimates, the queryable poll, trace history, and the wall stats. Polling therefore never uses up the agent's 120 queries/min.
- **New in web:**
  - contract types generated from `contracts/`;
  - pure `lib/trace.ts`, `lib/ride.ts`, `lib/wall.ts`, `lib/wifi.ts`;
  - a `ReconnectingSocket`;
  - pages `/ride`, `/trace/[id]`, `/trace/live`, `/qr`, and the `/agent?trace_id=…&suggestion=ride_rank` hand-off.
- **Order change vs. the current spec 07 text:** `submitted` is produced *after* `trips.raw` is acknowledged, with `at` taken when the request was accepted. This is the only way to meet the spec's own "no `submitted` is left dangling" failure rule. Latency is computed from `at` and the UI orders by `seq`, so nothing downstream changes. Task 12 updates spec 07 and DIAGRAMS §4.

**Tech Stack:**
- API: Python 3.12 (uv), fastapi 0.141.1, uvicorn 0.54.0 (+ `websockets` for WS support), clickhouse-connect 1.9.0, confluent-kafka 2.15.1, pydantic (via fastapi).
- Tests: pytest 9.1.1 + anyio, FastAPI `TestClient`.
- Stack tests: `websockets` (root dev dependency).
- Web: Next.js 16.3.6, React 19.2.8, Tailwind 4.3.3, vitest 5.0.2, @playwright/test 1.63.0 (`page.routeWebSocket`), plus new `json-schema-to-typescript` (dev) and `qrcode.react`.

**Spec:**
- `docs/specs/07-trace-your-ride.md` (the main spec) and `docs/specs/08-web-frontend.md` (`/ride`, `/trace/[id]`, `/trace/live`, `/qr`, the `/agent` hand-off).
- Also `docs/specs/03-stream-processor.md` §Trace checkpoints, `docs/specs/04-clickhouse-schema.md` (users), `docs/specs/05-grafana-dashboards.md` (Stand Health), `docs/specs/06-ai-data-agent.md` (ride context), `docs/specs/10-testing-soak-and-evals.md`, `contracts/events/trace_checkpoint.schema.json`, `contracts/events/trip.schema.json`, and `CLAUDE.md`.

Executors read the specs alongside this plan.

## Before you start (executor)
- A permission rule currently blocks the Read/Edit tools on `services/api/src/api/config.py` and on everything under `infra/clickhouse/`. The user says these are ordinary code files, so the rule is unintended.
  - **Before Task 1**, check that you can Read `services/api/src/api/config.py`.
  - If you can't, stop and ask the user to lift the rule (`/permissions`).
  - **Never** write those files through Bash to get around the rule.
- The D10 go/no-go (2 h unattended core run) has not been run yet. The user chose to start M4 anyway. Don't block on it.

## Global Constraints
- Python `>=3.12,<3.13`. Dependencies are pinned with `==` and locked by `uv.lock`. JS dependencies are pinned exactly (`pnpm add -E`). No `latest` image tags. Every new version is recorded in `docs/ARCHITECTURE.md` §9.
- Names are fixed:
  - topics `trips.raw`, `trace.checkpoints`, `sim.clock`;
  - tables `taxi.trips`, `taxi.trips_rejected`, `taxi.trace_events`, `taxi.zones`, and the view `taxi.sim_now`.
- Stages, their `seq` and emitting service are exactly: `submitted`=1 (api), `ingested`=2 (api/replayer), `validated`=3, `rejected`=3 (terminal), `enriched`=4, `aggregated`=5, `stored`=6 (processor), `queryable`=7 (api).
- Every checkpoint validates against `contracts/events/trace_checkpoint.schema.json`, and every student trip against `trip.schema.json`. `at` is `YYYY-MM-DDTHH:MM:SS.mmmZ`.
- Two clocks:
  - `pickup_ts`/`dropoff_ts` are sim time: naive NYC local, whole seconds.
  - Every latency is computed from `at` (wall, UTC) only, never from sim time.
  - "Rides today" on the wall is the wall-clock day, the same as Grafana panel 11.
- Validation for `POST /api/rides` (422 names the field):
  - `pickup_location_id`/`dropoff_location_id`: integers 1–263;
  - `payment`: `"card"|"cash"`;
  - `tip_amount`: a finite number 0–999 with ≤ 2 decimals;
  - `passenger_count`: an integer 1–6.
  - Absurd tips are **allowed**: the processor rejects them.
- Amounts are verbatim from spec 07:
  - `mta_tax` 0.50, `improvement_surcharge` 1.00;
  - `congestion_surcharge` 2.50 and `cbd_congestion_fee` 0.75 if either zone is `Yellow Zone`;
  - `airport_fee` 1.75 if pickup ∈ {132, 138}; `extra`/`tolls_amount` 0;
  - cash → `tip_amount` 0 and `payment_type` 2; card → `payment_type` 1; `ratecode_id` 1.
- Fallback estimate:
  - `distance = max(0.5, 1.3 × haversine_miles)`;
  - `duration = distance / 12 × 60 + 3`;
  - `fare = 3.00 + 3.50 × distance + 0.30 × duration`.
  - The median is used only when `n ≥ 5`. The LRU holds 2 000 zone pairs.
- Limits from env:
  - `RIDES_RATE_LIMIT_PER_MIN=3` (per IP; the 4th ride within a minute gets 429);
  - `RIDES_GLOBAL_PER_MIN=30` (new);
  - `TRACE_HISTORY_TTL_S=3600`, 20 000 traces max;
  - `TRACE_QUERYABLE_POLL_MS=250`, with the poll giving up after 10 s and at most 20 traces in flight.
  - The broker ack waits ≤ 2 s, then 503.
  - WS `/api/trace/{id}` closes after 10 min, or 60 s after `queryable`/`rejected`.
- Copy is verbatim from spec 07/08. For example:
  - "The dispatcher is busy, try again in a few seconds"
  - "The pipeline is catching its breath, try again in a moment"
  - "NYC taxi data doesn't record cash tips, so your tip will show as $0"
  - "Cash tips aren't recorded in NYC taxi data"
  - "Where does my ride rank for tips today?"
  - "From your phone to queryable in {s} s"
- ClickHouse users:
  - the agent's SQL stays `agent_ro`-only and guarded (CLAUDE.md invariant 4);
  - the api's own fixed reads use `trace_ro`, always with server-side parameters (`{name:Type}`), never string formatting.
- Visitor safety:
  - no names or free-text fields;
  - the zone search text never leaves the phone;
  - never log client IPs (only the in-memory rate-limit key).
- Logs are JSON lines, `service="api"`: `ride_created` (trace_id, method, payment), `queryable_timeout`, `ride_broker_down`, `trace_relay_error`.
- Web:
  - TypeScript strict; dark theme;
  - phone pages have a 16 px base font, ≥ 44 px tap targets, and no horizontal scroll at 360 px;
  - `/ride` first-load JS ≤ 150 kB gzipped;
  - purple = core pipeline, green = visitor-facing, amber for rejection (never red);
  - borough colours come from `lib/palette.ts`;
  - `prefers-reduced-motion` disables the travelling dot.
- Offline: nothing new calls the internet. QR codes are generated locally.

## Review Focus
1. **The broker is down or slow when a student books.**
   - `POST /api/rides` answers 503 "The pipeline is catching its breath…" within about 2 s, and emits no checkpoint.
   - A ride that got a 503 is never delivered later: the producer's `message.timeout.ms` is ≤ the 2 s wait.
   - Pinned by Task 3 `test_rides.py::test_broker_failure_is_503_and_emits_nothing` and `test_kafka.py::test_producer_gives_up_before_the_api_stops_waiting`.
2. **A phone locks its screen, or the tab closes mid-trace.** The server drops the WebSocket subscription at once, instead of holding it for 10 min. Pinned by Task 5 `test_trace_service.py::test_disconnect_releases_the_subscription`.
3. **A cash ride.**
   - `tip_amount` is recorded as 0 and the note is returned.
   - The enriched card never shows "null%".
   - The ride chip on `/agent` says "cash".
   - Pinned by Task 2 `test_estimate.py::test_amounts_cash_records_no_tip`, and by Task 7 `trace.test.ts` "enriched copy for a cash ride never shows null%" and "rideChip for cash".
4. **The api restarts mid-trace.** Memory then has only the checkpoints seen after the restart.
   - The history merges `taxi.trace_events` with memory and dedups by stage.
   - A trace restarted between `stored` and `queryable` gets its queryable poll re-armed.
   - Pinned by Task 5 `test_trace_service.py::test_history_merges_clickhouse_and_memory` and `::test_history_rearms_queryable_after_restart`.
5. **Unusual zone pairs:**
   - a zone without a centroid (264/265, duplicate shapefile ids);
   - a pair with < 5 real trips;
   - real medians under a minute or 0.1 mi.
   - The ride must still pass all 11 processor rules: distance > 0, duration ≥ 1 min so dropoff > pickup, fare > 0.
   - Pinned by Task 2 `test_estimate.py::test_estimate_is_always_a_valid_trip` and `test_trip.py::test_dropoff_after_pickup`.

---

## File map

```
services/api/
  pyproject.toml                 + websockets, + package src/rides
  src/api/config.py              + trace_user/password, rides_*, trace_* settings
  src/api/db.py                  + TraceDb protocol, ClickHouseTraceDb (trace_ro, parameterised)
  src/api/simclock.py            + fresh_sim_ts()
  src/api/kafka.py               KafkaOut: async send() with delivery report, send_nowait()
  src/api/main.py                + /api/rides, /api/rides/estimate, /api/trace/{id} (GET + WS), /api/trace/live, wiring
  src/rides/__init__.py
  src/rides/checkpoints.py       SEQ, TERMINAL, checkpoint(), wall_str(), parse_wall(), parse_checkpoint()
  src/rides/estimate.py          Zone, Estimate, Amounts, haversine_miles, from_median, from_centroids, amounts, ride_notes (pure)
  src/rides/trip.py              build_trip (pure)
  src/rides/service.py           RideService, RideRequest, RideRejected
  src/rides/history.py           TraceHistory (TTL + LRU, dedup by stage)
  src/rides/hub.py               Hub: per-trace and live fan-out queues, on_stored hook
  src/rides/relay.py             TraceRelay: trace.checkpoints consumer thread
  src/rides/queryable.py         QueryableDetector: batched trace_ro poll → queryable
  src/rides/trace_service.py     TraceService (history, follow, live, stats), serve()
  tests/conftest.py              + FakeTraceDb, FakeOut, BASE_ENV trace password
  tests/test_estimate.py test_trip.py test_kafka.py test_rides.py test_history.py test_hub.py
  tests/test_relay.py test_queryable.py test_trace_service.py
infra/clickhouse/users.d/stand.xml   + quota "trace", user trace_ro
infra/grafana/provisioning-ops/dashboards/stand-health.json   + 2 visitor-ride panels
infra/tests/test_stack.py        + trace_ro grants
infra/tests/test_rides_e2e.py    spec 07 acceptance 3-6 against the running stack (marker e2e)
scripts/burst_rides.py           spec 07 acceptance 7 (make burst)
scripts/eval_agent.py            ride-context questions now book real rides
docker-compose.yml               clickhouse gets CLICKHOUSE_TRACE_PASSWORD; web gets STAND_* env
.env.example                     + CLICKHOUSE_TRACE_USER/PASSWORD, RIDES_GLOBAL_PER_MIN, NEXT_PUBLIC_API_WS_ORIGIN
Makefile                         + test-e2e, burst, reset-rides
web/
  package.json                   + gen:types, qrcode.react, json-schema-to-typescript
  scripts/gen-types.mjs          contracts → lib/contracts/*.ts
  lib/contracts/trace-checkpoint.ts lib/contracts/trip.ts   (generated, committed)
  lib/trace.ts                   stages, copy, merge, hop latency, messages, ride summary (pure)
  lib/ws.ts                      wsUrl, backoffDelay, ReconnectingSocket
  lib/ride.ts                    popular zones, grouping/filter, tip maths, estimate line, last-trace storage
  lib/wall.ts                    /trace/live state (pure)
  lib/wifi.ts                    WIFI: QR payload with escaping
  lib/format.ts                  + formatSim (moved from components/agent/Header.tsx)
  lib/use-poll.ts                usePoll (moved from Header.tsx)
  lib/api.ts                     + zones, estimate, bookRide, getTrace
  app/ride/page.tsx  components/ride/{RideForm,ZonePicker,TipChips,PassengerStepper,EstimateCard}.tsx
  app/trace/[id]/page.tsx  components/trace/{TraceViewer,StageCard}.tsx
  app/trace/live/page.tsx  components/trace/LiveWall.tsx
  app/qr/page.tsx  components/QrCode.tsx
  app/agent/page.tsx  components/agent/{AgentKiosk,RidePrompt}.tsx
  app/page.tsx                   + "Trace your ride" tile
  app/globals.css                + phone base font, packet/spinner animations
  playwright.config.ts           desktop + iPhone 13 + Pixel 7 projects
  tests/unit/{trace,ws,ride,wall,wifi}.test.ts
  tests/e2e/{ride-mocks.ts,ride.spec.ts,trace.spec.ts,live.spec.ts,qr.spec.ts} + agent.spec.ts (hand-off)
docs/  DECISIONS (ADR-019), specs 04/05/07/08, DIAGRAMS §4, ARCHITECTURE §7/§9, RUNBOOK, IMPLEMENTATION_PLAN
```

---

### Task 1: `trace_ro` user, api config, `TraceDb`, fresh sim clock

**Files:**
- Modify: `infra/clickhouse/users.d/stand.xml`
- Modify: `docker-compose.yml` (clickhouse `environment`)
- Modify: `.env.example`
- Modify: `services/api/src/api/config.py`
- Modify: `services/api/src/api/db.py`
- Modify: `services/api/src/api/simclock.py`
- Modify: `services/api/tests/conftest.py`
- Test: `services/api/tests/test_config.py`, `services/api/tests/test_db.py`, `services/api/tests/test_simclock.py`, `infra/tests/test_stack.py`

**Interfaces:**
- Produces:
  - `Config` gets these fields: `trace_user: str`, `trace_password: str`, `rides_rate_limit_per_min: int`, `rides_global_per_min: int`, `trace_history_ttl_s: float`, `trace_queryable_poll_ms: int`.
  - `api.db.TraceDb` (Protocol): `async def query(self, sql: str, params: dict[str, Any]) -> Result`.
  - `api.db.ClickHouseTraceDb(cfg)` implements it as `trace_ro`. `DbError`/`DbUnavailable` have the same meaning as in `ClickHouseDb`.
  - `SimClockFollower.fresh_sim_ts(max_age_s: float = 10.0) -> str | None`.
  - `SimClockFollower.__init__(brokers, now: Callable[[], datetime] = lambda: datetime.now(UTC))`.
  - `conftest.FakeTraceDb`: `.results: dict[str, Result]`, `.calls: list[tuple[str, dict]]`, `.fail`.

- [ ] **Step 1: Write the failing tests**

Add to `services/api/tests/test_config.py`:

```python
def test_ride_and_trace_settings_have_spec_defaults():
    cfg = make_config()
    assert cfg.trace_user == "trace_ro" and cfg.trace_password == "t"
    assert cfg.rides_rate_limit_per_min == 3 and cfg.rides_global_per_min == 30
    assert cfg.trace_history_ttl_s == 3600 and cfg.trace_queryable_poll_ms == 250


def test_trace_password_is_required():
    env = {k: v for k, v in BASE_ENV.items() if k != "CLICKHOUSE_TRACE_PASSWORD"}
    with pytest.raises(ValueError, match="CLICKHOUSE_TRACE_PASSWORD"):
        Config.from_env(env)
```

(Import `BASE_ENV`, `make_config` from `conftest`, `Config` from `api.config`, and `pytest`, matching the file's existing imports.)

Add to `services/api/tests/test_db.py`:

```python
from types import SimpleNamespace

from api.db import ClickHouseTraceDb


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def query(self, sql: str, parameters: object = None) -> SimpleNamespace:
        self.calls.append((sql, parameters))
        return SimpleNamespace(column_names=("n",), column_types=(SimpleNamespace(name="UInt64"),), result_rows=[(2,)])


@pytest.mark.anyio
async def test_trace_db_sends_server_side_parameters():
    db = ClickHouseTraceDb(make_config())
    db._client = _Recorder()
    r = await db.query("SELECT count() AS n FROM taxi.trips WHERE trace_id = {id:UUID}", {"id": "00000000-0000-0000-0000-000000000001"})
    assert r.rows == [[2]] and r.names == ["n"]
    assert db._client.calls == [("SELECT count() AS n FROM taxi.trips WHERE trace_id = {id:UUID}",
                                 {"id": "00000000-0000-0000-0000-000000000001"})]


@pytest.mark.anyio
async def test_trace_db_operational_error_resets_client():
    db = ClickHouseTraceDb(make_config())
    db._client = _Boom(OperationalError("down"))
    with pytest.raises(DbUnavailable):
        await db.query("SELECT 1", {})
    assert db._client is None
```

`_Boom.query` in this file takes only `sql`. Change its signature to `def query(self, sql: str, parameters: object = None) -> None:` so both clients can use it.

Add to `services/api/tests/test_simclock.py`:

```python
from datetime import UTC, datetime


def test_fresh_sim_ts_only_while_heartbeats_are_recent():
    now = datetime(2026, 9, 28, 12, 0, 5, tzinfo=UTC)
    f = SimClockFollower("unused:9092", now=lambda: now)
    assert f.fresh_sim_ts() is None
    f.handle(json.dumps(MSG).encode())  # at = 12:00:00.000Z, 5 s old
    assert f.fresh_sim_ts() == "2025-10-07T08:00:00"
    now = datetime(2026, 9, 28, 12, 0, 11, tzinfo=UTC)  # 11 s old
    assert f.fresh_sim_ts() is None


def test_fresh_sim_ts_ignores_a_heartbeat_without_a_valid_at():
    f = SimClockFollower("unused:9092", now=lambda: datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC))
    f.handle(json.dumps({**MSG, "at": "yesterday"}).encode())
    assert f.fresh_sim_ts() is None
```

(`now` is rebound after the follower is created, but the lambda reads the variable at call time, so the second assert sees 11 s. That's intended.)

Add to `infra/tests/test_stack.py` after `test_agent_ro_can_read_whitelist`:

```python
@pytest.fixture(scope="module")
def trace_ro():
    return client("trace_ro", "CLICKHOUSE_TRACE_PASSWORD")


def test_trace_ro_reads_the_agent_whitelist_with_parameters(trace_ro):
    for t in ["trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"]:
        trace_ro.query(f"SELECT * FROM taxi.{t} LIMIT 1")
    r = trace_ro.query("SELECT count() FROM taxi.trips WHERE trace_id = {id:UUID}",
                       parameters={"id": str(uuid.uuid4())})
    assert r.result_rows == [(0,)]


@pytest.mark.parametrize("sql", ["SELECT count() FROM taxi.agent_log", "SELECT count() FROM system.query_log",
                                 "INSERT INTO taxi.trips (trip_id) VALUES ('x')"])
def test_trace_ro_is_read_only_and_cannot_read_visitor_text(trace_ro, sql):
    with pytest.raises(DatabaseError):
        trace_ro.query(sql)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/api && uv run pytest -q tests/test_config.py tests/test_db.py tests/test_simclock.py`
Expected: FAIL: `AttributeError: 'Config' object has no attribute 'trace_user'`, `ImportError: cannot import name 'ClickHouseTraceDb'`, and `TypeError: ... unexpected keyword argument 'now'`.

- [ ] **Step 3: Implement**

`services/api/tests/conftest.py`: add `"CLICKHOUSE_TRACE_PASSWORD": "t"` to `BASE_ENV`, and append:

```python
class FakeTraceDb:
    """query() answers by the first `results` key that is a substring of the SQL; records (sql, params)."""

    def __init__(self, results: dict[str, Result] | None = None) -> None:
        self.results = results or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail: Exception | None = None

    async def query(self, sql: str, params: dict[str, Any]) -> Result:
        self.calls.append((sql, params))
        if self.fail is not None:
            raise self.fail
        for needle, result in self.results.items():
            if needle in sql:
                return result
        return Result(columns=[], rows=[], elapsed_ms=1)  # "nothing found", like an empty table
```

`services/api/src/api/config.py`: add the six fields at the end of `Config` (after `golden_path`), and these to `from_env`:

```python
            trace_user=get("CLICKHOUSE_TRACE_USER", "trace_ro"),
            trace_password=get("CLICKHOUSE_TRACE_PASSWORD"),
            rides_rate_limit_per_min=int(get("RIDES_RATE_LIMIT_PER_MIN", "3")),
            rides_global_per_min=int(get("RIDES_GLOBAL_PER_MIN", "30")),
            trace_history_ttl_s=float(get("TRACE_HISTORY_TTL_S", "3600")),
            trace_queryable_poll_ms=int(get("TRACE_QUERYABLE_POLL_MS", "250")),
```

`services/api/src/api/db.py`: extract the result conversion from `_select_sync` into a module function, then add the trace client:

```python
def _to_result(r: Any, started: float) -> Result:
    return Result(columns=list(zip(r.column_names, [t.name for t in r.column_types], strict=True)),
                  rows=[[jsonable(v) for v in row] for row in r.result_rows],
                  elapsed_ms=int((time.perf_counter() - started) * 1000))


class TraceDb(Protocol):
    async def query(self, sql: str, params: dict[str, Any]) -> Result: ...


class ClickHouseTraceDb:
    """trace_ro (ADR-019): the api's own fixed, parameterised reads (estimates, queryable poll, trace history).
    Never the agent's SQL. Same grants as agent_ro, so "visible here" means "visible to the agent"."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._client: Any = None
        self._lock = threading.Lock()

    def _query_sync(self, sql: str, params: dict[str, Any]) -> Result:
        started = time.perf_counter()
        try:
            with self._lock:
                if self._client is None:
                    self._client = clickhouse_connect.get_client(
                        host=self._cfg.ch_host, port=self._cfg.ch_port, username=self._cfg.trace_user,
                        password=self._cfg.trace_password, database=self._cfg.ch_db,
                        autogenerate_session_id=False, connect_timeout=3)
                client = self._client
            r = client.query(sql, parameters=params)
        except OperationalError as exc:
            with self._lock:
                self._client = None
            raise DbUnavailable(clickhouse_message(exc)) from exc
        except DatabaseError as exc:
            raise DbError(clickhouse_message(exc)) from exc
        return _to_result(r, started)

    async def query(self, sql: str, params: dict[str, Any]) -> Result:
        return await asyncio.to_thread(self._query_sync, sql, params)
```

In `ClickHouseDb._select_sync`, replace the `return Result(...)` with `return _to_result(r, started)`.

`services/api/src/api/simclock.py`: add the imports `from collections.abc import Callable` and `from datetime import UTC, datetime`, then:

```python
    def __init__(self, brokers: str, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._brokers = brokers
        self._now = now
        # (the existing attributes stay as they are)

    def fresh_sim_ts(self, max_age_s: float = 10.0) -> str | None:
        """sim_ts of the latest heartbeat if its wall `at` is at most max_age_s old (spec 07 §Ride creation step 2)."""
        msg = self.latest
        if msg is None:
            return None
        try:
            at = datetime.strptime(str(msg["at"]), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        except (KeyError, ValueError):
            return None
        if (self._now() - at).total_seconds() > max_age_s:
            return None
        return str(msg["sim_ts"])
```

`infra/clickhouse/users.d/stand.xml`: add a quota next to `<agent>` in `<quotas>`:

```xml
        <trace>
            <interval>
                <duration>60</duration>
                <queries>1200</queries>
                <errors>600</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>120</execution_time>
            </interval>
        </trace>
```

Add a user after `<agent_ro>`:

```xml
        <!-- The api's own fixed, parameterised reads: ride estimates, the queryable poller, trace history, wall stats.
             Same profile and grants as agent_ro (so "visible to trace_ro" = "visible to the agent"), but its own
             quota: polling must never use up the agent's 120 queries/min (ADR-019). -->
        <trace_ro>
            <password from_env="CLICKHOUSE_TRACE_PASSWORD"/>
            <profile>agent</profile>
            <quota>trace</quota>
            <networks><ip>::/0</ip></networks>
            <grants>
                <query>GRANT SELECT ON taxi.trips</query>
                <query>GRANT SELECT ON taxi.trips_rejected</query>
                <query>GRANT SELECT ON taxi.agg_borough_minute</query>
                <query>GRANT SELECT ON taxi.zones</query>
                <query>GRANT SELECT ON taxi.sim_now</query>
                <query>GRANT SELECT ON taxi.trace_events</query>
            </grants>
        </trace_ro>
```

`docker-compose.yml`, clickhouse `environment`: add `CLICKHOUSE_TRACE_PASSWORD: ${CLICKHOUSE_TRACE_PASSWORD:?run make secrets}`. (The api already gets every key through `env_file: [.env]`.)

`.env.example`: in the ClickHouse block after the agent lines, add:

```
# The api's own reads (estimates, queryable poll, trace history): agent_ro's grants, its own quota (ADR-019)
CLICKHOUSE_TRACE_USER=trace_ro
CLICKHOUSE_TRACE_PASSWORD=
```

In the "Trace your ride [07]" block, add:

```
# Rides per minute across all phones together (the per-IP limit is RIDES_RATE_LIMIT_PER_MIN)
RIDES_GLOBAL_PER_MIN=30
```

`scripts/fill_secrets.py` already fills any empty `CLICKHOUSE_*_PASSWORD`, and `make secrets` adds missing keys from the template.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/api && uv run pytest -q && uv run ruff check src tests && uv run mypy src`
Expected: all pass, no lint or type errors.

Then, on the running stack:
Run: `make secrets && docker compose up -d --force-recreate clickhouse api && uv run pytest -q -m stack infra/tests/test_stack.py -k "trace_ro"`
Expected: 4 passed. `users.d` is read at start, so `--force-recreate clickhouse` is needed. The DDL volume is untouched.

- [ ] **Step 5: Commit**

```bash
git add infra/clickhouse/users.d/stand.xml docker-compose.yml .env.example services/api/src/api/config.py \
  services/api/src/api/db.py services/api/src/api/simclock.py services/api/tests infra/tests/test_stack.py
git commit -m "feat(api): trace_ro ClickHouse user, ride/trace settings, parameterised TraceDb, fresh sim clock"
```

---

### Task 2: Fare estimator, amounts and trip builder (pure, TDD)

**Files:**
- Modify: `services/api/pyproject.toml` (`[tool.hatch.build.targets.wheel] packages` += `"src/rides"`)
- Create: `services/api/src/rides/__init__.py` (empty)
- Create: `services/api/src/rides/checkpoints.py`
- Create: `services/api/src/rides/estimate.py`
- Create: `services/api/src/rides/trip.py`
- Test: `services/api/tests/test_estimate.py`, `services/api/tests/test_trip.py`, `services/api/tests/test_checkpoints.py`

**Interfaces:**
- Produces (`rides.checkpoints`):
  - `SEQ: dict[str, int]`, `TERMINAL: frozenset[str]`, `TRACE_TOPIC = "trace.checkpoints"`.
  - `wall_str(dt: datetime) -> str`, `parse_wall(s: str) -> datetime`.
  - `checkpoint(trace_id: str, trip_id: str, source: str, stage: str, detail: dict[str, Any], at: str, service: str = "api") -> dict[str, Any]`.
  - `ordered(cps: Iterable[dict]) -> list[dict]` (by `seq`, then `at`).
  - `parse_checkpoint(value: bytes | None) -> dict[str, Any] | None`.
- Produces (`rides.estimate`):
  - `Zone(location_id, zone, borough, service_zone, lat, lon)` and `Estimate(trip_distance, duration_min, fare_amount, method)`.
  - `Amounts(fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount, payment_type)`.
  - `haversine_miles(lat1, lon1, lat2, lon2) -> float`.
  - `from_median(n, distance, duration, fare) -> Estimate | None` and `from_centroids(pu: Zone, do: Zone) -> Estimate`.
  - `amounts(est, pu, do, payment: str, tip: float) -> Amounts` and `ride_notes(payment: str) -> list[str]`.
- Produces (`rides.trip`): `build_trip(*, trace_id: str, pickup_ts: datetime, est: Estimate, amt: Amounts, pu_location_id: int, do_location_id: int, passenger_count: int, produced_at: str) -> dict[str, Any]`.

- [ ] **Step 1: Write the failing tests**

`services/api/tests/test_checkpoints.py`:

```python
import json
from datetime import UTC, datetime

from contract_validator import assert_valid

from rides.checkpoints import SEQ, checkpoint, ordered, parse_checkpoint, parse_wall, wall_str

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"


def test_wall_str_round_trips_with_milliseconds():
    dt = datetime(2026, 10, 16, 10, 31, 7, 41_999, tzinfo=UTC)
    assert wall_str(dt) == "2026-10-16T10:31:07.041Z"
    assert parse_wall("2026-10-16T10:31:07.041Z") == datetime(2026, 10, 16, 10, 31, 7, 41_000, tzinfo=UTC)


def test_checkpoint_follows_the_contract():
    cp = checkpoint(TID, f"s-{TID}", "student", "submitted", {"payment": "card"}, "2026-10-16T10:31:07.000Z")
    assert cp["seq"] == 1 and cp["service"] == "api"
    assert_valid("trace_checkpoint", cp)


def test_seq_matches_the_contract():
    assert SEQ == {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
                   "stored": 6, "queryable": 7}


def test_ordered_by_seq_then_at():
    a = {"stage": "stored", "seq": 6, "at": "2026-10-16T10:31:08.000Z"}
    b = {"stage": "submitted", "seq": 1, "at": "2026-10-16T10:31:09.000Z"}
    assert ordered([a, b]) == [b, a]


def test_parse_checkpoint_accepts_good_and_ignores_garbage():
    good = checkpoint(TID, f"s-{TID}", "student", "ingested", {}, "2026-10-16T10:31:07.000Z")
    assert parse_checkpoint(json.dumps(good).encode()) == good
    for bad in [b"", b"not json", b"[1]", json.dumps({**good, "stage": "teleported"}).encode(),
                json.dumps({**good, "seq": "2"}).encode(), json.dumps({k: v for k, v in good.items() if k != "at"}).encode(),
                None]:
        assert parse_checkpoint(bad) is None
```

`services/api/tests/test_estimate.py`:

```python
import pytest

from rides.estimate import Estimate, Zone, amounts, from_centroids, from_median, haversine_miles, ride_notes

TIMES_SQ = Zone(230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985)
JFK = Zone(132, "JFK Airport", "Queens", "Airports", 40.641, -73.779)
ASTORIA = Zone(7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920)
NOWHERE = Zone(103, "Governor's Island/Ellis Island/Liberty Island", "Manhattan", "Yellow Zone", None, None)


def test_median_needs_five_trips():
    assert from_median(4, 3.1, 16.5, 19.5) is None
    assert from_median(5, None, 16.5, 19.5) is None  # quantile over zero rows is NaN → None
    est = from_median(214, 3.1, 16.54, 19.5)
    assert est == Estimate(3.1, 16.5, 19.5, "median of 214 real trips between these zones")


def test_haversine_one_degree_of_latitude():
    assert haversine_miles(0.0, 0.0, 1.0, 0.0) == pytest.approx(69.09, rel=1e-3)


def test_centroid_fallback_minimum_distance():
    same = Zone(7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920)
    est = from_centroids(ASTORIA, same)
    assert est.trip_distance == 0.5  # max(0.5, 1.3 × 0)
    assert est.duration_min == 5.5   # 0.5 / 12 × 60 + 3
    assert est.fare_amount == 6.4    # 3.00 + 3.50 × 0.5 + 0.30 × 5.5
    assert est.method.startswith("straight-line distance × 1.3")


def test_centroid_fallback_scales_with_distance():
    est = from_centroids(TIMES_SQ, JFK)
    d = 1.3 * haversine_miles(TIMES_SQ.lat, TIMES_SQ.lon, JFK.lat, JFK.lon)  # type: ignore[arg-type]
    assert est.trip_distance == round(d, 2)
    assert est.duration_min == round(d / 12 * 60 + 3, 1)
    assert est.fare_amount == round(3.0 + 3.5 * d + 0.3 * (d / 12 * 60 + 3), 2)


def test_zone_without_centroid_uses_a_typical_trip():
    est = from_centroids(NOWHERE, JFK)
    assert est.trip_distance == 2.5 and est.method == "typical NYC trip (no map point for this zone)"


def test_estimate_is_always_a_valid_trip():
    tiny = from_median(10, 0.0, 0.2, 0.0)
    assert tiny is not None
    assert tiny.trip_distance == 0.1 and tiny.duration_min == 1.0 and tiny.fare_amount == 3.0


def test_amounts_card_yellow_zone_airport_pickup_matches_the_spec_example():
    est = Estimate(3.1, 16.5, 19.5, "median of 214 real trips between these zones")
    a = amounts(est, JFK, TIMES_SQ, "card", 5.0)
    assert (a.mta_tax, a.improvement_surcharge, a.congestion_surcharge, a.cbd_congestion_fee) == (0.5, 1.0, 2.5, 0.75)
    assert a.airport_fee == 1.75 and a.extra == 0.0 and a.tolls_amount == 0.0
    assert a.tip_amount == 5.0 and a.payment_type == 1
    assert a.total_amount == 31.0  # 19.5 + 0.5 + 5 + 1 + 2.5 + 1.75 + 0.75


def test_amounts_spec_example_total():
    est = Estimate(3.1, 16.5, 19.5, "x")
    assert amounts(est, TIMES_SQ, JFK, "card", 5.0).total_amount == 29.25  # spec 07 response example


def test_amounts_cash_records_no_tip():
    a = amounts(Estimate(3.1, 16.5, 19.5, "x"), ASTORIA, JFK, "cash", 5.0)
    assert a.tip_amount == 0.0 and a.payment_type == 2
    assert a.congestion_surcharge == 0.0 and a.cbd_congestion_fee == 0.0 and a.airport_fee == 0.0
    assert a.total_amount == 21.0  # 19.5 + 0.5 + 1.0
    assert ride_notes("cash") == ["NYC taxi data doesn't record cash tips, so your tip will show as $0"]
    assert ride_notes("card") == []
```

`services/api/tests/test_trip.py`:

```python
from datetime import datetime

from contract_validator import assert_valid

from rides.estimate import Estimate, Zone, amounts
from rides.trip import build_trip

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
PU = Zone(230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985)
DO = Zone(132, "JFK Airport", "Queens", "Airports", 40.641, -73.779)


def trip(duration: float = 16.5) -> dict:
    est = Estimate(3.1, duration, 19.5, "x")
    return build_trip(trace_id=TID, pickup_ts=datetime(2025, 10, 9, 17, 42, 0), est=est,
                      amt=amounts(est, PU, DO, "card", 5.0), pu_location_id=230, do_location_id=132,
                      passenger_count=2, produced_at="2026-10-16T10:31:07.041Z")


def test_trip_follows_the_contract():
    t = trip()
    assert_valid("trip", t)
    assert t["trip_id"] == f"s-{TID}" and t["source"] == "student" and t["trace_id"] == TID and t["loop"] == 0
    assert t["pickup_ts"] == "2025-10-09T17:42:00" and t["passenger_count"] == 2 and t["ratecode_id"] == 1
    assert t["payment_type"] == 1 and t["tip_amount"] == 5.0 and t["total_amount"] == 29.25


def test_dropoff_after_pickup():
    assert trip(16.5)["dropoff_ts"] == "2025-10-09T17:58:30"
    assert trip(1.0)["dropoff_ts"] == "2025-10-09T17:43:00"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/api && uv run pytest -q tests/test_checkpoints.py tests/test_estimate.py tests/test_trip.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'rides'`.

- [ ] **Step 3: Implement**

`services/api/pyproject.toml`: `packages = ["src/api", "src/agent", "src/rides"]`, then `cd services/api && uv sync`.

`services/api/src/rides/checkpoints.py`:

```python
"""Trace checkpoints (contracts/events/trace_checkpoint.schema.json, spec 07 §Checkpoint catalogue)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

TRACE_TOPIC = "trace.checkpoints"
SEQ = {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
       "stored": 6, "queryable": 7}
TERMINAL = frozenset({"queryable", "rejected"})


def wall_str(dt: datetime) -> str:
    utc = dt.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def parse_wall(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def checkpoint(trace_id: str, trip_id: str, source: str, stage: str, detail: dict[str, Any], at: str,
               service: str = "api") -> dict[str, Any]:
    return {"trace_id": trace_id, "trip_id": trip_id, "source": source, "stage": stage, "seq": SEQ[stage],
            "service": service, "at": at, "detail": detail}


def ordered(cps: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Checkpoints can arrive out of order on the topic, so everything sorts by seq (spec 07 §Ordering)."""
    return sorted(cps, key=lambda c: (c["seq"], c["at"]))


def parse_checkpoint(value: bytes | None) -> dict[str, Any] | None:
    """A checkpoint from the topic, or None for anything malformed (the relay must never die on bad input)."""
    try:
        cp = json.loads(value or b"")
    except ValueError:
        return None
    if not isinstance(cp, dict):
        return None
    if not isinstance(cp.get("trace_id"), str) or cp.get("stage") not in SEQ:
        return None
    if not isinstance(cp.get("seq"), int) or not isinstance(cp.get("at"), str):
        return None
    return cp
```

`services/api/src/rides/estimate.py`:

```python
"""Fare estimate and ride amounts (spec 07 §Ride creation steps 3-4). Pure functions, TDD."""

from __future__ import annotations

import math
from dataclasses import dataclass

MIN_SAMPLES = 5
AIRPORT_PICKUPS = frozenset({132, 138})
EARTH_RADIUS_MI = 3958.8
TYPICAL_DISTANCE_MI = 2.5  # for a zone without a centroid (264/265, duplicate shapefile ids)
CASH_NOTE = "NYC taxi data doesn't record cash tips, so your tip will show as $0"


@dataclass(frozen=True)
class Zone:
    location_id: int
    zone: str
    borough: str
    service_zone: str
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class Estimate:
    trip_distance: float
    duration_min: float
    fare_amount: float
    method: str


@dataclass(frozen=True)
class Amounts:
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    congestion_surcharge: float
    airport_fee: float
    cbd_congestion_fee: float
    total_amount: float
    payment_type: int


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_MI * math.asin(math.sqrt(a))


def _estimate(distance: float, duration: float, fare: float, method: str) -> Estimate:
    # Floors keep every student ride valid for the processor (DISTANCE_NON_POSITIVE, DROPOFF_NOT_AFTER_PICKUP,
    # FARE_NON_POSITIVE): only the tip decides whether it is rejected.
    return Estimate(trip_distance=round(max(distance, 0.1), 2), duration_min=round(max(duration, 1.0), 1),
                    fare_amount=round(max(fare, 3.0), 2), method=method)


def from_median(n: int, distance: float | None, duration: float | None, fare: float | None) -> Estimate | None:
    """The median of real replayed trips between the two zones, or None if there are fewer than 5."""
    if n < MIN_SAMPLES or distance is None or duration is None or fare is None:
        return None
    return _estimate(distance, duration, fare, f"median of {n} real trips between these zones")


def from_centroids(pu: Zone, do: Zone) -> Estimate:
    if pu.lat is None or pu.lon is None or do.lat is None or do.lon is None:
        distance = TYPICAL_DISTANCE_MI
        method = "typical NYC trip (no map point for this zone)"
    else:
        distance = max(0.5, 1.3 * haversine_miles(pu.lat, pu.lon, do.lat, do.lon))
        method = "straight-line distance × 1.3 (too few real trips between these zones)"
    duration = distance / 12 * 60 + 3
    return _estimate(distance, duration, 3.00 + 3.50 * distance + 0.30 * duration, method)


def amounts(est: Estimate, pu: Zone, do: Zone, payment: str, tip: float) -> Amounts:
    yellow = "Yellow Zone" in (pu.service_zone, do.service_zone)
    tip_amount = round(tip, 2) if payment == "card" else 0.0  # NYC TLC data never records cash tips
    congestion = 2.50 if yellow else 0.0
    cbd = 0.75 if yellow else 0.0
    airport = 1.75 if pu.location_id in AIRPORT_PICKUPS else 0.0
    total = est.fare_amount + 0.50 + tip_amount + 1.00 + congestion + airport + cbd
    return Amounts(fare_amount=est.fare_amount, extra=0.0, mta_tax=0.50, tip_amount=tip_amount, tolls_amount=0.0,
                   improvement_surcharge=1.00, congestion_surcharge=congestion, airport_fee=airport,
                   cbd_congestion_fee=cbd, total_amount=round(total, 2), payment_type=1 if payment == "card" else 2)


def ride_notes(payment: str) -> list[str]:
    return [CASH_NOTE] if payment == "cash" else []
```

`services/api/src/rides/trip.py`:

```python
"""A student ride as a trips.raw message (contracts/events/trip.schema.json, source=student)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from rides.estimate import Amounts, Estimate


def _sim(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat(timespec="seconds")


def build_trip(*, trace_id: str, pickup_ts: datetime, est: Estimate, amt: Amounts, pu_location_id: int,
               do_location_id: int, passenger_count: int, produced_at: str) -> dict[str, Any]:
    dropoff = pickup_ts + timedelta(seconds=int(est.duration_min * 60))
    return {
        "trip_id": f"s-{trace_id}", "source": "student", "trace_id": trace_id, "loop": 0, "vendor_id": None,
        "pickup_ts": _sim(pickup_ts), "dropoff_ts": _sim(dropoff), "passenger_count": passenger_count,
        "trip_distance": est.trip_distance, "ratecode_id": 1, "store_and_fwd_flag": None,
        "pu_location_id": pu_location_id, "do_location_id": do_location_id, "payment_type": amt.payment_type,
        "fare_amount": amt.fare_amount, "extra": amt.extra, "mta_tax": amt.mta_tax, "tip_amount": amt.tip_amount,
        "tolls_amount": amt.tolls_amount, "improvement_surcharge": amt.improvement_surcharge,
        "congestion_surcharge": amt.congestion_surcharge, "airport_fee": amt.airport_fee,
        "cbd_congestion_fee": amt.cbd_congestion_fee, "total_amount": amt.total_amount, "produced_at": produced_at,
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/api && uv run pytest -q && uv run ruff check src tests && uv run mypy src`
Expected: all pass. The first card test expects 31.0: 19.5 + 0.5 + 5 + 1 + 2.5 + 1.75 + 0.75 (the JFK pickup adds the airport fee). The spec example (pickup Times Sq, dropoff JFK, so no airport fee) expects 29.25.

- [ ] **Step 5: Commit**

```bash
git add services/api/pyproject.toml services/api/uv.lock services/api/src/rides services/api/tests
git commit -m "feat(api): fare estimator, ride amounts, student trip builder, checkpoint helpers (spec 07, TDD)"
```

---

### Task 3: `KafkaOut`, `RideService`, `POST /api/rides`, `GET /api/rides/estimate`

**Files:**
- Create: `services/api/src/api/kafka.py`
- Create: `services/api/src/rides/service.py`
- Modify: `services/api/src/api/main.py` (`Deps`, request model, routes, exception handler)
- Modify: `services/api/tests/conftest.py` (`FakeOut`, `FakeRideClock`)
- Test: `services/api/tests/test_kafka.py`, `services/api/tests/test_rides.py`

**Interfaces:**
- Consumes:
  - Task 1: `TraceDb`, `Config.rides_*`, `SimClockFollower.fresh_sim_ts`.
  - Task 2: `rides.estimate.*`, `rides.trip.build_trip`, `rides.checkpoints.checkpoint/wall_str/TRACE_TOPIC`.
  - `api.ratelimit.TokenBucket(per_min, clock).allow(key) -> (bool, retry_after_s)`.
- Produces (`api.kafka`):
  - `Delivery(partition: int, offset: int)` (NamedTuple) and `KafkaDeliveryError(Exception)`.
  - `class Out(Protocol)`: `async def send(topic: str, key: str, value: dict[str, Any]) -> Delivery` and `def send_nowait(topic: str, key: str, value: dict[str, Any]) -> None`.
  - `KafkaOut(brokers, producer=None)` implements it. `.start()` and `.stop()` run the poll thread.
- Produces (`rides.service`):
  - `RideRequest(pickup_location_id, dropoff_location_id, payment, tip_amount, passenger_count)` (frozen dataclass).
  - `RideRejected(status: int, message: str, retry_after_s: int | None = None)`.
  - `RideService(cfg, db: TraceDb, out: Out, sim_clock: RideClock, publish: Callable[[dict], None] | None = None, clock=time.monotonic, wall=lambda: datetime.now(UTC))`.
  - Its methods: `async zones() -> dict[int, Zone]`, `async estimate(pu, do) -> Estimate`, `async create(req, client_ip) -> dict`, `async warm_up() -> None`.
- Produces (`api.main`): `Deps.rides: Any = None` and `Deps.trace: Any = None`.

- [ ] **Step 1: Write the failing tests**

Add `from api.kafka import Delivery, KafkaDeliveryError` to the imports at the top of `services/api/tests/conftest.py`, then append:

```python
class FakeOut:
    """Records every produced message in order; `fail` makes send() raise like a broker that never acked."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self.fail = False
        self.offset = 482112

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery:
        if self.fail:
            raise KafkaDeliveryError("Local: Message timed out")
        self.sent.append((topic, key, value))
        self.offset += 1
        return Delivery(0, self.offset)

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))


class FakeRideClock:
    def __init__(self, sim_ts: str | None = "2025-10-09T17:42:00.250") -> None:
        self.sim_ts = sim_ts
        self.latest: dict[str, Any] | None = {"sim_ts": sim_ts} if sim_ts else None

    def fresh_sim_ts(self, max_age_s: float = 10.0) -> str | None:
        return self.sim_ts
```

`services/api/tests/test_kafka.py`:

```python
import pytest

from api.kafka import Delivery, KafkaDeliveryError, KafkaOut


class _Msg:
    def __init__(self, partition: int, offset: int) -> None:
        self._p, self._o = partition, offset

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeProducer:
    def __init__(self, err: str | None = None, raise_on_produce: Exception | None = None) -> None:
        self.err, self.raise_on_produce = err, raise_on_produce
        self.pending: list = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.raise_on_produce is not None:
            raise self.raise_on_produce
        self.pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        cbs, self.pending = self.pending, []
        for cb in cbs:
            if cb is not None:
                cb(self.err, None if self.err else _Msg(0, 42))
        return len(cbs)

    def flush(self, timeout: float) -> int:
        return 0


@pytest.mark.anyio
async def test_send_waits_for_the_delivery_report():
    out = KafkaOut("unused:9092", producer=FakeProducer())
    out.start()
    try:
        assert await out.send("trips.raw", "k", {"a": 1}) == Delivery(0, 42)
    finally:
        out.stop()


@pytest.mark.anyio
async def test_send_raises_on_a_failed_delivery():
    out = KafkaOut("unused:9092", producer=FakeProducer(err="Local: Message timed out"))
    out.start()
    try:
        with pytest.raises(KafkaDeliveryError, match="timed out"):
            await out.send("trips.raw", "k", {"a": 1})
    finally:
        out.stop()


@pytest.mark.anyio
async def test_send_raises_when_the_local_queue_is_full():
    out = KafkaOut("unused:9092", producer=FakeProducer(raise_on_produce=BufferError("queue full")))
    with pytest.raises(KafkaDeliveryError):
        await out.send("trips.raw", "k", {})


def test_producer_gives_up_before_the_api_stops_waiting():
    """A ride that got a 503 must never be delivered later (spec 07 §Failure modes)."""
    from api.kafka import PRODUCER_CONFIG, SEND_WAIT_S

    assert PRODUCER_CONFIG["message.timeout.ms"] <= SEND_WAIT_S * 1000 - 500
```

`services/api/tests/test_rides.py`:

```python
import json

import pytest
from conftest import FakeDb, FakeOut, FakeRideClock, FakeTraceDb, make_config
from contract_validator import assert_valid
from fastapi.testclient import TestClient

from api.db import DbUnavailable, Result
from api.main import Deps, create_app
from rides.service import RideRejected, RideRequest, RideService

ZONES = Result(
    [("location_id", "UInt16"), ("zone", "String"), ("borough", "String"), ("service_zone", "String"),
     ("lat", "Nullable(Float64)"), ("lon", "Nullable(Float64)")],
    [[230, "Times Sq/Theatre District", "Manhattan", "Yellow Zone", 40.759, -73.985],
     [132, "JFK Airport", "Queens", "Airports", 40.641, -73.779],
     [161, "Midtown Center", "Manhattan", "Yellow Zone", 40.758, -73.978],
     [7, "Astoria", "Queens", "Boro Zone", 40.761, -73.920]], 1)
MEDIAN = Result([("n", "UInt64"), ("distance", "Float64"), ("duration", "Float64"), ("fare", "Float64")],
                [[214, 3.1, 16.5, 19.5]], 3)
FEW = Result(MEDIAN.columns, [[2, 3.0, 12.0, 15.0]], 3)
SIM_NOW = Result([("now_ts", "DateTime64(3)")], [["2025-10-09T08:00:00.500"]], 1)
CARD = RideRequest(230, 132, "card", 5.0, 1)


def service(db: FakeTraceDb | None = None, out: FakeOut | None = None, clock: FakeRideClock | None = None,
            **env: str) -> tuple[RideService, FakeTraceDb, FakeOut]:
    db = db or FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": MEDIAN, "taxi.sim_now": SIM_NOW})
    out = out or FakeOut()
    t = [0.0]
    svc = RideService(make_config(**env), db, out, clock or FakeRideClock(), clock=lambda: t[0])
    return svc, db, out


@pytest.mark.anyio
async def test_create_produces_the_trip_then_submitted_and_ingested():
    svc, db, out = service()
    r = await svc.create(CARD, "10.0.0.1")
    tid = r["trace_id"]
    assert r["trip_id"] == f"s-{tid}" and r["trace_url"] == f"/trace/{tid}" and r["pickup_ts"] == "2025-10-09T17:42:00"
    assert r["estimate"] == {"trip_distance": 3.1, "duration_min": 16.5, "fare_amount": 19.5, "total_amount": 29.25,
                             "method": "median of 214 real trips between these zones"}
    assert r["notes"] == []
    assert [(t, k) for t, k, _ in out.sent] == [("trips.raw", f"s-{tid}"), ("trace.checkpoints", tid),
                                                ("trace.checkpoints", tid)]
    trip, submitted, ingested = (v for _, _, v in out.sent)
    assert_valid("trip", trip)
    for cp in (submitted, ingested):
        assert_valid("trace_checkpoint", cp)
    assert (submitted["stage"], submitted["seq"], ingested["stage"], ingested["seq"]) == ("submitted", 1, "ingested", 2)
    assert submitted["detail"] == {"pickup_zone": "Times Sq/Theatre District", "dropoff_zone": "JFK Airport",
                                   "payment": "card", "tip_amount": 5.0, "fare_amount": 19.5,
                                   "estimate_method": "median of 214 real trips between these zones"}
    assert ingested["detail"] == {"topic": "trips.raw", "partition": 0, "offset": 482113}
    assert submitted["at"] <= ingested["at"]
    median_sql, params = next(c for c in db.calls if "quantile(0.5)" in c[0])
    assert params == {"pickup": 230, "dropoff": 132} and "{pickup:UInt16}" in median_sql


@pytest.mark.anyio
async def test_cash_ride_records_no_tip_and_explains_why():
    svc, _, out = service()
    r = await svc.create(RideRequest(230, 132, "cash", 5.0, 1), "10.0.0.1")
    assert r["notes"] == ["NYC taxi data doesn't record cash tips, so your tip will show as $0"]
    trip = out.sent[0][2]
    assert trip["tip_amount"] == 0.0 and trip["payment_type"] == 2
    assert out.sent[1][2]["detail"]["tip_amount"] == 5.0  # what the student sent, shown on the ride chip


@pytest.mark.anyio
async def test_few_real_trips_falls_back_to_centroids():
    db = FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": FEW})
    svc, _, _ = service(db=db)
    assert (await svc.estimate(230, 7)).method.startswith("straight-line distance × 1.3")


@pytest.mark.anyio
async def test_estimate_is_cached_per_zone_pair():
    svc, db, _ = service()
    await svc.estimate(230, 132)
    await svc.estimate(230, 132)
    assert sum("quantile(0.5)" in sql for sql, _ in db.calls) == 1


@pytest.mark.anyio
async def test_estimate_survives_clickhouse_down_once_zones_are_loaded():
    svc, db, _ = service()
    await svc.zones()
    db.fail = DbUnavailable("down")
    est = await svc.estimate(230, 132)
    assert est.method.startswith("straight-line distance")
    db.fail = None
    await svc.estimate(230, 132)  # the fallback was not cached: the median is tried again
    assert sum("quantile(0.5)" in sql for sql, _ in db.calls) == 2


@pytest.mark.anyio
async def test_stale_sim_clock_falls_back_to_sim_now():
    svc, db, out = service(clock=FakeRideClock(sim_ts=None))
    r = await svc.create(CARD, "10.0.0.1")
    assert r["pickup_ts"] == "2025-10-09T08:00:00"
    assert any("taxi.sim_now" in sql for sql, _ in db.calls)


@pytest.mark.anyio
async def test_broker_failure_is_503_and_emits_nothing():
    out = FakeOut()
    out.fail = True
    svc, _, _ = service(out=out)
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.0.1")
    assert e.value.status == 503 and e.value.message == "The pipeline is catching its breath, try again in a moment"
    assert out.sent == []


@pytest.mark.anyio
async def test_fourth_ride_from_one_ip_within_a_minute_is_429():
    svc, _, _ = service()
    for _ in range(3):
        await svc.create(CARD, "10.0.0.1")
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.0.1")
    assert e.value.status == 429 and e.value.retry_after_s is not None and e.value.retry_after_s > 0
    await svc.create(CARD, "10.0.0.2")  # another phone is not affected


@pytest.mark.anyio
async def test_global_cap_across_phones():
    svc, _, _ = service(RIDES_GLOBAL_PER_MIN="5")
    for i in range(5):
        await svc.create(CARD, f"10.0.1.{i}")
    with pytest.raises(RideRejected) as e:
        await svc.create(CARD, "10.0.1.99")
    assert e.value.status == 429 and e.value.message == "The dispatcher is busy, try again in a few seconds"


@pytest.mark.anyio
async def test_published_checkpoints_reach_the_hub_immediately():
    seen: list[str] = []
    db = FakeTraceDb({"FROM taxi.zones": ZONES, "quantile(0.5)": MEDIAN})
    svc = RideService(make_config(), db, FakeOut(), FakeRideClock(), publish=lambda cp: seen.append(cp["stage"]))
    await svc.create(CARD, "10.0.0.1")
    assert seen == ["submitted", "ingested"]


def app(svc: RideService) -> TestClient:
    return TestClient(create_app(Deps(cfg=make_config(), db=FakeDb(), sim_clock=FakeRideClock(), rides=svc)))


BODY = {"pickup_location_id": 230, "dropoff_location_id": 132, "payment": "card", "tip_amount": 5.0, "passenger_count": 1}


def test_post_rides_returns_201():
    svc, _, _ = service()
    r = app(svc).post("/api/rides", json=BODY)
    assert r.status_code == 201
    assert set(r.json()) == {"trace_id", "trip_id", "trace_url", "pickup_ts", "estimate", "notes"}


@pytest.mark.parametrize(("field", "value"), [
    ("pickup_location_id", 0), ("pickup_location_id", 264), ("pickup_location_id", "230"), ("pickup_location_id", 23.5),
    ("dropoff_location_id", -1), ("payment", "bitcoin"), ("tip_amount", -0.01), ("tip_amount", 999.01),
    ("tip_amount", 1.234), ("tip_amount", "5"), ("tip_amount", True), ("passenger_count", 0), ("passenger_count", 7),
    ("passenger_count", True),
])
def test_invalid_field_is_422_naming_it(field, value):
    svc, _, out = service()
    r = app(svc).post("/api/rides", json={**BODY, field: value})
    assert r.status_code == 422
    assert r.json()["detail"][0]["loc"][-1] == field
    assert out.sent == []


def test_missing_field_is_422_naming_it():
    svc, _, _ = service()
    r = app(svc).post("/api/rides", json={k: v for k, v in BODY.items() if k != "payment"})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "payment"


def test_nan_tip_is_422():
    svc, _, _ = service()
    raw = json.dumps(BODY).replace('"tip_amount": 5.0', '"tip_amount": NaN')
    r = app(svc).post("/api/rides", content=raw, headers={"Content-Type": "application/json"})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "tip_amount"


def test_absurd_tip_is_accepted_on_purpose():
    svc, _, _ = service()
    assert app(svc).post("/api/rides", json={**BODY, "tip_amount": 999}).status_code == 201


def test_rate_limit_is_429_with_retry_after():
    svc, _, _ = service(RIDES_RATE_LIMIT_PER_MIN="1")
    c = app(svc)
    assert c.post("/api/rides", json=BODY).status_code == 201
    r = c.post("/api/rides", json=BODY)
    assert r.status_code == 429 and int(r.headers["Retry-After"]) > 0
    assert r.json()["retry_after_s"] == int(r.headers["Retry-After"])


def test_broker_down_is_503():
    out = FakeOut()
    out.fail = True
    svc, _, _ = service(out=out)
    r = app(svc).post("/api/rides", json=BODY)
    assert r.status_code == 503 and r.json()["detail"] == "The pipeline is catching its breath, try again in a moment"


def test_estimate_endpoint():
    svc, _, _ = service()
    c = app(svc)
    r = c.get("/api/rides/estimate", params={"pickup_location_id": 230, "dropoff_location_id": 132})
    assert r.json() == {"trip_distance": 3.1, "duration_min": 16.5, "fare_amount": 19.5,
                        "method": "median of 214 real trips between these zones"}
    assert c.get("/api/rides/estimate", params={"pickup_location_id": 0, "dropoff_location_id": 132}).status_code == 422
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/api && uv run pytest -q tests/test_kafka.py tests/test_rides.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'api.kafka'`.

- [ ] **Step 3: Implement**

`services/api/src/api/kafka.py`:

```python
"""One Kafka producer for the api: student rides and queryable checkpoints (spec 07)."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any, NamedTuple, Protocol

from confluent_kafka import KafkaException, Producer

from api import log

SEND_WAIT_S = 2.5
# librdkafka gives up (and reports it) after 2 s. That is before POST /api/rides stops waiting, so a ride that got a
# 503 is never delivered later (spec 07 §Failure modes: "no submitted is left dangling").
PRODUCER_CONFIG: dict[str, Any] = {"linger.ms": 2, "acks": "all", "message.timeout.ms": 2000}


class Delivery(NamedTuple):
    partition: int
    offset: int


class KafkaDeliveryError(Exception):
    """The broker did not acknowledge the message."""


class Out(Protocol):
    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery: ...

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None: ...


def _resolve(fut: asyncio.Future[Delivery], result: Delivery | None, error: str | None) -> None:
    if fut.done():  # the caller already gave up
        return
    if error is not None:
        fut.set_exception(KafkaDeliveryError(error))
    else:
        assert result is not None
        fut.set_result(result)


class KafkaOut:
    def __init__(self, brokers: str, producer: Any = None) -> None:
        self._p = producer if producer is not None else Producer({"bootstrap.servers": brokers, **PRODUCER_CONFIG})
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, name="kafka-out", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._p.flush(3)

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._p.poll(0.1)  # runs the delivery callbacks

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery:
        """Produce and wait for the broker's acknowledgement (at most SEND_WAIT_S)."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Delivery] = loop.create_future()

        def on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                loop.call_soon_threadsafe(_resolve, fut, None, str(err))
            else:
                loop.call_soon_threadsafe(_resolve, fut, Delivery(msg.partition(), msg.offset()), None)

        try:
            self._p.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:
            raise KafkaDeliveryError(str(exc)) from exc
        try:
            return await asyncio.wait_for(fut, SEND_WAIT_S)
        except TimeoutError as exc:
            raise KafkaDeliveryError("no broker acknowledgement") from exc

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Fire and forget (checkpoints). A failure is logged, never raised."""

        def on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                log.event("checkpoint_dropped", topic=topic, trace_id=value.get("trace_id"), error=str(err)[:200])

        try:
            self._p.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=on_delivery)
        except (BufferError, KafkaException) as exc:
            log.event("checkpoint_dropped", topic=topic, trace_id=value.get("trace_id"), error=str(exc)[:200])
```

`services/api/src/rides/service.py`:

```python
"""Ride creation (spec 07 §Ride creation): validate → estimate → sim-clock timestamp → trips.raw → checkpoints."""

from __future__ import annotations

import math
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from api import log
from api.config import Config
from api.db import DbError, DbUnavailable, TraceDb
from api.kafka import KafkaDeliveryError, Out
from api.ratelimit import TokenBucket
from rides.checkpoints import TRACE_TOPIC, checkpoint, wall_str
from rides.estimate import Estimate, Zone, amounts, from_centroids, from_median, ride_notes
from rides.trip import build_trip

ZONES_SQL = "SELECT location_id, zone, borough, service_zone, lat, lon FROM taxi.zones"
MEDIAN_SQL = """
SELECT count() AS n,
       quantile(0.5)(trip_distance) AS distance,
       quantile(0.5)(duration_min)  AS duration,
       quantile(0.5)(fare_amount)   AS fare
FROM taxi.trips
WHERE source = 'replay' AND pu_location_id = {pickup:UInt16} AND do_location_id = {dropoff:UInt16}
"""
SIM_NOW_SQL = "SELECT now_ts FROM taxi.sim_now"
ESTIMATE_CACHE = 2000
BROKER_DOWN = "The pipeline is catching its breath, try again in a moment"
BUSY = "The dispatcher is busy, try again in a few seconds"


class RideClock(Protocol):
    def fresh_sim_ts(self, max_age_s: float = 10.0) -> str | None: ...


@dataclass(frozen=True)
class RideRequest:
    pickup_location_id: int
    dropoff_location_id: int
    payment: str
    tip_amount: float
    passenger_count: int


class RideRejected(Exception):
    def __init__(self, status: int, message: str, retry_after_s: int | None = None) -> None:
        super().__init__(message)
        self.status, self.message, self.retry_after_s = status, message, retry_after_s


class RideService:
    def __init__(self, cfg: Config, db: TraceDb, out: Out, sim_clock: RideClock,
                 publish: Callable[[dict[str, Any]], None] | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._cfg, self._db, self._out, self._sim, self._publish, self._wall = cfg, db, out, sim_clock, publish, wall
        self._per_ip = TokenBucket(cfg.rides_rate_limit_per_min, clock)
        self._global = TokenBucket(cfg.rides_global_per_min, clock)
        self._zones: dict[int, Zone] = {}
        self._estimates: OrderedDict[tuple[int, int], Estimate] = OrderedDict()

    async def warm_up(self) -> None:
        try:
            await self.zones()
        except (DbError, DbUnavailable) as exc:
            log.event("zones_warmup_failed", error=str(exc)[:200])

    async def zones(self) -> dict[int, Zone]:
        if not self._zones:
            r = await self._db.query(ZONES_SQL, {})
            self._zones = {int(row[0]): Zone(int(row[0]), str(row[1]), str(row[2]), str(row[3]), row[4], row[5])
                           for row in r.rows}
        return self._zones

    async def estimate(self, pu: int, do: int) -> Estimate:
        key = (pu, do)
        if key in self._estimates:
            self._estimates.move_to_end(key)
            return self._estimates[key]
        zones = await self.zones()
        if pu not in zones or do not in zones:
            raise RideRejected(503, "The zone list is still loading, try again in a moment")
        try:
            r = await self._db.query(MEDIAN_SQL, {"pickup": pu, "dropoff": do})
        except (DbError, DbUnavailable):
            return from_centroids(zones[pu], zones[do])  # not cached: try the median again next time
        n, distance, duration, fare = r.rows[0] if r.rows else (0, None, None, None)
        est = from_median(int(n), distance, duration, fare) or from_centroids(zones[pu], zones[do])
        self._estimates[key] = est
        while len(self._estimates) > ESTIMATE_CACHE:
            self._estimates.popitem(last=False)
        return est

    async def _pickup_ts(self) -> datetime:
        ts = self._sim.fresh_sim_ts()
        if ts is None:
            try:
                r = await self._db.query(SIM_NOW_SQL, {})
                ts = str(r.rows[0][0]) if r.rows and r.rows[0][0] is not None else None
            except (DbError, DbUnavailable):
                ts = None
        if ts is None:
            raise RideRejected(503, "The sim clock hasn't started yet, try again in a moment")
        return datetime.fromisoformat(ts[:19])  # whole seconds

    def _limit(self, client_ip: str) -> None:
        ok, retry = self._per_ip.allow(client_ip)
        if not ok:
            wait = math.ceil(retry)
            raise RideRejected(429, f"You've booked {self._cfg.rides_rate_limit_per_min} rides this minute. "
                                    f"Try again in {wait} s.", wait)
        ok, retry = self._global.allow("all")
        if not ok:
            raise RideRejected(429, BUSY, math.ceil(retry))

    async def create(self, req: RideRequest, client_ip: str) -> dict[str, Any]:
        self._limit(client_ip)
        accepted_at = self._wall()
        zones = await self.zones()
        est = await self.estimate(req.pickup_location_id, req.dropoff_location_id)
        pu, do = zones[req.pickup_location_id], zones[req.dropoff_location_id]
        amt = amounts(est, pu, do, req.payment, req.tip_amount)
        trace_id = str(uuid.uuid4())
        trip = build_trip(trace_id=trace_id, pickup_ts=await self._pickup_ts(), est=est, amt=amt,
                          pu_location_id=pu.location_id, do_location_id=do.location_id,
                          passenger_count=req.passenger_count, produced_at=wall_str(self._wall()))
        try:
            ack = await self._out.send("trips.raw", trip["trip_id"], trip)
        except KafkaDeliveryError as exc:
            log.event("ride_broker_down", trace_id=trace_id, error=str(exc)[:200])
            raise RideRejected(503, BROKER_DOWN) from exc
        # `submitted` goes out only after the ack, so a failed ride leaves nothing behind. Its `at` is still the
        # moment the request was accepted, and the UI orders by seq, so latencies stay honest.
        submitted = checkpoint(trace_id, trip["trip_id"], "student", "submitted",
                               {"pickup_zone": pu.zone, "dropoff_zone": do.zone, "payment": req.payment,
                                "tip_amount": req.tip_amount, "fare_amount": est.fare_amount,
                                "estimate_method": est.method}, wall_str(accepted_at))
        ingested = checkpoint(trace_id, trip["trip_id"], "student", "ingested",
                              {"topic": "trips.raw", "partition": ack.partition, "offset": ack.offset},
                              wall_str(self._wall()))
        for cp in (submitted, ingested):
            self._out.send_nowait(TRACE_TOPIC, trace_id, cp)
            if self._publish is not None:
                self._publish(cp)
        log.event("ride_created", trace_id=trace_id, method=est.method, payment=req.payment)
        return {"trace_id": trace_id, "trip_id": trip["trip_id"], "trace_url": f"/trace/{trace_id}",
                "pickup_ts": trip["pickup_ts"],
                "estimate": {"trip_distance": est.trip_distance, "duration_min": est.duration_min,
                             "fare_amount": est.fare_amount, "total_amount": amt.total_amount, "method": est.method},
                "notes": ride_notes(req.payment)}
```

`services/api/src/api/main.py`:
- Add `rides: Any = None` and `trace: Any = None` to `Deps`.
- Add the imports: `math`, `pydantic`'s `ValidationInfo`, and `rides.service`'s `RideRejected`, `RideRequest`.
- Add the request model:

```python
class RideBody(BaseModel):
    pickup_location_id: int = Field(ge=1, le=263)
    dropoff_location_id: int = Field(ge=1, le=263)
    payment: Literal["card", "cash"]
    tip_amount: float = Field(ge=0, le=999)  # absurd tips are allowed on purpose: the processor rejects them
    passenger_count: int = Field(ge=1, le=6)

    @field_validator("pickup_location_id", "dropoff_location_id", "passenger_count", mode="before")
    @classmethod
    def whole_number(cls, v: Any, info: ValidationInfo) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError(f"{info.field_name} must be a whole number")
        return v

    @field_validator("tip_amount", mode="before")
    @classmethod
    def money(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
            raise ValueError("tip_amount must be a number")
        if abs(round(v, 2) - v) > 1e-9:
            raise ValueError("tip_amount has at most 2 decimals")
        return v
```

Inside `create_app`, add:

```python
    @app.exception_handler(RideRejected)
    async def ride_rejected(_: Request, exc: RideRejected) -> JSONResponse:
        body: dict[str, Any] = {"detail": exc.message}
        headers: dict[str, str] = {}
        if exc.retry_after_s is not None:
            body["retry_after_s"] = exc.retry_after_s
            headers["Retry-After"] = str(exc.retry_after_s)
        return JSONResponse(body, status_code=exc.status, headers=headers)

    @app.get("/api/rides/estimate")
    async def ride_estimate(pickup_location_id: int = Query(ge=1, le=263),
                            dropoff_location_id: int = Query(ge=1, le=263)) -> dict[str, Any]:
        est = await deps.rides.estimate(pickup_location_id, dropoff_location_id)
        return {"trip_distance": est.trip_distance, "duration_min": est.duration_min,
                "fare_amount": est.fare_amount, "method": est.method}

    @app.post("/api/rides", status_code=201)
    async def create_ride(body: RideBody, request: Request) -> dict[str, Any]:
        # uvicorn --proxy-headers: the host comes from X-Forwarded-For. It's only a rate-limit key, never logged.
        client_ip = request.client.host if request.client else "unknown"
        req = RideRequest(body.pickup_location_id, body.dropoff_location_id, body.payment, round(body.tip_amount, 2),
                          body.passenger_count)
        result: dict[str, Any] = await deps.rides.create(req, client_ip)
        return result
```

(Also import `Query` from `fastapi`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/api && uv run pytest -q && uv run ruff check src tests && uv run mypy src`
Expected: all pass. If `("pickup_location_id", "230")` fails because the `mode="before"` validator doesn't run before pydantic's own int coercion, read the error. `mode="before"` validators run first on the raw JSON value, so a string reaches `whole_number` as `str`.

- [ ] **Step 5: Commit**

```bash
git add services/api/src services/api/tests
git commit -m "feat(api): POST /api/rides + GET /api/rides/estimate with per-IP and global limits, async producer"
```

---

### Task 4: Trace history and hub (pure fan-out)

**Files:**
- Create: `services/api/src/rides/history.py`
- Create: `services/api/src/rides/hub.py`
- Test: `services/api/tests/test_history.py`, `services/api/tests/test_hub.py`

**Interfaces:**
- Consumes: `rides.checkpoints.ordered`.
- Produces:
  - `TraceHistory(ttl_s: float, max_traces: int = 20_000, clock=time.monotonic)` with:
    - `add(cp) -> bool` (False if the stage is already known; the first one wins);
    - `get(trace_id) -> list[dict]` (ordered, `[]` if unknown or expired);
    - `merge(trace_id, cps) -> list[dict]`;
    - `has_stage(trace_id, stage) -> bool`;
    - `recent_student(n=20) -> list[tuple[str, list[dict]]]` (newest first).
  - `Hub(history, on_stored=None, queue_size=200)` with:
    - `.history`, `.on_stored` (a settable attribute);
    - `publish(cp) -> None`, which must be called on the event loop;
    - `follow(trace_id) -> asyncio.Queue` and `unfollow(trace_id, q)`;
    - `watch_live() -> asyncio.Queue` and `unwatch_live(q)`;
    - `follower_count(trace_id) -> int` and `live_count() -> int`.

- [ ] **Step 1: Write the failing tests**

`services/api/tests/test_history.py`:

```python
from rides.history import TraceHistory


def cp(tid: str, stage: str, seq: int, at: str = "2026-10-16T10:31:07.000Z", source: str = "student") -> dict:
    return {"trace_id": tid, "trip_id": f"s-{tid}", "source": source, "stage": stage, "seq": seq, "service": "api",
            "at": at, "detail": {}}


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_add_dedups_by_stage_and_keeps_the_first():
    h = TraceHistory(3600)
    first = cp("a", "stored", 6, "2026-10-16T10:31:08.000Z")
    assert h.add(first) is True
    assert h.add(cp("a", "stored", 6, "2026-10-16T10:31:09.000Z")) is False  # reprocessed after a restart
    assert h.get("a") == [first]


def test_get_orders_by_seq_not_arrival():
    h = TraceHistory(3600)
    for c in [cp("a", "stored", 6), cp("a", "submitted", 1), cp("a", "rejected", 3)]:
        h.add(c)
    assert [c["stage"] for c in h.get("a")] == ["submitted", "rejected", "stored"]


def test_ttl_expires_untouched_traces():
    clock = Clock()
    h = TraceHistory(ttl_s=10, clock=clock)
    h.add(cp("a", "submitted", 1))
    clock.t = 9
    h.add(cp("b", "submitted", 1))
    clock.t = 11
    assert h.get("a") == [] and len(h.get("b")) == 1


def test_lru_bound():
    h = TraceHistory(3600, max_traces=2)
    for tid in ["a", "b", "c"]:
        h.add(cp(tid, "submitted", 1))
    assert h.get("a") == [] and h.get("c") != []


def test_merge_and_has_stage():
    h = TraceHistory(3600)
    h.add(cp("a", "validated", 3))
    merged = h.merge("a", [cp("a", "submitted", 1), cp("a", "validated", 3, "2026-10-16T10:31:09.000Z")])
    assert [c["stage"] for c in merged] == ["submitted", "validated"]
    assert h.has_stage("a", "submitted") and not h.has_stage("a", "stored") and not h.has_stage("x", "stored")


def test_recent_student_newest_first_and_only_students():
    h = TraceHistory(3600)
    h.add(cp("s1", "submitted", 1))
    h.add(cp("r1", "ingested", 2, source="replay"))
    h.add(cp("s2", "submitted", 1))
    h.add(cp("s1", "ingested", 2))  # touching s1 makes it the newest
    assert [t for t, _ in h.recent_student(20)] == ["s1", "s2"]
    assert [t for t, _ in h.recent_student(1)] == ["s1"]
```

`services/api/tests/test_hub.py`:

```python
import asyncio

from test_history import cp

from rides.history import TraceHistory
from rides.hub import Hub


def test_followers_get_new_checkpoints_once():
    hub = Hub(TraceHistory(3600))
    q = hub.follow("a")
    other = hub.follow("b")
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("a", "submitted", 1))  # duplicate
    assert q.qsize() == 1 and other.qsize() == 0
    hub.unfollow("a", q)
    assert hub.follower_count("a") == 0


def test_live_watchers_get_every_source():
    hub = Hub(TraceHistory(3600))
    live = hub.watch_live()
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("r", "ingested", 2, source="replay"))
    assert live.qsize() == 2
    hub.unwatch_live(live)
    assert hub.live_count() == 0


def test_on_stored_fires_once_per_trace():
    seen: list[str] = []
    hub = Hub(TraceHistory(3600), on_stored=lambda c: seen.append(c["trace_id"]))
    hub.publish(cp("a", "stored", 6))
    hub.publish(cp("a", "stored", 6))
    hub.publish(cp("a", "enriched", 4))
    assert seen == ["a"]


def test_a_slow_client_never_blocks_publish():
    hub = Hub(TraceHistory(3600), queue_size=1)
    q = hub.follow("a")
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("a", "ingested", 2))  # queue full: dropped for this client, kept in history
    assert q.qsize() == 1 and len(hub.history.get("a")) == 2
    assert isinstance(q, asyncio.Queue)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/api && uv run pytest -q tests/test_history.py tests/test_hub.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'rides.history'`.

- [ ] **Step 3: Implement**

`services/api/src/rides/history.py`:

```python
"""Short-term trace history: trace_id → {stage: checkpoint} (spec 07 §Trace relay, History)."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any

from rides.checkpoints import ordered

Checkpoint = dict[str, Any]


class TraceHistory:
    """In-memory, least recently touched first. TTL and size bound so a long stand day can't grow it forever."""

    def __init__(self, ttl_s: float, max_traces: int = 20_000, clock: Callable[[], float] = time.monotonic) -> None:
        self._ttl, self._max, self._clock = ttl_s, max_traces, clock
        self._traces: OrderedDict[str, tuple[float, dict[str, Checkpoint]]] = OrderedDict()

    def _expire(self, now: float) -> None:
        while self._traces:
            tid, (touched, _) = next(iter(self._traces.items()))
            if now - touched <= self._ttl:
                return
            del self._traces[tid]

    def add(self, cp: Checkpoint) -> bool:
        """Store a checkpoint. False if this trace already has that stage (the first one wins)."""
        now = self._clock()
        self._expire(now)
        tid = cp["trace_id"]
        _, stages = self._traces.pop(tid, (now, {}))
        is_new = cp["stage"] not in stages
        if is_new:
            stages[cp["stage"]] = cp
        self._traces[tid] = (now, stages)
        while len(self._traces) > self._max:
            self._traces.popitem(last=False)
        return is_new

    def get(self, trace_id: str) -> list[Checkpoint]:
        self._expire(self._clock())
        entry = self._traces.get(trace_id)
        return ordered(entry[1].values()) if entry else []

    def merge(self, trace_id: str, cps: Iterable[Checkpoint]) -> list[Checkpoint]:
        for cp in cps:
            self.add(cp)
        return self.get(trace_id)

    def has_stage(self, trace_id: str, stage: str) -> bool:
        entry = self._traces.get(trace_id)
        return entry is not None and stage in entry[1]

    def recent_student(self, n: int = 20) -> list[tuple[str, list[Checkpoint]]]:
        self._expire(self._clock())
        out: list[tuple[str, list[Checkpoint]]] = []
        for tid, (_, stages) in reversed(self._traces.items()):
            if any(c.get("source") == "student" for c in stages.values()):
                out.append((tid, ordered(stages.values())))
                if len(out) == n:
                    break
        return out
```

`services/api/src/rides/hub.py`:

```python
"""Checkpoint fan-out to WebSocket clients. Event-loop only: the relay thread hands over with call_soon_threadsafe."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from rides.history import TraceHistory

Checkpoint = dict[str, Any]


def _offer(q: asyncio.Queue[Checkpoint], cp: Checkpoint) -> None:
    try:
        q.put_nowait(cp)
    except asyncio.QueueFull:
        pass  # a stuck client misses live updates; it gets the full history when it reconnects


class Hub:
    def __init__(self, history: TraceHistory, on_stored: Callable[[Checkpoint], None] | None = None,
                 queue_size: int = 200) -> None:
        self.history = history
        self.on_stored = on_stored
        self._size = queue_size
        self._followers: dict[str, set[asyncio.Queue[Checkpoint]]] = {}
        self._live: set[asyncio.Queue[Checkpoint]] = set()

    def publish(self, cp: Checkpoint) -> None:
        if not self.history.add(cp):
            return  # already seen: every stage reaches each client once
        for q in [*self._followers.get(cp["trace_id"], ()), *self._live]:
            _offer(q, cp)
        if cp["stage"] == "stored" and self.on_stored is not None:
            self.on_stored(cp)

    def follow(self, trace_id: str) -> asyncio.Queue[Checkpoint]:
        q: asyncio.Queue[Checkpoint] = asyncio.Queue(self._size)
        self._followers.setdefault(trace_id, set()).add(q)
        return q

    def unfollow(self, trace_id: str, q: asyncio.Queue[Checkpoint]) -> None:
        qs = self._followers.get(trace_id)
        if qs is not None:
            qs.discard(q)
            if not qs:
                del self._followers[trace_id]

    def watch_live(self) -> asyncio.Queue[Checkpoint]:
        q: asyncio.Queue[Checkpoint] = asyncio.Queue(self._size * 5)
        self._live.add(q)
        return q

    def unwatch_live(self, q: asyncio.Queue[Checkpoint]) -> None:
        self._live.discard(q)

    def follower_count(self, trace_id: str) -> int:
        return len(self._followers.get(trace_id, ()))

    def live_count(self) -> int:
        return len(self._live)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/api && uv run pytest -q && uv run ruff check src tests && uv run mypy src`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add services/api/src/rides/history.py services/api/src/rides/hub.py services/api/tests/test_history.py services/api/tests/test_hub.py
git commit -m "feat(api): in-memory trace history (TTL, LRU, dedup by stage) and checkpoint hub"
```

---

### Task 5: Relay consumer, queryable detector, WebSocket and history endpoints, wiring

**Files:**
- Modify: `services/api/pyproject.toml` (add `"websockets==15.0.1"` to `dependencies`: uvicorn needs it to serve WebSockets)
- Create: `services/api/src/rides/relay.py`
- Create: `services/api/src/rides/queryable.py`
- Create: `services/api/src/rides/trace_service.py`
- Modify: `services/api/src/api/main.py` (routes + `app_from_env` wiring + lifespan)
- Test: `services/api/tests/test_relay.py`, `services/api/tests/test_queryable.py`, `services/api/tests/test_trace_service.py`

**Interfaces:**
- Consumes: Task 1 `TraceDb`; Task 2 `rides.checkpoints.*`; Task 3 `KafkaOut`, `RideService`; Task 4 `TraceHistory`, `Hub`.
- Produces:
  - `TraceRelay(brokers)` with `.start(deliver: Callable[[dict], None])`, `.stop()`, and `.handle(value: bytes | None)`.
  - `QueryableDetector(db, emit, poll_ms, *, timeout_s=10.0, max_pending=20, clock=time.monotonic, wall=...)` with `.on_stored(cp)`, `async .tick()`, `async .run()`, and `.pending: int`.
  - `TraceService(hub, db, sim_clock, *, follow_max_s=600, after_terminal_s=60, clock_every_s=1.0, stats_every_s=5.0)` with:
    - `async history(trace_id) -> list[dict]`;
    - `async follow(ws, trace_id)` and `async live(ws)`;
    - `async stats() -> dict | None`;
    - `sim_ts() -> str | None`.
  - `serve(ws: WebSocket, work: Coroutine) -> None`.
  - `normalise_trace_id(s: str) -> str | None`.
- WS message shapes (the web reads these):
  - `/api/trace/{id}`:
    - `{"type":"history","checkpoints":[…],"sim_ts":str|null}`;
    - then `{"type":"checkpoint", …checkpoint}`.
  - `/api/trace/live`:
    - `{"type":"snapshot","traces":[{"trace_id","checkpoints"}],"sim_ts"}`;
    - `{"type":"clock","sim_ts"}` every 1 s;
    - `{"type":"stats","rides_today","rejected_today","median_ms":{stage: ms}}` every 5 s;
    - `{"type":"checkpoint", …checkpoint, "ambient": bool}`.
  - `GET /api/trace/{id}` → `{"trace_id","checkpoints"}`, or 404.

- [ ] **Step 1: Write the failing tests**

`services/api/tests/test_relay.py`:

```python
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
```

`services/api/tests/test_queryable.py`:

```python
from datetime import UTC, datetime

import pytest
from conftest import FakeTraceDb
from contract_validator import assert_valid

from api.db import DbUnavailable, Result
from rides.queryable import QueryableDetector

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"


def stored(tid: str = TID, source: str = "student") -> dict:
    return {"trace_id": tid, "trip_id": f"s-{tid}", "source": source, "stage": "stored", "seq": 6,
            "service": "processor", "at": "2026-10-16T10:31:08.000Z", "detail": {"table": "taxi.trips", "batch_rows": 9}}


def visible(*tids: str) -> Result:
    return Result([("trace_id", "Nullable(UUID)")], [[t] for t in tids], 1)


def detector(db: FakeTraceDb, **kw) -> tuple[QueryableDetector, list[dict], list[float]]:
    emitted: list[dict] = []
    t = [0.0]
    d = QueryableDetector(db, emitted.append, 250, clock=lambda: t[0],
                          wall=lambda: datetime(2026, 10, 16, 10, 31, 8, 590_000, tzinfo=UTC), **kw)
    return d, emitted, t


@pytest.mark.anyio
async def test_emits_queryable_when_the_row_is_visible():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, emitted, _ = detector(db)
    d.on_stored(stored())
    await d.tick()
    assert emitted == [] and d.pending == 1
    db.results["FROM taxi.trips"] = visible(TID)
    await d.tick()
    assert d.pending == 0
    (q,) = emitted
    assert_valid("trace_checkpoint", q)
    assert (q["stage"], q["seq"], q["service"], q["source"]) == ("queryable", 7, "api", "student")
    assert q["detail"] == {"table": "taxi.trips", "visible_after_ms": 590}
    sql, params = db.calls[-1]
    assert "{ids:Array(UUID)}" in sql and params == {"ids": [TID]}


@pytest.mark.anyio
async def test_one_query_polls_every_pending_trace():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, _, _ = detector(db)
    for i in range(3):
        d.on_stored(stored(f"00000000-0000-0000-0000-00000000000{i}"))
    await d.tick()
    assert len(db.calls) == 1 and len(db.calls[0][1]["ids"]) == 3


@pytest.mark.anyio
async def test_times_out_after_ten_seconds():
    d, emitted, t = detector(FakeTraceDb({"FROM taxi.trips": visible()}))
    d.on_stored(stored())
    t[0] = 10.0
    await d.tick()
    assert d.pending == 0 and emitted == []


@pytest.mark.anyio
async def test_at_most_twenty_in_flight_the_rest_wait():
    db = FakeTraceDb({"FROM taxi.trips": visible()})
    d, _, _ = detector(db)
    ids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(25)]
    for tid in ids:
        d.on_stored(stored(tid))
    assert d.pending == 20
    db.results["FROM taxi.trips"] = visible(*ids[:20])
    await d.tick()
    assert d.pending == 5  # the waiting five were promoted


@pytest.mark.anyio
async def test_database_down_does_not_kill_the_poller():
    db = FakeTraceDb()
    d, emitted, _ = detector(db)
    d.on_stored(stored())
    db.fail = DbUnavailable("down")
    await d.safe_tick()  # logged, not raised
    assert d.pending == 1 and emitted == []
```

`services/api/tests/test_trace_service.py`:

```python
import asyncio
import json
import time

import pytest
from conftest import FakeDb, FakeRideClock, FakeTraceDb, make_config
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_history import cp

from api.db import DbUnavailable, Result
from api.main import Deps, create_app
from rides.history import TraceHistory
from rides.hub import Hub
from rides.trace_service import TraceService, normalise_trace_id

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
EVENTS_COLUMNS = [("trace_id", "UUID"), ("trip_id", "String"), ("source", "String"), ("stage", "String"),
                  ("seq", "UInt8"), ("service", "String"), ("at", "DateTime64(3, 'UTC')"), ("detail", "String")]


def ch_rows(*stages: tuple[str, int, str]) -> Result:
    return Result(EVENTS_COLUMNS, [[TID, f"s-{TID}", "student", s, seq, "api", at, json.dumps({"k": s})]
                                   for s, seq, at in stages], 2)


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.closed: int | None = None

    async def send_json(self, data: dict) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000) -> None:
        self.closed = code


def svc(db: FakeTraceDb | None = None, **kw) -> tuple[TraceService, Hub]:
    hub = Hub(TraceHistory(3600))
    return TraceService(hub, db or FakeTraceDb(), FakeRideClock(), **kw), hub


@pytest.mark.anyio
async def test_history_merges_clickhouse_and_memory():
    # after an api restart, memory only has what arrived since; ClickHouse has the start
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, "2026-10-16T10:31:07.000+00:00"),
                                                   ("ingested", 2, "2026-10-16T10:31:07.041+00:00"))})
    s, hub = svc(db)
    hub.publish(cp(TID, "ingested", 2, "2026-10-16T10:31:07.041Z"))
    hub.publish(cp(TID, "validated", 3, "2026-10-16T10:31:07.103Z"))
    got = await s.history(TID)
    assert [c["stage"] for c in got] == ["submitted", "ingested", "validated"]
    assert got[0]["at"] == "2026-10-16T10:31:07.000Z" and got[0]["detail"] == {"k": "submitted"}
    sql, params = db.calls[-1]
    assert "{id:UUID}" in sql and params == {"id": TID}


@pytest.mark.anyio
async def test_history_uses_memory_when_clickhouse_is_down():
    db = FakeTraceDb()
    db.fail = DbUnavailable("down")
    s, hub = svc(db)
    hub.publish(cp(TID, "submitted", 1))
    assert [c["stage"] for c in await s.history(TID)] == ["submitted"]


@pytest.mark.anyio
async def test_history_rearms_queryable_after_restart():
    rearmed: list[str] = []
    db = FakeTraceDb({"taxi.trace_events": ch_rows(("submitted", 1, "2026-10-16T10:31:07.000+00:00"),
                                                   ("stored", 6, "2026-10-16T10:31:08.000+00:00"))})
    s, hub = svc(db)
    hub.on_stored = lambda c: rearmed.append(c["stage"])
    await s.history(TID)
    assert rearmed == ["stored"]


@pytest.mark.anyio
async def test_follow_sends_history_then_new_stages_once_then_closes_after_terminal():
    s, hub = svc(after_terminal_s=0.05)
    hub.publish(cp(TID, "submitted", 1))
    ws = FakeWs()
    task = asyncio.create_task(s.follow(ws, TID))
    for _ in range(20):
        await asyncio.sleep(0)
    hub.publish(cp(TID, "ingested", 2))
    hub.publish(cp(TID, "ingested", 2))
    hub.publish(cp(TID, "queryable", 7))
    await asyncio.wait_for(task, 1)
    assert ws.sent[0]["type"] == "history" and [c["stage"] for c in ws.sent[0]["checkpoints"]] == ["submitted"]
    assert [(m["type"], m["stage"]) for m in ws.sent[1:]] == [("checkpoint", "ingested"), ("checkpoint", "queryable")]
    assert ws.closed == 1000 and hub.follower_count(TID) == 0


@pytest.mark.anyio
async def test_follow_closes_at_the_max_duration():
    s, hub = svc(follow_max_s=0.05)
    ws = FakeWs()
    await asyncio.wait_for(s.follow(ws, TID), 1)
    assert ws.sent == [{"type": "history", "checkpoints": [], "sim_ts": "2025-10-09T17:42:00.250"}]
    assert ws.closed == 1000


@pytest.mark.anyio
async def test_live_sends_snapshot_clock_stats_and_flags_ambient():
    db = FakeTraceDb({"countIf(stage = 'submitted')": Result([("rides", "UInt64"), ("rejected", "UInt64")], [[57, 6]], 1),
                      "lagInFrame": Result([("stage", "String"), ("p50_ms", "Float64")], [["ingested", 40.0]], 1)})
    s, hub = svc(db, clock_every_s=0.01, stats_every_s=10)
    hub.publish(cp(TID, "submitted", 1))
    ws = FakeWs()
    task = asyncio.create_task(s.live(ws))
    await asyncio.sleep(0.03)
    hub.publish(cp("r", "ingested", 2, source="replay"))
    await asyncio.sleep(0.03)
    task.cancel()
    types = [m["type"] for m in ws.sent]
    assert types[0] == "snapshot" and ws.sent[0]["traces"][0]["trace_id"] == TID
    assert "clock" in types
    assert {"type": "stats", "rides_today": 57, "rejected_today": 6, "median_ms": {"ingested": 40}} in ws.sent
    (ambient,) = [m for m in ws.sent if m["type"] == "checkpoint"]
    assert ambient["ambient"] is True and ambient["trace_id"] == "r"
    assert hub.live_count() == 0


@pytest.mark.anyio
async def test_stats_are_cached_between_clients():
    db = FakeTraceDb()
    s, _ = svc(db, stats_every_s=60)
    await s.stats()
    await s.stats()
    assert len(db.calls) == 2  # one "today" query + one hops query, once


def test_normalise_trace_id():
    assert normalise_trace_id(TID.upper()) == TID
    assert normalise_trace_id("live") is None and normalise_trace_id("'; DROP TABLE x; --") is None


def client(s: TraceService) -> TestClient:
    return TestClient(create_app(Deps(cfg=make_config(), db=FakeDb(), sim_clock=FakeRideClock(), trace=s)))


def test_ws_route_sends_history_and_rejects_bad_ids():
    s, hub = svc(follow_max_s=0.2)
    hub.publish(cp(TID, "submitted", 1))
    c = client(s)
    with c.websocket_connect(f"/api/trace/{TID}") as ws:
        first = ws.receive_json()
        assert first["type"] == "history" and first["checkpoints"][0]["stage"] == "submitted"
    with pytest.raises(WebSocketDisconnect) as e:
        with c.websocket_connect("/api/trace/not-a-uuid") as ws:
            ws.receive_json()
    assert e.value.code == 1008


def test_disconnect_releases_the_subscription():
    s, hub = svc()  # follow_max_s = 10 min: only the disconnect can end it
    with client(s).websocket_connect(f"/api/trace/{TID}") as ws:
        ws.receive_json()
        assert hub.follower_count(TID) == 1
    for _ in range(50):  # the app task finishes on the TestClient's portal thread
        if hub.follower_count(TID) == 0:
            break
        time.sleep(0.02)
    assert hub.follower_count(TID) == 0


def test_live_route_is_not_parsed_as_a_trace_id():
    s, _ = svc()
    with client(s).websocket_connect("/api/trace/live") as ws:
        assert ws.receive_json()["type"] == "snapshot"


def test_get_trace_history():
    s, hub = svc()
    hub.publish(cp(TID, "submitted", 1))
    c = client(s)
    assert c.get(f"/api/trace/{TID}").json()["checkpoints"][0]["stage"] == "submitted"
    assert c.get("/api/trace/00000000-0000-0000-0000-000000000000").status_code == 404
    assert c.get("/api/trace/nope").status_code == 404


def test_uvicorn_can_serve_websockets():
    import importlib.util

    assert importlib.util.find_spec("websockets") is not None  # without it uvicorn answers every WS with 404
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/api && uv run pytest -q tests/test_relay.py tests/test_queryable.py tests/test_trace_service.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'rides.relay'`.

- [ ] **Step 3: Implement**

`services/api/pyproject.toml`: add `"websockets==15.0.1"` to `dependencies`, then `cd services/api && uv lock && uv sync`. If uv can't resolve 15.0.1 together with uvicorn 0.54.0, pin the version `uv add "uvicorn[standard]==0.54.0" --dry-run` reports for websockets. Record it in Task 12.

`services/api/src/rides/relay.py`:

```python
"""Relay consumer: every checkpoint on trace.checkpoints, from "now", handed to the event loop (spec 07)."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaException

from api import log
from rides.checkpoints import TRACE_TOPIC, parse_checkpoint


def consumer_config(brokers: str) -> dict[str, Any]:
    # A throwaway group without commits: every api instance sees everything from "now". A short fetch wait keeps
    # each hop visible on the phone within ~50 ms instead of librdkafka's default 500 ms.
    return {"bootstrap.servers": brokers, "group.id": f"api-relay-{uuid.uuid4()}", "enable.auto.commit": False,
            "auto.offset.reset": "latest", "fetch.wait.max.ms": 50}


class TraceRelay:
    def __init__(self, brokers: str) -> None:
        self._brokers = brokers
        self._deliver: Callable[[dict[str, Any]], None] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, deliver: Callable[[dict[str, Any]], None]) -> None:
        self._deliver = deliver
        self._thread = threading.Thread(target=self._run, name="trace-relay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def handle(self, value: bytes | None) -> None:
        cp = parse_checkpoint(value)
        if cp is None or self._deliver is None:
            return
        try:
            self._deliver(cp)
        except RuntimeError:  # the event loop is shutting down
            self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._consume()
            except KafkaException as exc:
                log.event("trace_relay_error", error=str(exc)[:300])
                self._stop.wait(2)

    def _consume(self) -> None:
        consumer = Consumer(consumer_config(self._brokers))
        try:
            consumer.subscribe([TRACE_TOPIC])
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is not None and msg.error() is None:
                    self.handle(msg.value())
        finally:
            consumer.close()
```

`services/api/src/rides/queryable.py`:

```python
"""Queryable detector (spec 07): after `stored`, poll as trace_ro until the row is visible, then emit `queryable`."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from api import log
from api.db import DbError, DbUnavailable, TraceDb
from rides.checkpoints import checkpoint, parse_wall, wall_str

# One query per tick for every pending trace (ADR-019), so a burst of 50 rides costs 4 queries/s, not 200.
VISIBLE_SQL = "SELECT DISTINCT trace_id FROM taxi.trips WHERE trace_id IN {ids:Array(UUID)}"


@dataclass
class _Pending:
    stored: dict[str, Any]
    started: float


class QueryableDetector:
    def __init__(self, db: TraceDb, emit: Callable[[dict[str, Any]], None], poll_ms: int, *,
                 timeout_s: float = 10.0, max_pending: int = 20, clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._db, self._emit, self._poll_s = db, emit, poll_ms / 1000
        self._timeout, self._max, self._clock, self._wall = timeout_s, max_pending, clock, wall
        self._pending: dict[str, _Pending] = {}
        self._waiting: deque[dict[str, Any]] = deque(maxlen=500)
        self._wake = asyncio.Event()

    @property
    def pending(self) -> int:
        return len(self._pending)

    def on_stored(self, cp: dict[str, Any]) -> None:
        if cp["trace_id"] in self._pending:
            return
        if len(self._pending) < self._max:
            self._pending[cp["trace_id"]] = _Pending(cp, self._clock())
        else:
            self._waiting.append(cp)
        self._wake.set()

    def _queryable(self, stored: dict[str, Any]) -> dict[str, Any]:
        now = self._wall()
        ms = max(0, int((now - parse_wall(stored["at"])).total_seconds() * 1000))
        return checkpoint(stored["trace_id"], stored["trip_id"], stored["source"], "queryable",
                          {"table": "taxi.trips", "visible_after_ms": ms}, wall_str(now))

    async def tick(self) -> None:
        if not self._pending:
            return
        ids = list(self._pending)
        r = await self._db.query(VISIBLE_SQL, {"ids": ids})
        visible = {str(row[0]) for row in r.rows}
        now = self._clock()
        for tid in ids:
            p = self._pending[tid]
            if tid in visible:
                del self._pending[tid]
                self._emit(self._queryable(p.stored))
            elif now - p.started >= self._timeout:
                del self._pending[tid]
                log.event("queryable_timeout", trace_id=tid, source=p.stored["source"])
        while self._waiting and len(self._pending) < self._max:
            self.on_stored(self._waiting.popleft())

    async def safe_tick(self) -> None:
        try:
            await self.tick()
        except (DbError, DbUnavailable) as exc:
            log.event("queryable_poll_failed", error=str(exc)[:200])

    async def run(self) -> None:
        while True:
            if not self._pending:
                self._wake.clear()
                await self._wake.wait()
            await self.safe_tick()
            await asyncio.sleep(self._poll_s)
```

`services/api/src/rides/trace_service.py`:

```python
"""History + live WebSocket streams for the trace viewers (spec 07 §Trace relay)."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Coroutine
from datetime import datetime
from typing import Any, Protocol

from starlette.websockets import WebSocket, WebSocketDisconnect

from api import log
from api.db import DbError, DbUnavailable, TraceDb
from rides.checkpoints import TERMINAL, wall_str
from rides.hub import Hub

HISTORY_SQL = ("SELECT trace_id, trip_id, source, stage, seq, service, at, detail FROM taxi.trace_events "
               "WHERE trace_id = {id:UUID} ORDER BY seq, at")
# "Today" is the wall-clock event day, like Grafana panel 11: these are visitors' rides, not replayed trips.
TODAY_SQL = ("SELECT countIf(stage = 'submitted') AS rides, countIf(stage = 'rejected') AS rejected "
             "FROM taxi.trace_events WHERE source = 'student' AND at >= toStartOfDay(now64(3))")
HOPS_SQL = """
SELECT stage, round(quantile(0.5)(ms)) AS p50_ms
FROM
(
    SELECT stage, seq,
           dateDiff('millisecond',
                    lagInFrame(at) OVER (PARTITION BY trace_id ORDER BY seq ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
                    at) AS ms,
           row_number() OVER (PARTITION BY trace_id ORDER BY seq) AS rn
    FROM taxi.trace_events
    WHERE at >= now64(3) - INTERVAL 15 MINUTE
)
WHERE rn > 1
GROUP BY stage
ORDER BY any(seq)
"""


class Sender(Protocol):
    async def send_json(self, data: Any) -> None: ...

    async def close(self, code: int = 1000) -> None: ...


class LatestClock(Protocol):
    latest: dict[str, Any] | None


def normalise_trace_id(s: str) -> str | None:
    try:
        return str(uuid.UUID(s))
    except ValueError:
        return None


def _json_object(text: Any) -> dict[str, Any]:
    try:
        value = json.loads(text) if isinstance(text, str) else text
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def _row_checkpoint(row: dict[str, Any]) -> dict[str, Any]:
    return {"trace_id": str(row["trace_id"]), "trip_id": row["trip_id"], "source": row["source"],
            "stage": row["stage"], "seq": int(row["seq"]), "service": row["service"],
            "at": wall_str(datetime.fromisoformat(str(row["at"]))), "detail": _json_object(row["detail"])}


class TraceService:
    def __init__(self, hub: Hub, db: TraceDb, sim_clock: LatestClock, *, follow_max_s: float = 600,
                 after_terminal_s: float = 60, clock_every_s: float = 1.0, stats_every_s: float = 5.0) -> None:
        self._hub, self._db, self._sim = hub, db, sim_clock
        self._follow_max, self._after_terminal = follow_max_s, after_terminal_s
        self._clock_every, self._stats_every = clock_every_s, stats_every_s
        self._stats: dict[str, Any] | None = None
        self._stats_at = float("-inf")

    def sim_ts(self) -> str | None:
        latest = self._sim.latest
        return str(latest["sim_ts"]) if latest and "sim_ts" in latest else None

    async def history(self, trace_id: str) -> list[dict[str, Any]]:
        """Memory ∪ taxi.trace_events, dedup by stage. After an api restart, memory only has what came since."""
        try:
            r = await self._db.query(HISTORY_SQL, {"id": trace_id})
            stored = [_row_checkpoint(dict(zip(r.names, row, strict=True))) for row in r.rows]
        except (DbError, DbUnavailable):
            stored = []
        cps = self._hub.history.merge(trace_id, stored) if stored else self._hub.history.get(trace_id)
        stages = {c["stage"] for c in cps}
        if "stored" in stages and not stages & TERMINAL and self._hub.on_stored is not None:
            # restarted between `stored` and `queryable`: poll again so the viewer still reaches the end
            self._hub.on_stored(next(c for c in cps if c["stage"] == "stored"))
        return cps

    async def follow(self, ws: Sender, trace_id: str) -> None:
        q = self._hub.follow(trace_id)  # subscribe first, so nothing published during the history read is lost
        try:
            cps = await self.history(trace_id)
            seen = {c["stage"] for c in cps}
            await ws.send_json({"type": "history", "checkpoints": cps, "sim_ts": self.sim_ts()})
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._follow_max
            if seen & TERMINAL:
                deadline = min(deadline, loop.time() + self._after_terminal)
            while (left := deadline - loop.time()) > 0:
                try:
                    cp = await asyncio.wait_for(q.get(), left)
                except TimeoutError:
                    break
                if cp["stage"] in seen:
                    continue
                seen.add(cp["stage"])
                await ws.send_json({"type": "checkpoint", **cp})
                if cp["stage"] in TERMINAL:
                    deadline = min(deadline, loop.time() + self._after_terminal)
            await ws.close(1000)
        finally:
            self._hub.unfollow(trace_id, q)

    async def stats(self) -> dict[str, Any] | None:
        now = asyncio.get_running_loop().time()
        if now - self._stats_at < self._stats_every:
            return self._stats
        try:
            today = await self._db.query(TODAY_SQL, {})
            hops = await self._db.query(HOPS_SQL, {})
        except (DbError, DbUnavailable):
            return self._stats
        row = dict(zip(today.names, today.rows[0], strict=True)) if today.rows else {}
        self._stats = {"type": "stats", "rides_today": int(row.get("rides") or 0),
                       "rejected_today": int(row.get("rejected") or 0),
                       "median_ms": {str(r[0]): int(r[1]) for r in hops.rows if len(r) > 1 and r[1] is not None}}
        self._stats_at = now
        return self._stats

    async def live(self, ws: Sender) -> None:
        q = self._hub.watch_live()
        try:
            traces = [{"trace_id": t, "checkpoints": c} for t, c in self._hub.history.recent_student(20)]
            await ws.send_json({"type": "snapshot", "traces": traces, "sim_ts": self.sim_ts()})
            loop = asyncio.get_running_loop()
            next_clock = next_stats = loop.time()
            while True:
                now = loop.time()
                if now >= next_clock:
                    await ws.send_json({"type": "clock", "sim_ts": self.sim_ts()})
                    next_clock = now + self._clock_every
                if now >= next_stats:
                    stats = await self.stats()
                    if stats is not None:
                        await ws.send_json(stats)
                    next_stats = now + self._stats_every
                try:
                    cp = await asyncio.wait_for(q.get(), max(0.0, min(next_clock, next_stats) - loop.time()))
                except TimeoutError:
                    continue
                await ws.send_json({"type": "checkpoint", **cp, "ambient": cp.get("source") != "student"})
        finally:
            self._hub.unwatch_live(q)


async def serve(ws: WebSocket, work: Coroutine[Any, Any, None]) -> None:
    """Run `work` until it ends or the client goes away (a phone locking its screen), whichever comes first."""

    async def disconnected() -> None:
        while (await ws.receive())["type"] != "websocket.disconnect":
            pass

    tasks = [asyncio.ensure_future(work), asyncio.ensure_future(disconnected())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for t in tasks:
            t.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception) and not isinstance(r, WebSocketDisconnect | RuntimeError):
            log.event("ws_error", error=log.error_label(r), where=log.error_where(r))
```

`services/api/src/api/main.py`, inside `create_app`. **Register `/api/trace/live` before `/api/trace/{trace_id}`**, or "live" is parsed as an id:

```python
    @app.websocket("/api/trace/live")
    async def trace_live(ws: WebSocket) -> None:
        await ws.accept()
        await serve(ws, deps.trace.live(ws))

    @app.websocket("/api/trace/{trace_id}")
    async def trace_follow(ws: WebSocket, trace_id: str) -> None:
        tid = normalise_trace_id(trace_id)
        if tid is None:
            await ws.close(code=1008)
            return
        await ws.accept()
        await serve(ws, deps.trace.follow(ws, tid))

    @app.get("/api/trace/{trace_id}")
    async def trace_history(trace_id: str) -> dict[str, Any]:
        tid = normalise_trace_id(trace_id)
        cps = await deps.trace.history(tid) if tid else []
        if not cps:
            raise HTTPException(404, "No such ride.")
        return {"trace_id": tid, "checkpoints": cps}
```

(Import `WebSocket` from `fastapi`, and `serve`, `normalise_trace_id` from `rides.trace_service`.)

`app_from_env()`: build and wire everything, and extend the lifespan:

```python
    out = KafkaOut(cfg.brokers)
    trace_db = ClickHouseTraceDb(cfg)
    hub = Hub(TraceHistory(cfg.trace_history_ttl_s))

    def emit(cp: dict[str, Any]) -> None:
        out.send_nowait(TRACE_TOPIC, cp["trace_id"], cp)  # → taxi.trace_events (Kafka engine) and back via the relay
        hub.publish(cp)

    detector = QueryableDetector(trace_db, emit, cfg.trace_queryable_poll_ms)
    hub.on_stored = detector.on_stored
    rides = RideService(cfg, trace_db, out, follower, publish=hub.publish)
    trace = TraceService(hub, trace_db, follower)
    relay = TraceRelay(cfg.brokers)
    deps = Deps(cfg=cfg, db=db, sim_clock=follower, pool=pool, agent=agent, golden=tuple(golden),
                rides=rides, trace=trace)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()
        follower.start()
        out.start()
        relay.start(lambda cp: loop.call_soon_threadsafe(hub.publish, cp))
        tasks = [asyncio.create_task(pool.health_loop(cfg.llm_health_interval_s)),
                 asyncio.create_task(detector.run()), asyncio.create_task(rides.warm_up())]
        yield
        for t in tasks:
            t.cancel()
        relay.stop()
        out.stop()
        follower.stop()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/api && uv run pytest -q && uv run ruff check src tests && uv run mypy src`
Expected: all pass.

`test_live_sends_snapshot_clock_stats_and_flags_ambient` cancels the task after about 60 ms. If it's flaky on a loaded machine, raise the sleeps to 0.1 s, not the assertions.

- [ ] **Step 5: Rebuild the api and smoke-test the WebSocket through Caddy**

Run `docker compose up -d --build --wait api`, then run this script from the scratchpad with `cd services/api && uv run python <scratchpad>/ws_smoke.py`. The api venv has `websockets` from Step 3.

```python
import asyncio, json, websockets
async def main():
    async with websockets.connect("ws://localhost/api/trace/live") as ws:
        print(json.loads(await ws.recv())["type"])
asyncio.run(main())
```

Expected: `snapshot`. That proves uvicorn serves WebSockets and Caddy proxies the upgrade.

- [ ] **Step 6: Commit**

```bash
git add services/api/pyproject.toml services/api/uv.lock services/api/src services/api/tests
git commit -m "feat(api): trace relay, batched queryable detector, WS /api/trace/{id} + /api/trace/live, GET history"
```

---

### Task 6: Stack end-to-end tests, burst test, Stand Health panels, Makefile targets

**Files:**
- Modify: `pyproject.toml` (root: dev += `"websockets==15.0.1"`, or the version Task 5 pinned; marker `e2e`; addopts)
- Create: `infra/tests/test_rides_e2e.py`
- Create: `scripts/burst_rides.py`
- Modify: `infra/grafana/provisioning-ops/dashboards/stand-health.json` (+ panels 15, 16)
- Modify: `Makefile` (`test-e2e`, `burst`, `reset-rides`, `.PHONY`)

**Interfaces:**
- Consumes: the running stack (`make up`) with Tasks 1–5 deployed. The api is on `127.0.0.1:8000` (it trusts `X-Forwarded-For`, spec 09) and Caddy is on `localhost:80`.
- Produces: `make test-e2e` (spec 07 acceptance 3–6) and `make burst` (acceptance 7).

- [ ] **Step 1: Root test config**

Root `pyproject.toml`:
- `dev = ["pytest==9.1.1", "ruff==0.16.9", "chdb==4.4.0", "websockets==15.0.1"]`
- `markers = ["stack: needs the running compose stack (make up)", "e2e: trace-your-ride end to end on the running stack (make test-e2e)"]`
- `addopts = "-m 'not stack and not e2e'"`

Then `uv lock && uv sync`.

- [ ] **Step 2: Write the end-to-end tests**

`infra/tests/test_rides_e2e.py`:

```python
"""Trace your ride end to end on the running stack (spec 07 acceptance 3-6): make up && make test-e2e.

POSTs go straight to the api on 127.0.0.1:8000 so each test can use its own X-Forwarded-For (uvicorn trusts it,
spec 09) and never trips the per-IP limit. WebSockets go through Caddy, the path a phone takes.
"""

import asyncio
import json
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

import clickhouse_connect
import pytest
import websockets

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "contracts"))
from contract_validator import assert_valid  # noqa: E402

pytestmark = pytest.mark.e2e
API = "http://127.0.0.1:8000"
CADDY_WS = "ws://localhost"
HAPPY = ["submitted", "ingested", "validated", "enriched", "aggregated", "stored", "queryable"]


def env() -> dict[str, str]:
    return dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
                if "=" in line and not line.lstrip().startswith("#"))


def unique_ip() -> str:
    n = uuid.uuid4().int
    return f"10.77.{n % 250}.{(n >> 8) % 250 + 1}"


def book(**overrides: object) -> dict:
    body = {"pickup_location_id": 161, "dropoff_location_id": 230, "payment": "card", "tip_amount": 3.0,
            "passenger_count": 1, **overrides}
    req = urllib.request.Request(f"{API}/api/rides", method="POST", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "X-Forwarded-For": unique_ip()})
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 201
        return json.load(r)


async def _collect(trace_id: str, until: set[str], timeout_s: float, base: str) -> tuple[list[dict], list[dict]]:
    history: list[dict] = []
    live: list[dict] = []
    deadline = time.monotonic() + timeout_s
    async with websockets.connect(f"{base}/api/trace/{trace_id}") as ws:
        while (left := deadline - time.monotonic()) > 0:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
            except (TimeoutError, websockets.ConnectionClosed):
                break
            if msg.pop("type") == "history":
                history = msg["checkpoints"]
            else:
                live.append(msg)
            if {c["stage"] for c in history + live} & until:
                break
    return history, live


def collect(trace_id: str, until: set[str], timeout_s: float, base: str = CADDY_WS) -> tuple[list[dict], list[dict]]:
    return asyncio.run(_collect(trace_id, until, timeout_s, base))


def by_seq(cps: list[dict]) -> list[str]:
    return [c["stage"] for c in sorted(cps, key=lambda c: (c["seq"], c["at"]))]


def ms(a: str, b: str) -> float:
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    return (datetime.strptime(b, fmt) - datetime.strptime(a, fmt)).total_seconds() * 1000


def test_normal_ride_reaches_queryable_within_5s():
    started = time.monotonic()
    ride = book()
    history, live = collect(ride["trace_id"], {"queryable", "rejected"}, 10)
    elapsed = time.monotonic() - started
    cps = history + live
    assert by_seq(cps) == HAPPY
    assert len({c["stage"] for c in cps}) == len(cps), "a stage arrived twice"
    for c in cps:
        assert_valid("trace_checkpoint", c)
    assert elapsed < 5, f"phone → queryable took {elapsed:.1f} s"
    first, last = min(c["at"] for c in cps), max(c["at"] for c in cps)
    print(f"\nsubmitted → queryable {ms(first, last):.0f} ms (wall), client saw it in {elapsed:.2f} s")


def test_absurd_tip_is_rejected_and_nothing_follows():
    ride = book(tip_amount=999)
    history, live = collect(ride["trace_id"], {"queryable"}, 6)  # listens the full 6 s: nothing may follow
    cps = history + live
    assert by_seq(cps) == ["submitted", "ingested", "rejected"]
    rejected = next(c for c in cps if c["stage"] == "rejected")
    assert "TIP_UNREALISTIC" in rejected["detail"]["reasons"]


def test_reconnect_after_stored_gives_full_history_without_duplicates():
    ride = book()
    collect(ride["trace_id"], {"stored"}, 10)
    history, live = collect(ride["trace_id"], {"queryable"}, 5)
    stages = [c["stage"] for c in history + live]
    assert sorted(stages, key=HAPPY.index) == HAPPY and len(stages) == len(set(stages))


def test_api_restart_keeps_history():
    ride = book()
    collect(ride["trace_id"], {"queryable"}, 10)
    e = env()
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="agent_ro",
                                       password=e["CLICKHOUSE_AGENT_PASSWORD"], database="taxi")
    deadline = time.monotonic() + 20  # the Kafka engine flushes taxi.trace_events every few seconds
    while time.monotonic() < deadline:
        n = ch.query("SELECT uniqExact(stage) FROM taxi.trace_events WHERE trace_id = {id:UUID}",
                     parameters={"id": ride["trace_id"]}).result_rows[0][0]
        if n == 7:
            break
        time.sleep(1)
    assert n == 7, "the checkpoints never reached taxi.trace_events"
    subprocess.run(["docker", "compose", "restart", "api"], cwd=ROOT, check=True, capture_output=True)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{API}/api/healthz", timeout=2) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(1)
    history, _ = collect(ride["trace_id"], {"queryable"}, 5)
    assert by_seq(history) == HAPPY
```

- [ ] **Step 3: Write the burst script**

`scripts/burst_rides.py`:

```python
"""make burst (spec 07 acceptance 7): 50 rides in 10 s from 10 simulated phones against the running api.

Pass = no 5xx, every accepted ride reaches a terminal stage, each phone's 4th and 5th ride within the minute
get 429, and the p95 of POST /api/rides is under 300 ms. Run it with no other rides booked in the last minute:
10 phones × 3 rides is exactly the global cap of 30.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import websockets

API = "http://127.0.0.1:8000"
WS = "ws://127.0.0.1:8000"
PHONES = [f"10.88.0.{i}" for i in range(1, 11)]
RIDES_PER_PHONE = 5
SPAN_S = 10.0
BODY = {"pickup_location_id": 161, "dropoff_location_id": 230, "payment": "card", "tip_amount": 3.0,
        "passenger_count": 1}


def post(ip: str) -> tuple[int, float, dict[str, Any]]:
    req = urllib.request.Request(f"{API}/api/rides", method="POST", data=json.dumps(BODY).encode(),
                                 headers={"Content-Type": "application/json", "X-Forwarded-For": ip})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, time.monotonic() - started, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, time.monotonic() - started, json.loads(e.read() or b"{}")
    except OSError:
        return 0, time.monotonic() - started, {}


async def terminal(trace_id: str, timeout_s: float = 30) -> str | None:
    deadline = time.monotonic() + timeout_s
    try:
        async with websockets.connect(f"{WS}/api/trace/{trace_id}") as ws:
            while (left := deadline - time.monotonic()) > 0:
                msg = json.loads(await asyncio.wait_for(ws.recv(), left))
                stages = [c["stage"] for c in msg["checkpoints"]] if msg["type"] == "history" else [msg["stage"]]
                for s in stages:
                    if s in ("queryable", "rejected"):
                        return s
    except (TimeoutError, websockets.ConnectionClosed, OSError):
        return None
    return None


async def main() -> int:
    loop = asyncio.get_running_loop()
    pool = ThreadPoolExecutor(max_workers=16)
    # Round-robin over the phones, spread evenly over SPAN_S: every phone's 4th ride lands well inside its minute.
    schedule = [(i * SPAN_S / (len(PHONES) * RIDES_PER_PHONE), ip)
                for i, ip in enumerate(ip for _ in range(RIDES_PER_PHONE) for ip in PHONES)]
    started = time.monotonic()

    async def fire(at: float, ip: str) -> tuple[str, tuple[int, float, dict[str, Any]]]:
        await asyncio.sleep(max(0.0, at - (time.monotonic() - started)))
        return ip, await loop.run_in_executor(pool, post, ip)

    results = await asyncio.gather(*(fire(at, ip) for at, ip in schedule))
    statuses = [s for _, (s, _, _) in results]
    created = [b["trace_id"] for _, (s, _, b) in results if s == 201]
    latencies = sorted(dt for _, (s, dt, _) in results if s == 201)
    p95 = latencies[int(0.95 * (len(latencies) - 1))] if latencies else float("inf")
    per_phone: dict[str, list[int]] = {}
    for ip, (s, _, _) in results:
        per_phone.setdefault(ip, []).append(s)
    ends = await asyncio.gather(*(terminal(t) for t in created))

    checks = {
        "no 5xx or connection errors": all(0 < s < 500 for s in statuses),
        "every phone: 3 accepted, then 429": all(v == [201, 201, 201, 429, 429] for v in per_phone.values()),
        "every accepted ride reached a terminal stage": all(e is not None for e in ends),
        "p95 POST /api/rides < 300 ms": p95 < 0.3,
    }
    print(f"{len(results)} requests: {statuses.count(201)} × 201, {statuses.count(429)} × 429, "
          f"others {[s for s in statuses if s not in (201, 429)]}")
    print(f"terminal stages: {sorted((e or 'none') for e in ends)}")
    print(f"POST p95 {p95 * 1000:.0f} ms")
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
```

- [ ] **Step 4: Stand Health panels (spec 07 §Observability)**

Run this once from the repo root. It copies the "E2E latency p95" stat (id 3), so the datasource and options match. Then check that `git diff` shows only the two appended panels.

```python
import copy, json
from pathlib import Path

p = Path("infra/grafana/provisioning-ops/dashboards/stand-health.json")
d = json.loads(p.read_text())
e2e = next(x for x in d["panels"] if x["id"] == 3)

rides = copy.deepcopy(e2e)
rides.update(id=15, title="Visitor rides today", description="Rides booked on /ride since midnight (wall clock, the event day). Info only.",
             gridPos={"x": 0, "y": 26, "w": 6, "h": 5})
rides["targets"][0]["rawSql"] = ("-- Wall-clock day: the event day, not the sim day.\n"
                                 "SELECT countIf(stage = 'submitted') AS rides\nFROM taxi.trace_events\n"
                                 "WHERE source = 'student' AND at >= toStartOfDay(now64(3))")
rides["fieldConfig"]["defaults"]["unit"] = "none"
rides["fieldConfig"]["defaults"]["thresholds"]["steps"] = [{"color": "#1D9E75", "value": None}]
rides["options"]["colorMode"] = "none"

latency = copy.deepcopy(e2e)
latency.update(id=16, title="Visitor ride p95 · phone → queryable (last 60 min)",
               description="submitted → queryable per student ride, wall clock. Red above 5 s (the M4 exit bar).",
               gridPos={"x": 6, "y": 26, "w": 6, "h": 5})
latency["targets"][0]["rawSql"] = (
    "-- Wall clock only (at): from the phone's request to the row being visible to the agent.\n"
    "SELECT quantile(0.95)(ms) AS p95_ms\nFROM\n(\n"
    "    SELECT trace_id, dateDiff('millisecond', minIf(at, stage = 'submitted'), minIf(at, stage = 'queryable')) AS ms\n"
    "    FROM taxi.trace_events\n"
    "    WHERE source = 'student' AND at >= now64(3) - INTERVAL 60 MINUTE\n"
    "    GROUP BY trace_id\n"
    "    HAVING countIf(stage = 'submitted') > 0 AND countIf(stage = 'queryable') > 0\n)")

d["panels"] += [rides, latency]
p.write_text(json.dumps(d, indent=2, ensure_ascii=False) + "\n")
```

Run: `uv run pytest -q infra/tests/test_grafana_dashboards.py`
Expected: PASS. `test_query_runs_against_the_schema` runs the two new queries against the DDL in chDB, and `test_two_clocks_never_mixed` accepts `now64` next to `at`.

- [ ] **Step 5: Makefile**

Add after `test-stack`:

```make
test-e2e: ## Trace your ride end to end on the running stack (spec 07 acceptance 3-6)
	uv run pytest -q -m e2e

burst: ## 50 rides in 10 s from 10 simulated phones (spec 07 acceptance 7). Book no other rides in the minute before.
	uv run python scripts/burst_rides.py

reset-rides: ## Delete every visitor ride (trips, rejects, trace events). Asks first.
	@read -p "Delete all student rides? [y/N] " a && [ "$$a" = y ]
	$(COMPOSE) exec -T clickhouse clickhouse-client --user admin --password "$(CLICKHOUSE_ADMIN_PASSWORD)" --multiquery -q \
	  "DELETE FROM taxi.trips WHERE source = 'student'; DELETE FROM taxi.trips_rejected WHERE source = 'student'; DELETE FROM taxi.trace_events WHERE source = 'student'"
```

Add `test-e2e burst reset-rides` to `.PHONY`.

- [ ] **Step 6: Run everything against the stack**

Run: `make up && make test-stack && make test-e2e && sleep 60 && make burst`
Expected:
- `make test-stack` passes.
- `make test-e2e` gives 4 passed, and the first test prints the submitted → queryable time.
- `make burst` prints 4 × PASS.

The `sleep 60` lets the global bucket refill after the e2e rides. If the 5 s test fails, don't loosen the assertion. Read which hop is slow first: the relay's `fetch.wait.max.ms`, the processor's 1 s commit interval (`stored` comes after the INSERT at the commit), and the 250 ms poll are the usual suspects. Report the hop timings.

Open Stand Health (`http://127.0.0.1:3002`) and check that panels 15 and 16 show numbers.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock infra/tests/test_rides_e2e.py scripts/burst_rides.py \
  infra/grafana/provisioning-ops/dashboards/stand-health.json Makefile
git commit -m "test: trace-your-ride e2e + burst on the running stack; Stand Health visitor-ride panels; make reset-rides"
```

---

### Task 7: Web foundations: generated contract types, trace/ride/ws/wifi libs, api client, phone styles

**Files:**
- Modify: `web/package.json` (scripts `gen:types`, `typecheck`; deps)
- Create: `web/scripts/gen-types.mjs`
- Create (generated, committed): `web/lib/contracts/trace-checkpoint.ts`, `web/lib/contracts/trip.ts`
- Create: `web/lib/trace.ts`, `web/lib/ws.ts`, `web/lib/ride.ts`, `web/lib/wifi.ts`, `web/lib/use-poll.ts`
- Modify: `web/lib/format.ts` (+ `formatSim`, moved), `web/components/agent/Header.tsx` (import `formatSim` and `usePoll` from `lib/`)
- Modify: `web/lib/api.ts`
- Modify: `web/app/globals.css`
- Modify: `.env.example` (`NEXT_PUBLIC_API_WS_ORIGIN`)
- Test: `web/tests/unit/trace.test.ts`, `web/tests/unit/ws.test.ts`, `web/tests/unit/ride.test.ts`, `web/tests/unit/wifi.test.ts`

**Interfaces:**
- Produces (`lib/trace.ts`):
  - Types: `Checkpoint` (= generated `TraceCheckpoint`), `Stage`.
  - Constants: `HAPPY_PATH`, `TERMINAL`, `STAGE_TITLE`, `REASON_TEXT`.
  - Functions:
    - `isTraceId(s)`, `waitingText(next: Stage)`, `stageCopy(cp)`;
    - `mergeCheckpoints(have, incoming)`, `hopMs(cps)`, `totalMs(cps)`;
    - `toTraceMessage(raw) -> TraceMessage | null`, `applyMessage(view, msg)`;
    - `rideSummary(cps) -> RideSummary | null`, `rideChip(summary)`, `money(x)`.
  - `EMPTY_VIEW`.
- Produces (`lib/ws.ts`):
  - `BACKOFF_MS`, `backoffDelay(attempt)`, `wsUrl(path, loc, origin?)`;
  - `SocketFactory`, `browserSocket`;
  - `ReconnectingSocket(url, onMessage, onStatus?, factory?)` with `.stop()`;
  - `SocketStatus = "connecting" | "open" | "retrying"`.
- Produces (`lib/ride.ts`):
  - `POPULAR_ZONE_IDS` and `TipChoice`;
  - `tipAmount(choice, fare)`, `groupByBorough(zones)`, `filterZones(zones, q)`, `estimateLine(est)`;
  - `LAST_TRACE_KEY`, `saveLastTrace(id, storage?)`, `loadLastTrace(storage?)`.
- Produces (`lib/wifi.ts`): `wifiQrPayload(ssid, password)`.
- Produces (`lib/api.ts`):
  - types `Zone`, `RideEstimate`, `RideRequest`, `RideCreated`, `BookResult`, `TraceHistory`;
  - `getZones()`, `getEstimate(pu, dropoff)`, `bookRide(body)`, `getTrace(id)`.
- Produces: `lib/use-poll.ts` `usePoll(load, everyMs)` and `lib/format.ts` `formatSim(ts)`.

- [ ] **Step 1: Dependencies and type generation**

```bash
cd web
pnpm add -E qrcode.react@4.2.0
pnpm add -E -D json-schema-to-typescript@15.0.4
```

(If either version doesn't exist in the registry, take the newest release of the same major, and note it for ARCHITECTURE §9 in Task 12.)

`web/scripts/gen-types.mjs`:

```js
// pnpm gen:types: TypeScript types from contracts/events/*.schema.json (spec 08 acceptance 6).
// Changing a contract field changes these files, and `pnpm typecheck` (which runs this first) breaks.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { compile } from "json-schema-to-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const events = join(here, "..", "..", "contracts", "events");
const out = join(here, "..", "lib", "contracts");
const TARGETS = [
  ["trace_checkpoint", "TraceCheckpoint", "trace-checkpoint.ts"],
  ["trip", "Trip", "trip.ts"],
];

mkdirSync(out, { recursive: true });
for (const [file, name, target] of TARGETS) {
  const schema = JSON.parse(readFileSync(join(events, `${file}.schema.json`), "utf8"));
  delete schema.title; // the title would become the type name
  delete schema.examples;
  const ts = await compile(schema, name, {
    bannerComment: `/* Generated by \`pnpm gen:types\` from contracts/events/${file}.schema.json. Do not edit. */`,
    additionalProperties: false,
    unreachableDefinitions: false,
  });
  writeFileSync(join(out, target), ts);
}
```

`web/package.json` scripts: add `"gen:types": "node scripts/gen-types.mjs"`, and change `"typecheck"` to `"pnpm gen:types && tsc --noEmit"`.

Run: `pnpm gen:types && sed -n 1,40p lib/contracts/trace-checkpoint.ts`
Expected: an `export interface TraceCheckpoint` with `stage: "submitted" | "ingested" | … | "queryable"` and `detail: { [k: string]: unknown }`.

If ESLint complains about the generated files, add `"lib/contracts/**"` to the ignores in `eslint.config.mjs`.

- [ ] **Step 2: Write the failing unit tests**

`web/tests/unit/trace.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  applyMessage, EMPTY_VIEW, hopMs, isTraceId, mergeCheckpoints, rideChip, rideSummary, stageCopy, toTraceMessage,
  totalMs, waitingText, type Checkpoint,
} from "@/lib/trace";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const cp = (stage: Checkpoint["stage"], seq: number, at: string, detail: Record<string, unknown> = {}): Checkpoint => ({
  trace_id: TID, trip_id: `s-${TID}`, source: "student", stage, seq, service: "processor", at, detail,
});
const S = cp("submitted", 1, "2026-10-16T10:31:07.000Z", { pickup_zone: "Midtown Center", dropoff_zone: "JFK Airport", payment: "card", tip_amount: 5, fare_amount: 70.5 });
const I = cp("ingested", 2, "2026-10-16T10:31:07.041Z", { topic: "trips.raw", partition: 0, offset: 482113 });
const Q = cp("queryable", 7, "2026-10-16T10:31:08.800Z", { table: "taxi.trips", visible_after_ms: 590 });

describe("merge and latency", () => {
  it("dedups by stage (first wins) and orders by seq, not arrival", () => {
    const later = { ...I, at: "2026-10-16T10:31:09.000Z" };
    expect(mergeCheckpoints([Q, I], [S, later]).map((c) => c.stage)).toEqual(["submitted", "ingested", "queryable"]);
    expect(mergeCheckpoints([I], [later])[0]!.at).toBe(I.at);
  });
  it("hop latency comes from wall time between consecutive stages", () => {
    expect(hopMs([Q, S, I])).toEqual({ ingested: 41, queryable: 1759 });
    expect(totalMs([S, I, Q])).toBe(1800);
    expect(totalMs([S, I])).toBeNull();
  });
});

describe("stage copy (spec 07)", () => {
  it("fills the detail values in", () => {
    expect(stageCopy(I)).toBe("Safely stored in Redpanda: topic trips.raw, partition 0, offset 482113. It can't get lost now.");
    expect(stageCopy(cp("validated", 3, S.at, { rules_checked: 11 }))).toBe("The stream processor checked 11 data-quality rules. All passed ✓");
    expect(stageCopy(cp("aggregated", 5, S.at, { window_start: "2025-10-09T17:42:00", borough: "Manhattan" })))
      .toBe("Counted in the Manhattan · 17:42 bucket on the live dashboard.");
    expect(stageCopy(cp("stored", 6, S.at, { table: "taxi.trips", batch_rows: 142 }))).toBe("Written to ClickHouse (taxi.trips) in a batch of 142 trips.");
    expect(stageCopy(Q)).toBe("The AI agent can now see your ride. Ask it something!");
  });
  it("rejected names the reason in plain English", () => {
    expect(stageCopy(cp("rejected", 3, S.at, { reasons: ["TIP_UNREALISTIC"] })))
      .toBe("Rejected: the tip is unrealistically high. Our pipeline rejects about 1 in 10 real NYC trips for problems like this. Try a normal tip!");
  });
  it("enriched copy for a card ride shows the tip %", () => {
    expect(stageCopy(cp("enriched", 4, S.at, { tip_pct: 20, duration_min: 16.5 }))).toBe("Added zone and borough names, and worked out your tip (20%) and duration.");
  });
  it("enriched copy for a cash ride never shows null%", () => {
    const text = stageCopy(cp("enriched", 4, S.at, { tip_pct: null, duration_min: 16.5 }));
    expect(text).not.toContain("null");
    expect(text).toBe("Added zone and borough names, and worked out your duration. Cash tips aren't recorded, so there's no tip %.");
  });
  it("waiting text names who is next", () => {
    expect(waitingText("validated")).toBe("waiting for the stream processor…");
    expect(waitingText("stored")).toBe("waiting for ClickHouse…");
  });
});

describe("messages", () => {
  it("parses history and checkpoint frames and drops garbage", () => {
    expect(toTraceMessage({ type: "history", checkpoints: [S, { nope: 1 }], sim_ts: "2025-10-09T17:42:00" }))
      .toEqual({ type: "history", checkpoints: [S], sim_ts: "2025-10-09T17:42:00" });
    expect(toTraceMessage({ type: "checkpoint", ...I })).toEqual({ type: "checkpoint", checkpoint: I });
    expect(toTraceMessage({ type: "checkpoint", stage: "teleported" })).toBeNull();
    expect(toTraceMessage("x")).toBeNull();
  });
  it("a reconnect's history merges without duplicates", () => {
    let v = applyMessage(EMPTY_VIEW, { type: "history", checkpoints: [S], sim_ts: null });
    v = applyMessage(v, { type: "checkpoint", checkpoint: I });
    v = applyMessage(v, { type: "history", checkpoints: [S, I, Q], sim_ts: "2025-10-09T17:42:00" });
    expect(v.checkpoints.map((c) => c.stage)).toEqual(["submitted", "ingested", "queryable"]);
    expect(v.simTs).toBe("2025-10-09T17:42:00");
  });
});

describe("ride summary", () => {
  it("rideChip for card", () => {
    expect(rideChip(rideSummary([S])!)).toBe("Midtown Center → JFK Airport, $5 tip");
    expect(rideChip({ ...rideSummary([S])!, tip: 3.9 })).toBe("Midtown Center → JFK Airport, $3.90 tip");
  });
  it("rideChip for cash", () => {
    const cash = cp("submitted", 1, S.at, { ...S.detail, payment: "cash" });
    expect(rideChip(rideSummary([cash])!)).toBe("Midtown Center → JFK Airport, cash");
  });
  it("no submitted checkpoint, no summary", () => expect(rideSummary([I])).toBeNull());
  it("trace id check", () => {
    expect(isTraceId(TID)).toBe(true);
    expect(isTraceId("live")).toBe(false);
  });
});
```

`web/tests/unit/ws.test.ts`:

```ts
import { afterEach, describe, expect, it, vi } from "vitest";
import { backoffDelay, ReconnectingSocket, wsUrl, type SocketFactory, type SocketHandlers } from "@/lib/ws";

afterEach(() => vi.useRealTimers());

describe("wsUrl", () => {
  it("same origin, wss behind https, dev override", () => {
    expect(wsUrl("/api/trace/live", { protocol: "http:", host: "192.168.8.10" })).toBe("ws://192.168.8.10/api/trace/live");
    expect(wsUrl("/api/trace/live", { protocol: "https:", host: "stand.local" })).toBe("wss://stand.local/api/trace/live");
    expect(wsUrl("/api/trace/live", { protocol: "http:", host: "localhost:3000" }, "ws://localhost:8000/")).toBe("ws://localhost:8000/api/trace/live");
  });
});

it("backoff is 0.5, 1, 2, 5 s and then stays at 5 s", () => {
  expect([0, 1, 2, 3, 4, 9].map(backoffDelay)).toEqual([500, 1000, 2000, 5000, 5000, 5000]);
});

it("reconnects with backoff, resets after a successful open, and stops for good", () => {
  vi.useFakeTimers();
  const made: { h: SocketHandlers; closed: boolean }[] = [];
  const factory: SocketFactory = (_url, h) => {
    const s = { h, closed: false, close() { this.closed = true; } };
    made.push(s);
    return s;
  };
  const got: unknown[] = [];
  const statuses: string[] = [];
  const sock = new ReconnectingSocket("ws://x/api/trace/1", (m) => got.push(m), (s) => statuses.push(s), factory);
  made[0]!.h.open();
  made[0]!.h.message('{"type":"history"}');
  made[0]!.h.message("not json");
  expect(got).toEqual([{ type: "history" }]);

  made[0]!.h.close(1006);
  vi.advanceTimersByTime(499);
  expect(made).toHaveLength(1);
  vi.advanceTimersByTime(1);
  expect(made).toHaveLength(2);
  made[1]!.h.close(1006);
  vi.advanceTimersByTime(999);
  expect(made).toHaveLength(2);
  vi.advanceTimersByTime(1);
  expect(made).toHaveLength(3);
  made[2]!.h.open();
  made[2]!.h.close(1000);
  vi.advanceTimersByTime(500);
  expect(made).toHaveLength(4); // back to 0.5 s after a good connection

  sock.stop();
  expect(made[3]!.closed).toBe(true);
  made[3]!.h.close(1000);
  vi.advanceTimersByTime(10_000);
  expect(made).toHaveLength(4);
  expect(statuses.slice(0, 3)).toEqual(["connecting", "open", "retrying"]);
});
```

`web/tests/unit/ride.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { Zone } from "@/lib/api";
import { estimateLine, filterZones, groupByBorough, loadLastTrace, saveLastTrace, tipAmount } from "@/lib/ride";

const ZONES: Zone[] = [
  { location_id: 255, zone: "Williamsburg (North Side)", borough: "Brooklyn" },
  { location_id: 79, zone: "East Village", borough: "Manhattan" },
  { location_id: 161, zone: "Midtown Center", borough: "Manhattan" },
  { location_id: 132, zone: "JFK Airport", borough: "Queens" },
];

describe("tips", () => {
  it("percentages of the estimated fare, rounded to cents", () => {
    expect(tipAmount({ kind: "pct", pct: 20 }, 19.5)).toBe(3.9);
    expect(tipAmount({ kind: "pct", pct: 15 }, 19.5)).toBe(2.93);
    expect(tipAmount({ kind: "pct", pct: 25 }, null)).toBe(0);
    expect(tipAmount({ kind: "none" }, 19.5)).toBe(0);
  });
  it("custom is clamped to 0-999 with no other warning (the processor decides)", () => {
    expect(tipAmount({ kind: "custom", amount: 999 }, 19.5)).toBe(999);
    expect(tipAmount({ kind: "custom", amount: 5000 }, 19.5)).toBe(999);
    expect(tipAmount({ kind: "custom", amount: -3 }, 19.5)).toBe(0);
    expect(tipAmount({ kind: "custom", amount: Number.NaN }, 19.5)).toBe(0);
    expect(tipAmount({ kind: "custom", amount: 4.567 }, 19.5)).toBe(4.57);
  });
});

describe("zones", () => {
  it("groups by borough in api order", () => {
    expect(groupByBorough(ZONES).map(([b, zs]) => [b, zs.length])).toEqual([["Brooklyn", 1], ["Manhattan", 2], ["Queens", 1]]);
  });
  it("filters by zone or borough, case-insensitively", () => {
    expect(filterZones(ZONES, "jfk").map((z) => z.location_id)).toEqual([132]);
    expect(filterZones(ZONES, "manhattan")).toHaveLength(2);
    expect(filterZones(ZONES, "  ")).toHaveLength(4);
  });
});

it("estimate line", () => {
  expect(estimateLine({ trip_distance: 3.1, duration_min: 16.5, fare_amount: 19.5, method: "median of 214 real trips between these zones" }))
    .toBe("~3.1 mi · ~17 min · fare ~$19.50 (median of 214 real trips between these zones)");
});

it("last trace survives in storage, and a broken storage never throws", () => {
  const data = new Map<string, string>();
  const storage = { getItem: (k: string) => data.get(k) ?? null, setItem: (k: string, v: string) => void data.set(k, v) };
  saveLastTrace("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f", storage);
  expect(loadLastTrace(storage)).toBe("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f");
  data.set("nyc-taxi-live:last-trace", "garbage");
  expect(loadLastTrace(storage)).toBeNull();
  const broken = { getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } };
  expect(() => saveLastTrace("x", broken)).not.toThrow();
  expect(loadLastTrace(broken)).toBeNull();
  expect(loadLastTrace()).toBeNull(); // no window in the test environment
});
```

`web/tests/unit/wifi.test.ts`:

```ts
import { expect, it } from "vitest";
import { wifiQrPayload } from "@/lib/wifi";

it("WPA payload with special characters escaped", () => {
  expect(wifiQrPayload("NYC-Taxi-Live", "p@ss")).toBe("WIFI:T:WPA;S:NYC-Taxi-Live;P:p@ss;;");
  expect(wifiQrPayload('My;Net,"x"', "a:b\\c")).toBe('WIFI:T:WPA;S:My\\;Net\\,\\"x\\";P:a\\:b\\\\c;;');
});

it("an open network has no password field", () => {
  expect(wifiQrPayload("Open", "")).toBe("WIFI:T:nopass;S:Open;;");
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd web && pnpm test`
Expected: FAIL with `Failed to resolve import "@/lib/trace"` (and ws, ride, wifi).

- [ ] **Step 4: Implement**

`web/lib/trace.ts`:

```ts
import type { TraceCheckpoint } from "@/lib/contracts/trace-checkpoint";

export type Checkpoint = TraceCheckpoint;
export type Stage = Checkpoint["stage"];

export const HAPPY_PATH: readonly Stage[] = ["submitted", "ingested", "validated", "enriched", "aggregated", "stored", "queryable"];
export const TERMINAL: ReadonlySet<Stage> = new Set<Stage>(["queryable", "rejected"]);
const STAGES: ReadonlySet<string> = new Set<string>([...HAPPY_PATH, "rejected"]);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const isTraceId = (s: string): boolean => UUID.test(s);

export const STAGE_TITLE: Record<Stage, string> = {
  submitted: "Your phone", ingested: "Redpanda", validated: "Data-quality check", rejected: "Rejected",
  enriched: "Enrichment", aggregated: "Live dashboard", stored: "ClickHouse", queryable: "AI agent",
};

const WAITING: Record<Stage, string> = {
  submitted: "waiting for the API…", ingested: "waiting for Redpanda…", validated: "waiting for the stream processor…",
  rejected: "waiting for the stream processor…", enriched: "waiting for the stream processor…",
  aggregated: "waiting for the stream processor…", stored: "waiting for ClickHouse…",
  queryable: "waiting for the AI agent to see it…",
};
/** The line under the next card while it hasn't arrived yet (spec 07 §Failure modes: "waiting for the stream processor…"). */
export const waitingText = (next: Stage): string => WAITING[next];

/** Plain English for the processor's reason codes (spec 03 §Validation rules). */
export const REASON_TEXT: Record<string, string> = {
  TIP_UNREALISTIC: "the tip is unrealistically high", TIP_NEGATIVE: "the tip is negative",
  FARE_NON_POSITIVE: "the fare is zero or negative", TOTAL_NEGATIVE: "the total is negative",
  DISTANCE_NON_POSITIVE: "the distance is zero", DISTANCE_TOO_LONG: "the distance is over 200 miles",
  DROPOFF_NOT_AFTER_PICKUP: "the drop-off isn't after the pickup", DURATION_TOO_LONG: "the ride took over 6 hours",
  LOCATION_INVALID: "a zone id doesn't exist", PASSENGER_COUNT_INVALID: "the passenger count is impossible",
  SCHEMA_INVALID: "the message didn't match the contract",
};

type Detail = Checkpoint["detail"];
const num = (d: Detail, k: string): number | null => { const v = d[k]; return typeof v === "number" ? v : null; };
const text = (d: Detail, k: string): string | null => { const v = d[k]; return typeof v === "string" ? v : null; };
const strings = (d: Detail, k: string): string[] => { const v = d[k]; return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : []; };

export function stageCopy(cp: Checkpoint): string {
  const d = cp.detail;
  switch (cp.stage) {
    case "submitted":
      return "Your phone sent the ride to our API.";
    case "ingested":
      return `Safely stored in Redpanda: topic ${text(d, "topic") ?? "trips.raw"}, partition ${num(d, "partition") ?? "?"}, offset ${num(d, "offset") ?? "?"}. It can't get lost now.`;
    case "validated":
      return `The stream processor checked ${num(d, "rules_checked") ?? 11} data-quality rules. All passed ✓`;
    case "rejected": {
      const reasons = strings(d, "reasons").map((r) => REASON_TEXT[r] ?? r);
      return `Rejected: ${reasons.length ? reasons.join(" and ") : "a data-quality rule failed"}. Our pipeline rejects about 1 in 10 real NYC trips for problems like this. Try a normal tip!`;
    }
    case "enriched": {
      const tip = num(d, "tip_pct");
      return tip === null
        ? "Added zone and borough names, and worked out your duration. Cash tips aren't recorded, so there's no tip %."
        : `Added zone and borough names, and worked out your tip (${tip}%) and duration.`;
    }
    case "aggregated":
      return `Counted in the ${text(d, "borough") ?? "?"} · ${(text(d, "window_start") ?? "").slice(11, 16)} bucket on the live dashboard.`;
    case "stored":
      return `Written to ClickHouse (${text(d, "table") ?? "taxi.trips"}) in a batch of ${num(d, "batch_rows") ?? "?"} trips.`;
    case "queryable":
      return "The AI agent can now see your ride. Ask it something!";
  }
}

/** Dedup by stage (the first one wins) and order by seq, never by arrival (spec 07 §Ordering). */
export function mergeCheckpoints(have: readonly Checkpoint[], incoming: readonly Checkpoint[]): Checkpoint[] {
  const byStage = new Map<Stage, Checkpoint>();
  for (const cp of [...have, ...incoming]) if (!byStage.has(cp.stage)) byStage.set(cp.stage, cp);
  return [...byStage.values()].sort((a, b) => a.seq - b.seq || a.at.localeCompare(b.at));
}

/** Milliseconds since the previous checkpoint. Wall time `at` only (CLAUDE.md invariant 3). */
export function hopMs(cps: readonly Checkpoint[]): Partial<Record<Stage, number>> {
  const sorted = mergeCheckpoints(cps, []);
  const out: Partial<Record<Stage, number>> = {};
  for (let i = 1; i < sorted.length; i++) {
    out[sorted[i]!.stage] = Math.max(0, Date.parse(sorted[i]!.at) - Date.parse(sorted[i - 1]!.at));
  }
  return out;
}

export function totalMs(cps: readonly Checkpoint[]): number | null {
  const s = cps.find((c) => c.stage === "submitted");
  const q = cps.find((c) => c.stage === "queryable");
  return s && q ? Date.parse(q.at) - Date.parse(s.at) : null;
}

export type TraceMessage =
  | { type: "history"; checkpoints: Checkpoint[]; sim_ts: string | null }
  | { type: "checkpoint"; checkpoint: Checkpoint };

export function isCheckpoint(x: unknown): x is Checkpoint {
  if (typeof x !== "object" || x === null) return false;
  const c = x as Record<string, unknown>;
  return typeof c.trace_id === "string" && typeof c.stage === "string" && STAGES.has(c.stage)
    && typeof c.seq === "number" && typeof c.at === "string" && typeof c.detail === "object" && c.detail !== null;
}

export function toTraceMessage(raw: unknown): TraceMessage | null {
  if (typeof raw !== "object" || raw === null) return null;
  const m = raw as Record<string, unknown>;
  if (m.type === "history" && Array.isArray(m.checkpoints)) {
    return { type: "history", checkpoints: m.checkpoints.filter(isCheckpoint), sim_ts: typeof m.sim_ts === "string" ? m.sim_ts : null };
  }
  if (m.type === "checkpoint") {
    const rest: Record<string, unknown> = { ...m };
    delete rest.type;
    delete rest.ambient;
    return isCheckpoint(rest) ? { type: "checkpoint", checkpoint: rest } : null;
  }
  return null;
}

export type TraceView = { checkpoints: Checkpoint[]; simTs: string | null };
export const EMPTY_VIEW: TraceView = { checkpoints: [], simTs: null };

export function applyMessage(view: TraceView, msg: TraceMessage): TraceView {
  if (msg.type === "history") return { checkpoints: mergeCheckpoints(view.checkpoints, msg.checkpoints), simTs: msg.sim_ts ?? view.simTs };
  return { ...view, checkpoints: mergeCheckpoints(view.checkpoints, [msg.checkpoint]) };
}

export type RideSummary = { pickupZone: string; dropoffZone: string; payment: "card" | "cash"; tip: number; fare: number | null };

export function rideSummary(cps: readonly Checkpoint[]): RideSummary | null {
  const s = cps.find((c) => c.stage === "submitted");
  if (!s) return null;
  const pu = text(s.detail, "pickup_zone");
  const dz = text(s.detail, "dropoff_zone");
  if (!pu || !dz) return null;
  return { pickupZone: pu, dropoffZone: dz, payment: text(s.detail, "payment") === "cash" ? "cash" : "card",
    tip: num(s.detail, "tip_amount") ?? 0, fare: num(s.detail, "fare_amount") };
}

export const money = (x: number): string => (Number.isInteger(x) ? `$${x}` : `$${x.toFixed(2)}`);

/** "Midtown Center → JFK Airport, $5 tip" (spec 08 §/agent). */
export const rideChip = (s: RideSummary): string =>
  `${s.pickupZone} → ${s.dropoffZone}, ${s.payment === "cash" ? "cash" : `${money(s.tip)} tip`}`;
```

`web/lib/ws.ts`:

```ts
/** WebSocket with reconnect + backoff (spec 08 §/trace/[id]: 0.5, 1, 2, 5 s). Messages are parsed JSON. */
export const BACKOFF_MS = [500, 1000, 2000, 5000] as const;
export const backoffDelay = (attempt: number): number => BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)]!;

/** Same origin on the stand (Caddy serves everything). In dev, NEXT_PUBLIC_API_WS_ORIGIN points at the api. */
export function wsUrl(path: string, loc: { protocol: string; host: string }, origin?: string): string {
  if (origin) return `${origin.replace(/\/$/, "")}${path}`;
  return `${loc.protocol === "https:" ? "wss:" : "ws:"}//${loc.host}${path}`;
}

export type SocketHandlers = { open: () => void; close: (code: number) => void; message: (data: string) => void };
export type SocketLike = { close: () => void };
export type SocketFactory = (url: string, handlers: SocketHandlers) => SocketLike;
export type SocketStatus = "connecting" | "open" | "retrying";

export const browserSocket: SocketFactory = (url, h) => {
  const ws = new WebSocket(url);
  ws.onopen = () => h.open();
  ws.onclose = (e) => h.close(e.code);
  ws.onmessage = (e) => h.message(String(e.data));
  return ws;
};

export class ReconnectingSocket {
  private attempt = 0;
  private socket: SocketLike | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;

  constructor(
    private readonly url: string,
    private readonly onMessage: (data: unknown) => void,
    private readonly onStatus: (s: SocketStatus) => void = () => {},
    private readonly factory: SocketFactory = browserSocket,
  ) {
    this.connect();
  }

  private connect(): void {
    if (this.stopped) return;
    this.onStatus("connecting");
    this.socket = this.factory(this.url, {
      open: () => {
        this.attempt = 0;
        this.onStatus("open");
      },
      message: (data) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(data);
        } catch {
          return; // a malformed frame is skipped, never fatal
        }
        this.onMessage(parsed);
      },
      close: () => {
        if (this.stopped) return;
        this.onStatus("retrying");
        this.timer = setTimeout(() => this.connect(), backoffDelay(this.attempt++));
      },
    });
  }

  stop(): void {
    this.stopped = true;
    if (this.timer !== null) clearTimeout(this.timer);
    this.socket?.close();
  }
}
```

`web/lib/ride.ts`:

```ts
import type { RideEstimate, Zone } from "@/lib/api";
import { isTraceId } from "@/lib/trace";

/** spec 08 §/ride "Popular": JFK, LaGuardia, Times Sq/Theatre District, Midtown Center, East Village, Williamsburg (N). */
export const POPULAR_ZONE_IDS = [132, 138, 230, 161, 79, 255] as const;

export type TipChoice = { kind: "none" } | { kind: "pct"; pct: 15 | 20 | 25 } | { kind: "custom"; amount: number };

export function tipAmount(choice: TipChoice, fare: number | null): number {
  switch (choice.kind) {
    case "none":
      return 0;
    case "pct":
      return fare === null ? 0 : Math.round(fare * choice.pct) / 100; // cents first: 19.50 × 15 % = $2.93
    case "custom": {
      const cents = Number.isFinite(choice.amount) ? Math.round(choice.amount * 100) / 100 : 0;
      return Math.min(999, Math.max(0, cents)); // no warning for absurd tips: the processor rejects them (spec 08)
    }
  }
}

export function groupByBorough(zones: readonly Zone[]): [string, Zone[]][] {
  const groups = new Map<string, Zone[]>();
  for (const z of zones) groups.set(z.borough, [...(groups.get(z.borough) ?? []), z]);
  return [...groups.entries()];
}

export function filterZones(zones: readonly Zone[], query: string): Zone[] {
  const q = query.trim().toLowerCase();
  return q ? zones.filter((z) => z.zone.toLowerCase().includes(q) || z.borough.toLowerCase().includes(q)) : [...zones];
}

export const estimateLine = (e: RideEstimate): string =>
  `~${e.trip_distance.toFixed(1)} mi · ~${Math.round(e.duration_min)} min · fare ~$${e.fare_amount.toFixed(2)} (${e.method})`;

export const LAST_TRACE_KEY = "nyc-taxi-live:last-trace";
type KeyValue = Pick<Storage, "getItem" | "setItem">;

function browserStorage(): KeyValue | null {
  try {
    return window.localStorage;
  } catch {
    return null; // no window (tests, server) or blocked storage (private mode)
  }
}

export function saveLastTrace(id: string, storage: KeyValue | null = browserStorage()): void {
  try {
    storage?.setItem(LAST_TRACE_KEY, id);
  } catch {
    /* storage full or blocked: "continue watching" is a convenience */
  }
}

export function loadLastTrace(storage: KeyValue | null = browserStorage()): string | null {
  try {
    const v = storage?.getItem(LAST_TRACE_KEY) ?? null;
    return v && isTraceId(v) ? v : null;
  } catch {
    return null;
  }
}
```

`web/lib/wifi.ts`:

```ts
/** The WIFI: QR format phones understand. \ ; , : " must be backslash-escaped in SSID and password. */
const esc = (s: string): string => s.replace(/([\\;,:"])/g, "\\$1");

export function wifiQrPayload(ssid: string, password: string): string {
  return password ? `WIFI:T:WPA;S:${esc(ssid)};P:${esc(password)};;` : `WIFI:T:nopass;S:${esc(ssid)};;`;
}
```

`web/lib/use-poll.ts`: move `usePoll` out of `components/agent/Header.tsx` unchanged, add `"use client";` at the top, and export it.

`web/lib/format.ts`: move `formatSim` out of `Header.tsx` unchanged (keep its doc comment).

`Header.tsx` then imports both. If any test imports `formatSim` from the Header, point it at `@/lib/format`.

`web/lib/api.ts`: append:

```ts
import type { Checkpoint } from "@/lib/trace";

export type Zone = { location_id: number; zone: string; borough: string };
export type RideEstimate = { trip_distance: number; duration_min: number; fare_amount: number; method: string };
export type RideRequest = { pickup_location_id: number; dropoff_location_id: number; payment: "card" | "cash"; tip_amount: number; passenger_count: number };
export type RideCreated = { trace_id: string; trip_id: string; trace_url: string; pickup_ts: string; estimate: RideEstimate & { total_amount: number }; notes: string[] };
export type BookResult = { ok: true; ride: RideCreated } | { ok: false; status: number; message: string; retryAfterS?: number };
export type TraceHistory = { trace_id: string; checkpoints: Checkpoint[] };

export const getZones = () => getJson<Zone[]>("/api/zones");
export const getEstimate = (pickup: number, dropoff: number) =>
  getJson<RideEstimate>(`/api/rides/estimate?pickup_location_id=${pickup}&dropoff_location_id=${dropoff}`);
export const getTrace = (id: string) => getJson<TraceHistory>(`/api/trace/${encodeURIComponent(id)}`);

export async function bookRide(body: RideRequest): Promise<BookResult> {
  let r: Response;
  try {
    r = await fetch("/api/rides", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  } catch {
    return { ok: false, status: 0, message: "Can't reach the stand. Are you on the stand Wi-Fi?" };
  }
  if (r.status === 201) return { ok: true, ride: (await r.json()) as RideCreated };
  const data = (await r.json().catch(() => ({}))) as { detail?: unknown; retry_after_s?: number };
  const message = typeof data.detail === "string" ? data.detail : "Something went wrong. Try again in a moment.";
  return { ok: false, status: r.status, message, retryAfterS: data.retry_after_s };
}
```

(Put the `import type` at the top of the file with the other imports.)

`web/app/globals.css`: append:

```css
/* Phone pages (/ride, /trace/[id]) wrap their content in .phone: 16 px base instead of the kiosk's 20 px (spec 08). */
html:has(.phone) { font-size: 16px; }

/* The packet dot travelling to the next stage card, and the "waiting for…" spinner. */
@keyframes packet {
  from { transform: translateY(-0.75rem); opacity: 0; }
  50% { opacity: 1; }
  to { transform: translateY(0.75rem); opacity: 0; }
}
.packet { animation: packet 900ms ease-in-out infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.spinner {
  display: inline-block; width: 1rem; height: 1rem; border-radius: 9999px;
  border: 2px solid currentColor; border-right-color: transparent; animation: spin 800ms linear infinite;
}
@media (prefers-reduced-motion: reduce) {
  .packet { animation: none; opacity: 1; transform: none; }
  .spinner { animation: none; }
}
```

`.env.example`, Web block:

```
# Dev only: where the browser opens WebSockets when the api isn't behind the same origin (pnpm dev → ws://localhost:8000).
# Leave empty on the stand: Caddy serves web and api on one origin.
NEXT_PUBLIC_API_WS_ORIGIN=
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd web && pnpm test && pnpm typecheck && pnpm lint`
Expected: all pass.

Then break a contract on purpose to check spec 08 acceptance 6. Rename `"stage"` to `"stage_name"` in `contracts/events/trace_checkpoint.schema.json` (both in `required` and `properties`) and run `pnpm typecheck`. It must fail in `lib/trace.ts`. Revert with `git checkout contracts/events/trace_checkpoint.schema.json && pnpm gen:types`.

- [ ] **Step 6: Commit**

```bash
git add web/package.json web/pnpm-lock.yaml web/scripts web/lib web/components/agent/Header.tsx web/app/globals.css \
  web/tests/unit web/eslint.config.mjs .env.example
git commit -m "feat(web): contract types, trace/ride/ws/wifi libs, ride + trace api client, phone styles"
```

---

### Task 8: `/ride` booking page (mobile-first)

**Files:**
- Create: `web/app/ride/page.tsx`
- Create: `web/components/ride/RideForm.tsx`, `ZonePicker.tsx`, `TipChips.tsx`, `PassengerStepper.tsx`, `EstimateCard.tsx`
- Modify: `web/playwright.config.ts` (projects)
- Create: `web/tests/e2e/ride-mocks.ts`, `web/tests/e2e/ride.spec.ts`

**Interfaces:**
- Consumes (Task 7): `getZones`, `getEstimate`, `bookRide`, `Zone`, `RideEstimate`, plus `POPULAR_ZONE_IDS`, `TipChoice`, `tipAmount`, `groupByBorough`, `filterZones`, `estimateLine`, `saveLastTrace`, `loadLastTrace`, and `BOROUGH_COLORS`.
- Produces:
  - The page `/ride`.
  - `tests/e2e/ride-mocks.ts` exports `TID`, `ZONES`, `ESTIMATE`, `CREATED`, `mockRideApi(page)`, and `mockTraceSocket(page, onConnect)`. Task 9 extends it with the checkpoint fixtures.
- Accessible names the tests rely on (keep them):
  - buttons "Pickup zone" and "Drop-off zone" (the picker triggers);
  - a dialog per picker with a search box "Search zones", a region "Popular", and zone buttons named by the zone;
  - buttons "Card", "Cash", "$0", "15%", "20%", "25%", "Custom";
  - a spinbutton "Custom tip in dollars";
  - buttons "Fewer passengers" and "More passengers";
  - the button "Book ride";
  - test ids `estimate`, `tip-amount`, `ride-error`, `passengers`.

- [ ] **Step 1: Playwright projects**

`web/playwright.config.ts`:

```ts
import { defineConfig, devices } from "@playwright/test";

const PHONES = /(ride|trace)\.spec\.ts/;

export default defineConfig({
  testDir: "tests/e2e",
  use: { baseURL: "http://127.0.0.1:3100" },
  projects: [
    { name: "desktop", testMatch: /(agent|live|qr)\.spec\.ts/, use: { ...devices["Desktop Chrome"], viewport: { width: 1920, height: 1080 } } },
    // spec 08 acceptance 1: iPhone 13 and Pixel 7 viewports. Chromium for both, so no WebKit download is needed offline.
    { name: "iphone-13", testMatch: PHONES, use: { ...devices["iPhone 13"], browserName: "chromium" } },
    { name: "pixel-7", testMatch: PHONES, use: { ...devices["Pixel 7"] } },
  ],
  webServer: {
    command: "pnpm build && pnpm start -p 3100",
    url: "http://127.0.0.1:3100/agent",
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    env: { STAND_WIFI_SSID: "Test;Net", STAND_WIFI_PASSWORD: "p@ss", STAND_PUBLIC_URL: "http://192.168.8.10" },
  },
});
```

Run `pnpm test:e2e --project=desktop`. Expected: the existing agent tests still pass.

- [ ] **Step 2: Write the mocks and the failing tests**

`web/tests/e2e/ride-mocks.ts`:

```ts
import type { Page, WebSocketRoute } from "@playwright/test";

export const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
// api order: sorted by borough, then zone
export const ZONES = [
  { location_id: 255, zone: "Williamsburg (North Side)", borough: "Brooklyn" },
  { location_id: 79, zone: "East Village", borough: "Manhattan" },
  { location_id: 161, zone: "Midtown Center", borough: "Manhattan" },
  { location_id: 230, zone: "Times Sq/Theatre District", borough: "Manhattan" },
  { location_id: 7, zone: "Astoria", borough: "Queens" },
  { location_id: 132, zone: "JFK Airport", borough: "Queens" },
  { location_id: 138, zone: "LaGuardia Airport", borough: "Queens" },
];
export const ESTIMATE = { trip_distance: 3.1, duration_min: 16.5, fare_amount: 19.5, method: "median of 214 real trips between these zones" };
export const CREATED = {
  trace_id: TID, trip_id: `s-${TID}`, trace_url: `/trace/${TID}`, pickup_ts: "2025-10-09T17:42:00",
  estimate: { ...ESTIMATE, total_amount: 29.25 }, notes: [],
};

/** Mocks every /api call /ride makes. Returns the POST bodies it received. */
export async function mockRideApi(page: Page, post: { status: number; json: unknown } = { status: 201, json: CREATED }) {
  const posted: unknown[] = [];
  await page.route("**/api/zones", (r) => r.fulfill({ json: ZONES }));
  await page.route("**/api/rides/estimate**", (r) => r.fulfill({ json: ESTIMATE }));
  await page.route("**/api/rides", (r) => {
    posted.push(r.request().postDataJSON());
    return r.fulfill({ status: post.status, json: post.json });
  });
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
  return posted;
}

export async function mockTraceSocket(page: Page, onConnect: (ws: WebSocketRoute, n: number) => void) {
  let n = 0;
  await page.routeWebSocket(/\/api\/trace\//, (ws) => onConnect(ws, n++));
}

export const send = (ws: WebSocketRoute, msg: unknown) => ws.send(JSON.stringify(msg));
```

`web/tests/e2e/ride.spec.ts`:

```ts
import { gzipSync } from "node:zlib";
import { expect, test, type Page } from "@playwright/test";
import { CREATED, mockRideApi, mockTraceSocket, send, TID } from "./ride-mocks";

async function pick(page: Page, trigger: string, search: string | null, zone: string) {
  await page.getByRole("button", { name: trigger }).click();
  const dialog = page.getByRole("dialog");
  if (search) await dialog.getByRole("searchbox", { name: "Search zones" }).fill(search);
  const scope = search ? dialog : dialog.getByRole("region", { name: "Popular" });
  await scope.getByRole("button", { name: zone, exact: true }).first().click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
}

test("book a card ride: popular pickup, searched drop-off, 20 % tip → /trace/[id]", async ({ page }) => {
  const posted = await mockRideApi(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Times Sq/Theatre District");
  await pick(page, "Drop-off zone", "jfk", "JFK Airport");
  await expect(page.getByTestId("estimate")).toHaveText("~3.1 mi · ~17 min · fare ~$19.50 (median of 214 real trips between these zones)");
  await page.getByRole("button", { name: "20%" }).click();
  await expect(page.getByTestId("tip-amount")).toHaveText("$3.90");
  await page.getByRole("button", { name: "More passengers" }).click();
  await expect(page.getByTestId("passengers")).toHaveText("2");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page).toHaveURL(new RegExp(`/trace/${TID}$`));
  expect(posted).toEqual([{ pickup_location_id: 230, dropoff_location_id: 132, payment: "card", tip_amount: 3.9, passenger_count: 2 }]);
});

test("cash shows the info line; a custom absurd tip is sent without a warning", async ({ page }) => {
  const posted = await mockRideApi(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Midtown Center");
  await pick(page, "Drop-off zone", "astoria", "Astoria");
  await page.getByRole("button", { name: "Cash" }).click();
  await expect(page.getByText("Cash tips aren't recorded in NYC taxi data")).toBeVisible();
  await page.getByRole("button", { name: "Card" }).click();
  await page.getByRole("button", { name: "Custom" }).click();
  await page.getByRole("spinbutton", { name: "Custom tip in dollars" }).fill("999");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page).toHaveURL(/\/trace\//);
  expect(posted[0]).toMatchObject({ payment: "card", tip_amount: 999 });
});

test("a rate-limited booking shows the api's message and stays on /ride", async ({ page }) => {
  await mockRideApi(page, { status: 429, json: { detail: "You've booked 3 rides this minute. Try again in 20 s.", retry_after_s: 20 } });
  await page.goto("/ride");
  await pick(page, "Pickup zone", null, "Midtown Center");
  await pick(page, "Drop-off zone", null, "JFK Airport");
  await page.getByRole("button", { name: "Book ride" }).click();
  await expect(page.getByTestId("ride-error")).toHaveText("You've booked 3 rides this minute. Try again in 20 s.");
  await expect(page).toHaveURL(/\/ride$/);
});

test("Book ride waits for both zones", async ({ page }) => {
  await mockRideApi(page);
  await page.goto("/ride");
  await expect(page.getByRole("button", { name: "Book ride" })).toBeDisabled();
});

test("reopening /ride offers to continue watching the last ride", async ({ page }) => {
  await mockRideApi(page);
  await page.addInitScript((id) => localStorage.setItem("nyc-taxi-live:last-trace", id), TID);
  await page.goto("/ride");
  await expect(page.getByRole("link", { name: "Continue watching your ride" })).toHaveAttribute("href", CREATED.trace_url);
});

test("no horizontal scroll at 360 px, closed and with the picker open", async ({ page }) => {
  await mockRideApi(page);
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto("/ride");
  const overflow = () => page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  expect(await overflow()).toBeLessThanOrEqual(0);
  await page.getByRole("button", { name: "Pickup zone" }).click();
  expect(await overflow()).toBeLessThanOrEqual(0);
});

test("first-load JS is at most 150 kB gzipped (spec 08 acceptance 4)", async ({ page }) => {
  await mockRideApi(page);
  const bodies: Promise<Buffer>[] = [];
  page.on("response", (r) => {
    if (r.request().resourceType() === "script") bodies.push(r.body());
  });
  await page.goto("/ride", { waitUntil: "networkidle" });
  const total = (await Promise.all(bodies)).reduce((n, b) => n + gzipSync(b).length, 0);
  console.log(`/ride first-load JS: ${(total / 1024).toFixed(1)} kB gzipped`);
  expect(total).toBeLessThanOrEqual(150 * 1024);
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd web && pnpm test:e2e --project=pixel-7 ride.spec.ts`
Expected: FAIL. `/ride` returns 404, so the "Pickup zone" button is never found.

- [ ] **Step 4: Implement the page and its components**

`web/app/ride/page.tsx`:

```tsx
import type { Metadata } from "next";
import { RideForm } from "@/components/ride/RideForm";

export const metadata: Metadata = { title: "Book a ride · NYC Taxi Live" };

export default function RidePage() {
  return (
    <main className="phone mx-auto flex min-h-dvh w-full max-w-md flex-col gap-5 px-4 py-6">
      <h1 className="text-2xl font-semibold">Book a pretend taxi ride</h1>
      <p className="text-muted">Pick two NYC zones, send the ride, and watch it travel through our data pipeline.</p>
      <RideForm />
    </main>
  );
}
```

`web/components/ride/ZonePicker.tsx`:

```tsx
"use client";
import { useMemo, useState } from "react";
import type { Zone } from "@/lib/api";
import { BOROUGH_COLORS } from "@/lib/palette";
import { filterZones, groupByBorough, POPULAR_ZONE_IDS } from "@/lib/ride";

/** A big button that opens a full-screen, searchable zone list grouped by borough. The search text stays on the phone. */
export function ZonePicker({ label, zones, value, onChange }: { label: string; zones: Zone[] | null; value: Zone | null; onChange: (z: Zone) => void }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const popular = useMemo(() => POPULAR_ZONE_IDS.flatMap((id) => zones?.find((z) => z.location_id === id) ?? []), [zones]);
  const groups = useMemo(() => groupByBorough(filterZones(zones ?? [], query)), [zones, query]);
  const pick = (z: Zone) => {
    onChange(z);
    setOpen(false);
    setQuery("");
  };
  const option = "min-h-11 rounded-lg px-3 text-left text-base hover:bg-panel focus-visible:outline-2 focus-visible:outline-visitor";
  return (
    <>
      <button type="button" aria-label={label} aria-haspopup="dialog" disabled={!zones} onClick={() => setOpen(true)}
        className="flex min-h-16 w-full flex-col justify-center rounded-xl border-2 border-line bg-panel px-4 py-2 text-left disabled:opacity-50">
        <span className="text-sm text-muted">{label}</span>
        <span className="text-lg font-medium">{value ? value.zone : zones ? "Choose a zone" : "Loading zones…"}</span>
        {value && <span className="text-sm text-muted">{value.borough}</span>}
      </button>
      {open && (
        <div role="dialog" aria-modal="true" aria-label={label} className="fixed inset-0 z-50 flex flex-col bg-surface">
          <div className="flex items-center gap-2 border-b border-line p-3">
            <input type="search" aria-label="Search zones" placeholder="Search zones…" value={query} autoFocus
              onChange={(e) => setQuery(e.target.value)} autoComplete="off"
              className="min-h-12 min-w-0 flex-1 rounded-lg border border-line bg-panel px-3 text-base text-ink outline-none focus-visible:border-visitor" />
            <button type="button" onClick={() => setOpen(false)} className="min-h-12 px-3 text-muted">Cancel</button>
          </div>
          <div className="flex-1 overflow-y-auto px-3 pb-8">
            {!query.trim() && popular.length > 0 && (
              <section aria-label="Popular" className="py-3">
                <h2 className="pb-2 text-sm font-semibold text-muted">Popular</h2>
                <div className="flex flex-wrap gap-2">
                  {popular.map((z) => (
                    <button key={z.location_id} type="button" onClick={() => pick(z)}
                      className="min-h-11 rounded-full border border-visitor px-4 text-sm text-visitor-ink">{z.zone}</button>
                  ))}
                </div>
              </section>
            )}
            {groups.map(([borough, list]) => (
              <section key={borough} aria-label={borough}>
                <h2 className="sticky top-0 flex items-center gap-2 bg-surface py-2 text-sm font-semibold">
                  <span aria-hidden className="size-3 rounded-full" style={{ background: BOROUGH_COLORS[borough] ?? "#9a9890" }} />
                  {borough}
                </h2>
                <ul className="flex flex-col">
                  {list.map((z) => (
                    <li key={z.location_id}><button type="button" onClick={() => pick(z)} className={`${option} w-full`}>{z.zone}</button></li>
                  ))}
                </ul>
              </section>
            ))}
            {groups.length === 0 && <p className="py-8 text-center text-muted">No zone matches that.</p>}
          </div>
        </div>
      )}
    </>
  );
}
```

(The empty-result message deliberately doesn't echo the query.)

`web/components/ride/TipChips.tsx`:

```tsx
"use client";
import { useState } from "react";
import { money } from "@/lib/trace";
import { tipAmount, type TipChoice } from "@/lib/ride";

const PRESETS: { label: string; choice: TipChoice }[] = [
  { label: "$0", choice: { kind: "none" } },
  { label: "15%", choice: { kind: "pct", pct: 15 } },
  { label: "20%", choice: { kind: "pct", pct: 20 } },
  { label: "25%", choice: { kind: "pct", pct: 25 } },
];
const same = (a: TipChoice, b: TipChoice) => a.kind === b.kind && (a.kind !== "pct" || (b.kind === "pct" && a.pct === b.pct));

export function TipChips({ value, onChange, fare }: { value: TipChoice; onChange: (c: TipChoice) => void; fare: number | null }) {
  const [custom, setCustom] = useState("");
  const chip = (on: boolean) => `min-h-11 flex-1 rounded-full border-2 px-3 font-medium ${on ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-line"} disabled:opacity-40`;
  return (
    <fieldset className="flex flex-col gap-2">
      <legend className="pb-2 font-medium">Tip <span data-testid="tip-amount" className="float-right tabular-nums text-muted">{money(tipAmount(value, fare))}</span></legend>
      <div className="flex gap-2">
        {PRESETS.map((p) => (
          <button key={p.label} type="button" aria-pressed={same(value, p.choice)} disabled={p.choice.kind === "pct" && fare === null}
            onClick={() => onChange(p.choice)} className={chip(same(value, p.choice))}>{p.label}</button>
        ))}
        <button type="button" aria-pressed={value.kind === "custom"} onClick={() => onChange({ kind: "custom", amount: Number(custom) || 0 })}
          className={chip(value.kind === "custom")}>Custom</button>
      </div>
      {value.kind === "custom" && (
        <label className="flex items-center gap-2">
          <span aria-hidden>$</span>
          <input type="number" inputMode="decimal" min={0} max={999} step={0.01} aria-label="Custom tip in dollars" value={custom} autoFocus
            onChange={(e) => { setCustom(e.target.value); onChange({ kind: "custom", amount: Number(e.target.value) }); }}
            className="min-h-11 w-32 rounded-lg border border-line bg-panel px-3 text-base text-ink" />
        </label>
      )}
    </fieldset>
  );
}
```

`web/components/ride/PassengerStepper.tsx`:

```tsx
"use client";

export function PassengerStepper({ value, onChange }: { value: number; onChange: (n: number) => void }) {
  const btn = "size-11 rounded-full border-2 border-line text-xl disabled:opacity-40";
  return (
    <div className="flex items-center justify-between">
      <span className="font-medium">Passengers</span>
      <div className="flex items-center gap-4">
        <button type="button" aria-label="Fewer passengers" disabled={value <= 1} onClick={() => onChange(value - 1)} className={btn}>−</button>
        <span data-testid="passengers" className="w-6 text-center text-xl tabular-nums">{value}</span>
        <button type="button" aria-label="More passengers" disabled={value >= 6} onClick={() => onChange(value + 1)} className={btn}>+</button>
      </div>
    </div>
  );
}
```

`web/components/ride/EstimateCard.tsx`:

```tsx
import type { RideEstimate } from "@/lib/api";
import { estimateLine } from "@/lib/ride";

export function EstimateCard({ ready, estimate }: { ready: boolean; estimate: RideEstimate | null }) {
  return (
    <div className="rounded-xl border border-line bg-panel px-4 py-3 text-base">
      {!ready ? <span className="text-muted">Pick both zones to see an estimate.</span>
        : estimate ? <span data-testid="estimate">{estimateLine(estimate)}</span>
        : <span className="text-muted">Estimating…</span>}
    </div>
  );
}
```

`web/components/ride/RideForm.tsx`:

```tsx
"use client";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState, useSyncExternalStore } from "react";
import { bookRide, getEstimate, getZones, type RideEstimate, type Zone } from "@/lib/api";
import { loadLastTrace, saveLastTrace, tipAmount, type TipChoice } from "@/lib/ride";
import { EstimateCard } from "./EstimateCard";
import { PassengerStepper } from "./PassengerStepper";
import { TipChips } from "./TipChips";
import { ZonePicker } from "./ZonePicker";

const noSubscribe = () => () => {};
const pairKey = (a: Zone | null, b: Zone | null) => (a && b ? `${a.location_id}-${b.location_id}` : null);

export function RideForm() {
  const router = useRouter();
  const [zones, setZones] = useState<Zone[] | null>(null);
  const [pickup, setPickup] = useState<Zone | null>(null);
  const [dropoff, setDropoff] = useState<Zone | null>(null);
  // The estimate remembers which zone pair it is for, so changing a zone never shows the old pair's estimate.
  const [estimated, setEstimated] = useState<{ key: string; estimate: RideEstimate } | null>(null);
  const [payment, setPayment] = useState<"card" | "cash">("card");
  const [tip, setTip] = useState<TipChoice>({ kind: "pct", pct: 15 });
  const [passengers, setPassengers] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // localStorage exists only in the browser: null on the server, the stored id after hydration.
  const lastTrace = useSyncExternalStore(noSubscribe, loadLastTrace, () => null);

  useEffect(() => {
    let alive = true;
    let retry: ReturnType<typeof setTimeout>;
    const load = () => getZones().then((z) => { if (alive) setZones(z); }).catch(() => { if (alive) retry = setTimeout(load, 3000); });
    load();
    return () => { alive = false; clearTimeout(retry); };
  }, []);

  useEffect(() => {
    const key = pairKey(pickup, dropoff);
    if (!pickup || !dropoff || !key) return;
    let alive = true;
    getEstimate(pickup.location_id, dropoff.location_id)
      .then((e) => { if (alive) setEstimated({ key, estimate: e }); })
      .catch(() => undefined); // the api estimates again on booking, so this is only a preview
    return () => { alive = false; };
  }, [pickup, dropoff]);

  const estimate = estimated && estimated.key === pairKey(pickup, dropoff) ? estimated.estimate : null;
  const fare = estimate?.fare_amount ?? null;
  const book = async () => {
    if (!pickup || !dropoff || busy) return;
    setBusy(true);
    setError(null);
    const result = await bookRide({ pickup_location_id: pickup.location_id, dropoff_location_id: dropoff.location_id,
      payment, tip_amount: tipAmount(tip, fare), passenger_count: passengers });
    if (result.ok) {
      saveLastTrace(result.ride.trace_id);
      router.push(result.ride.trace_url);
      return;
    }
    setBusy(false);
    setError(result.message);
  };

  const toggle = (on: boolean) => `min-h-12 flex-1 rounded-xl border-2 font-medium ${on ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-line"}`;
  return (
    <form className="flex flex-col gap-5" onSubmit={(e) => { e.preventDefault(); void book(); }}>
      {lastTrace && (
        <Link href={`/trace/${lastTrace}`} className="rounded-xl border border-visitor px-4 py-3 text-center text-visitor-ink">
          Continue watching your ride
        </Link>
      )}
      <ZonePicker label="Pickup zone" zones={zones} value={pickup} onChange={setPickup} />
      <ZonePicker label="Drop-off zone" zones={zones} value={dropoff} onChange={setDropoff} />
      <EstimateCard ready={!!pickup && !!dropoff} estimate={estimate} />
      <fieldset className="flex flex-col gap-2">
        <legend className="pb-2 font-medium">Payment</legend>
        <div className="flex gap-2">
          <button type="button" aria-pressed={payment === "card"} onClick={() => setPayment("card")} className={toggle(payment === "card")}>Card</button>
          <button type="button" aria-pressed={payment === "cash"} onClick={() => setPayment("cash")} className={toggle(payment === "cash")}>Cash</button>
        </div>
        {payment === "cash" && <p className="text-sm text-muted">Cash tips aren&apos;t recorded in NYC taxi data</p>}
      </fieldset>
      <TipChips value={tip} onChange={setTip} fare={fare} />
      <PassengerStepper value={passengers} onChange={setPassengers} />
      {error && <p data-testid="ride-error" role="alert" className="rounded-xl bg-[#4a3300] px-4 py-3 text-[#ffe7b0]">{error}</p>}
      <button type="submit" disabled={!pickup || !dropoff || busy}
        className="min-h-14 rounded-xl bg-visitor text-xl font-semibold text-black disabled:opacity-40">
        {busy ? "Booking…" : "Book ride"}
      </button>
    </form>
  );
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd web && pnpm lint && pnpm typecheck && pnpm test:e2e --project=iphone-13 --project=pixel-7 ride.spec.ts`
Expected: 14 passed (7 tests × 2 phones). The size test prints the kB figure. Note it for Task 12.

If a zone button is ambiguous (for example "JFK Airport" in both Popular and Queens), keep `.first()` in the helper. Don't rename the accessible names.

- [ ] **Step 6: Visual check**

Load the `frontend-design` skill for a quick visual pass of `/ride` at 390 × 844 (iPhone 13). Check it at the stand's real brightness if possible. It's the first page every visitor sees on their phone.

Keep every accessible name and test id the tests use, and all tap targets ≥ 44 px. Re-run Step 5 after any change.

- [ ] **Step 7: Commit**

```bash
git add web/app/ride web/components/ride web/playwright.config.ts web/tests/e2e/ride-mocks.ts web/tests/e2e/ride.spec.ts
git commit -m "feat(web): /ride booking page (zones, estimate, payment, tip, passengers) with iPhone 13 + Pixel 7 e2e"
```

---

### Task 9: `/trace/[id]` viewer (phone)

**Files:**
- Create: `web/app/trace/[id]/page.tsx`
- Create: `web/components/trace/TraceViewer.tsx`, `web/components/trace/StageCard.tsx`
- Modify: `web/tests/e2e/ride-mocks.ts` (+ checkpoint fixtures)
- Create: `web/tests/e2e/trace.spec.ts`

**Interfaces:**
- Consumes (Task 7):
  - `lib/trace`: `applyMessage`, `EMPTY_VIEW`, `HAPPY_PATH`, `TERMINAL`, `hopMs`, `totalMs`, `rideSummary`, `money`, `waitingText`, `stageCopy`, `STAGE_TITLE`, `isTraceId`, `toTraceMessage`;
  - `lib/ws`: `ReconnectingSocket`, `wsUrl`;
  - `usePoll`, `formatSim`, `getSimClock`.
- Produces:
  - The page `/trace/[id]`.
  - Test ids: `stages` (an `<ol>`), `stage-<name>` on each `<li>` with `data-state` ∈ `done|rejected|next|future`, and `total`.
  - Links "Where does my ride rank for tips today?" and "Book another ride".

- [ ] **Step 1: Extend the mocks and write the failing tests**

Append to `web/tests/e2e/ride-mocks.ts`:

```ts
const T0 = Date.parse("2026-10-16T10:31:07.000Z");
type Stage = "submitted" | "ingested" | "validated" | "rejected" | "enriched" | "aggregated" | "stored" | "queryable";
const cp = (stage: Stage, seq: number, ms: number, detail: Record<string, unknown>, service = "processor") => ({
  trace_id: TID, trip_id: `s-${TID}`, source: "student", stage, seq, service, at: new Date(T0 + ms).toISOString(), detail,
});
export const CP = {
  submitted: cp("submitted", 1, 0, { pickup_zone: "Times Sq/Theatre District", dropoff_zone: "JFK Airport", payment: "card", tip_amount: 3.9, fare_amount: 19.5, estimate_method: "median of 214 real trips between these zones" }, "api"),
  ingested: cp("ingested", 2, 41, { topic: "trips.raw", partition: 0, offset: 482113 }, "api"),
  validated: cp("validated", 3, 103, { rules_checked: 11 }),
  enriched: cp("enriched", 4, 104, { pu_zone: "Times Sq/Theatre District", pu_borough: "Manhattan", do_zone: "JFK Airport", do_borough: "Queens", tip_pct: 20, duration_min: 16.5 }),
  aggregated: cp("aggregated", 5, 104, { window_start: "2025-10-09T17:42:00", borough: "Manhattan" }),
  stored: cp("stored", 6, 1210, { table: "taxi.trips", batch_rows: 142 }),
  queryable: cp("queryable", 7, 1800, { table: "taxi.trips", visible_after_ms: 590 }, "api"),
  rejected: cp("rejected", 3, 98, { reasons: ["TIP_UNREALISTIC"] }),
};
export const HAPPY = ["submitted", "ingested", "validated", "enriched", "aggregated", "stored", "queryable"] as const;
```

`web/tests/e2e/trace.spec.ts`:

```ts
import { expect, test, type Page } from "@playwright/test";
import { CP, HAPPY, mockRideApi, mockTraceSocket, send, TID } from "./ride-mocks";

async function simClock(page: Page) {
  await page.route("**/api/sim/clock", (r) => r.fulfill({ json: { sim_ts: "2025-10-09T17:42:00", speed: 60, paused: false, mode: "live" } }));
}

function streamRest(ws: Parameters<Parameters<typeof mockTraceSocket>[1]>[0], rest: readonly (keyof typeof CP)[]) {
  rest.forEach((s, i) => setTimeout(() => send(ws, { type: "checkpoint", ...CP[s] }), 150 * (i + 1)));
}

test("all 7 stages light up in order, with hop latencies, the total and the agent hand-off", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: "2025-10-09T17:42:00" });
    streamRest(ws, ["validated", "enriched", "aggregated", "stored", "queryable"]);
  });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByText("Times Sq/Theatre District → JFK Airport")).toBeVisible();
  await expect(page.getByTestId("stage-validated")).toHaveAttribute("data-state", /next|done/);
  await expect(page.getByTestId("stage-queryable")).toHaveAttribute("data-state", "done");
  const items = page.getByTestId("stages").getByRole("listitem");
  expect(await items.evaluateAll((els) => els.map((e) => e.getAttribute("data-testid")))).toEqual(HAPPY.map((s) => `stage-${s}`));
  for (const s of HAPPY) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "done");
  await expect(page.getByTestId("stage-ingested")).toContainText("+41 ms");
  await expect(page.getByTestId("stage-ingested")).toContainText("partition 0, offset 482113");
  await expect(page.getByTestId("stage-enriched")).toContainText("tip (20%)");
  await expect(page.getByTestId("stage-aggregated")).toContainText("Manhattan · 17:42");
  await expect(page.getByTestId("total")).toHaveText("From your phone to queryable in 1.8 s");
  await expect(page.getByRole("link", { name: "Where does my ride rank for tips today?" }))
    .toHaveAttribute("href", `/agent?trace_id=${TID}&suggestion=ride_rank`);
});

test("an absurd tip shows the amber rejected card and the rest stays grey", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
    streamRest(ws, ["rejected"]);
  });
  await page.goto(`/trace/${TID}`);
  const rejected = page.getByTestId("stage-rejected");
  await expect(rejected).toHaveAttribute("data-state", "rejected");
  await expect(rejected).toContainText("Rejected: the tip is unrealistically high.");
  await expect(page.getByTestId("stage-validated")).toHaveCount(0);
  for (const s of ["enriched", "aggregated", "stored", "queryable"]) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "future");
  await expect(page.getByRole("link", { name: "Book another ride" })).toHaveAttribute("href", "/ride");
  await expect(page.getByTestId("total")).toHaveCount(0);
});

test("a dropped socket reconnects and re-renders from history without duplicates", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws, n) => {
    if (n === 0) {
      send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
      send(ws, { type: "checkpoint", ...CP.validated });
      setTimeout(() => ws.close(), 100);
    } else {
      send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested, CP.validated, CP.enriched, CP.aggregated], sim_ts: null });
    }
  });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByTestId("stage-aggregated")).toHaveAttribute("data-state", "done");
  await expect(page.locator('[data-state="done"]')).toHaveCount(5);
  await expect(page.getByTestId("stages").getByRole("listitem")).toHaveCount(7);
});

test("while the processor is down the next card says so", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null }));
  await page.goto(`/trace/${TID}`);
  const next = page.getByTestId("stage-validated");
  await expect(next).toHaveAttribute("data-state", "next");
  await expect(next).toContainText("waiting for the stream processor…");
});

test("an unknown ride says so after 15 s", async ({ page }) => {
  await page.clock.install();
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [], sim_ts: null }));
  await page.goto(`/trace/${TID}`);
  await expect(page.getByText("Looking for your ride…")).toBeVisible();
  await page.clock.fastForward(16_000);
  await expect(page.getByRole("link", { name: "Book a new one" })).toHaveAttribute("href", "/ride");
});

test("a malformed id never opens a socket", async ({ page }) => {
  let opened = false;
  await mockTraceSocket(page, () => { opened = true; });
  await page.goto("/trace/not-a-ride");
  await expect(page.getByRole("link", { name: "Book a new one" })).toBeVisible();
  expect(opened).toBe(false);
});

test("book on /ride and watch all 7 stages (spec 08 acceptance 1)", async ({ page }) => {
  await mockRideApi(page);
  await mockTraceSocket(page, (ws) => {
    send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested], sim_ts: null });
    streamRest(ws, ["validated", "enriched", "aggregated", "stored", "queryable"]);
  });
  await page.goto("/ride");
  await page.getByRole("button", { name: "Pickup zone" }).click();
  await page.getByRole("dialog").getByRole("region", { name: "Popular" }).getByRole("button", { name: "Times Sq/Theatre District" }).click();
  await page.getByRole("button", { name: "Drop-off zone" }).click();
  await page.getByRole("dialog").getByRole("region", { name: "Popular" }).getByRole("button", { name: "JFK Airport" }).click();
  await page.getByRole("button", { name: "Book ride" }).click();
  for (const s of HAPPY) await expect(page.getByTestId(`stage-${s}`)).toHaveAttribute("data-state", "done");
});

test("no horizontal scroll at 360 px", async ({ page }) => {
  await simClock(page);
  await mockTraceSocket(page, (ws) => send(ws, { type: "history", checkpoints: [CP.submitted, CP.ingested, CP.rejected], sim_ts: null }));
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto(`/trace/${TID}`);
  await expect(page.getByTestId("stage-rejected")).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && pnpm test:e2e --project=pixel-7 trace.spec.ts`
Expected: FAIL. `/trace/<id>` returns 404.

- [ ] **Step 3: Implement**

`web/app/trace/[id]/page.tsx`:

```tsx
import type { Metadata } from "next";
import { TraceViewer } from "@/components/trace/TraceViewer";

export const metadata: Metadata = { title: "Your ride · NYC Taxi Live" };

export default async function TracePage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return (
    <main className="phone mx-auto min-h-dvh w-full max-w-md px-4 py-6">
      <TraceViewer traceId={id.toLowerCase()} />
    </main>
  );
}
```

`web/components/trace/StageCard.tsx`:

```tsx
import { STAGE_TITLE, stageCopy, type Checkpoint, type Stage } from "@/lib/trace";

export type CardState = "done" | "rejected" | "next" | "future";

const STYLE: Record<CardState, string> = {
  done: "border-visitor bg-visitor-fill text-visitor-ink",
  rejected: "border-amber bg-[#4a3300] text-[#ffe7b0]",
  next: "border-pipeline text-ink",
  future: "border-line text-muted opacity-60",
};

export function StageCard({ stage, state, checkpoint, hopMs, waiting, waitingS, connector }: {
  stage: Stage; state: CardState; checkpoint?: Checkpoint; hopMs?: number; waiting: string | null; waitingS: number | null; connector: boolean;
}) {
  return (
    <li data-testid={`stage-${stage}`} data-state={state} className="flex flex-col">
      {connector && (
        <div aria-hidden className="relative mx-auto h-6 w-0.5 bg-line">
          {state === "next" && <span className="packet absolute -left-1 top-1.5 size-2.5 rounded-full bg-visitor" />}
        </div>
      )}
      <div className={`rounded-xl border-2 px-4 py-3 transition-colors duration-300 ${STYLE[state]}`}>
        <div className="flex items-baseline justify-between gap-3">
          <h2 className="font-semibold">{STAGE_TITLE[stage]}</h2>
          {(state === "done" || state === "rejected") && hopMs !== undefined && (
            <span className="shrink-0 text-sm tabular-nums">+{hopMs} ms</span>
          )}
        </div>
        {checkpoint && <p className="mt-1 text-sm leading-snug">{stageCopy(checkpoint)}</p>}
        {state === "next" && waiting && (
          <p className="mt-1 flex items-center gap-2 text-sm text-muted">
            <span className="spinner" aria-hidden />
            {waiting}{waitingS !== null && waitingS >= 5 ? ` (${waitingS} s)` : ""}
          </p>
        )}
      </div>
    </li>
  );
}
```

`web/components/trace/TraceViewer.tsx`:

```tsx
"use client";
import Link from "next/link";
import { useEffect, useReducer, useRef, useState } from "react";
import { getSimClock } from "@/lib/api";
import { formatSim } from "@/lib/format";
import {
  applyMessage, EMPTY_VIEW, HAPPY_PATH, hopMs, isTraceId, money, rideSummary, TERMINAL, toTraceMessage, totalMs, waitingText,
  type Stage,
} from "@/lib/trace";
import { usePoll } from "@/lib/use-poll";
import { ReconnectingSocket, wsUrl } from "@/lib/ws";
import { StageCard, type CardState } from "./StageCard";

const NOT_FOUND_MS = 15_000;
const WITH_REJECTED: Stage[] = ["submitted", "ingested", "rejected", "enriched", "aggregated", "stored", "queryable"];

function NotFound() {
  return (
    <p role="status" className="rounded-xl border border-line px-4 py-3">
      We can&apos;t find this ride. <Link href="/ride" className="font-semibold text-visitor-ink underline">Book a new one</Link>
    </p>
  );
}

export function TraceViewer({ traceId }: { traceId: string }) {
  const valid = isTraceId(traceId);
  const [view, dispatch] = useReducer(applyMessage, EMPTY_VIEW);
  const [now, setNow] = useState(() => Date.now());
  const [lost, setLost] = useState(false);
  const socket = useRef<ReconnectingSocket | null>(null);
  const clock = usePoll(getSimClock, 5000);

  useEffect(() => {
    if (!valid) return;
    const s = new ReconnectingSocket(wsUrl(`/api/trace/${traceId}`, window.location, process.env.NEXT_PUBLIC_API_WS_ORIGIN), (raw) => {
      const msg = toTraceMessage(raw);
      if (msg) dispatch(msg);
    });
    socket.current = s;
    return () => s.stop();
  }, [traceId, valid]);

  const cps = view.checkpoints;
  const terminal = cps.some((c) => TERMINAL.has(c.stage));
  useEffect(() => {
    if (terminal) socket.current?.stop(); // nothing more will come: don't reconnect when the server closes
  }, [terminal]);
  useEffect(() => {
    if (terminal) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [terminal]);
  useEffect(() => {
    if (!valid || cps.length > 0) return;
    const id = setTimeout(() => setLost(true), NOT_FOUND_MS);
    return () => clearTimeout(id);
  }, [valid, cps.length]);

  if (!valid) return <NotFound />;

  const rejected = cps.some((c) => c.stage === "rejected");
  const stages = rejected ? WITH_REJECTED : [...HAPPY_PATH];
  const byStage = new Map(cps.map((c) => [c.stage, c]));
  const hops = hopMs(cps);
  const next = rejected ? null : (stages.find((s) => !byStage.has(s)) ?? null);
  const last = cps.at(-1);
  const waitingS = last ? Math.max(0, Math.floor((now - Date.parse(last.at)) / 1000)) : null;
  const summary = rideSummary(cps);
  const total = totalMs(cps);
  const state = (s: Stage): CardState =>
    byStage.has(s) ? (s === "rejected" ? "rejected" : "done") : s === next ? "next" : "future";
  const sim = clock?.sim_ts ?? view.simTs;

  return (
    <div className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <h1 className="text-xl font-semibold">{summary ? `${summary.pickupZone} → ${summary.dropoffZone}` : "Your ride"}</h1>
        {summary && (
          <p className="text-sm text-muted">
            {summary.fare !== null && `fare ~${money(summary.fare)} · `}
            {summary.payment === "cash" ? "cash (tip not recorded)" : `${money(summary.tip)} tip`}
          </p>
        )}
        <p className="text-sm text-muted">sim clock <span className="tabular-nums text-ink">{sim ? formatSim(sim) : "…"}</span></p>
      </header>
      {cps.length === 0 && (lost ? <NotFound /> : <p role="status" className="text-muted">Looking for your ride…</p>)}
      <ol data-testid="stages" className="flex flex-col">
        {stages.map((s, i) => (
          <StageCard key={s} stage={s} state={state(s)} checkpoint={byStage.get(s)} hopMs={hops[s]}
            waiting={s === next ? waitingText(s) : null} waitingS={s === next ? waitingS : null} connector={i > 0} />
        ))}
      </ol>
      {total !== null && (
        <div data-testid="total" role="status" className="rounded-xl bg-visitor-fill px-4 py-3 text-center text-lg font-semibold text-visitor-ink">
          From your phone to queryable in {(total / 1000).toFixed(1)} s
        </div>
      )}
      {byStage.has("queryable") && (
        <Link href={`/agent?trace_id=${traceId}&suggestion=ride_rank`}
          className="flex min-h-14 items-center justify-center rounded-xl bg-visitor px-4 text-center text-lg font-semibold text-black">
          Where does my ride rank for tips today?
        </Link>
      )}
      {rejected && (
        <Link href="/ride" className="flex min-h-14 items-center justify-center rounded-xl border-2 border-amber text-lg font-semibold">
          Book another ride
        </Link>
      )}
    </div>
  );
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd web && pnpm lint && pnpm typecheck && pnpm test:e2e --project=iphone-13 --project=pixel-7`
Expected: `ride.spec.ts` and `trace.spec.ts` pass on both phones.

If "an unknown ride says so after 15 s" is flaky because the page clock also drives the mocked socket's timers, move `page.clock.install()` after `page.goto()` and use `page.clock.runFor(16_000)`.

- [ ] **Step 5: Visual check**

Load the `frontend-design` skill for a visual pass of `/trace/[id]` at 390 × 844. Check both happy and rejected, and with `prefers-reduced-motion` emulated. It must be legible at arm's length, the green "lights up" must be obvious, and the amber must never read as red. Keep the test ids and `data-state` values. Re-run Step 4.

- [ ] **Step 6: Commit**

```bash
git add web/app/trace web/components/trace web/tests/e2e/ride-mocks.ts web/tests/e2e/trace.spec.ts
git commit -m "feat(web): /trace/[id] stage viewer with hop latencies, rejected path, reconnect, agent hand-off link"
```

---

### Task 10: "Rank my ride" hand-off on `/agent`, hub tile, eval harness books real rides

**Files:**
- Modify: `web/app/agent/page.tsx` (reads `searchParams`)
- Modify: `web/components/agent/AgentKiosk.tsx` (`rideTraceId` prop, `ask` options)
- Create: `web/components/agent/RidePrompt.tsx`
- Modify: `web/app/page.tsx` (third tile)
- Modify: `web/tests/e2e/agent.spec.ts`
- Modify: `scripts/eval_agent.py`, `scripts/tests/test_eval_agent.py`

**Interfaces:**
- Consumes:
  - Task 7: `getTrace`, `rideSummary`, `rideChip`, `isTraceId`.
  - Task 5: `GET /api/trace/{id}`.
  - Task 3: `POST /api/rides`.
  - M3: `AskBody` with `origin: "ride"` and `trace_id`. The api already checks that the trace exists and injects the ride context (spec 06).
- Produces:
  - `AgentKiosk({ rideTraceId?: string })`.
  - `RIDE_RANK_QUESTION = "Where does my ride rank for tips today?"`, which must match `evals/agent_golden_questions.yaml` id `ride_rank`.
  - Test ids `ride-prompt` and `ride-chip`.
  - `eval_agent.create_rides(api, ch) -> dict[str, str]` and `eval_agent.ride_question(q, rides) -> tuple[str, dict]`.

- [ ] **Step 1: Write the failing tests**

Append to `web/tests/e2e/agent.spec.ts`:

```ts
const RIDE_TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const RIDE_SUBMITTED = {
  trace_id: RIDE_TID, trip_id: `s-${RIDE_TID}`, source: "student", stage: "submitted", seq: 1, service: "api",
  at: "2026-10-16T10:31:07.000Z",
  detail: { pickup_zone: "Midtown Center", dropoff_zone: "JFK Airport", payment: "card", tip_amount: 5, fare_amount: 70.5 },
};

test("the ride hand-off shows the ride chip and asks with the ride context", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.route(`**/api/trace/${RIDE_TID}`, (r) => r.fulfill({ json: { trace_id: RIDE_TID, checkpoints: [RIDE_SUBMITTED] } }));
  let body: unknown;
  await page.route("**/api/agent/ask", async (r) => {
    body = r.request().postDataJSON();
    await r.fulfill({ status: 200, contentType: "text/event-stream", body: FIXTURE });
  });
  await page.goto(`/agent?trace_id=${RIDE_TID}&suggestion=ride_rank`);
  await expect(page.getByTestId("ride-chip")).toHaveText("Your ride: Midtown Center → JFK Airport, $5 tip");
  await page.getByTestId("ride-prompt").getByRole("button", { name: "Where does my ride rank for tips today?" }).click();
  await expect(page.getByTestId("answer")).toBeVisible();
  expect(body).toEqual({ question: "Where does my ride rank for tips today?", origin: "ride", trace_id: RIDE_TID });
});

test("a malformed ride id is ignored: no prompt, normal kiosk", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.goto("/agent?trace_id=not-a-uuid&suggestion=ride_rank");
  await expect(page.getByRole("button", { name: "Which borough has the best tippers?" })).toBeVisible();
  await expect(page.getByTestId("ride-prompt")).toHaveCount(0);
});
```

Append to `scripts/tests/test_eval_agent.py` (keep its existing imports and `sys.path` line):

```python
from eval_agent import ride_question


def test_ride_question_fills_both_placeholders_and_uses_the_right_ride():
    rides = {"card": "00000000-0000-0000-0000-00000000000c", "cash": "00000000-0000-0000-0000-00000000000a"}
    q = {"id": "ride_rank", "question": "Where does my ride rank for tips today?", "origin": "ride", "ride": "card",
         "reference_sql": "SELECT '{trace_id_card}', '{trace_id_cash}'"}
    sql, payload = ride_question(q, rides)
    assert sql == "SELECT '00000000-0000-0000-0000-00000000000c', '00000000-0000-0000-0000-00000000000a'"
    assert payload == {"question": q["question"], "origin": "ride", "trace_id": rides["card"]}
    assert ride_question({**q, "ride": "cash"}, rides)[1]["trace_id"] == rides["cash"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && pnpm test:e2e --project=desktop agent.spec.ts` and `uv run pytest -q scripts/tests/test_eval_agent.py`
Expected: FAIL. The web test fails because `ride-chip` isn't found. The Python test fails with `ImportError: cannot import name 'ride_question'`.

- [ ] **Step 3: Implement the web side**

`web/app/agent/page.tsx`:

```tsx
import type { Metadata } from "next";
import { AgentKiosk } from "@/components/agent/AgentKiosk";
import { isTraceId } from "@/lib/trace";

export const metadata: Metadata = { title: "Ask the data · NYC Taxi Live" };

type Search = Promise<Record<string, string | string[] | undefined>>;

export default async function AgentPage({ searchParams }: { searchParams: Search }) {
  const sp = await searchParams;
  // /agent?trace_id=…&suggestion=ride_rank comes from the trace viewer's "rank my ride" button (spec 07 §Hand-off).
  const id = typeof sp.trace_id === "string" ? sp.trace_id.toLowerCase() : "";
  const rideTraceId = sp.suggestion === "ride_rank" && isTraceId(id) ? id : undefined;
  return <AgentKiosk rideTraceId={rideTraceId} />;
}
```

`web/components/agent/RidePrompt.tsx`:

```tsx
"use client";
import { useEffect, useState } from "react";
import { getTrace } from "@/lib/api";
import { rideChip, rideSummary, type RideSummary } from "@/lib/trace";

export const RIDE_RANK_QUESTION = "Where does my ride rank for tips today?"; // = evals/agent_golden_questions.yaml ride_rank

export function RidePrompt({ traceId, onAsk }: { traceId: string; onAsk: () => void }) {
  const [summary, setSummary] = useState<RideSummary | null>(null);
  useEffect(() => {
    let alive = true;
    getTrace(traceId).then((t) => { if (alive) setSummary(rideSummary(t.checkpoints)); }).catch(() => undefined);
    return () => { alive = false; };
  }, [traceId]);
  return (
    <div data-testid="ride-prompt" className="flex flex-col gap-3 rounded-2xl border-2 border-visitor bg-visitor-fill p-4 text-visitor-ink">
      {summary && <p data-testid="ride-chip" className="self-start rounded-full bg-black/30 px-4 py-1 text-lg">Your ride: {rideChip(summary)}</p>}
      <button type="button" onClick={onAsk}
        className="min-h-14 rounded-xl bg-visitor px-5 text-left text-xl font-semibold text-black outline-offset-2 focus-visible:outline-2 focus-visible:outline-visitor-ink">
        {RIDE_RANK_QUESTION}
      </button>
    </div>
  );
}
```

`web/components/agent/AgentKiosk.tsx`:
- Signature: `export function AgentKiosk({ rideTraceId }: { rideTraceId?: string } = {})`.
- Add a type `type AskOpts = { suggestionId?: string; rideTraceId?: string };`.
- Change `last` to `useRef<{ question: string; opts: AskOpts } | null>(null)`.
- Change `ask` to `async (question: string, opts: AskOpts = {}) => {…}`, with:

```ts
      const origin = opts.rideTraceId ? "ride" : opts.suggestionId ? "suggested" : "typed";
      for await (const event of askAgent({ question, origin, suggestion_id: opts.suggestionId, trace_id: opts.rideTraceId }, controller.signal)) {
```

(`JSON.stringify` drops the `undefined` keys, so a typed question still posts exactly `{question, origin}`.)

- `last.current = { question, opts };`, and `retry` calls `ask(last.current.question, last.current.opts)`.
- Every `ask(s.question, s.id)` becomes `ask(s.question, { suggestionId: s.id })`: in `SuggestionList` and in `AttractScreen`'s `onPick`.
- In the "Ask" section, directly above `<AskForm …/>`:

```tsx
          {rideTraceId && <RidePrompt traceId={rideTraceId} onAsk={() => void ask(RIDE_RANK_QUESTION, { rideTraceId })} />}
```

`web/app/page.tsx`: add the tile between the two existing ones, and use `md:grid-cols-3`:

```ts
  { href: "/ride", title: "Trace your ride", text: "Book a pretend taxi ride on your phone and watch it cross the pipeline." },
```

- [ ] **Step 4: Implement the eval side**

`scripts/eval_agent.py`: add below `ask()`:

```python
# One card and one cash ride for the ride-context questions (evals/agent_golden_questions.yaml: ride: card | cash)
EVAL_RIDES = {
    "card": {"pickup_location_id": 161, "dropoff_location_id": 132, "payment": "card", "tip_amount": 5.0, "passenger_count": 1},
    "cash": {"pickup_location_id": 161, "dropoff_location_id": 132, "payment": "cash", "tip_amount": 5.0, "passenger_count": 1},
}


def create_rides(api: str, ch: Any, timeout_s: float = 20) -> dict[str, str]:
    """Book the eval rides through POST /api/rides and wait until agent_ro can see both (spec 07 queryable)."""
    ids = {kind: request("POST", f"{api}/api/rides", body, headers={"X-Forwarded-For": "10.250.2.1"})["trace_id"]
           for kind, body in EVAL_RIDES.items()}
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rows = ch.query("SELECT DISTINCT toString(trace_id) FROM taxi.trips WHERE trace_id IN {ids:Array(UUID)}",
                        parameters={"ids": list(ids.values())}).result_rows
        if {r[0] for r in rows} == set(ids.values()):
            return ids
        time.sleep(0.5)
    raise SystemExit("the eval rides never reached taxi.trips: is the processor running?")


def ride_question(q: dict[str, Any], rides: dict[str, str]) -> tuple[str, dict[str, Any]]:
    """(reference_sql, payload) for a ride-context question, with the harness's own ride ids filled in."""
    sql = q["reference_sql"].replace("{trace_id_card}", rides["card"]).replace("{trace_id_cash}", rides["cash"])
    return sql, {"question": q["question"], "origin": "ride", "trace_id": rides[q["ride"]]}
```

In `main()`:
- After `sim_now = wait_until_frozen(ch)`, add `rides = create_rides(args.api, ch) if any(q.get("origin") == "ride" for q in questions) else {}`. The processor keeps running while the replayer is paused.
- Replace the `if q.get("origin") == "ride": … skipped … continue` block and the payload lines with:

```python
            if q.get("origin") == "ride":
                reference_sql, payload = ride_question(q, rides)
            else:
                reference_sql = q["reference_sql"]
                payload = {"question": q["question"], "origin": q.get("origin", "typed")}
                if q.get("suggested"):
                    payload["suggestion_id"] = q["id"]
```

- Use `ch.query(reference_sql)` instead of `ch.query(q["reference_sql"])`, and `"reference_sql": reference_sql` in the result dict.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd web && pnpm lint && pnpm typecheck && pnpm test:e2e --project=desktop` and `uv run pytest -q scripts/tests`
Expected: all pass, including the existing agent tests.

- [ ] **Step 6: Run the eval on the stack (CLAUDE.md: record the score)**

Run: `make up && make eval MODEL=taxi-qwen2.5-coder-14b MODE=oneshot`
Expected:
- the 5 suggested questions stay 5/5;
- `ride_rank` and `ride_rank_cash` are now scored, not skipped.

Put the summary line (score, suggested 5/5, p95) in the commit message. If a ride question fails, record it as it is. Don't change the prompt in this task: CLAUDE.md requires an eval per prompt change, and that's a separate decision for the user.

- [ ] **Step 7: Commit**

```bash
git add web/app/agent/page.tsx web/components/agent web/app/page.tsx web/tests/e2e/agent.spec.ts scripts/eval_agent.py scripts/tests/test_eval_agent.py
git commit -m "feat: rank-my-ride hand-off on /agent, hub tile, eval books real rides

make eval (oneshot, taxi-qwen2.5-coder-14b): <paste score line here>"
```

---

### Task 11: `/trace/live` wall and `/qr` page (cut line 2 applies to `/trace/live` only)

**Files:**
- Create: `web/lib/wall.ts`, `web/tests/unit/wall.test.ts`
- Create: `web/components/QrCode.tsx`, `web/components/trace/LiveWall.tsx`
- Create: `web/app/trace/live/page.tsx`, `web/app/qr/page.tsx`
- Modify: `docker-compose.yml` (web `environment`)
- Create: `web/tests/e2e/live.spec.ts`, `web/tests/e2e/qr.spec.ts`

**Interfaces:**
- Consumes:
  - Task 5: the WS `/api/trace/live` message shapes.
  - Task 7: `isCheckpoint`, `mergeCheckpoints`, `TERMINAL`, `HAPPY_PATH`, `STAGE_TITLE`, `ReconnectingSocket`, `wsUrl`, `wifiQrPayload`, `formatSim`.
  - `qrcode.react`'s `QRCodeSVG`.
- Produces:
  - `lib/wall.ts`: `Dot`, `Wall`, `WallStats`, `EMPTY_WALL`, `FADE_MS`, `STALE_MS`, `dotLabel`, `applyWall(wall, raw, now)`, `pruneWall(wall, now)`.
  - Test ids: `box-<stage>`, `dot` (with `data-ambient`), `rides-today`, `rejected-today`, `ride-qr`, `qr-wifi`, `qr-ride`.

- [ ] **Step 1: Write the failing tests**

`web/tests/unit/wall.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { applyWall, EMPTY_WALL, FADE_MS, pruneWall, STALE_MS } from "@/lib/wall";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const cp = (stage: string, seq: number, at = "2026-10-16T10:31:07.000Z", detail: Record<string, unknown> = {}, source = "student") =>
  ({ trace_id: TID, trip_id: `s-${TID}`, source, stage, seq, service: "processor", at, detail });
const NOW = Date.parse("2026-10-16T10:31:10.000Z");

describe("live wall", () => {
  it("a checkpoint moves the dot, and enriched adds the pickup borough to the label", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("submitted", 1), ambient: false }, NOW);
    expect(w.dots[TID]).toMatchObject({ stage: "submitted", label: "3f2c", ambient: false, terminal: false });
    w = applyWall(w, { type: "checkpoint", ...cp("enriched", 4, undefined, { pu_borough: "Manhattan" }), ambient: false }, NOW);
    expect(w.dots[TID]).toMatchObject({ stage: "enriched", label: "3f2c · Manhattan" });
  });
  it("a late checkpoint never moves a dot backwards", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("stored", 6), ambient: false }, NOW);
    w = applyWall(w, { type: "checkpoint", ...cp("aggregated", 5), ambient: false }, NOW);
    expect(w.dots[TID]!.stage).toBe("stored");
  });
  it("terminal dots fade 20 s after they finish; stalled ones after 60 s", () => {
    let w = applyWall(EMPTY_WALL, { type: "checkpoint", ...cp("queryable", 7), ambient: false }, NOW);
    w = applyWall(w, { type: "checkpoint", ...cp("ingested", 2), trace_id: "r", ambient: true }, NOW);
    expect(Object.keys(pruneWall(w, NOW + FADE_MS - 1).dots)).toHaveLength(2);
    expect(Object.keys(pruneWall(w, NOW + FADE_MS + 1).dots)).toEqual(["r"]);
    expect(Object.keys(pruneWall(w, NOW + STALE_MS + 1).dots)).toEqual([]);
  });
  it("the snapshot skips rides that finished long ago", () => {
    const old = { trace_id: "old", checkpoints: [{ ...cp("submitted", 1, "2026-10-16T10:00:00.000Z"), trace_id: "old" }, { ...cp("queryable", 7, "2026-10-16T10:00:02.000Z"), trace_id: "old" }] };
    const fresh = { trace_id: TID, checkpoints: [cp("submitted", 1, "2026-10-16T10:31:09.000Z")] };
    const w = applyWall(EMPTY_WALL, { type: "snapshot", traces: [old, fresh], sim_ts: "2025-10-09T17:42:00" }, NOW);
    expect(Object.keys(w.dots)).toEqual([TID]);
    expect(w.simTs).toBe("2025-10-09T17:42:00");
  });
  it("clock and stats frames, and garbage is ignored", () => {
    let w = applyWall(EMPTY_WALL, { type: "clock", sim_ts: "2025-10-09T17:43:00" }, NOW);
    w = applyWall(w, { type: "stats", rides_today: 57, rejected_today: 6, median_ms: { ingested: 40 } }, NOW);
    expect(w.simTs).toBe("2025-10-09T17:43:00");
    expect(w.stats).toEqual({ ridesToday: 57, rejectedToday: 6, medianMs: { ingested: 40 } });
    expect(applyWall(w, "nope", NOW)).toBe(w);
    expect(applyWall(w, { type: "checkpoint", stage: "teleported" }, NOW)).toBe(w);
  });
});
```

`web/tests/e2e/live.spec.ts`:

```ts
import { expect, test } from "@playwright/test";

const TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f";
const at = () => new Date().toISOString();
const student = (stage: string, seq: number, detail: Record<string, unknown> = {}) =>
  ({ type: "checkpoint", trace_id: TID, trip_id: `s-${TID}`, source: "student", stage, seq, service: "processor", at: at(), detail, ambient: false });

test("student dots move box to box, ambient dots are small, and the side column shows today's numbers", async ({ page }) => {
  await page.routeWebSocket(/\/api\/trace\/live/, (ws) => {
    ws.send(JSON.stringify({ type: "snapshot", traces: [], sim_ts: "2025-10-09T17:42:00" }));
    ws.send(JSON.stringify({ type: "stats", rides_today: 57, rejected_today: 6, median_ms: { ingested: 40, validated: 62 } }));
    ws.send(JSON.stringify(student("submitted", 1)));
    ws.send(JSON.stringify({ type: "checkpoint", trace_id: "9d1e0000-0000-4000-8000-000000000000", trip_id: "r3-000000042", source: "replay",
      stage: "ingested", seq: 2, service: "replayer", at: at(), detail: {}, ambient: true }));
    setTimeout(() => ws.send(JSON.stringify(student("enriched", 4, { pu_borough: "Manhattan" }))), 300);
  });
  await page.goto("/trace/live");
  await expect(page.getByTestId("box-enriched").getByTestId("dot")).toHaveText("3f2c · Manhattan");
  await expect(page.getByTestId("box-submitted").getByTestId("dot")).toHaveCount(0);
  await expect(page.getByTestId("box-ingested").locator('[data-ambient="true"]')).toHaveCount(1);
  await expect(page.getByTestId("rides-today")).toHaveText("57");
  await expect(page.getByTestId("rejected-today")).toHaveText("6");
  await expect(page.getByTestId("box-validated")).toContainText("62 ms");
  await expect(page.getByTestId("ride-qr").locator("svg")).toBeVisible();
  await expect(page.getByTestId("ride-qr")).toContainText("http://192.168.8.10/ride");
});
```

`web/tests/e2e/qr.spec.ts`:

```ts
import { expect, test } from "@playwright/test";

// playwright.config.ts starts the server with STAND_WIFI_SSID=Test;Net and STAND_PUBLIC_URL=http://192.168.8.10
test("two big QR codes: join the Wi-Fi, then open /ride", async ({ page }) => {
  await page.goto("/qr");
  await expect(page.getByTestId("qr-wifi").locator("svg")).toBeVisible();
  await expect(page.getByTestId("qr-wifi")).toContainText("Test;Net");
  await expect(page.getByTestId("qr-ride").locator("svg")).toBeVisible();
  await expect(page.getByTestId("qr-ride")).toContainText("http://192.168.8.10/ride");
  const box = await page.getByTestId("qr-ride").locator("svg").boundingBox();
  expect(box!.width).toBeGreaterThanOrEqual(300); // scannable from a step away
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd web && pnpm test` and `pnpm test:e2e --project=desktop live.spec.ts qr.spec.ts`
Expected: FAIL. `@/lib/wall` doesn't resolve, and `/qr` is a 404. `/trace/live` renders the "can't find this ride" viewer, so `box-enriched` is missing.

- [ ] **Step 3: Implement**

`web/lib/wall.ts`:

```ts
import { isCheckpoint, mergeCheckpoints, TERMINAL, type Checkpoint, type Stage } from "@/lib/trace";

export const FADE_MS = 20_000; // spec 08: a dot fades out 20 s after its terminal stage
export const STALE_MS = 60_000; // a ride that stopped moving (processor down) leaves the wall after a minute

export type Dot = { traceId: string; label: string; stage: Stage; seq: number; ambient: boolean; terminal: boolean; updatedAt: number };
export type WallStats = { ridesToday: number; rejectedToday: number; medianMs: Partial<Record<Stage, number>> };
export type Wall = { dots: Record<string, Dot>; simTs: string | null; stats: WallStats | null };
export const EMPTY_WALL: Wall = { dots: {}, simTs: null, stats: null };

/** The first 4 characters of the trace id, plus the pickup borough once it's known (spec 08 §/trace/live). */
export const dotLabel = (traceId: string, borough?: string | null): string => `${traceId.slice(0, 4)}${borough ? ` · ${borough}` : ""}`;

function advance(dot: Dot | undefined, cp: Checkpoint, ambient: boolean, now: number): Dot {
  const b = cp.detail.pu_borough;
  const label = cp.stage === "enriched" && typeof b === "string" ? dotLabel(cp.trace_id, b) : (dot?.label ?? dotLabel(cp.trace_id));
  if (dot && cp.seq < dot.seq) return { ...dot, label }; // arrived late: never move a dot backwards
  return { traceId: cp.trace_id, label, stage: cp.stage, seq: cp.seq, ambient, terminal: TERMINAL.has(cp.stage), updatedAt: now };
}

function stripped(m: Record<string, unknown>): unknown {
  const rest: Record<string, unknown> = { ...m };
  delete rest.type;
  delete rest.ambient;
  return rest;
}

export function applyWall(wall: Wall, raw: unknown, now: number): Wall {
  if (typeof raw !== "object" || raw === null) return wall;
  const m = raw as Record<string, unknown>;
  switch (m.type) {
    case "snapshot": {
      const dots = { ...wall.dots };
      for (const t of Array.isArray(m.traces) ? (m.traces as { checkpoints?: unknown }[]) : []) {
        const cps = mergeCheckpoints([], Array.isArray(t?.checkpoints) ? t.checkpoints.filter(isCheckpoint) : []);
        let dot: Dot | undefined;
        for (const c of cps) dot = advance(dot, c, false, Date.parse(c.at));
        if (dot && !(dot.terminal && now - dot.updatedAt > FADE_MS)) dots[dot.traceId] = dot;
      }
      return { ...wall, dots, simTs: typeof m.sim_ts === "string" ? m.sim_ts : wall.simTs };
    }
    case "checkpoint": {
      const cp = stripped(m);
      if (!isCheckpoint(cp)) return wall;
      return { ...wall, dots: { ...wall.dots, [cp.trace_id]: advance(wall.dots[cp.trace_id], cp, m.ambient === true, now) } };
    }
    case "clock":
      return typeof m.sim_ts === "string" ? { ...wall, simTs: m.sim_ts } : wall;
    case "stats": {
      const median = typeof m.median_ms === "object" && m.median_ms !== null ? (m.median_ms as Partial<Record<Stage, number>>) : {};
      return { ...wall, stats: { ridesToday: Number(m.rides_today) || 0, rejectedToday: Number(m.rejected_today) || 0, medianMs: median } };
    }
    default:
      return wall;
  }
}

export function pruneWall(wall: Wall, now: number): Wall {
  const keep = Object.entries(wall.dots).filter(([, d]) => now - d.updatedAt <= (d.terminal ? FADE_MS : STALE_MS));
  return keep.length === Object.keys(wall.dots).length ? wall : { ...wall, dots: Object.fromEntries(keep) };
}
```

`web/components/QrCode.tsx`:

```tsx
"use client";
import { QRCodeSVG } from "qrcode.react";

/** Generated locally: no network needed (offline-first). Black on white with a quiet zone, so any phone camera reads it. */
export function QrCode({ value, size = 320, title }: { value: string; size?: number; title: string }) {
  return (
    <div className="rounded-2xl bg-white p-4">
      <QRCodeSVG value={value} size={size} marginSize={2} level="M" bgColor="#ffffff" fgColor="#000000" title={title} />
    </div>
  );
}
```

`web/components/trace/LiveWall.tsx`:

```tsx
"use client";
import { useEffect, useReducer } from "react";
import { QrCode } from "@/components/QrCode";
import { formatSim } from "@/lib/format";
import { HAPPY_PATH, STAGE_TITLE, type Stage } from "@/lib/trace";
import { applyWall, EMPTY_WALL, pruneWall, type Dot, type Wall } from "@/lib/wall";
import { ReconnectingSocket, wsUrl } from "@/lib/ws";

type Action = { kind: "message"; raw: unknown; now: number } | { kind: "tick"; now: number };
const reduce = (w: Wall, a: Action): Wall => (a.kind === "message" ? applyWall(w, a.raw, a.now) : pruneWall(w, a.now));
// Architecture image colours: the phone and the agent are visitor-facing (green), the rest is the core pipeline (purple).
const VISITOR_STAGES = new Set<Stage>(["submitted", "queryable"]);

function Box({ stage, dots, medianMs }: { stage: Stage; dots: Dot[]; medianMs?: number }) {
  const tone = stage === "rejected" ? "border-amber bg-[#4a3300] text-[#ffe7b0]"
    : VISITOR_STAGES.has(stage) ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-pipeline bg-pipeline-fill text-pipeline-ink";
  return (
    <section data-testid={`box-${stage}`} aria-label={STAGE_TITLE[stage]} className={`flex min-h-48 flex-col gap-3 rounded-2xl border-2 p-4 ${tone}`}>
      <h2 className="text-xl font-semibold">{STAGE_TITLE[stage]}</h2>
      {medianMs !== undefined && <p className="text-sm opacity-80">median hop {medianMs} ms</p>}
      <div className="flex flex-wrap content-start gap-2">
        {dots.map((d) => d.ambient
          ? <span key={d.traceId} data-testid="dot" data-ambient="true" aria-hidden className="size-3 rounded-full bg-current opacity-40" />
          : <span key={d.traceId} data-testid="dot" data-ambient="false" className="rounded-full bg-visitor px-3 py-1 text-base font-semibold text-black">{d.label}</span>)}
      </div>
    </section>
  );
}

export function LiveWall({ standUrl }: { standUrl: string }) {
  const [wall, dispatch] = useReducer(reduce, EMPTY_WALL);
  useEffect(() => {
    const s = new ReconnectingSocket(wsUrl("/api/trace/live", window.location, process.env.NEXT_PUBLIC_API_WS_ORIGIN),
      (raw) => dispatch({ kind: "message", raw, now: Date.now() }));
    const tick = setInterval(() => dispatch({ kind: "tick", now: Date.now() }), 1000);
    return () => { s.stop(); clearInterval(tick); };
  }, []);
  const dots = Object.values(wall.dots);
  const at = (stage: Stage) => dots.filter((d) => d.stage === stage);
  const median = wall.stats?.medianMs ?? {};
  return (
    <div className="grid min-h-dvh grid-cols-[minmax(0,1fr)_20rem] gap-6 p-6">
      <main className="flex flex-col gap-4">
        <header className="flex items-baseline justify-between">
          <h1 className="text-4xl font-semibold">Rides crossing the pipeline, live</h1>
          <p className="text-xl text-muted">sim clock <span className="tabular-nums text-ink">{wall.simTs ? formatSim(wall.simTs) : "…"}</span></p>
        </header>
        <div className="grid grid-cols-7 gap-3">
          {HAPPY_PATH.map((s) => <Box key={s} stage={s} dots={at(s)} medianMs={median[s]} />)}
          <div className="col-start-3"><Box stage="rejected" dots={at("rejected")} /></div>
        </div>
      </main>
      <aside className="flex flex-col gap-6 rounded-2xl border border-line bg-panel p-6 text-xl">
        <div><p className="text-muted">Rides today</p><p data-testid="rides-today" className="text-5xl font-semibold tabular-nums">{wall.stats?.ridesToday ?? "–"}</p></div>
        <div><p className="text-muted">Rejected today</p><p data-testid="rejected-today" className="text-5xl font-semibold tabular-nums text-amber">{wall.stats?.rejectedToday ?? "–"}</p></div>
        <figure data-testid="ride-qr" className="mt-auto flex flex-col items-center gap-3">
          <QrCode value={`${standUrl}/ride`} size={220} title="Open the ride page" />
          <figcaption className="text-center">Book your own ride:<br /><span className="font-semibold">{standUrl}/ride</span></figcaption>
        </figure>
      </aside>
    </div>
  );
}
```

`web/app/trace/live/page.tsx`:

```tsx
import type { Metadata } from "next";
import { LiveWall } from "@/components/trace/LiveWall";

export const metadata: Metadata = { title: "Live rides · NYC Taxi Live" };
export const dynamic = "force-dynamic"; // STAND_PUBLIC_URL is read when the page is served, never baked into the image

export default function LivePage() {
  const stand = (process.env.STAND_PUBLIC_URL || "http://192.168.8.10").replace(/\/$/, "");
  return <LiveWall standUrl={stand} />;
}
```

`web/app/qr/page.tsx`:

```tsx
import type { Metadata } from "next";
import { QrCode } from "@/components/QrCode";
import { wifiQrPayload } from "@/lib/wifi";

export const metadata: Metadata = { title: "Join · NYC Taxi Live" };
export const dynamic = "force-dynamic"; // read the Wi-Fi settings at request time, server side only (spec 08)

export default function QrPage() {
  const ssid = process.env.STAND_WIFI_SSID || "NYC-Taxi-Live";
  const password = process.env.STAND_WIFI_PASSWORD || "";
  const stand = (process.env.STAND_PUBLIC_URL || "http://192.168.8.10").replace(/\/$/, "");
  return (
    <main className="grid min-h-dvh grid-cols-1 place-items-center gap-10 p-8 md:grid-cols-2">
      <figure data-testid="qr-wifi" className="flex flex-col items-center gap-4">
        <QrCode value={wifiQrPayload(ssid, password)} title="Join the stand Wi-Fi" />
        <figcaption className="text-center text-3xl font-semibold">1. Join Wi-Fi <span className="text-visitor-ink">{ssid}</span></figcaption>
      </figure>
      <figure data-testid="qr-ride" className="flex flex-col items-center gap-4">
        <QrCode value={`${stand}/ride`} title="Open the ride page" />
        <figcaption className="text-center text-3xl font-semibold">2. Open <span className="text-visitor-ink">{stand}/ride</span></figcaption>
      </figure>
    </main>
  );
}
```

`docker-compose.yml`, `web` service: add

```yaml
    environment:
      # Read at request time by the /qr and /trace/live server components, never baked into the image (spec 08)
      STAND_PUBLIC_URL: ${STAND_PUBLIC_URL}
      STAND_WIFI_SSID: ${STAND_WIFI_SSID}
      STAND_WIFI_PASSWORD: ${STAND_WIFI_PASSWORD}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd web && pnpm test && pnpm lint && pnpm typecheck && pnpm test:e2e && cd .. && uv run pytest -q infra/tests/test_compose.py`
Expected: all pass. The full Playwright run covers all three projects.

- [ ] **Step 5: Visual check on the real screens**

Load the `frontend-design` skill for a visual pass of `/trace/live` at 1920 × 1080, and of `/qr` on the tablet size. Both are read from a few metres away: check the dot label size, the purple/green/amber contrast, and that ambient dots stay quiet. Keep the test ids. Re-run Step 4.

- [ ] **Step 6: Commit**

```bash
git add web/lib/wall.ts web/tests/unit/wall.test.ts web/components/QrCode.tsx web/components/trace/LiveWall.tsx \
  web/app/trace/live web/app/qr web/tests/e2e/live.spec.ts web/tests/e2e/qr.spec.ts docker-compose.yml
git commit -m "feat(web): /trace/live wall (student + ambient dots, today's numbers, ride QR) and /qr join page"
```

---

### Task 12: M4 exit: docs, ADR-019, versions, runbook, the real-phone check

**Files:**
- Modify: `docs/DECISIONS.md` (ADR-019)
- Modify: `docs/specs/07-trace-your-ride.md`, `docs/specs/04-clickhouse-schema.md`, `docs/specs/05-grafana-dashboards.md`, `docs/specs/08-web-frontend.md`
- Modify: `docs/DIAGRAMS.md` §4, then `make diagrams-sync`
- Modify: `docs/ARCHITECTURE.md` §7, §9
- Modify: `docs/RUNBOOK.md`
- Modify: `docs/IMPLEMENTATION_PLAN.md` (tick M4)

- [ ] **Step 1: ADR-019**

Append to `docs/DECISIONS.md`, in the same format as ADR-018:

```markdown
## ADR-019: The api's own reads use `trace_ro`, not `agent_ro`
**Context.** Spec 07 ran the queryable poller and the fare-estimate query as `agent_ro`. `agent_ro` has one shared quota of 120 queries/min (its safety limit). Polling every 250 ms during a 50-ride burst, plus the estimates, would use up that quota and break the AI agent mid-event.
**Decision.**
- A new user `trace_ro` has exactly `agent_ro`'s profile and SELECT grants, and its own quota (`trace`, 1 200 queries/min).
- It serves only the api's own fixed, parameterised queries: estimates, the queryable poll (one batched `IN {ids:Array(UUID)}` query per tick), trace history, and the wall stats.
- The agent's SQL still runs only as `agent_ro`, behind the guard.
**Consequences.**
- ClickHouse has no row policies, so "visible to `trace_ro`" means the same as "visible to `agent_ro`": the `queryable` stage still proves the agent can see the ride.
- There's one more password in `.env`, which `make secrets` fills.
```

- [ ] **Step 2: Spec and diagram updates (the spec stays the source of truth)**

Spec 07:
- §Config: add `RIDES_GLOBAL_PER_MIN`, `CLICKHOUSE_TRACE_*`.
- §Ride creation steps 5–7:
  - produce the trip first, and wait for the ack;
  - then produce `submitted` (with `at` = the moment the request was accepted) and `ingested`;
  - explain why, in one sentence (no dangling `submitted`; latency comes from `at`, and ordering from `seq`).
- Step 3: "as `trace_ro` (ADR-019)", and the fallback floors (≥ 0.1 mi, ≥ 1 min, ≥ $3, and 2.5 mi when a zone has no centroid).
- The catalogue's `submitted` detail keys: + `fare_amount`.
- Queryable detector: it's batched, as `trace_ro`, and it re-arms after an api restart.
- History: always merges memory with `taxi.trace_events`.
- Interfaces: + `GET /api/trace/{trace_id}`.
- The WS message shapes: `snapshot`, `clock`, `stats`, and `checkpoint` with `ambient`.

Spec 04:
- The users table: + a `trace_ro` row.
- The Interfaces: the api's own reads.

Spec 05: the Stand Health table gets "Visitor rides today" and "Visitor ride p95 phone → queryable (red > 5 s)".

Spec 08:
- §Config: `STAND_PUBLIC_URL` is read at request time by the `/qr` and `/trace/live` server components, not `NEXT_PUBLIC_STAND_URL`; + `NEXT_PUBLIC_API_WS_ORIGIN` (dev only).
- §Tech choices: the stage animations are CSS keyframes, not `motion`, to keep `/ride` and `/trace/[id]` light. Say so in one line, so it isn't a silent change.

`docs/DIAGRAMS.md` §4: reorder the api steps to `API->>RP: trips.raw (key = trip_id)`, then `RP-->>API: delivery ack (partition, offset)`, then `API->>RP: trace.checkpoints: submitted + ingested`. Then:

Run: `make diagrams-sync && make docs-check`
Expected: in sync, all Mermaid renders, contracts valid.

- [ ] **Step 3: Architecture and versions**

- ARCHITECTURE §7 (Security posture): one line on `trace_ro` (read-only, its own quota, parameterised queries only).
- ARCHITECTURE §9 (Versions): `websockets` (api + root dev), `json-schema-to-typescript`, `qrcode.react`, with the exact versions from the lock files.
- Record the `/ride` first-load JS measured in Task 8 (kB gzipped) next to the web versions.

- [ ] **Step 4: RUNBOOK**

Add to RUNBOOK:
- §1 "One week before" and §3 "Morning setup": **the real-IP check**.
  1. From phone A on the stand Wi-Fi, book 4 rides within a minute. The 4th must say "You've booked 3 rides this minute…".
  2. Right away, book one from phone B. It must succeed.
  3. If phone B is also refused, Docker Desktop is hiding the phones' real IPs (they all share one address). Then set `RIDES_RATE_LIMIT_PER_MIN=30` and `AGENT_RATE_LIMIT_PER_MIN=60` in `.env` and run `make up`, so only the global caps apply. Record which case happened.
- §4 "During the day": `make reset-rides` (it asks first) clears visitor rides, for example before a second session.
- §5 "Recovery playbook":
  - "a trace stops at `ingested`" → the processor is down (`make logs S=processor`);
  - "no `queryable`" → check `queryable_timeout` in `make logs S=api`.

- [ ] **Step 5: Final verification (all green before ticking M4)**

Run, in order:
1. `make lint`
2. `make test` (unit + contract + web unit + Playwright with mocks)
3. `make up && make test-stack`
4. `make test-e2e`
5. `sleep 60 && make burst`
6. `make docs-check`

Expected: every command exits 0. Paste the `make test-e2e` timing line and the `make burst` summary into the commit message.

Then run the **M4 exit check with a real phone** (IMPLEMENTATION_PLAN M4 exit):
1. Connect a phone to the stand router, open `/qr` on the tablet, scan both codes, and book a normal ride. All 7 stages must light up in < 5 s.
2. Book a $999 tip. It must show `rejected`.
3. Tap "Where does my ride rank for tips today?". The agent must answer with the ride context.
4. Do the real-IP check from Step 4.
5. On the same phone (or Chrome DevTools with mobile emulation, over the stand router), run Lighthouse mobile on `/ride`. Performance must be ≥ 90 (spec 08 acceptance 4). Record the score in ARCHITECTURE §9 next to the bundle size.
6. Unplug the router's internet uplink and repeat step 1 (spec 08 acceptance 7).

If there's no phone or router at hand, say so in the handoff. Don't claim the exit is met.

- [ ] **Step 6: Tick the plan and commit**

In `docs/IMPLEMENTATION_PLAN.md`, tick the M4 items this plan delivered. Note under M4 that the processor checkpoints, the `trace_events` Kafka engine, the replayer sampling and the two Grafana panels already existed from M1/M2.

```bash
git add docs
git commit -m "docs: M4 trace your ride: ADR-019 trace_ro, spec 04/05/07/08 + DIAGRAMS §4, versions, runbook real-IP check

make test-e2e: <timing line>
make burst: <summary>"
```
