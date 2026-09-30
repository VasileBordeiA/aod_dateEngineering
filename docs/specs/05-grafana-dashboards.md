# Spec 05: Grafana dashboards

## Purpose
The "live picture" on the big screen: what NYC taxis are doing *right now* (sim time), readable from 3 metres away. A second, operator-only dashboard shows stand health.

## Responsibilities
- Two Grafana instances from the same image (ADR-017):
  - **`grafana`** (public): the `grafana_ro` datasource and the NYC Taxi Live dashboard (`infra/grafana/provisioning/`). Kiosk mode, anonymous read-only access, served under `/grafana/` behind Caddy.
  - **`grafana-ops`** (operator): the `grafana_ops` datasource and the Stand Health dashboard (`infra/grafana/provisioning-ops/`). It's published on `127.0.0.1:3002` only, not behind Caddy, and needs the admin login.
- Dashboards are JSON in git. Grafana has no volume, so every restart re-provisions from those files.
- Offline operation: the plugin is baked into the image, and the map uses no internet tiles.

## Interfaces
| Direction | What |
|---|---|
| in | ClickHouse as `grafana_ro` (spec 04) |
| in (file) | `data/taxi_zones.geojson` mounted at `/usr/share/grafana/public/maps/taxi_zones.geojson` (spec 01) |
| out | `http://<stand>/grafana/d/nyc-taxi-live?kiosk&refresh=5s` (public) and `http://localhost:3002/d/stand-health?kiosk` (operator laptop only) |

## Config
- Image `infra/grafana/Dockerfile`: `FROM grafana/grafana-oss:<pinned>` plus `grafana cli plugins install grafana-clickhouse-datasource <pinned>` **at build time**. The venue has no internet at runtime.
- Environment:
  - `GF_SERVER_ROOT_URL=%(protocol)s://%(domain)s/grafana/`, `GF_SERVER_SERVE_FROM_SUB_PATH=true`
  - `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`, `GF_SECURITY_ALLOW_EMBEDDING=true`
  - `GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD}`, `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/dashboards/nyc-taxi-live.json`
  - `GF_ANALYTICS_CHECK_FOR_UPDATES=false`, `GF_ANALYTICS_REPORTING_ENABLED=false`, `GF_NEWS_NEWS_FEED_ENABLED=false`
  - `GF_GEOMAP_DEFAULT_BASELAYER_CONFIG` = a `geojson` layer on `public/maps/taxi_zones.geojson` (dark outlines), and `GF_GEOMAP_ENABLE_CUSTOM_BASELAYERS=false`. Every geomap then uses the zone outlines, never a tile server. A panel's own `basemap` option is ignored.
- `grafana-ops` differs only in: `GF_AUTH_ANONYMOUS_ENABLED=false`, no sub-path (`GF_SERVER_ROOT_URL=http://localhost:3002/`), home dashboard `stand-health.json`, no geojson mount.
- Datasources: `grafana-clickhouse-datasource`, host `clickhouse`, port 8123, protocol http, default database `taxi`, `queryTimeout: 15`.
  - `grafana`: uid `clickhouse`, user `grafana_ro`.
  - `grafana-ops`: uid `clickhouse-ops`, user `grafana_ops` (`CLICKHOUSE_OPS_PASSWORD`).
- Both dashboards: `"timezone": "utc"`, `"refresh": "5s"`, `"liveNow": false`, a dark theme, and large fonts.

## Behaviour

### ⚠️ Sim-time charts: known Grafana gotcha
The **Time series** panel always draws its x-axis over the dashboard's *wall-clock* time range. Our data lives on the *sim* clock (October 2025, running at 60×), so a time series panel would show nothing.
- **Preferred:** the **Trend** panel. The x field is sim time as epoch ms, with the x field unit set to `time:HH:mm`.
- **Fallback:** the **Bar chart** panel. The x field is `formatDateTime(window_start,'%H:%i')` (string), stacked by borough.

**Decided at M2 (ADR-016): Trend.** The query pivots boroughs into columns with `sumIf` (one frame, no transformations), `t` is epoch ms with unit `time:HH:mm`, and `min: 0` is set on the borough series only (on `t` it would stretch the axis back to 1970).
- The **Stat sparkline** has the same problem: it's drawn over the wall-clock range, so sim-time sparklines stay empty. The big screen has no sparklines. The trend chart shows the shape.
- Every sim-time series follows this pattern. Queries never use `$__timeFilter`. They filter relative to `taxi.sim_now`.

### Dashboard 1: "NYC Taxi Live" (uid `nyc-taxi-live`, big screen, 1920×1080)
Layout: 3 rows, no scrolling. The header row has the title text panel ("Live: NYC yellow taxis, October 2025 replayed at 60×").

| # | Panel | Type | Query (sketch) |
|---|---|---|---|
| 1 | **Sim clock** | Stat (text) | `SELECT formatDateTime(now_ts, '%a %d %b · %H:%i') AS sim_clock FROM taxi.sim_now` |
| 2 | Trips in the last sim hour | Stat | `SELECT count() FROM taxi.trips WHERE pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 1 HOUR` |
| 3 | Revenue in the last sim hour | Stat (USD) | same filter, `round(sum(total_amount))` |
| 4 | Average fare (last sim hour) | Stat (USD) | `round(avg(fare_amount), 2)` |
| 5 | **Trips per minute by borough** (last 6 sim hours) | Trend, stacked | see A |
| 6 | **Where the taxis are** (pickups, last sim hour) | Geomap | see B |
| 7 | Top 10 pickup zones (last sim hour) | Bar gauge, one neutral hue | `SELECT pu_zone, count() c FROM taxi.trips WHERE pickup_ts >= … - INTERVAL 1 HOUR GROUP BY pu_zone ORDER BY c DESC LIMIT 10` |
| 8 | Card tip % by borough (last 24 sim hours) | Bar chart | see C |
| 9 | **Rejected by the pipeline** (last 24 sim hours) | Stat % + bar gauge (two Grafana panels) | see D |
| 10 | Pipeline latency per stage (last 15 wall minutes) | Bar chart (ms) | see E |
| 11 | Rides created by visitors (today, wall) | Stat | `SELECT count() FROM taxi.trips WHERE source='student' AND stored_at >= toStartOfDay(now64(3))` |

```sql
-- A: trips per minute by borough, pivoted to one column per borough (EWR, Unknown, N/A left out).
-- argMax(…, emitted_at) keeps the latest version of a window the processor re-emitted.
SELECT toUnixTimestamp64Milli(window_start) AS t,
       sumIf(trips, borough = 'Manhattan') AS `Manhattan`,
       sumIf(trips, borough = 'Brooklyn') AS `Brooklyn`,
       sumIf(trips, borough = 'Queens') AS `Queens`,
       sumIf(trips, borough = 'Bronx') AS `Bronx`,
       sumIf(trips, borough = 'Staten Island') AS `Staten Island`
FROM
(
    SELECT window_start, borough, argMax(trips, emitted_at) AS trips
    FROM taxi.agg_borough_minute
    WHERE window_start >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 6 HOUR
    GROUP BY window_start, borough
)
GROUP BY window_start
ORDER BY window_start;

-- B: geomap markers (size = pickups, one light colour #c4bdf5 on the dark zone outlines).
-- The base layer is the GeoJSON from GF_GEOMAP_DEFAULT_BASELAYER_CONFIG, no tiles.
SELECT z.zone, z.lat AS latitude, z.lon AS longitude, count() AS pickups
FROM taxi.trips AS t
INNER JOIN taxi.zones AS z ON z.location_id = t.pu_location_id
WHERE t.pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 1 HOUR
  AND z.lat IS NOT NULL
GROUP BY z.zone, z.lat, z.lon;

-- C: card tip % by borough
SELECT pu_borough AS borough, round(avg(tip_pct), 1) AS avg_tip_pct, count() AS card_trips
FROM taxi.trips
WHERE is_card AND tip_pct IS NOT NULL
  AND pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 24 HOUR
  AND pu_borough NOT IN ('Unknown', 'N/A')
GROUP BY borough
HAVING card_trips >= 30
ORDER BY avg_tip_pct DESC;

-- D: rejected by reason
SELECT reason, count() AS trips
FROM taxi.trips_rejected
ARRAY JOIN reasons AS reason
WHERE pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 24 HOUR
GROUP BY reason
ORDER BY trips DESC;

-- E: latency per stage (wall time only!)
SELECT stage,
       round(quantile(0.5)(ms))  AS p50_ms,
       round(quantile(0.95)(ms)) AS p95_ms
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
ORDER BY any(seq);
```

Visual rules (load the `dataviz` skill when building):
- One fixed colour per borough across all panels. Use the same palette in the web UI (spec 08).
- Large stat fonts. No more than 11 panels (panel 9 counts once).
- Rejected reasons and top zones use a single neutral hue (`#9a9890`), not red for everything. Rejection is healthy behaviour, not an error.
- Latency p50/p95 is an ordinal pair of one hue (purple = core pipeline): p50 `#7f74e0`, p95 `#c4bdf5`.
- Visitor rides (panel 11) use the visitor-facing green `#1D9E75`.

#### Borough palette
The categorical order is the CVD-safety mechanism, so stacks follow it top to bottom. The dataviz validator passes it on Grafana's dark panel surface `#181b1f`: worst adjacent CVD ΔE 8.4, normal-vision ΔE 19.3, every slot ≥ 3:1. `infra/tests/test_grafana_dashboards.py` reads this table, and `web/lib/palette.ts` must copy it.

| Borough | Colour |
|---|---|
| Manhattan | `#3987e5` |
| Brooklyn | `#d95926` |
| Queens | `#199e70` |
| Bronx | `#c98500` |
| Staten Island | `#d55181` |
| EWR | `#008300` |

### Dashboard 2: "Stand Health" (uid `stand-health`, in `grafana-ops`, operator laptop only)
Wall-clock stats use coloured backgrounds when they have thresholds. Info-only stats stay plain. Agent panels show "no questions" instead of a threshold colour when `agent_log` has no rows.

| Panel | Query idea | Threshold |
|---|---|---|
| Freshness | `dateDiff('millisecond', max(stored_at), now64(3))` on `taxi.trips` | green < 3 s, red > 10 s |
| Ingest rate | rows with `stored_at` in the last 60 s, ÷ 60 | red if 0 |
| E2E latency p95 | `quantile(0.95)(dateDiff('ms', produced_at, stored_at))`, last 5 min | red > 5 s |
| Sim speed check | Over replay rows stored in the last 60 s: (max − min `pickup_ts`) ÷ (max − min `stored_at`) | expect ≈ REPLAY_SPEED |
| Rejected share | rejected ÷ (valid + rejected), last 24 sim hours | info only |
| Active parts per table | `SELECT table, count() FROM system.parts WHERE database='taxi' AND active GROUP BY table` | red > 300 |
| Agent questions / 10 min, success %, p95 latency, provider split, refusals | from `taxi.agent_log` (rates over the last 60 min) | success < 90% is amber. Success = `avgIf(success, NOT refused)`, since refusing off-topic questions is correct |
| Agent errors (last 20) | `SELECT asked_at, provider, error FROM taxi.agent_log WHERE NOT success ORDER BY asked_at DESC LIMIT 20` | n/a |
| ClickHouse slow queries | `system.query_log`, `query_duration_ms > 2000`, last 15 min | n/a |
| Visitor rides today | `SELECT countIf(stage='submitted') FROM taxi.trace_events WHERE source='student' AND at >= toStartOfDay(now64(3))` | info only |
| Visitor ride p95 phone → queryable | `quantile(0.95)` of `dateDiff('ms', at_submitted, at_queryable)` per trace, over today's student traces | green ≤ 2 s, red > 5 s |

This dashboard reads `taxi.agent_log` and `system.query_log`, which `grafana_ro` must **not** have: anonymous viewers can send arbitrary SQL through Grafana's query API (spec 04).
- A hidden folder is **not** enough. Grafana OSS has no datasource permissions, so any viewer can query any datasource of the org by uid, and `/api/frontend/settings` lists them all.
- So the `grafana_ops` datasource exists only in `grafana-ops` (ADR-017). The public Grafana never learns that it exists.

## Failure modes
| Failure | Behaviour |
|---|---|
| ClickHouse slow or down | Panels show "No data" or an error. Stand Health freshness goes red. Nothing crashes |
| Query limits hit | The grafana profile has a 15 s timeout. Each panel query must run in < 500 ms (acceptance criterion) |
| No internet | Works: the plugin is baked in and the map has no tiles |

## Observability
Grafana's own logs go to stdout. Stand Health *is* the observability for everything else.

## Acceptance criteria
1. A fresh `make up` shows both dashboards already provisioned, with no manual clicks. The home dashboard is NYC Taxi Live.
2. With Wi-Fi off, all panels, including the geomap with zone outlines, render.
3. With the replayer running at 60×, panels 1–5 visibly change on every 5 s refresh.
4. Every panel query runs in < 500 ms on a backfilled dataset of ≥ 7 sim days (check via `system.query_log`).
5. Anonymous users can view but cannot edit. The public Grafana has neither Stand Health nor the `clickhouse-ops` datasource, and `grafana-ops` answers 401 without a login.
6. The kiosk URL works through Caddy at `/grafana/…` and inside an iframe on `/architecture`.

## Out of scope
Grafana alerting to external channels, Prometheus metrics, and per-visitor dashboards.
