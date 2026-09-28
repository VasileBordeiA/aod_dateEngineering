CREATE VIEW IF NOT EXISTS taxi.sim_now
AS SELECT
    max(pickup_ts)                 AS now_ts,
    toDate(max(pickup_ts))         AS today,
    toStartOfHour(max(pickup_ts))  AS this_hour
FROM taxi.trips
WHERE source = 'replay'
COMMENT 'The current SIMULATED time. Use it for "now", "today", "last hour", e.g. WHERE pickup_ts >= (SELECT now_ts FROM taxi.sim_now) - INTERVAL 1 HOUR';
