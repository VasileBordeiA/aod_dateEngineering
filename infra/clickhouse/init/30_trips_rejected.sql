CREATE TABLE IF NOT EXISTS taxi.trips_rejected
(
    trip_id     String                        COMMENT 'Id of the rejected trip.',
    source      LowCardinality(String)        COMMENT '''replay'' or ''student''.',
    trace_id    Nullable(UUID)                COMMENT 'Set if the trip was traced.',
    pickup_ts   Nullable(DateTime64(3))       COMMENT 'Pickup time (NYC local, simulated), NULL if unparseable.',
    reasons     Array(LowCardinality(String)) COMMENT 'Every validation rule the trip failed, e.g. [''FARE_NON_POSITIVE'',''TOTAL_NEGATIVE'']. Use arrayJoin(reasons) to count by reason.',
    payload     String                        COMMENT 'The original raw message as JSON.' CODEC(ZSTD(3)),
    rejected_at DateTime64(3, 'UTC')          COMMENT 'Pipeline internal: wall-clock UTC time of rejection.'
)
ENGINE = ReplacingMergeTree(rejected_at)
PARTITION BY toYYYYMM(rejected_at)
ORDER BY trip_id
COMMENT 'Trips the stream processor rejected as invalid (data-quality dead-letter table). Reason codes: SCHEMA_INVALID, FARE_NON_POSITIVE, TOTAL_NEGATIVE, DISTANCE_NON_POSITIVE, DISTANCE_TOO_LONG, DROPOFF_NOT_AFTER_PICKUP, DURATION_TOO_LONG, LOCATION_INVALID, PASSENGER_COUNT_INVALID, TIP_NEGATIVE, TIP_UNREALISTIC.';
