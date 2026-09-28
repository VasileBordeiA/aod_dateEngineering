CREATE TABLE IF NOT EXISTS taxi.agg_borough_minute
(
    window_start     DateTime64(3)          COMMENT 'Start of the 1-minute window (NYC local, simulated clock).',
    window_end       DateTime64(3)          COMMENT 'End of the window (exclusive).',
    borough          LowCardinality(String) COMMENT 'Pickup borough.',
    trips            UInt32                 COMMENT 'Valid trips picked up in this borough in this minute.',
    revenue          Float64                COMMENT 'Sum of total_amount in USD.',
    avg_fare         Float64                COMMENT 'Average fare_amount in USD.',
    avg_distance     Float64                COMMENT 'Average trip_distance in miles.',
    card_trips       UInt32                 COMMENT 'Trips paid by card.',
    avg_card_tip_pct Nullable(Float64)      COMMENT 'Average tip_pct over card trips. NULL if none.',
    student_trips    UInt32                 COMMENT 'How many of the trips were created by stand visitors.',
    emitted_at       DateTime64(3, 'UTC')   COMMENT 'Pipeline internal: when the processor closed the window.'
)
ENGINE = ReplacingMergeTree(emitted_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, borough)
COMMENT 'Pre-aggregated trips per pickup borough per minute, computed live by the stream processor. Fast for time series. Use taxi.trips for anything else.';
