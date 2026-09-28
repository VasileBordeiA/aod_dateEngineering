CREATE TABLE IF NOT EXISTS taxi.agent_log
(
    asked_at       DateTime64(3, 'UTC'),
    request_id     UUID,
    origin         LowCardinality(String),   -- suggested | typed | ride
    suggestion_id  LowCardinality(String),   -- '' unless origin = suggested
    trace_id       Nullable(UUID),
    question       String,
    provider       LowCardinality(String),   -- local | internal | cache
    model          LowCardinality(String),
    mode           LowCardinality(String),   -- tools | oneshot | cached
    sql_attempts   Array(String),
    final_sql      String,
    success        Bool,
    refused        Bool,
    error          String,
    result_rows    UInt32,
    tool_calls     UInt8,
    latency_ms     UInt32,
    first_event_ms UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(asked_at)
ORDER BY asked_at
COMMENT 'Operator-only log of agent questions. NOT visible to the agent.';
