CREATE TABLE IF NOT EXISTS taxi.trace_events
(
    trace_id UUID                   COMMENT 'Trace id of the trip.',
    trip_id  String                 COMMENT 'Trip id.',
    source   LowCardinality(String) COMMENT '''replay'' or ''student''.',
    stage    LowCardinality(String) COMMENT 'submitted, ingested, validated, rejected, enriched, aggregated, stored, queryable.',
    seq      UInt8                  COMMENT 'Stage order 1-7 (rejected = 3).',
    service  LowCardinality(String) COMMENT 'Service that emitted the checkpoint.',
    at       DateTime64(3, 'UTC')   COMMENT 'Wall-clock UTC time the stage completed. Latency between stages = difference of at.',
    detail   String                 COMMENT 'Stage details as JSON.'
)
ENGINE = MergeTree
ORDER BY (trace_id, seq, at)
TTL toDateTime(at) + INTERVAL 14 DAY
COMMENT 'One row per pipeline stage a traced trip passed through. Use it for pipeline latency questions.';

CREATE TABLE IF NOT EXISTS taxi.trace_events_queue
(
    trace_id UUID, trip_id String, source String, stage String,
    seq UInt8, service String, at String, detail String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'trace.checkpoints',
         kafka_group_name = 'clickhouse-trace-events',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_skip_broken_messages = 100;
-- JSONEachRow reads the nested `detail` object into a String column
-- (input_format_json_read_objects_as_strings, on by default in recent versions; verify at M0).

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi.trace_events_mv TO taxi.trace_events AS
SELECT trace_id, trip_id, source, stage, seq, service,
       parseDateTime64BestEffort(at, 3, 'UTC') AS at, detail
FROM taxi.trace_events_queue;
