# Architecture Decision Records

Format: context → decision → consequences → alternatives rejected. To change a decision, add a new ADR that supersedes the old one. Don't edit history.

---

## ADR-001: Quix Streams as the stream processor
**Status:** accepted · 2026-09-28

**Context:** We need clean, enrich, and windowed aggregates, built in under 2 weeks by a small team, and explainable to students in 30 seconds.

**Decision:** Quix Streams (pure Python, Kafka-native, StreamingDataFrame API, built-in tumbling windows, state in RocksDB).

**Consequences:**
- The processing code reads like pandas, which students recognise.
- Quix creates its own repartition and changelog topics (`group_by`, windows). These are visible in Redpanda Console, which is fine and even educational.
- We need a custom sink for ClickHouse (ADR-003).

**Rejected:**
- **Flink:** impressive, but heavy (JVM, more RAM, slow iteration).
- **Bytewax:** similar, with a smaller community.
- **ClickHouse materialized views only:** that would hide the "stream processing" stage we want to show.

## ADR-002: Redpanda as the broker
**Status:** accepted

**Decision:** A single-node Redpanda in dev-container mode, plus Redpanda Console.

**Consequences:** Kafka API compatibility (any client works), a single binary, low memory, and a nice UI for showing topics live.

**Rejected:** Apache Kafka (KRaft) is heavier and has no bundled console. NATS and Redis Streams aren't the industry-standard vocabulary.

## ADR-003: Processor writes to ClickHouse through a custom `BatchingSink`
**Status:** accepted

**Context:** The trace feature needs a `stored` checkpoint emitted *after* the row is durably in ClickHouse.

**Decision:** Implement a Quix `BatchingSink` subclass using `clickhouse-connect`. It flushes on each Quix checkpoint (`commit_interval=1s`). After a successful insert, it produces `stored` checkpoints for traced rows in the batch.

**Consequences:**
- We control batching (and avoid ClickHouse's "too many parts" problem) and know exactly when data is stored.
- Delivery is at least once. Tables are `ReplacingMergeTree` so duplicates collapse on merge.

**Rejected:** The ClickHouse Kafka engine for trips can't tell us when a row is stored. We *do* use the Kafka engine for `trace_events`, as a second ingestion pattern worth showing.

## ADR-004: Simulated time model
**Status:** accepted

**Decision:**
- Replay original dates at `REPLAY_SPEED`× from `REPLAY_START_TS`, looping with a +35-day offset.
- `*_ts` columns are sim time (naive NYC local) and `*_at` columns are wall time (UTC).
- "Now" is `taxi.sim_now` / the `sim.clock` topic.

**Consequences:**
- Dashboards can't use Grafana's time picker directly. Panels filter relative to `sim_now`.
- It's honest ("this is October 2025, replayed") and deterministic for evals.

**Rejected:**
- Rebasing timestamps to today's date is confusing when sped up (the sim runs into the future).
- Real-time 1× is too slow to see patterns during a visit.

## ADR-005: Custom trace topic instead of OpenTelemetry
**Status:** accepted

**Decision:** Each stage emits a small JSON checkpoint to `trace.checkpoints`. The API relays them over WebSocket to an animated page.

**Consequences:** It's very visual and simple, with no extra infra. It isn't standard observability tooling, so we mention OTel in the talking points.

**Rejected:** OTel + Jaeger is more setup and less friendly for students. We might add it later as a stretch goal.

## ADR-006: JSON messages with JSON Schema contracts, no schema registry
**Status:** accepted

**Decision:** Messages are UTF-8 JSON, and `contracts/events/*.schema.json` is authoritative. Tests validate producers and consumers against the schemas.

**Consequences:** Messages are human-readable in Redpanda Console at the stand. They're slightly bigger, which is irrelevant at ~200 msg/s.

**Rejected:** Avro or Protobuf with the Redpanda schema registry: correct for production, but it hurts the on-screen story.

## ADR-007: OpenAI-compatible LLM client; local model by default; internal gateway optional
**Status:** accepted

**Context:** The company prefers a local model plus compatibility with its internal AI resource (OpenAI-compatible, API key). Venue internet is unreliable.

**Decision:**
- One client (the `openai` Python SDK), with providers configured as `{base_url, api_key, model}`.
- `LLM_ORDER=local,internal`.
- Ollama runs **natively** on macOS for Metal acceleration.
- Provider health checks, a fallback chain, and cached reference SQL for suggested questions.

**Consequences:**
- The demo never depends on the internet.
- Local-model text-to-SQL quality is the main risk, which is why we have an eval harness, `AGENT_MODE=oneshot` as a fallback, and a curated schema with comments.

**Rejected:**
- An Anthropic-native client: not needed, since the gateway is OpenAI-compatible. A second adapter is ~50 lines if it's ever required.
- The ClickHouse MCP server gives less control over guardrails and over SSE step events.

## ADR-008: The agent reads a curated schema; column comments are the data dictionary
**Status:** accepted

**Decision:**
- Every column in agent-visible tables has a ClickHouse `COMMENT`.
- `describe_table` returns name, type, and comment, plus 3 sample rows.
- Known pitfalls live in comments, not in the prompt. The main one: tips are recorded for card payments only.

**Consequences:** Improving the data model improves the agent. That's the core teaching message.

## ADR-009: Money as Float64
**Status:** accepted

**Decision:** Monetary columns are `Float64`.

**Consequences:** LLM-generated arithmetic (`tip/fare`, `avg`, `round`) works without Decimal scale surprises. Values are display-rounded to 2 decimals.

**Rejected:** `Decimal(10,2)` is correct for accounting, but not needed for an analytics demo. Mention it as a talking point.

## ADR-010: Single entry point with Caddy, own travel router
**Status:** accepted

**Decision:**
- Caddy on :80 routes `/`, `/api/*`, and `/grafana/*`, and passes through WebSocket and SSE.
- A dedicated travel router SSID gives the laptop a static IP, which the QR codes point to.

**Consequences:** Phones reach the stand even when the university Wi-Fi isolates clients. Only one port is exposed.

**Rejected:**
- Relying on venue Wi-Fi.
- Public tunnels (these depend on the internet). They're kept as an emergency fallback in the RUNBOOK.

## ADR-011: Backfill history through the real pipeline
**Status:** accepted

**Decision:** Before the event, run the replayer with `REPLAY_SPEED=max` until about day 8 of the month. The replayer persists its position, and live replay continues from there.

**Consequences:** From the first visitor, the agent can answer "rush hour" and "today versus last week" questions. The data is consistent because it went through the same validation and enrichment code.

## ADR-012: Server-Sent Events for the agent, WebSocket for traces
**Status:** accepted

**Decision:** The agent uses SSE: one request, many ordered step events, and it works with a plain POST via `fetch` streaming. Traces use WebSocket, a long-lived subscription per trace or for the live wall.

**Consequences:** Caddy must not buffer either (`flush_interval -1`).

## ADR-013: Aggregation reads `trips.enriched`; implementation notes from M1
**Status:** accepted · 2026-09-28

**Context:** Building M1 with Quix Streams 3.26.

**Decision:**
- The windowed aggregation is a second StreamingDataFrame on `trips.enriched`, not a `group_by` branch off the validated stream. Branching, `group_by`, and windows then never interact.
- `avg_speed_mph` is rounded to 2 decimals.
- The processor integration test runs the processor as a subprocess against testcontainers Redpanda and ClickHouse, the latter with the real init SQL and users.
- `agent_ro` keeps `readonly=1`, which clickhouse-connect handles. Its hard row cap is `max_result_rows=5000` with `throw`:
  - `break` is block-granular and let 10 000 rows through a 500 cap.
  - clickhouse-connect reads about 1.5 k rows of `system.settings` when it connects.
  - The SQL guard's `LIMIT 200` is the normal limit.
- The Grafana datasource sets `queryTimeout: 15`, because the plugin's default violates the profile constraint (≤ 30).
- Trace checkpoints are ordered by `seq`, not by arrival. `stored` comes from a second producer right after the INSERT and can overtake the checkpoints Quix flushes at commit.
- Windows close **per key**, so rare boroughs (EWR) are aggregated late.

**Consequences:** The aggregates are computed from exactly what is stored in `taxi.trips`. There is one extra consumer of `trips.enriched`, which is negligible.

## ADR-014: `trips.raw` and `trips.enriched` are single-partition
**Status:** accepted · 2026-09-28 · supersedes the "3 partitions" line in spec 02

**Context:** With 3 partitions consumed concurrently, `pickup_ts` went backwards by up to ~450 sim-seconds after the `group_by` repartition. Quix dropped those records as late. At 60× a 180 s grace hides most of it, but a max-speed backfill silently lost aggregates. The integration test caught it.

**Decision:** The two event-time streams have exactly one partition. The other topics keep 3.

**Consequences:**
- Event time stays strictly ordered from the replayer into the windows. No late records were observed in a 283 k-row backfill.
- The throughput ceiling is one partition (well above 10 k msg/s), which is irrelevant at about 200 msg/s. One processor consumes everything anyway.

## ADR-015: Size-based topic retention
**Status:** accepted · 2026-09-28 · supersedes "6 h retention"

**Context:** The processor writes records with their event-time timestamps (October 2025). Time-based retention (`retention.ms=6 h`) therefore deleted freshly written `trips.enriched` segments immediately. The aggregation consumer hit "offset out of range" and lost data.

**Decision:** Every delete-policy topic uses `retention.ms=-1` with `retention.bytes=1 GiB` per partition (`TOPIC_RETENTION_BYTES`). `create-topics.sh` converges existing topics to this config.

**Consequences:** Retention no longer depends on timestamps. Worst case this is about 9 GiB of broker disk (9 partitions). ClickHouse remains the system of record.
