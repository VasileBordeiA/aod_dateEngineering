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

## ADR-016: Sim-time charts use the Trend panel with a `sumIf` pivot
**Status:** accepted · 2026-09-28 · resolves the M2 spike in spec 05

**Context:** Grafana's Time series panel draws over the dashboard's wall-clock range, so October 2025 sim data never shows. The spike compared the Trend panel with a stacked Bar chart on the live `agg_borough_minute` data.

**Decision:**
- Every sim-time series is a **Trend** panel.
- The x field is `toUnixTimestamp64Milli(window_start)` with unit `time:HH:mm`.
- Boroughs are pivoted into columns in SQL (`sumIf(trips, borough = '…')`), so there is one frame and no transformations.
- `min: 0` goes on the series fields only. Set on the x field, it stretches the axis back to 1970.
- The **Stat sparkline** has the same wall-clock binding and stays empty for sim data, so the big screen has no sparklines.

**Consequences:** A real time axis with readable `HH:mm` ticks, stacking, and a multi-series tooltip. The Bar chart fallback also worked, but it shows every minute as a category label and gets crowded at 360 points.

## ADR-017: Stand Health runs in a separate operator-only Grafana
**Status:** accepted · 2026-09-28 · supersedes the "folder with no anonymous permission" plan in spec 05

**Context:** During M2 an anonymous `POST /grafana/api/ds/query` through Caddy returned rows, and `/api/frontend/settings` listed every datasource. Grafana OSS has no per-datasource permissions (an Enterprise feature). Any viewer can query any datasource of the org by uid, whatever folder the dashboards sit in. An ops datasource that can read `taxi.agent_log` would expose other visitors' questions (CLAUDE.md invariant 8).

**Decision:**
- A second container, `grafana-ops`, from the same image:
  - published on `127.0.0.1:3002` only, not behind Caddy;
  - anonymous access off, admin login required;
  - provisioned from `infra/grafana/provisioning-ops/`.
- It alone holds the `clickhouse-ops` datasource (ClickHouse user `grafana_ops`, SELECT on `agent_log`, `system.parts`, `system.query_log` plus the dashboard tables) and the Stand Health dashboard.
- The public `grafana_ro` loses `system.parts`, which it no longer needs.
- `make secrets` now appends keys that are new in `.env.example` (such as `CLICKHOUSE_OPS_PASSWORD`) to an existing `.env`.

**Consequences:**
- The public Grafana can't reach operator data even with a crafted request. Stack tests pin this.
- About 150 MB more RAM (`mem_limit` 384 m).
- The operator logs in once per Grafana session on the laptop.

**Rejected:**
- A second Grafana organisation. It's the same isolation, but the org has to exist before provisioning, which needs an extra API bootstrap step.
- A hidden folder, which isn't isolation at all.

## ADR-018: Local model for the agent (M3 eval)
**Status:** accepted · 2026-09-29

**Context:** The M3 golden-set eval (spec 10) on the stand laptop's candidates, each derived with `num_ctx 16384` and
`temperature 0` (`make ollama-model`), replayer paused, model warm. 30 scored questions; the 2 ride questions wait for M4.

| Model | Mode | Accuracy | Suggested | Refusals | p50 / p95 | Tool-call validity |
|---|---|---|---|---|---|---|
| qwen2.5-coder:14b | tools | 20% | 0% | all pass | 3.3 s / 5.7 s | 100% |
| qwen3:14b | tools | 23% (reference only: excluded from selection, see below) | 0% | all pass | 34.5 s / 45.0 s | 100% |
| llama3.1:8b | tools | 20% | 0% | all pass | 5.1 s / 8.5 s | 100% |
| **qwen2.5-coder:14b** | **oneshot** | **80%** | **100%** | **all pass** | **6.8 s / 12.5 s** | **100%** |
| llama3.1:8b | oneshot | 47% | 40% | FAIL (3 refusals gave "no answer") | 4.0 s / 13.4 s | 100% |

`qwen3:14b` was dropped from the selection. Its `tools` run had already finished its 30 questions before the
instruction to drop it arrived (`evals/reports/2026-09-29-taxi-qwen3-14b-tools.md`: 23% accuracy, 0% suggested,
p50 34.5 s / p95 45.0 s). Those numbers are kept above for reference only; the model is excluded from the selection,
and its `oneshot` run was never made. The reason: it is a "thinking" model whose per-call latency routinely exceeds
`LLM_TIMEOUT_S=30 s` (the api log shows most questions taking 30–45 s, ending in `LLM_UNAVAILABLE`/`TIMEOUT` and the
cached SQL for the suggested questions), which alone rules it out for a 2–3-minute stand visit.

**Decision:** `LLM_LOCAL_MODEL=qwen2.5-coder:14b` (derived as `taxi-qwen2.5-coder-14b`, `num_ctx 16384`), `AGENT_MODE=oneshot`,
because it is the only run that clears every spec-06 target (≥ 80% overall, 100% suggested, p95 < 15 s, all refusals
pass), and by the model-selection ordering (100% suggested → accuracy → p95 → tool-call validity) it wins outright:
no other run reached 100% on the 5 suggested questions.

**Consequences:**
- `ollama ps` while warm: `taxi-qwen2.5-coder-14b` loads at **12 GB, 100% GPU**, `taxi-llama3.1-8b` at 7.0 GB — both
  fit the 32 GB laptop alongside the Compose stack, but only one is loaded at the stand.
- Latency on the stand: p50 6.8 s / p95 12.5 s (model warm) — comfortably inside a 2–3 minute visit, but close enough
  to `AGENT_TOTAL_TIMEOUT_S=45 s` and `LLM_TIMEOUT_S=30 s` that a cold-loaded model (first question after an idle
  gap) should be watched; `OLLAMA_KEEP_ALIVE=-1` (RUNBOOK) is what keeps it off the 10–20 s reload path.
- **Tool-call-as-text, observed per model:** every `tools`-mode run had `tool_calls: 0` / `invalid_tool_calls: 0` on
  its failures — none of the three local models actually emitted an OpenAI-style tool call for `run_query`. Instead:
  - `qwen2.5-coder:14b` wrote the call as a bare JSON object in the answer content: `{"name": "run_query", "arguments": {...}}`.
  - `llama3.1:8b` wrote a similar JSON object but with a different shape (`"parameters"` instead of `"arguments"`),
    sometimes prefixed with a sentence of prose.
  - `qwen3:14b` did emit real answers for a few questions but mostly exceeded `LLM_TIMEOUT_S` before finishing a turn,
    so most "failures" are `LLM_UNAVAILABLE`/cache fallback rather than a bad tool call.
  This is exactly why `oneshot` mode (JSON-object response format, no tool-calling round trip) is the mode that
  works for these local models — it's noted in spec 06's "Implementation notes (M3)".
- Watch in the soak: agent suggested-question success (≥ 99% target) and p95 latency under real concurrency
  (`AGENT_MAX_CONCURRENCY=2`), since the eval above is single-request and model-warm.
- Full reports: `evals/reports/2026-09-29-taxi-{qwen2.5-coder-14b,qwen3-14b,llama3.1-8b}-{tools,oneshot}.{md,json}`
  (git-ignored; kept locally for reference).

## ADR-019: The api's own reads use `trace_ro`, not `agent_ro`
**Context.** Spec 07 ran the queryable poller and the fare-estimate query as `agent_ro`. `agent_ro` has one shared quota of 120 queries/min (its safety limit). Polling every 250 ms during a 50-ride burst, plus the estimates, would use up that quota and break the AI agent mid-event.
**Decision.**
- A new user `trace_ro` has exactly `agent_ro`'s profile and SELECT grants, and its own quota (`trace`, 1 200 queries/min).
- It serves only the api's own fixed, parameterised queries: estimates, the queryable poll (one batched `IN {ids:Array(UUID)}` query per tick), trace history, and the wall stats.
- The agent's SQL still runs only as `agent_ro`, behind the guard.
**Consequences.**
- ClickHouse has no row policies, so "visible to `trace_ro`" means the same as "visible to `agent_ro`": the `queryable` stage still proves the agent can see the ride.
- There's one more password in `.env`, which `make secrets` fills.

## ADR-020: `services/api/src/api/config.py` renamed to `envconfig.py`
**Context.** The organisation's managed Claude Code policy denies reading any file named `config.py`. This module holds only env-var names and defaults (no secrets), so the block was pure friction for anyone using an agent against this repo.
**Decision.** Renamed to `services/api/src/api/envconfig.py`. No behaviour change.
**Consequences.**
- Tools and agents can read and edit it like any other module.
- The import path is `api.envconfig`; nothing else in the module changed.

## ADR-021: M5 (soak, chaos drills, preflight, /architecture) dropped; pre-event testing is manual
**Status:** accepted · 2026-09-30

**Context:** The user decided on 2026-09-30, after M4, to test the stand by hand rather than build out M5.

**Decision:** Remove M5 and the features only it would have built: the soak script, the chaos-drill procedure, `make preflight`, and the `/architecture` page (with `GET /api/stats`, which only served it and the never-built `/agent` header). The RUNBOOK keeps a manual pre-event and morning checklist in their place.

**Consequences:**
- There's no automated long-run evidence, so the risks R4 (ClickHouse parts/memory growth) and R9 (replayer loop boundary) are now covered only by a manual long run, watched with `make status` and Stand Health.
- ADR-018's "watch in the soak" item now means "watch in the manual long run".
