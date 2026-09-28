# CLAUDE.md — NYC Taxi Live (data-engineering demo stand)

A live streaming-data demo for a company stand at a university event. NYC TLC taxi trips are replayed as a live stream through **Redpanda → Quix Streams → ClickHouse**. Visitors see the results three ways: a **Grafana** dashboard, a **transparent text-to-SQL AI agent** that always shows its SQL, and **"trace your ride"**, where a student creates a ride from their phone and watches it move through every stage.

It runs on **one macOS laptop** (32 GB), fully offline except the optional internal AI gateway. Visitors get two to three minutes, so everything must work the first time, every time.

## Read before changing anything
- `docs/IMPLEMENTATION_PLAN.md`: milestones, cut lines, and what's in scope right now
- `docs/ARCHITECTURE.md`: components, the sim-time model, ports, resource budget
- `docs/specs/NN-*.md`: one spec per component. **The spec is the source of truth.**
- `contracts/events/*.schema.json`: message contracts (JSON Schema 2020-12)
- `docs/DECISIONS.md`: why things are the way they are. Don't silently reverse a decision.
- `docs/DIAGRAMS.md`: all Mermaid diagrams. It's the source of truth, and the specs embed copies via `scripts/sync_diagrams.py`.

## Repo map
```
contracts/events/      JSON Schemas for every Kafka message (authoritative)
evals/                 agent golden questions (make eval)
docs/                  plan, architecture, decisions, runbook, demo script, specs
services/replayer/     Python: Parquet → trips.raw at N× speed, sim.clock, control API
services/processor/    Python/Quix Streams: validate, enrich, window, ClickHouse sink
services/api/          Python/FastAPI: agent (SSE), rides, trace relay (WS), zones
web/                   Next.js + TypeScript: /agent /ride /trace/[id] /trace/live /architecture /qr
infra/clickhouse/      DDL (init/*.sql), users.d/*.xml, zone loader
infra/grafana/         provisioning/ datasources + dashboards JSON
infra/caddy/           Caddyfile (single entry point on :80)
infra/redpanda/        topic bootstrap script
scripts/               data prep, preflight, soak, burst test
data/                  git-ignored: raw downloads, prepared parquet, zones.csv, state/
```
Nothing outside `docs/`, `contracts/` and `evals/` exists yet. Build it by following the specs, in the milestone order in the plan.

## Commands (Makefile targets to implement at M0)
| Command | What it does |
|---|---|
| `make data` | Download TLC month + zone files, prepare sorted replay parquet + `zones.csv` (spec 01) |
| `make up` / `make down` | Start or stop the Compose stack (Ollama runs natively, not in Compose) |
| `make backfill UNTIL=2025-10-14T07:00:00` | Replay at max speed through the real pipeline up to UNTIL |
| `make preflight` | Stand-readiness checks (see RUNBOOK). Must be green before opening. |
| `make test` | Unit + integration tests (Python: pytest, web: vitest + Playwright) |
| `make eval [MODEL=…]` | Run `evals/agent_golden_questions.yaml` against the configured LLM |
| `make soak HOURS=10` | Long run with synthetic visitors |
| `make pause` / `make resume` / `make speed S=120` | Replayer control |
| `make logs S=processor` | Tail one service |
| `make status` | Containers, memory, processor lag, freshness, LLM status |
| `make secrets` / `make images` / `make migrate` | Fill empty passwords in `.env` / build + pull all images / re-apply ClickHouse DDL |
| `make kiosk` / `make llm ORDER=internal,local` | Open the stand browser windows / switch LLM provider order live |
| `make test-e2e` / `make burst` | End-to-end tests against the running stack / 50-ride burst test |
| `make reset-rides` / `make export-log` | Delete student rides / export `taxi.agent_log` to CSV |
| `make diagrams-sync` / `make docs-check` | Copy diagrams from `docs/DIAGRAMS.md` into specs / verify docs (Mermaid renders, contracts valid, copies in sync) |

## Stack & conventions
- **Python 3.12**, managed with `uv`, one `pyproject.toml` per service. Lint and format with `ruff`, type-check with `mypy --strict` on `services/*/src`. Tests use `pytest` (+ `testcontainers` for integration).
- **TypeScript strict**, Next.js App Router, `pnpm`, ESLint, Tailwind, Recharts. Tests: vitest + Playwright.
- Kafka client: `confluent-kafka` (replayer, api), Quix Streams (processor). ClickHouse client: `clickhouse-connect`.
- LLM client: the `openai` Python SDK only. Every provider is OpenAI-compatible, so providers differ by `base_url`/`api_key`/`model`.
- Pin exact image and package versions at M0 and record them in `docs/ARCHITECTURE.md#versions`. Never use `latest` tags.
- Config comes only from env vars (`.env.example` lists every key). No config constants hidden in code.
- Log structured JSON to stdout with `service`, `event`, `trip_id`/`trace_id` when relevant.

## Invariants (do not break)
1. **Contracts first.** To change a message: update the JSON Schema and the spec, then the code. Producers and consumers validate against `contracts/` in tests.
2. **Names are fixed.**
   - Topics: `trips.raw`, `trips.enriched`, `trips.rejected`, `agg.borough_minute`, `trace.checkpoints`, `sim.clock`.
   - Tables: `taxi.trips`, `taxi.trips_rejected`, `taxi.agg_borough_minute`, `taxi.zones`, `taxi.trace_events`, `taxi.agent_log`, and the view `taxi.sim_now`.
3. **Two clocks, never mixed.**
   - `*_ts` = **sim time**: naive NYC local, advances at REPLAY_SPEED×.
   - `*_at` = **wall time**: UTC.
   - "Now" and "today" in dashboards and agent SQL mean `taxi.sim_now`, never `now()`.
   - Latencies are computed only from `*_at`.
4. **The agent is read-only, and guardrails stay on.**
   - The agent connects only as `agent_ro`. Every SQL statement goes through the SQL guard.
   - Never add a bypass flag or a "debug" path that skips the guard. Never let the agent read `agent_log` or `system.*`.
5. **Every stage emits a trace checkpoint** for any record with a non-null `trace_id`. This covers exactly the stages in spec 07, in order.
6. **Offline-first.** The demo must fully work with only the local Ollama model. The internal AI gateway is optional.
7. **No secrets in git.** `.env` is ignored. Never log API keys or passwords.
8. **Visitor safety.** Never display one visitor's free text to other visitors, and collect no personal data.

## How to work here
- **TDD** for the processor validation and enrichment rules, the SQL guard, the replayer pacing (fake clock), and the fare estimator. These are pure functions, so keep them that way.
- After any change to the agent prompt, tools, or model, run `make eval` and paste the score in the PR/commit message. Don't merge a regression on the 5 `suggested: true` questions.
- Every spec ends with **Acceptance criteria**. A task is done when those pass, not when the code compiles.
- Prefer boring, explainable code. A student may look at it on the big screen.
- Frontend: load the `frontend-design` skill for new UI, and `dataviz` before building charts. Visual language: purple = core pipeline, green = visitor-facing (matches the architecture image).
- For the Claude-related details of the agent (tool-calling loop patterns), load the `claude-api` skill only if a provider switches to Anthropic. The default is OpenAI-compatible.
- When you're unsure whether something is in scope, check the cut lines in `docs/IMPLEMENTATION_PLAN.md`. Scope is cut in that order.
