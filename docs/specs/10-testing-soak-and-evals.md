# Spec 10: Testing and agent evals

## Purpose
Prove before the event that the stand works under visitor load, with bad input, and with a local LLM. "Works on my desk for five minutes" doesn't count.

## Responsibilities
- The test pyramid per service (unit → integration → end-to-end).
- The agent eval harness (`make eval`) and the golden set (`evals/agent_golden_questions.yaml`).
- The burst test.
- Reports saved under `evals/reports/`, git-ignored except for the final pre-event report.

## Interfaces
| Command | What |
|---|---|
| `make test` | All unit + integration tests: pytest per service, contract tests, vitest, Playwright with a mocked API |
| `make test-e2e` | Against the running stack: ride → trace, agent → answer, through Caddy |
| `make eval [MODEL=…] [MODE=tools\|oneshot] [PROVIDER=local\|internal]` | Golden-set evaluation |
| `make burst` | 50 rides in 10 s from 10 fake IPs |

## Config
The same `.env` as the stack. The eval harness talks to the **running api** (`http://127.0.0.1:8000`) and to ClickHouse as `agent_ro`, and uses the replayer control API.

## Behaviour

### Test pyramid
| Layer | Replayer | Processor | API / agent | Web |
|---|---|---|---|---|
| Unit (pure) | pacing with a fake clock, loop offset, trip_id, sampling | every validation rule, enrichment, reducer | SQL guard (attack list + must-pass), fare estimator, SSE encoder, rate limiter, provider failover state machine | SSE parser, chart spec → Recharts props, formatters |
| Contract | produced messages vs `trip.schema.json` | output messages vs schemas | rides message, checkpoints vs schemas | generated types compile |
| Integration (testcontainers: Redpanda + ClickHouse with the real init SQL and users; the processor runs as a subprocess) | restart → deduped | 1 000 fixture trips → exact counts, checkpoint order, ClickHouse outage | agent loop with a **scripted fake LLM** (an OpenAI-compatible stub server) → exact SSE sequence; rides → WS stages | n/a |
| E2E (running stack) | n/a | n/a | ride through Caddy, all 7 stages < 5 s; agent answer with the real model | Playwright on phone viewports against the stack |

The fake LLM is a tiny FastAPI app that returns scripted `chat.completions` responses (tool calls, then a final answer). Use it for deterministic agent-loop tests, and in CI where there's no model.

### Agent eval harness (`scripts/eval_agent.py`)
1. **Freeze time:** `POST /pause` on the replayer, so `taxi.sim_now` stays fixed and the reference SQL and agent SQL see the same data. Resume at the end, even on failure.
2. **Ride-context setup:** create 2 rides through `POST /api/rides` (one normal card ride, one cash ride) and wait for `queryable`. Substitute their trace ids for `{trace_id_card}` and `{trace_id_cash}` in the questions and reference SQL.
3. For each entry: call `POST /api/agent/ask` and collect all SSE events. Take the agent's rows from the **last `rows` event**, and run `reference_sql` as `agent_ro`.
4. **Check types** (`check.type`):

   | Type | Passes when |
   |---|---|
   | `top1` | the first value of the reference's first row appears in the agent's first row (any column; strings case-insensitive, numbers within tolerance) |
   | `top_k_set` | the set of first-column values of the reference's top `k` rows equals the set of values in some column of the agent's top `k` rows |
   | `scalar` | the first row of the agent's result contains a number within `tolerance` (relative, default 0.02) of each reference `value_columns` value in row 0 |
   | `contains_value` | for every reference row, the values of `value_columns` (`match: all` or `any`) appear somewhere in the agent's result, within tolerance |
   | `mapping` | for every reference row, the agent has a row containing the reference `key_column` value and a number within tolerance of its `value_columns` |
   | `answer_only` | only `answer_mentions` is checked (for knowledge that lives in column comments) |
   | `refusal` | an `answer` is present and **no `rows` event** was emitted. A query the guard or the DB user blocked still passes |

   `pct: true` accepts the value either as a percentage or as a fraction (0–1).

   Optional `answer_mentions: [..]`: the answer text must contain at least one of these (case-insensitive). It's used for caveats such as "card".
5. **Metrics per question:** pass/fail, latency (first event, total), tool calls, repairs, provider, and mode.
6. **Report:** `evals/reports/<YYYY-MM-DD>-<model>-<mode>.md` + `.json`, with overall accuracy, accuracy for `suggested: true`, p50/p95 latency, tool-call validity, and a table of failures with the agent's SQL next to the reference SQL.
7. **Targets** (spec 06 acceptance): overall ≥ 80%, **suggested = 100%**, p95 < 15 s (model warm), all `refusal` cases pass.

### Burst test (`scripts/burst_rides.py`)
- Send 50 `POST /api/rides` in 10 s straight to `api:8000`, with `X-Forwarded-For` spread over 10 fake IPs and 10% absurd tips.
- **Expect:** 0 × 5xx, 429s only from the per-IP limit, all traces reaching a terminal stage within 15 s, and p95 `POST` latency < 300 ms.

## Failure modes
| Failure | Behaviour |
|---|---|
| Eval on moving data | Prevented by the pause step. If it's skipped, the results are invalid, so the harness refuses to run unless `paused=true` |
| Model not warm | The harness sends one warm-up request that isn't scored |

## Observability
Reports are Markdown so they can be pasted into PRs.

## Acceptance criteria
1. `make test` passes on a clean clone with Docker available, needing only Docker and uv/pnpm.
2. `make eval` produces a report, and meets the targets with the chosen model (spec 06).
3. The burst test passes.

## Out of scope
Load testing beyond about 100 concurrent visitors, security penetration testing (the guardrails are tested in unit tests), and cross-browser testing beyond iOS Safari, Android Chrome, and desktop Chrome.
