# NYC Taxi Live 🚕

A live data-engineering pipeline you can talk to, built for our stand at the university event.

It replays real NYC taxi trips as a live stream, processes them in real time, and lets visitors:
- **watch** the city's taxi traffic on a live Grafana dashboard,
- **ask** an AI data agent questions in plain English, and see the exact SQL it writes,
- **trace their own ride**: scan a QR code, book a fake taxi, and watch it travel through every stage of the pipeline.

```
NYC TLC Parquet ─► replayer ─► Redpanda ─► Quix Streams ─► ClickHouse ─┬─► Grafana dashboard
                                   ▲        (clean, enrich,            ├─► AI data agent (text-to-SQL)
      student phone (QR) ─► API ───┘         aggregate)                └─► trace viewer
```
See [docs/DIAGRAMS.md](docs/DIAGRAMS.md) for the full set of diagrams.

## Quickstart (once implemented)
```bash
cp .env.example .env            # fill in passwords; LLM_INTERNAL_* optional
brew install ollama && ollama pull qwen2.5-coder:14b && ollama serve   # native, for the Metal GPU
make data                       # download + prepare one month of TLC data
make up                         # start the stack
make backfill UNTIL=2025-10-14T07:00:00   # optional: pre-load history
make preflight                  # everything green? open the stand
```
The stand entry point is `http://<laptop-ip>/`. Key pages are `/agent`, `/ride`, `/trace/live`, `/grafana`, and `/architecture`.

## Documentation
| Doc | Purpose |
|---|---|
| [CLAUDE.md](CLAUDE.md) | Conventions and invariants for anyone (human or Claude) working in the repo |
| [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) | Milestones, day-by-day plan, cut lines, risks |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, data flow, sim-time model, ports, budget |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Architecture decision records |
| [docs/DIAGRAMS.md](docs/DIAGRAMS.md) | All diagrams (Mermaid) |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operating the stand: setup, preflight, recovery |
| [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md) | What to say at the stand |
| [docs/specs/](docs/specs/) | One spec per component (01–10) |
| [contracts/events/](contracts/events/) | JSON Schemas for every stream message |
| [evals/](evals/) | Golden questions for the AI agent |

## Data
NYC Taxi & Limousine Commission [Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), yellow taxi, one month. The data is public and contains no personal information.
