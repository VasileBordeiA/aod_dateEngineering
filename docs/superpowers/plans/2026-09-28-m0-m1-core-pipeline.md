# M0 + M1 Core Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real NYC taxi trips stream from a Parquet file through Redpanda and a Quix Streams processor into ClickHouse (validated, enriched, and aggregated per borough-minute) on one `make up`, with the M1 exit check green.

**Architecture:**
- A one-off data-prep script (DuckDB + geopandas) produces a pickup-sorted replay file and a zones table.
- A Python **replayer** publishes trips paced by a simulated clock and persists its position.
- A Quix Streams **processor** has two input dataframes:
  - `trips.raw` goes through validate → enrich → sinks.
  - `trips.enriched` goes through group_by borough → 1-min tumbling window → sink.
  - Both emit trace checkpoints.
- Everything runs in Docker Compose. ClickHouse users and limits come from `users.d` XML.

**Tech Stack:** Python 3.12 (uv), quixstreams 3.26.0, confluent-kafka 2.15.1, clickhouse-connect 1.9.0, pyarrow 25.0.1, duckdb 1.5.5, geopandas 1.1.4, Redpanda v26.2.3, ClickHouse 26.3.34 (LTS), Grafana OSS 12.4.3 + grafana-clickhouse-datasource 4.21.3, Caddy 2.11.4, pytest 9.1.1, testcontainers 4.15.0, chdb 4.4.0.

**Spec:**
- `docs/specs/01-data-and-replayer.md`, `02-topics-and-event-contracts.md`, `03-stream-processor.md`, `04-clickhouse-schema.md`, `09-infrastructure-and-networking.md`
- `docs/ARCHITECTURE.md`, `CLAUDE.md`, `contracts/events/*.schema.json`

Executors read the specs alongside this plan.

## Global Constraints
- Python `>=3.12,<3.13` for every project. Dependencies are pinned with `==` and locked by `uv.lock`. No `latest` image tags.
- Topic names are exactly `trips.raw`, `trips.enriched`, `trips.rejected`, `agg.borough_minute`, `trace.checkpoints`, `sim.clock`.
- Tables are exactly `taxi.trips`, `taxi.trips_rejected`, `taxi.agg_borough_minute`, `taxi.zones`, `taxi.trace_events`, `taxi.agent_log`, and the view `taxi.sim_now`.
- `*_ts` fields are **sim time**: naive ISO `YYYY-MM-DDTHH:MM:SS[.fff]`, never converted. `*_at` fields are **wall time**: UTC `YYYY-MM-DDTHH:MM:SS.mmmZ`.
- Every Kafka message validates against `contracts/events/<name>.schema.json`. Tests use `contracts/contract_validator.py`.
- Config comes only from env vars listed in `.env.example`. Logs are JSON lines on stdout with `service` and `event`.
- Replay speed defaults to 60. Loop offset is 35 days. Trace sample rate is 0.001. The clock heartbeat is 1 s, and state is saved every 5 s.
- Validation reason codes are exactly the 11 in `rejected_trip.schema.json`. `TIP_UNREALISTIC` = `tip > max(PROCESSOR_TIP_MAX_ABS=100, 3 × fare)`.
- The window is a 1-minute tumbling window, keyed by `pu_borough`, with `PROCESSOR_WINDOW_GRACE_S=180` (sim seconds). The Quix commit interval is 1 s.
- Only Caddy binds `0.0.0.0` (port 80). Every other published port binds `127.0.0.1`.
- Spec deviation (decided here, docs updated in Task 10): the aggregation reads `trips.enriched` as a second StreamingDataFrame instead of branching `group_by` off the validated stream. `avg_speed_mph` is rounded to 2 decimals.

## Review Focus
1. **Host timezone ≠ UTC** (the dev Mac is in Europe/Bucharest). Stored `pickup_ts` must equal the message value exactly. Task 8 `test_rows.py::test_sim_timestamps_are_utc_aware_and_unshifted` pins this, and Task 9 integration asserts the stored value.
2. **Garbage on `trips.raw`** (non-JSON, a JSON array, missing fields). The record goes to `trips.rejected` as `SCHEMA_INVALID` and the processor keeps running. Pinned by Task 8 `test_rules.py::test_annotate_*` and Task 9 integration (`b"not json"`).
3. **ClickHouse unreachable at startup or mid-run.** The sink raises `SinkBackpressureError`, never drops the batch, and never emits `stored` early. Pinned by Task 9 `test_sinks.py::test_connection_error_becomes_backpressure`.
4. **Replayer restarted** (crash, `make down && make up`). It resumes at the saved `row_idx`/`sim_ts` with no jump and no gap. Pinned by Task 6 `test_backfill_stops_at_until_and_live_resumes_there` and Task 5 `test_save_is_atomic`.
5. **Process frozen or laptop asleep** for more than 10 s. No burst of backlog rows is replayed. Pinned by Task 5 `test_clock.py::test_wall_gap_is_not_replayed`.

---

## File map

```
.gitignore  pyproject.toml  uv.lock  Makefile  docker-compose.yml
contracts/contract_validator.py            shared JSON-Schema validator (tests of every service)
contracts/tests/test_examples.py
scripts/fill_secrets.py                    make secrets
scripts/prepare_data.py                    make data
scripts/smoke_pipeline.py                  make smoke (M1 exit check)
scripts/check_mermaid.sh                   make docs-check
scripts/tests/test_fill_secrets.py  scripts/tests/test_prepare_data.py
infra/redpanda/create-topics.sh
infra/clickhouse/init/{00_database,10_zones,20_trips,21_sim_now,30_trips_rejected,40_agg_borough_minute,50_trace_events,60_agent_log}.sql
infra/clickhouse/init/90_load_zones.sh
infra/clickhouse/config.d/stand.xml  infra/clickhouse/users.d/stand.xml
infra/tests/test_clickhouse_ddl.py         chdb, no Docker
infra/tests/test_stack.py                  marker `stack`: against the running compose stack
infra/grafana/Dockerfile  infra/grafana/provisioning/datasources/clickhouse.yaml
infra/caddy/Caddyfile
services/replayer/{pyproject.toml,uv.lock,Dockerfile,.dockerignore}
services/replayer/src/replayer/{__init__,config,clock,messages,state,source,engine,control,producer,log,health,main}.py
services/replayer/tests/{conftest,test_config,test_clock,test_messages,test_state,test_source,test_engine,test_control,test_integration}.py
services/processor/{pyproject.toml,uv.lock,Dockerfile,.dockerignore}
services/processor/src/processor/{__init__,config,times,rules,zones,enrich,aggregate,checkpoints,rows,sinks,log,health,app}.py
services/processor/tests/{conftest,test_config,test_times,test_rules,test_zones,test_enrich,test_aggregate,test_checkpoints,test_rows,test_sinks,test_integration}.py
services/processor/tests/fixtures/zones.csv
```
`processor/rules.py` and `processor/times.py` are **stdlib-only**, because `scripts/prepare_data.py` imports them through `sys.path` to compute the data-quality report with the exact same rules.

---

### Task 1: Repository bootstrap, contract validator, secrets, Makefile

**Files:**
- Create: `.gitignore`, `pyproject.toml`, `Makefile`, `contracts/contract_validator.py`, `contracts/tests/test_examples.py`, `scripts/fill_secrets.py`, `scripts/tests/test_fill_secrets.py`, `scripts/check_mermaid.sh`
- Modify: `.env.example` (add `PROCESSOR_STATE_DIR`)

**Interfaces:**
- Produces: `contract_validator.assert_valid(name: str, message: dict) -> None` (raises `AssertionError` listing every violation), `contract_validator.errors(name, message) -> list[str]`, and `contract_validator.schema(name) -> dict`. `name` is the file stem without `.schema.json`: `trip`, `enriched_trip`, `rejected_trip`, `borough_minute_agg`, `trace_checkpoint`, `sim_clock`.
- Produces: `make` targets `help secrets data up down status logs test test-integration test-stack smoke backfill pause resume speed migrate images reset lint diagrams-sync docs-check`. Later tasks' services are wired in by those tasks.

- [ ] **Step 1: Initialise git and commit the planning package on `main`, then branch**

```bash
cd /Users/vasilebordei/Desktop/learning/aod_dataEngineering
git init -b main
cat > .gitignore <<'EOF'
.env
data/
exports/
evals/reports/
scripts/soak_reports/
.venv/
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
.mypy_cache/
node_modules/
.DS_Store
EOF
git add -A && git commit -m "docs: planning package (specs, contracts, evals, diagrams)"
git switch -c feat/m0-m1-core-pipeline
```

- [ ] **Step 2: Root tooling project**

`pyproject.toml`:
```toml
[project]
name = "nyctaxi-tools"
version = "0.1.0"
description = "Repo-level scripts and tests (data prep, contracts, infra checks)"
requires-python = ">=3.12,<3.13"
dependencies = [
    "duckdb==1.5.5",
    "pyarrow==25.0.1",
    "geopandas==1.1.4",
    "jsonschema==4.26.0",
    "referencing==0.37.0",
    "pyyaml==6.0.3",
    "clickhouse-connect==1.9.0",
    "confluent-kafka==2.15.1",
]

[dependency-groups]
dev = ["pytest==9.1.1", "ruff==0.16.9", "chdb==4.4.0"]

[tool.uv]
package = false

[tool.pytest.ini_options]
testpaths = ["contracts/tests", "scripts/tests", "infra/tests"]
markers = ["stack: needs the running compose stack (make up)"]
addopts = "-m 'not stack'"

[tool.ruff]
line-length = 110
target-version = "py312"
```
Run: `uv lock && uv sync`. Expected: `.venv` is created with Python 3.12.

- [ ] **Step 3: Write the failing contract tests**

`contracts/tests/test_examples.py`:
```python
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import contract_validator as cv  # noqa: E402

EXPECTED = {"trip", "enriched_trip", "rejected_trip", "borough_minute_agg", "trace_checkpoint", "sim_clock"}


def test_expected_schema_set():
    assert set(cv.schema_names()) == EXPECTED


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_schema_is_valid_2020_12(name):
    Draft202012Validator.check_schema(cv.schema(name))


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_examples_validate(name):
    examples = cv.schema(name).get("examples", [])
    assert examples, f"{name} has no examples"
    for example in examples:
        cv.assert_valid(name, example)


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_unknown_field_is_rejected(name):
    example = dict(cv.schema(name)["examples"][0])
    example["__unexpected"] = 1
    assert cv.errors(name, example)


def test_assert_valid_lists_the_problem():
    bad = dict(cv.schema("trip")["examples"][0], source="taxi")
    with pytest.raises(AssertionError, match="source"):
        cv.assert_valid("trip", bad)
```
Run: `uv run pytest contracts/tests -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'contract_validator'`.

- [ ] **Step 4: Implement the validator**

`contracts/contract_validator.py`:
```python
"""Validate messages against contracts/events/*.schema.json. Shared by the tests of every service."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

EVENTS_DIR = Path(__file__).resolve().parent / "events"


@lru_cache(maxsize=1)
def _schemas() -> dict[str, dict[str, Any]]:
    return {
        p.name.removesuffix(".schema.json"): json.loads(p.read_text())
        for p in sorted(EVENTS_DIR.glob("*.schema.json"))
    }


@lru_cache(maxsize=1)
def _registry() -> Registry:
    return Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in _schemas().values())


def schema_names() -> list[str]:
    return list(_schemas())


def schema(name: str) -> dict[str, Any]:
    return _schemas()[name]


def errors(name: str, message: Any) -> list[str]:
    validator = Draft202012Validator(_schemas()[name], registry=_registry(), format_checker=FormatChecker())
    return [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in validator.iter_errors(message)]


def assert_valid(name: str, message: Any) -> None:
    problems = errors(name, message)
    if problems:
        raise AssertionError(f"{name} contract violated:\n  " + "\n  ".join(problems))
```
Run: `uv run pytest contracts/tests -q`. Expected: `20 passed`.

- [ ] **Step 5: Write the failing secrets test**

`scripts/tests/test_fill_secrets.py`:
```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fill_secrets import fill  # noqa: E402

TEMPLATE = """ADMIN_TOKEN=
CLICKHOUSE_ADMIN_PASSWORD=
CLICKHOUSE_AGENT_PASSWORD=keepme
GRAFANA_ADMIN_PASSWORD=
LLM_INTERNAL_API_KEY=
STAND_WIFI_PASSWORD=
REPLAY_SPEED=60
"""


def test_fills_only_empty_generated_secrets():
    text, filled = fill(TEMPLATE)
    values = dict(line.split("=", 1) for line in text.strip().splitlines())
    assert filled == ["ADMIN_TOKEN", "CLICKHOUSE_ADMIN_PASSWORD", "GRAFANA_ADMIN_PASSWORD"]
    assert len(values["ADMIN_TOKEN"]) == 32 and len(values["CLICKHOUSE_ADMIN_PASSWORD"]) == 32
    assert values["CLICKHOUSE_AGENT_PASSWORD"] == "keepme"
    assert values["LLM_INTERNAL_API_KEY"] == "" and values["STAND_WIFI_PASSWORD"] == ""
    assert values["REPLAY_SPEED"] == "60"


def test_is_idempotent():
    once, _ = fill(TEMPLATE)
    twice, filled = fill(once)
    assert twice == once and filled == []
```
Run: `uv run pytest scripts/tests/test_fill_secrets.py -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'fill_secrets'`.

- [ ] **Step 6: Implement `scripts/fill_secrets.py`**

```python
"""Fill empty generated secrets in .env (make secrets). Never overwrites a value that is already set.

User-provided secrets (LLM_INTERNAL_API_KEY, STAND_WIFI_PASSWORD) are left alone.
"""

from __future__ import annotations

import re
import secrets
import sys
from pathlib import Path

GENERATED = re.compile(r"^(CLICKHOUSE_[A-Z]+_PASSWORD|GRAFANA_ADMIN_PASSWORD|ADMIN_TOKEN)=\s*$")


def fill(text: str) -> tuple[str, list[str]]:
    lines, filled = [], []
    for line in text.splitlines():
        match = GENERATED.match(line)
        if match:
            line = f"{match.group(1)}={secrets.token_hex(16)}"
            filled.append(match.group(1))
        lines.append(line)
    return "\n".join(lines) + "\n", filled


def main(path: str = ".env") -> int:
    env_file = Path(path)
    text, filled = fill(env_file.read_text())
    env_file.write_text(text)
    print("generated:", ", ".join(filled) if filled else "nothing (all set)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
```
Run: `uv run pytest scripts/tests/test_fill_secrets.py -q`. Expected: `2 passed`.

- [ ] **Step 7: Add `PROCESSOR_STATE_DIR` to `.env.example`**

In `.env.example`, after the line `PROCESSOR_ZONES_PATH=/data/zones.csv`, add:
```
# Quix state (RocksDB windows); a Docker volume in compose
PROCESSOR_STATE_DIR=/state
```

- [ ] **Step 8: Makefile**

Recipe lines must be indented with a **tab**.
```make
SHELL := /bin/bash
.DEFAULT_GOAL := help
-include .env
export
COMPOSE := docker compose
CTRL := http://127.0.0.1:8001

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

secrets: ## Create .env from the template (if missing) and fill empty generated secrets
	@test -f .env || cp .env.example .env
	@uv run --no-project python scripts/fill_secrets.py .env

data: ## Download + prepare one TLC month (spec 01)
	uv run python scripts/prepare_data.py

up: ## Start the stack (Ollama runs natively, not here)
	@test -f .env || (echo "run: make secrets" && exit 1)
	@test -f data/zones.csv || (echo "run: make data" && exit 1)
	$(COMPOSE) up -d --build --wait
	@pgrep -x caffeinate >/dev/null || (nohup caffeinate -dimsu >/dev/null 2>&1 &)
	@$(COMPOSE) ps

down: ## Stop the stack (volumes are kept)
	$(COMPOSE) down
	-@pkill -x caffeinate || true

reset: ## DANGER: stop the stack and delete ALL volumes (data, offsets, replay position)
	@read -p "Delete all volumes? [y/N] " a && [ "$$a" = "y" ]
	$(COMPOSE) down -v

status: ## Containers, memory, processor lag, replayer status
	@$(COMPOSE) ps
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
	@$(COMPOSE) exec -T redpanda rpk group describe $(PROCESSOR_CONSUMER_GROUP) 2>/dev/null | head -8 || true
	@curl -fsS $(CTRL)/status 2>/dev/null && echo || echo "replayer control not reachable"

logs: ## Tail one service: make logs S=processor
	$(COMPOSE) logs -f --tail=100 $(S)

images: ## Build and pull every image (do this at home, with internet)
	$(COMPOSE) build && $(COMPOSE) pull --ignore-buildable

test: ## Unit + contract tests (no Docker needed)
	uv run pytest -q
	cd services/replayer && uv run pytest -q
	cd services/processor && uv run pytest -q

test-integration: ## testcontainers integration tests (needs Docker running)
	cd services/replayer && uv run pytest -q -m integration
	cd services/processor && uv run pytest -q -m integration

test-stack: ## Checks against the running stack (after make up)
	uv run pytest -q -m stack

lint: ## ruff + mypy
	uv run ruff check scripts contracts infra
	cd services/replayer && uv run ruff check src tests && uv run mypy src
	cd services/processor && uv run ruff check src tests && uv run mypy src

smoke: ## M1 exit check: rows flowing, rejects with reasons, freshness
	uv run python scripts/smoke_pipeline.py

backfill: ## Replay at max speed through the real pipeline: make backfill UNTIL=2025-10-14T07:00:00
	@test -n "$(UNTIL)" || (echo "usage: make backfill UNTIL=2025-10-14T07:00:00" && exit 1)
	$(COMPOSE) stop replayer
	$(COMPOSE) run --rm --no-deps -e REPLAY_SPEED=max -e REPLAY_UNTIL_TS=$(UNTIL) replayer
	@echo "waiting for processor lag to reach 0…"
	@until [ "$$($(COMPOSE) exec -T redpanda rpk group describe $(PROCESSOR_CONSUMER_GROUP) | awk '/^TOTAL-LAG/{print $$2}')" = "0" ]; do sleep 5; done
	$(COMPOSE) start replayer

pause: ## Pause the replayer (sim clock freezes)
	curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" $(CTRL)/pause && echo
resume: ## Resume the replayer
	curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" $(CTRL)/resume && echo
speed: ## Change replay speed: make speed S=120
	curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" -d '{"speed": $(S)}' $(CTRL)/speed && echo

migrate: ## Re-apply the idempotent ClickHouse DDL
	for f in infra/clickhouse/init/*.sql; do echo "$$f"; $(COMPOSE) exec -T clickhouse clickhouse-client --user admin --password "$(CLICKHOUSE_ADMIN_PASSWORD)" --multiquery < $$f || exit 1; done

diagrams-sync: ## Copy diagrams from docs/DIAGRAMS.md into the specs
	python3 scripts/sync_diagrams.py

docs-check: ## Diagrams in sync + all Mermaid renders + contracts valid
	python3 scripts/sync_diagrams.py --check
	bash scripts/check_mermaid.sh
	uv run pytest -q contracts/tests

.PHONY: help secrets data up down reset status logs images test test-integration test-stack lint smoke backfill pause resume speed migrate diagrams-sync docs-check
```

- [ ] **Step 9: Mermaid checker used by `make docs-check`**

`scripts/check_mermaid.sh`:
```bash
#!/bin/bash
# Render every ```mermaid block in the repo's Markdown; exit 1 if any fails (make docs-check).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$(mktemp -d)"
python3 - "$ROOT" "$OUT" <<'PY'
import os, re, sys, glob
root, out = sys.argv[1], sys.argv[2]
for f in glob.glob(f"{root}/**/*.md", recursive=True):
    if "/node_modules/" in f: continue
    for i, block in enumerate(re.findall(r"```mermaid\n(.*?)```", open(f).read(), re.S)):
        name = os.path.relpath(f, root).replace("/", "_")[:-3]
        open(f"{out}/{name}_{i + 1:02d}.mmd", "w").write(block)
PY
fail=0
for f in "$OUT"/*.mmd; do
  if npx -y -p @mermaid-js/mermaid-cli mmdc -q -i "$f" -o "${f%.mmd}.svg" >/dev/null 2>&1; then echo "ok   $(basename "$f")"
  else echo "FAIL $(basename "$f")"; fail=1; fi
done
exit $fail
```
Run: `chmod +x scripts/check_mermaid.sh && make docs-check`. Expected: every line reads `ok`, and the contract tests pass.

- [ ] **Step 10: Run everything and commit**

Run: `make secrets && uv run pytest -q`. Expected: `.env` is created, 6 values are generated (`CLICKHOUSE_{ADMIN,INGEST,AGENT,GRAFANA}_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, `ADMIN_TOKEN`), and `22 passed`.
```bash
git add .gitignore pyproject.toml uv.lock Makefile contracts/contract_validator.py contracts/tests scripts/fill_secrets.py scripts/tests/test_fill_secrets.py scripts/check_mermaid.sh .env.example
git commit -m "chore: repo bootstrap, contract validator, secrets, Makefile"
```

---

### Task 2: Data preparation (`make data`)

**Files:**
- Create: `scripts/prepare_data.py`, `scripts/tests/test_prepare_data.py`
- Depends on: `services/processor/src/processor/rules.py` + `times.py` (Task 8). **Do Task 8 Steps 1–6 first if executing strictly in order**, or temporarily run only the non-report tests. The recommended execution order is Task 1 → Task 8 (pure modules) → Task 2.

**Interfaces:**
- Consumes: `processor.rules.check_trip(trip: Mapping, tip_max_abs: float = 100.0) -> list[str]` (Task 8).
- Produces files:
  - `data/replay/trips_<month>.parquet` with columns `row_idx, vendor_id, pickup_ts (timestamp[us]), dropoff_ts, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee, total_amount`
  - `data/zones.csv` (`location_id,borough,zone,service_zone,lat,lon`)
  - `data/taxi_zones.geojson`
  - `data/replay/prep_report.json`
- Produces functions: `month_bounds(month) -> (str, str)`, `select_list(source_columns) -> str`, `prepare_replay(raw, out, month) -> int`, `count_reasons(rows, tip_max_abs=100.0) -> (Counter, dict[str, Counter])`, `zones_frames(lookup_df, shapes_gdf) -> (DataFrame, GeoDataFrame)`.

- [ ] **Step 1: Write the failing tests**

`scripts/tests/test_prepare_data.py`:
```python
import sys
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.geometry import box

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import prepare_data as prep  # noqa: E402


def tlc_table(pickups, *, with_cbd: bool = False) -> pa.Table:
    n = len(pickups)
    cols = {
        "VendorID": pa.array([2] * n, pa.int32()),
        "tpep_pickup_datetime": pa.array(pickups, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([p.replace(minute=59) for p in pickups], pa.timestamp("us")),
        "passenger_count": pa.array([1.0] * n),
        "trip_distance": pa.array([2.5] * n),
        "RatecodeID": pa.array([1.0] * n),
        "store_and_fwd_flag": pa.array(["N"] * n),
        "PULocationID": pa.array([161] * n, pa.int32()),
        "DOLocationID": pa.array([237] * n, pa.int32()),
        "payment_type": pa.array([1] * n, pa.int64()),
        "fare_amount": pa.array([15.0] * n),
        "extra": pa.array([0.0] * n),
        "mta_tax": pa.array([0.5] * n),
        "tip_amount": pa.array([3.0] * n),
        "tolls_amount": pa.array([0.0] * n),
        "improvement_surcharge": pa.array([1.0] * n),
        "total_amount": pa.array([22.0] * n),
        "congestion_surcharge": pa.array([2.5] * n),
        "Airport_fee": pa.array([1.75] * n),
    }
    if with_cbd:
        cols["cbd_congestion_fee"] = pa.array([0.75] * n)
    return pa.table(cols)


def test_month_bounds_handles_december():
    assert prep.month_bounds("2025-10") == ("2025-10-01", "2025-11-01")
    assert prep.month_bounds("2025-12") == ("2025-12-01", "2026-01-01")


def test_prepare_replay_filters_sorts_and_maps_columns(tmp_path):
    pickups = [
        datetime(2025, 10, 9, 8, 0), datetime(2009, 1, 1, 0, 0),  # out of month
        datetime(2025, 10, 1, 0, 5), datetime(2025, 11, 1, 0, 0),  # next month
        datetime(2025, 10, 31, 23, 59),
    ]
    raw = tmp_path / "raw.parquet"
    pq.write_table(tlc_table(pickups), raw)
    out = tmp_path / "replay" / "trips.parquet"

    assert prep.prepare_replay(raw, out, "2025-10") == 3

    t = pq.read_table(out).to_pylist()
    assert [r["row_idx"] for r in t] == [0, 1, 2]
    assert [r["pickup_ts"] for r in t] == [datetime(2025, 10, 1, 0, 5), datetime(2025, 10, 9, 8, 0),
                                          datetime(2025, 10, 31, 23, 59)]
    assert t[0]["airport_fee"] == 1.75 and t[0]["cbd_congestion_fee"] is None
    assert t[0]["passenger_count"] == 1 and isinstance(t[0]["passenger_count"], int)
    assert pq.read_schema(out).field("pickup_ts").type == pa.timestamp("us")


def test_missing_required_column_raises():
    with pytest.raises(ValueError, match="pickup_ts"):
        prep.select_list(["VendorID", "PULocationID"])


def test_count_reasons_uses_processor_rules():
    good = {"pickup_ts": datetime(2025, 10, 7, 8, 0), "dropoff_ts": datetime(2025, 10, 7, 8, 10),
            "trip_distance": 1.0, "pu_location_id": 1, "do_location_id": 2, "payment_type": 1,
            "fare_amount": 10.0, "tip_amount": 1.0, "total_amount": 12.0, "passenger_count": 1}
    refund = dict(good, fare_amount=-10.0, total_amount=-12.0)
    zero = dict(good, trip_distance=0.0, pickup_ts=datetime(2025, 10, 8, 9, 0), dropoff_ts=datetime(2025, 10, 8, 9, 5))
    total, per_day = prep.count_reasons([good, refund, zero])
    assert total["rows"] == 3 and total["rejected_rows"] == 2
    assert total["FARE_NON_POSITIVE"] == 1 and total["TOTAL_NEGATIVE"] == 1 and total["DISTANCE_NON_POSITIVE"] == 1
    assert per_day["2025-10-07"]["rows"] == 2 and per_day["2025-10-08"]["DISTANCE_NON_POSITIVE"] == 1


def test_zones_frames_dissolves_duplicates_and_keeps_unmatched_ids():
    shapes = gpd.GeoDataFrame(
        {"LocationID": [1, 1, 2]},
        geometry=[box(1_000_000, 200_000, 1_001_000, 201_000), box(1_001_000, 200_000, 1_002_000, 201_000),
                  box(1_010_000, 210_000, 1_011_000, 211_000)],
        crs=2263,
    )
    lookup = pd.DataFrame({"LocationID": [1, 2, 264], "Borough": ["EWR", "Queens", "Unknown"],
                           "Zone": ["Newark Airport", "Jamaica Bay", "NV"], "service_zone": ["EWR", "Boro Zone", None]})
    zones, geo = prep.zones_frames(lookup, shapes)
    assert list(zones["location_id"]) == [1, 2, 264]
    assert zones.loc[zones.location_id == 1, "lat"].notna().all()
    assert zones.loc[zones.location_id == 264, "lat"].isna().all()
    assert zones.loc[zones.location_id == 264, "service_zone"].item() == "Unknown"
    assert len(geo) == 2 and geo.crs.to_epsg() == 4326
    assert set(geo.columns) >= {"location_id", "zone", "borough", "geometry"}
    lat = zones.loc[zones.location_id == 1, "lat"].item()
    assert 40.0 < lat < 41.5  # a NY State Plane box lands near NYC
```
Run: `uv run pytest scripts/tests/test_prepare_data.py -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'prepare_data'`.

- [ ] **Step 2: Implement `scripts/prepare_data.py`**

```python
"""Download one month of NYC TLC yellow-taxi data and prepare it for replay (spec 01, `make data`).

Outputs (under DATA_DIR, default ./data):
  replay/trips_<month>.parquet   pickup-sorted, contiguous row_idx, renamed columns, only rows inside the month
  replay/prep_report.json        data-quality preview computed with the stream processor's own rules
  zones.csv                      location_id,borough,zone,service_zone,lat,lon
  taxi_zones.geojson             simplified zone polygons (EPSG:4326) for the Grafana geomap
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "processor" / "src"))
from processor.rules import check_trip  # noqa: E402  (stdlib-only module shared with the processor)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

# (target column, candidate source columns, DuckDB cast)
COLUMNS: list[tuple[str, tuple[str, ...], str]] = [
    ("vendor_id", ("VendorID",), "INTEGER"),
    ("pickup_ts", ("tpep_pickup_datetime",), "TIMESTAMP"),
    ("dropoff_ts", ("tpep_dropoff_datetime",), "TIMESTAMP"),
    ("passenger_count", ("passenger_count",), "INTEGER"),
    ("trip_distance", ("trip_distance",), "DOUBLE"),
    ("ratecode_id", ("RatecodeID",), "INTEGER"),
    ("store_and_fwd_flag", ("store_and_fwd_flag",), "VARCHAR"),
    ("pu_location_id", ("PULocationID",), "INTEGER"),
    ("do_location_id", ("DOLocationID",), "INTEGER"),
    ("payment_type", ("payment_type",), "INTEGER"),
    ("fare_amount", ("fare_amount",), "DOUBLE"),
    ("extra", ("extra",), "DOUBLE"),
    ("mta_tax", ("mta_tax",), "DOUBLE"),
    ("tip_amount", ("tip_amount",), "DOUBLE"),
    ("tolls_amount", ("tolls_amount",), "DOUBLE"),
    ("improvement_surcharge", ("improvement_surcharge",), "DOUBLE"),
    ("congestion_surcharge", ("congestion_surcharge",), "DOUBLE"),
    ("airport_fee", ("Airport_fee", "airport_fee"), "DOUBLE"),
    ("cbd_congestion_fee", ("cbd_congestion_fee",), "DOUBLE"),
    ("total_amount", ("total_amount",), "DOUBLE"),
]
REQUIRED = {"pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "trip_distance",
            "payment_type", "fare_amount", "tip_amount", "total_amount"}


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = (int(x) for x in month.split("-"))
    start = date(year, mon, 1)
    nxt = date(year + (mon == 12), mon % 12 + 1, 1)
    return start.isoformat(), nxt.isoformat()


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as resp:
        size = int(resp.headers.get("Content-Length", -1))
    if dest.exists() and dest.stat().st_size == size:
        print(f"  cached  {dest}")
        return dest
    print(f"  get     {url}")
    part = dest.with_suffix(dest.suffix + ".part")
    urllib.request.urlretrieve(url, part)
    part.replace(dest)
    return dest


def select_list(source_columns: Iterable[str]) -> str:
    by_lower = {c.lower(): c for c in source_columns}
    parts = []
    for target, candidates, cast in COLUMNS:
        src = next((by_lower[c.lower()] for c in candidates if c.lower() in by_lower), None)
        if src is None:
            if target in REQUIRED:
                raise ValueError(f"required column for {target!r} missing (looked for {candidates})")
            parts.append(f"CAST(NULL AS {cast}) AS {target}")
        else:
            parts.append(f'CAST("{src}" AS {cast}) AS {target}')
    return ",\n               ".join(parts)


def prepare_replay(raw: Path, out: Path, month: str) -> int:
    start, nxt = month_bounds(month)
    con = duckdb.connect()
    cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{raw}')").fetchall()]
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT row_number() OVER (ORDER BY pickup_ts, dropoff_ts, pu_location_id) - 1 AS row_idx, *
          FROM (SELECT {select_list(cols)} FROM read_parquet('{raw}'))
          WHERE pickup_ts >= TIMESTAMP '{start}' AND pickup_ts < TIMESTAMP '{nxt}'
          ORDER BY row_idx
        ) TO '{out}' (FORMAT parquet, ROW_GROUP_SIZE 100000)
    """)
    return pq.ParquetFile(out).metadata.num_rows


def iter_rows(path: Path) -> Iterator[dict[str, Any]]:
    for batch in pq.ParquetFile(path).iter_batches(batch_size=100_000):
        yield from batch.to_pylist()


def count_reasons(rows: Iterable[dict[str, Any]], tip_max_abs: float = 100.0
                  ) -> tuple[Counter[str], dict[str, Counter[str]]]:
    total: Counter[str] = Counter()
    per_day: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        reasons = check_trip({**row, "trip_id": "prep", "source": "replay"}, tip_max_abs)
        day = per_day[row["pickup_ts"].date().isoformat()]
        total["rows"] += 1
        day["rows"] += 1
        if reasons:
            total["rejected_rows"] += 1
            day["rejected_rows"] += 1
        for reason in reasons:
            total[reason] += 1
            day[reason] += 1
    return total, per_day


def build_report(raw: Path, replay: Path, month: str) -> dict[str, Any]:
    start, nxt = month_bounds(month)
    con = duckdb.connect()
    raw_rows, dmin, dmax = con.execute(f"""
        SELECT (SELECT count(*) FROM read_parquet('{raw}')),
               min(tpep_pickup_datetime), max(tpep_pickup_datetime)
        FROM read_parquet('{raw}')
        WHERE tpep_pickup_datetime < TIMESTAMP '{start}' OR tpep_pickup_datetime >= TIMESTAMP '{nxt}'
    """).fetchone()
    total, per_day = count_reasons(iter_rows(replay))
    by_weekday: dict[str, list[int]] = defaultdict(list)
    for day, counts in per_day.items():
        by_weekday[date.fromisoformat(day).strftime("%A")].append(counts["rows"])
    avg = {wd: sum(v) / len(v) for wd, v in by_weekday.items()}
    busiest = max(avg, key=avg.__getitem__)
    first = date.fromisoformat(start) + timedelta(days=7)  # leave a week of history for the backfill
    while first.strftime("%A") != busiest:
        first += timedelta(days=1)
    return {
        "month": month,
        "raw_rows": raw_rows,
        "kept_rows": total["rows"],
        "dropped_out_of_month": raw_rows - total["rows"],
        "dropped_pickup_min": str(dmin) if dmin else None,
        "dropped_pickup_max": str(dmax) if dmax else None,
        "rejected_rows": total["rejected_rows"],
        "rejected_pct": round(100 * total["rejected_rows"] / max(total["rows"], 1), 2),
        "by_reason": {k: v for k, v in total.most_common() if k not in ("rows", "rejected_rows")},
        "per_day": {d: dict(c) for d, c in sorted(per_day.items())},
        "avg_trips_by_weekday": {k: round(v) for k, v in sorted(avg.items(), key=lambda kv: -kv[1])},
        "suggested_backfill_until": datetime.combine(first, datetime.min.time()).replace(hour=7).isoformat(),
    }


def zones_frames(lookup: pd.DataFrame, shapes: gpd.GeoDataFrame) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    shapes = shapes.to_crs(2263)
    dissolved = shapes[["LocationID", "geometry"]].dissolve(by="LocationID", as_index=False)
    centroids = dissolved.geometry.centroid.to_crs(4326)  # centroid in a projected CRS, then reproject
    cent = pd.DataFrame({"location_id": dissolved["LocationID"].astype(int),
                         "lat": centroids.y.round(6), "lon": centroids.x.round(6)})
    lk = lookup.rename(columns={"LocationID": "location_id", "Borough": "borough", "Zone": "zone"})
    lk = lk[["location_id", "borough", "zone", "service_zone"]].copy()
    lk["location_id"] = lk["location_id"].astype(int)
    lk[["borough", "zone", "service_zone"]] = lk[["borough", "zone", "service_zone"]].fillna("Unknown")
    zones = lk.merge(cent, on="location_id", how="left").sort_values("location_id").reset_index(drop=True)
    geo = dissolved.assign(geometry=dissolved.geometry.simplify(20)).to_crs(4326)
    geo = geo.rename(columns={"LocationID": "location_id"}).merge(
        lk[["location_id", "zone", "borough"]], on="location_id", how="left")
    return zones, geo


def build_zones(lookup_csv: Path, shapes_zip: Path, out_csv: Path, out_geojson: Path) -> dict[str, Any]:
    with zipfile.ZipFile(shapes_zip) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".shp"))
    shapes = gpd.read_file(f"zip://{shapes_zip}!{member}")
    zones, geo = zones_frames(pd.read_csv(lookup_csv), shapes)
    zones.to_csv(out_csv, index=False)
    out_geojson.unlink(missing_ok=True)
    geo.to_file(out_geojson, driver="GeoJSON")
    missing = zones.loc[zones["lat"].isna(), "location_id"].tolist()
    return {"zones": len(zones), "with_centroid": int(zones["lat"].notna().sum()), "without_geometry": missing,
            "geojson_features": len(geo), "geojson_bytes": out_geojson.stat().st_size}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Prepare NYC TLC data for replay (spec 01).")
    ap.add_argument("--month", default=os.environ.get("TLC_MONTH", "2025-10"))
    ap.add_argument("--data-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "./data")))
    ap.add_argument("--skip-report", action="store_true", help="skip the (slow) data-quality report")
    args = ap.parse_args(argv)
    data, raw_dir = args.data_dir, args.data_dir / "raw"

    print("1/4 download")
    trips_raw = download(f"{BASE_URL}/trip-data/yellow_tripdata_{args.month}.parquet",
                         raw_dir / f"yellow_tripdata_{args.month}.parquet")
    lookup = download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", raw_dir / "taxi_zone_lookup.csv")
    shapes = download(f"{BASE_URL}/misc/taxi_zones.zip", raw_dir / "taxi_zones.zip")

    print("2/4 replay file")
    replay = data / "replay" / f"trips_{args.month}.parquet"
    print(f"  {prepare_replay(trips_raw, replay, args.month):,} rows → {replay}")

    print("3/4 zones")
    zinfo = build_zones(lookup, shapes, data / "zones.csv", data / "taxi_zones.geojson")
    print(f"  {zinfo['zones']} zones, {zinfo['with_centroid']} with centroid, no geometry: {zinfo['without_geometry']}")

    if args.skip_report:
        return 0
    print("4/4 data-quality report (applies the processor's validation rules to every row)")
    report = build_report(trips_raw, replay, args.month) | {"zones": zinfo}
    (data / "replay" / "prep_report.json").write_text(json.dumps(report, indent=2))
    print(f"  rejected {report['rejected_rows']:,} of {report['kept_rows']:,} ({report['rejected_pct']}%)")
    print(f"  by reason: {report['by_reason']}")
    print(f"  suggested: REPLAY_START_TS / backfill UNTIL = {report['suggested_backfill_until']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```
Run: `uv run pytest scripts/tests/test_prepare_data.py -q`. Expected: `5 passed`.

- [ ] **Step 3: Run the real preparation**

Run: `make data`. Expected, in about 3–6 minutes on first run:
- Three downloads (about 75 MB of Parquet).
- `~4,000,000 rows → data/replay/trips_2025-10.parquet`.
- `265 zones, ≥258 with centroid, no geometry: [...]` (ids such as 57/104/105/264/265).
- A rejected percentage of roughly 2–5%, and a suggested backfill date.

Then check:
```bash
ls -la data/replay data/zones.csv data/taxi_zones.geojson
uv run python -c "import json;r=json.load(open('data/replay/prep_report.json'));print(r['kept_rows'],r['rejected_pct'],r['by_reason'],r['suggested_backfill_until'])"
```
Expected: `taxi_zones.geojson` < 2 MB. Record `suggested_backfill_until`, which Task 10 uses.

- [ ] **Step 4: Commit**

```bash
git add scripts/prepare_data.py scripts/tests/test_prepare_data.py
git commit -m "feat(data): TLC download + replay file + zones + quality report (make data)"
```

---
### Task 3: ClickHouse schema, users and limits (files + chDB tests)

**Files:**
- Create: `infra/clickhouse/init/*.sql` (extracted verbatim from spec 04), `infra/clickhouse/init/90_load_zones.sh`, `infra/clickhouse/config.d/stand.xml`, `infra/clickhouse/users.d/stand.xml`, `infra/tests/test_clickhouse_ddl.py`

**Interfaces:**
- Produces: the database `taxi` with the tables in the Global Constraints. Column order of `taxi.trips` (used by Task 8 `rows.py`) is exactly the order in `20_trips.sql`.
- Produces: users `ingest_writer`, `agent_ro`, `grafana_ro`, with passwords from the env vars `CLICKHOUSE_{INGEST,AGENT,GRAFANA}_PASSWORD`, plus `admin` created by the official image from `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`.

- [ ] **Step 1: Write the failing DDL tests**

`infra/tests/test_clickhouse_ddl.py`:
```python
"""The DDL in infra/clickhouse/init must equal spec 04 and must apply cleanly (checked in embedded chDB)."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INIT = ROOT / "infra" / "clickhouse" / "init"
SPEC = ROOT / "docs" / "specs" / "04-clickhouse-schema.md"
AGENT_TABLES = ["trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"]


def spec_files() -> dict[str, str]:
    ddl = re.search(r"### DDL\n\n```sql\n(.*?)```", SPEC.read_text(), re.S).group(1)
    return {name: body.strip() + "\n"
            for name, body in re.findall(r"-- (\d\d_\w+\.sql)\n(.*?)(?=\n-- \d\d_\w+\.sql\n|\Z)", ddl, re.S)}


def statements() -> list[str]:
    out = []
    for f in sorted(INIT.glob("*.sql")):
        for chunk in re.split(r";\s*\n", f.read_text()):
            body = "\n".join(line for line in chunk.splitlines() if not line.strip().startswith("--")).strip()
            if body:
                out.append(body)
    return out


def test_init_files_equal_spec_04():
    expected = spec_files()
    assert sorted(expected) == sorted(p.name for p in INIT.glob("*.sql"))
    for name, body in expected.items():
        assert (INIT / name).read_text() == body, f"{name} differs from spec 04 — re-extract it"


@pytest.fixture(scope="module")
def ch():
    from chdb import session
    s = session.Session()
    for stmt in statements():
        if "ENGINE = Kafka" in stmt or "trace_events_mv" in stmt:
            continue  # needs a broker; covered by infra/tests/test_stack.py
        s.query(stmt)
    yield s
    s.close()


def q(s, sql: str) -> list[list[str]]:
    return [line.split("\t") for line in s.query(sql, "TSV").bytes().decode().strip().splitlines() if line]


def test_tables_exist(ch):
    names = {r[0] for r in q(ch, "SHOW TABLES FROM taxi")}
    assert names >= {"agent_log", "agg_borough_minute", "sim_now", "trace_events", "trips", "trips_rejected", "zones"}


def test_every_agent_visible_column_has_a_comment(ch):
    tables = ",".join(f"'{t}'" for t in AGENT_TABLES)
    missing = q(ch, f"SELECT table, name FROM system.columns WHERE database='taxi' AND table IN ({tables}) AND comment=''")
    assert missing == []


def test_sim_now_ignores_student_rides(ch):
    ch.query("""INSERT INTO taxi.trips (trip_id, source, loop, pickup_ts, dropoff_ts, pickup_hour, pickup_dow,
        trip_distance, duration_min, pu_location_id, pu_zone, pu_borough, pu_service_zone, do_location_id, do_zone,
        do_borough, do_service_zone, payment_type, payment_type_name, is_card, is_airport, fare_amount, extra, mta_tax,
        tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee, cbd_congestion_fee,
        total_amount, produced_at, processed_at) VALUES
        ('r0-000000001','replay',0,'2025-10-07 08:00:00','2025-10-07 08:10:00',8,2,1,10,161,'a','Manhattan','Yellow Zone',
         237,'b','Manhattan','Yellow Zone',1,'Credit card',true,false,10,0,0.5,2,0,1,2.5,0,0.75,16.75,now64(3),now64(3)),
        ('s-00000000-0000-0000-0000-000000000001','student',0,'2025-10-09 08:00:00','2025-10-09 08:10:00',8,4,1,10,161,
         'a','Manhattan','Yellow Zone',237,'b','Manhattan','Yellow Zone',1,'Credit card',true,false,10,0,0.5,2,0,1,2.5,0,
         0.75,16.75,now64(3),now64(3))""")
    assert q(ch, "SELECT now_ts, today, this_hour FROM taxi.sim_now") == [
        ["2025-10-07 08:00:00.000", "2025-10-07", "2025-10-07 08:00:00"]]
```
Run: `uv run pytest infra/tests/test_clickhouse_ddl.py -q`. Expected: FAIL (the `infra/clickhouse/init` files don't exist, so the file-set assertion fails).

- [ ] **Step 2: Extract the DDL from spec 04 into init files**

```bash
mkdir -p infra/clickhouse/init infra/clickhouse/config.d infra/clickhouse/users.d
uv run python - <<'EOF'
import re, pathlib
spec = pathlib.Path("docs/specs/04-clickhouse-schema.md").read_text()
ddl = re.search(r"### DDL\n\n```sql\n(.*?)```", spec, re.S).group(1)
out = pathlib.Path("infra/clickhouse/init")
for name, body in re.findall(r"-- (\d\d_\w+\.sql)\n(.*?)(?=\n-- \d\d_\w+\.sql\n|\Z)", ddl, re.S):
    (out / name).write_text(body.strip() + "\n"); print(name)
EOF
```
Expected output: `00_database.sql 10_zones.sql 20_trips.sql 21_sim_now.sql 30_trips_rejected.sql 40_agg_borough_minute.sql 50_trace_events.sql 60_agent_log.sql` (one per line).

- [ ] **Step 3: Zones loader, server config, users**

`infra/clickhouse/init/90_load_zones.sh`:
```bash
#!/bin/bash
# Load the taxi zones once (spec 04 §Zones loader). Runs from /docker-entrypoint-initdb.d on the first start.
set -euo pipefail
ch() { clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"; }
if [ ! -s /data/zones.csv ]; then
  echo "90_load_zones: /data/zones.csv is missing or empty; run 'make data' first" >&2
  exit 1
fi
if [ "$(ch --query 'SELECT count() FROM taxi.zones')" = "0" ]; then
  ch --input_format_csv_empty_as_default=1 --query "INSERT INTO taxi.zones FORMAT CSVWithNames" < /data/zones.csv
fi
echo "90_load_zones: taxi.zones has $(ch --query 'SELECT count() FROM taxi.zones') rows"
```
Run: `chmod +x infra/clickhouse/init/90_load_zones.sh`

`infra/clickhouse/config.d/stand.xml`:
```xml
<clickhouse>
    <!-- Required by the sim-time model: *_ts values are stored and shown unconverted (ARCHITECTURE §4). -->
    <timezone>UTC</timezone>
    <max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio>
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>
</clickhouse>
```

`infra/clickhouse/users.d/stand.xml`:
```xml
<clickhouse>
    <profiles>
        <ingest>
            <max_memory_usage>1000000000</max_memory_usage>
        </ingest>
        <agent>
            <readonly>1</readonly>
            <max_execution_time>5</max_execution_time>
            <timeout_overflow_mode>throw</timeout_overflow_mode>
            <max_result_rows>500</max_result_rows>
            <result_overflow_mode>break</result_overflow_mode>
            <max_rows_to_read>100000000</max_rows_to_read>
            <max_memory_usage>1000000000</max_memory_usage>
            <max_threads>2</max_threads>
            <max_query_size>20000</max_query_size>
            <allow_introspection_functions>0</allow_introspection_functions>
        </agent>
        <grafana>
            <readonly>2</readonly>
            <max_execution_time>15</max_execution_time>
            <max_memory_usage>1000000000</max_memory_usage>
            <max_threads>2</max_threads>
            <constraints>
                <max_execution_time><max>30</max></max_execution_time>
                <max_memory_usage><max>1000000000</max></max_memory_usage>
                <max_threads><max>4</max></max_threads>
            </constraints>
        </grafana>
    </profiles>
    <quotas>
        <agent>
            <interval>
                <duration>60</duration>
                <queries>120</queries>
                <errors>60</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>120</execution_time>
            </interval>
        </agent>
    </quotas>
    <users>
        <!-- Ports are published on 127.0.0.1 only, so ::/0 means "the Compose network + this laptop". -->
        <ingest_writer>
            <password from_env="CLICKHOUSE_INGEST_PASSWORD"/>
            <profile>ingest</profile>
            <quota>default</quota>
            <networks><ip>::/0</ip></networks>
            <grants>
                <query>GRANT INSERT ON taxi.trips</query>
                <query>GRANT INSERT ON taxi.trips_rejected</query>
                <query>GRANT INSERT ON taxi.agg_borough_minute</query>
                <query>GRANT INSERT ON taxi.agent_log</query>
            </grants>
        </ingest_writer>
        <agent_ro>
            <password from_env="CLICKHOUSE_AGENT_PASSWORD"/>
            <profile>agent</profile>
            <quota>agent</quota>
            <networks><ip>::/0</ip></networks>
            <grants>
                <query>GRANT SELECT ON taxi.trips</query>
                <query>GRANT SELECT ON taxi.trips_rejected</query>
                <query>GRANT SELECT ON taxi.agg_borough_minute</query>
                <query>GRANT SELECT ON taxi.zones</query>
                <query>GRANT SELECT ON taxi.sim_now</query>
                <query>GRANT SELECT ON taxi.trace_events</query>
            </grants>
        </agent_ro>
        <grafana_ro>
            <password from_env="CLICKHOUSE_GRAFANA_PASSWORD"/>
            <profile>grafana</profile>
            <quota>default</quota>
            <networks><ip>::/0</ip></networks>
            <grants>
                <query>GRANT SELECT ON taxi.*</query>
                <query>GRANT SELECT ON system.parts</query>
                <query>GRANT SELECT ON system.query_log</query>
            </grants>
        </grafana_ro>
    </users>
</clickhouse>
```

- [ ] **Step 4: Run the DDL tests**

Run: `uv run pytest infra/tests/test_clickhouse_ddl.py -q`. Expected: `4 passed`.

- [ ] **Step 5: Commit**

```bash
git add infra/clickhouse infra/tests/test_clickhouse_ddl.py
git commit -m "feat(clickhouse): schema, users/profiles/quotas, zones loader (spec 04)"
```

---

### Task 4: Compose foundation: Redpanda, topics, ClickHouse, Console, Grafana, Caddy (M0 exit)

**Files:**
- Create: `docker-compose.yml`, `infra/redpanda/create-topics.sh`, `infra/grafana/Dockerfile`, `infra/grafana/provisioning/datasources/clickhouse.yaml`, `infra/caddy/Caddyfile`, `infra/tests/test_stack.py`
- Modify: `docs/ARCHITECTURE.md` §9 (pinned versions)

**Interfaces:**
- Consumes: Task 3 ClickHouse files, `data/zones.csv` and `data/taxi_zones.geojson` (Task 2), and `.env` (Task 1).
- Produces:
  - Compose services `redpanda` (internal `redpanda:9092`, host `localhost:19092`), `redpanda-init`, `redpanda-console` (`127.0.0.1:8080`), `clickhouse` (`127.0.0.1:8123`/`9000`), `grafana` (`127.0.0.1:3001`, sub-path `/grafana/`), and `caddy` (`0.0.0.0:80`).
  - Volumes `redpanda-data`, `clickhouse-data`, `processor-state`, `replayer-state`.
  - Tasks 7 and 9 append the `replayer` and `processor` services.

- [ ] **Step 1: Start Docker Desktop**

Run: `open -a Docker && until docker info >/dev/null 2>&1; do sleep 2; done && docker info --format '{{.MemTotal}} {{.NCPU}}'`. Expected: prints memory and CPU count.

Then set Docker Desktop → Settings → Resources to Memory ≥ 14 GB and CPUs ≥ 6 (spec 09) if they're lower.

- [ ] **Step 2: Topic bootstrap script**

`infra/redpanda/create-topics.sh`:
```bash
#!/bin/bash
# Create the six topics (spec 02). Idempotent: an existing topic is fine.
set -euo pipefail
RETENTION="${TOPIC_RETENTION_MS:-21600000}"

create() {
  local name="$1"; shift
  local out
  out="$(rpk topic create "$name" "$@" 2>&1)" || true
  if grep -qE "^${name//./\\.}\s+(OK|TOPIC_ALREADY_EXISTS)" <<<"$out"; then
    echo "topic ${name}: ready"
  else
    echo "topic ${name}: FAILED"; echo "$out"; exit 1
  fi
}

for t in trips.raw trips.enriched trips.rejected agg.borough_minute trace.checkpoints; do
  create "$t" -p 3 -r 1 -c "retention.ms=${RETENTION}" -c cleanup.policy=delete
done
create sim.clock -p 1 -r 1 -c cleanup.policy=compact
rpk topic list
```

- [ ] **Step 3: Grafana image, datasource, and Caddy**

`infra/grafana/Dockerfile`:
```Dockerfile
FROM grafana/grafana-oss:12.4.3
# Bake the plugin in: the venue has no internet at runtime (spec 05).
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
USER root
RUN mkdir -p /opt/grafana-plugins \
 && grafana cli --pluginsDir /opt/grafana-plugins plugins install grafana-clickhouse-datasource 4.21.3 \
 && chown -R 472:0 /opt/grafana-plugins
USER 472
```

`infra/grafana/provisioning/datasources/clickhouse.yaml`:
```yaml
apiVersion: 1
datasources:
  - name: ClickHouse
    uid: clickhouse
    type: grafana-clickhouse-datasource
    isDefault: true
    editable: false
    jsonData:
      host: clickhouse
      port: 8123
      protocol: http
      username: grafana_ro
      defaultDatabase: taxi
    secureJsonData:
      password: ${CLICKHOUSE_GRAFANA_PASSWORD}
```

`infra/caddy/Caddyfile` (M0 version. M3 adds `/api/*` and the web app, as in spec 09):
```caddyfile
:80 {
	# Grafana runs with serve_from_sub_path=true, so keep the /grafana prefix
	handle /grafana/* {
		reverse_proxy grafana:3000
	}
	handle {
		respond "NYC Taxi Live: the web UI arrives in M3. The dashboard is at /grafana/" 200
	}
}
```

- [ ] **Step 4: `docker-compose.yml` (foundation services)**

```yaml
name: nyctaxi

x-logging: &logging
  driver: json-file
  options: { max-size: "10m", max-file: "3" }

services:
  redpanda:
    image: redpandadata/redpanda:v26.2.3
    command:
      - redpanda
      - start
      - --mode=dev-container
      - --smp=1
      - --memory=1G
      - --reserve-memory=0M
      - --overprovisioned
      - --default-log-level=warn
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda:9092,external://localhost:19092
      - --rpc-addr=redpanda:33145
      - --advertise-rpc-addr=redpanda:33145
    ports: ["127.0.0.1:19092:19092", "127.0.0.1:9644:9644"]
    volumes: ["redpanda-data:/var/lib/redpanda/data"]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -Eq 'Healthy:.+true'"]
      interval: 5s
      timeout: 5s
      retries: 30
    mem_limit: 1536m
    restart: unless-stopped
    logging: *logging

  redpanda-init:
    image: redpandadata/redpanda:v26.2.3
    depends_on: { redpanda: { condition: service_healthy } }
    entrypoint: ["/bin/bash", "/scripts/create-topics.sh"]
    environment:
      RPK_BROKERS: redpanda:9092
      TOPIC_RETENTION_MS: ${TOPIC_RETENTION_MS:-21600000}
    volumes: ["./infra/redpanda:/scripts:ro"]
    restart: "no"
    logging: *logging

  redpanda-console:
    image: redpandadata/console:v3.12.0
    depends_on: { redpanda: { condition: service_healthy } }
    environment:
      KAFKA_BROKERS: redpanda:9092
    ports: ["127.0.0.1:8080:8080"]
    mem_limit: 256m
    restart: unless-stopped
    logging: *logging

  clickhouse:
    image: clickhouse/clickhouse-server:26.3.34
    depends_on: { redpanda-init: { condition: service_completed_successfully } }
    environment:
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_ADMIN_PASSWORD:?run make secrets}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
      CLICKHOUSE_INGEST_PASSWORD: ${CLICKHOUSE_INGEST_PASSWORD:?run make secrets}
      CLICKHOUSE_AGENT_PASSWORD: ${CLICKHOUSE_AGENT_PASSWORD:?run make secrets}
      CLICKHOUSE_GRAFANA_PASSWORD: ${CLICKHOUSE_GRAFANA_PASSWORD:?run make secrets}
    ports: ["127.0.0.1:8123:8123", "127.0.0.1:9000:9000"]
    volumes:
      - clickhouse-data:/var/lib/clickhouse
      - ./infra/clickhouse/init:/docker-entrypoint-initdb.d:ro
      - ./infra/clickhouse/config.d/stand.xml:/etc/clickhouse-server/config.d/stand.xml:ro
      - ./infra/clickhouse/users.d/stand.xml:/etc/clickhouse-server/users.d/stand.xml:ro
      - ./data/zones.csv:/data/zones.csv:ro
    ulimits: { nofile: { soft: 262144, hard: 262144 } }
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8123/ping | grep -q Ok"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 20s
    mem_limit: 4g
    restart: unless-stopped
    logging: *logging

  grafana:
    build: infra/grafana
    depends_on: { clickhouse: { condition: service_healthy } }
    environment:
      GF_SERVER_ROOT_URL: "%(protocol)s://%(domain)s/grafana/"
      GF_SERVER_SERVE_FROM_SUB_PATH: "true"
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_SECURITY_ALLOW_EMBEDDING: "true"
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:?run make secrets}
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_NEWS_NEWS_FEED_ENABLED: "false"
      CLICKHOUSE_GRAFANA_PASSWORD: ${CLICKHOUSE_GRAFANA_PASSWORD}
    ports: ["127.0.0.1:3001:3000"]
    volumes:
      - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./data/taxi_zones.geojson:/usr/share/grafana/public/maps/taxi_zones.geojson:ro
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:3000/api/health | grep -q ok"]
      interval: 10s
      timeout: 5s
      retries: 12
    mem_limit: 384m
    restart: unless-stopped
    logging: *logging

  caddy:
    image: caddy:2.11.4-alpine
    depends_on: { grafana: { condition: service_healthy } }
    ports: ["0.0.0.0:80:80"]
    volumes: ["./infra/caddy/Caddyfile:/etc/caddy/Caddyfile:ro"]
    mem_limit: 128m
    restart: unless-stopped
    logging: *logging

volumes:
  redpanda-data:
  clickhouse-data:
  processor-state:
  replayer-state:
```
Run: `docker compose config -q`. Expected: no output (valid).

- [ ] **Step 5: Write the stack tests (spec 02 acceptance 1, spec 04 acceptance 1–6)**

`infra/tests/test_stack.py`:
```python
"""Checks against the running compose stack (make up && make test-stack)."""

import json
import time
import urllib.request
import uuid
from base64 import b64encode
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, ConfigResource

pytestmark = pytest.mark.stack
ROOT = Path(__file__).resolve().parents[2]
TOPICS = {"trips.raw", "trips.enriched", "trips.rejected", "agg.borough_minute", "trace.checkpoints", "sim.clock"}


@lru_cache(maxsize=1)
def env() -> dict[str, str]:
    """Read lazily so collecting this module never needs a .env (it is deselected by default)."""
    return dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
                if "=" in line and not line.lstrip().startswith("#"))


def client(user: str, password_key: str):
    return clickhouse_connect.get_client(host="localhost", port=8123, username=user,
                                         password=env()[password_key], database="taxi")


@pytest.fixture(scope="module")
def admin():
    return client("admin", "CLICKHOUSE_ADMIN_PASSWORD")


@pytest.fixture(scope="module")
def agent():
    return client("agent_ro", "CLICKHOUSE_AGENT_PASSWORD")


def test_topics_exist_with_configs():
    a = AdminClient({"bootstrap.servers": "localhost:19092"})
    md = a.list_topics(timeout=10)
    assert TOPICS <= set(md.topics)
    assert all(len(md.topics[t].partitions) == 3 for t in TOPICS - {"sim.clock"})
    assert len(md.topics["sim.clock"].partitions) == 1
    res = a.describe_configs([ConfigResource("topic", "trips.raw"), ConfigResource("topic", "sim.clock")])
    configs = {r.name: f.result() for r, f in res.items()}
    assert configs["trips.raw"]["retention.ms"].value == "21600000"
    assert configs["sim.clock"]["cleanup.policy"].value == "compact"


def test_tables(admin):
    names = {r[0] for r in admin.query("SHOW TABLES FROM taxi").result_rows}
    assert names == {"agent_log", "agg_borough_minute", "sim_now", "trace_events", "trace_events_mv",
                     "trace_events_queue", "trips", "trips_rejected", "zones"}


def test_zones_loaded(admin):
    total, with_centroid = admin.query("SELECT count(), countIf(lat IS NOT NULL) FROM taxi.zones").result_rows[0]
    assert total == 265 and with_centroid >= 258


@pytest.mark.parametrize("sql", [
    "INSERT INTO taxi.agent_log (question) VALUES ('x')",
    "ALTER TABLE taxi.trips DELETE WHERE 1",
    "DROP TABLE taxi.trips",
    "CREATE TABLE taxi.x (a UInt8) ENGINE = Memory",
    "SET max_execution_time = 100",
    "SELECT 1 SETTINGS max_result_rows = 10000",
    "SELECT * FROM taxi.agent_log",
    "SELECT * FROM system.users",
])
def test_agent_ro_cannot(agent, sql):
    with pytest.raises(DatabaseError):
        agent.command(sql)


def test_agent_ro_result_rows_are_capped(agent):
    assert len(agent.query("SELECT number FROM numbers(10000)").result_rows) <= 500


def test_agent_ro_heavy_query_is_killed_fast(agent):
    start = time.monotonic()
    with pytest.raises(DatabaseError):
        agent.query("SELECT count() FROM numbers(1000000000000) AS a CROSS JOIN numbers(1000000000000) AS b")
    assert time.monotonic() - start < 6


def test_agent_ro_can_read_whitelist(agent):
    for t in ["trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"]:
        agent.query(f"SELECT * FROM taxi.{t} LIMIT 1")


def test_ingest_writer_can_insert_but_not_read(admin):
    ingest = client("ingest_writer", "CLICKHOUSE_INGEST_PASSWORD")
    ingest.insert("trips_rejected", [["stack-test", "replay", None, None, ["SCHEMA_INVALID"], "{}",
                                      datetime.now(timezone.utc)]],
                  column_names=["trip_id", "source", "trace_id", "pickup_ts", "reasons", "payload", "rejected_at"])
    with pytest.raises(DatabaseError):
        ingest.query("SELECT count() FROM taxi.trips")
    admin.command("DELETE FROM taxi.trips_rejected WHERE trip_id = 'stack-test'")


def test_trace_checkpoint_reaches_trace_events_via_kafka_engine(admin):
    trace_id = str(uuid.uuid4())
    cp = {"trace_id": trace_id, "trip_id": "r0-000000001", "source": "replay", "stage": "ingested", "seq": 2,
          "service": "replayer", "at": "2026-10-16T08:00:00.000Z", "detail": {"partition": 0, "offset": 1}}
    p = Producer({"bootstrap.servers": "localhost:19092"})
    p.produce("trace.checkpoints", key=trace_id, value=json.dumps(cp))
    p.flush(10)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        rows = admin.query(f"SELECT stage, detail FROM taxi.trace_events WHERE trace_id = '{trace_id}'").result_rows
        if rows:
            assert rows[0][0] == "ingested" and json.loads(rows[0][1]) == {"partition": 0, "offset": 1}
            return
        time.sleep(1)
    pytest.fail("checkpoint did not reach taxi.trace_events within 15 s")


def test_grafana_datasource_is_healthy_through_caddy():
    auth = b64encode(f"admin:{env()['GRAFANA_ADMIN_PASSWORD']}".encode()).decode()
    req = urllib.request.Request("http://localhost/grafana/api/datasources/uid/clickhouse/health",
                                 headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=10) as r:
        assert json.load(r)["status"] == "OK"


def test_console_is_up():
    with urllib.request.urlopen("http://127.0.0.1:8080/", timeout=10) as r:
        assert r.status == 200
```

- [ ] **Step 6: Start the stack and run the stack tests**

Run: `make up`. Expected: every service `Healthy`/`Started`, and `redpanda-init` exited 0.
Run: `docker compose logs redpanda-init | grep ready`. Expected: 6 lines, `topic …: ready`.
Run: `make test-stack`. Expected: `18 passed`.

If a check fails:
- **`agent_ro` connect fails with a message about changing settings in readonly mode:** switch the `agent` profile to `<readonly>2</readonly>` and add a `<constraints>` block with `<readonly/>` for every limit in that profile, as spec 04 describes. `docker compose restart clickhouse`, re-run, and note it for the Task 10 ADR.
- **`detail` isn't parsed as a String:** add `SETTINGS input_format_json_read_objects_as_strings = 1` to the Kafka engine table in spec 04 *and* in `50_trace_events.sql`, then `make reset && make up`.

- [ ] **Step 7: Record pinned versions in ARCHITECTURE §9**

Replace the `TBD` rows in `docs/ARCHITECTURE.md` §9 with:

| Component | Version | Verified on |
|---|---|---|
| Redpanda (`redpandadata/redpanda`) | v26.2.3 | 2026-09-28 |
| Redpanda Console | v3.12.0 | 2026-09-28 |
| ClickHouse (`clickhouse/clickhouse-server`, 26.3 LTS) | 26.3.34 | 2026-09-28 |
| Grafana OSS + `grafana-clickhouse-datasource` | 12.4.3 + 4.21.3 | 2026-09-28 |
| Caddy 2 | 2.11.4-alpine | 2026-09-28 |
| Python / uv | 3.12 / 0.11.13 | 2026-09-28 |
| quixstreams | 3.26.0 | 2026-09-28 |
| confluent-kafka, clickhouse-connect, pyarrow, duckdb | 2.15.1, 1.9.0, 25.0.1, 1.5.5 | 2026-09-28 |
| fastapi, openai, sqlglot | pin at M3 | |
| Node / pnpm / Next.js | pin at M3 | |
| Ollama + chosen model tag | 0.30.8 + TBD after eval (M3) | |

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml infra/redpanda infra/grafana infra/caddy infra/tests/test_stack.py docs/ARCHITECTURE.md
git commit -m "feat(infra): compose foundation — Redpanda, topics, ClickHouse, Console, Grafana, Caddy (M0)"
```

---
### Task 5: Replayer core: config, sim clock, messages, state, source

**Files:**
- Create: `services/replayer/pyproject.toml`, `services/replayer/src/replayer/{__init__,config,clock,messages,state,source}.py`
- Test: `services/replayer/tests/{conftest,test_config,test_clock,test_messages,test_state,test_source}.py`

**Interfaces:**
- Produces:
  - `Config.from_env(env: dict | None) -> Config` with fields `brokers, parquet_path, speed (float | None = max), start_ts, until_ts, loop_offset_days, trace_sample_rate, clock_interval_s, state_path, control_port, admin_token` and the property `mode ("live" | "backfill")`.
  - `SimClock(sim_start, speed, monotonic=time.monotonic, max_gap_s=10.0)` with `now() tick() pause() resume() set_speed(s)` and the properties `speed`, `paused`.
  - `ManualClock(sim_start)` with `now() tick() advance_to(ts)` and `paused = False`.
  - `messages`:
    - `format_sim(dt, *, millis=False) -> str`, `format_wall(dt_aware) -> str`, `trip_id(loop, row_idx) -> str`
    - `sampled_trace_id(trip_id, rate) -> str | None`
    - `build_trip(row, *, loop, offset_days, trace_sample_rate, produced_at) -> dict`
    - `build_clock(*, sim_ts, at, speed, loop, row_idx, paused) -> dict`
    - `build_ingested_checkpoint(trip, *, topic, partition, offset, at) -> dict`
  - `ReplayState(loop, row_idx, sim_ts)`, `load_state(path) -> ReplayState | None`, `save_state(path, state)`.
  - `ReplaySource(path)` with `num_rows`, `first_row_at_or_after(ts) -> int`, `rows_from(row_idx) -> Iterator[dict]`.

- [ ] **Step 1: Project file**

`services/replayer/pyproject.toml`:
```toml
[project]
name = "nyctaxi-replayer"
version = "0.1.0"
description = "Replays NYC TLC trips into Redpanda on a simulated clock (spec 01)"
requires-python = ">=3.12,<3.13"
dependencies = ["confluent-kafka==2.15.1", "pyarrow==25.0.1", "numpy==2.5.3"]

[dependency-groups]
dev = ["pytest==9.1.1", "jsonschema==4.26.0", "referencing==0.37.0", "ruff==0.16.9", "mypy==2.3.1",
       "testcontainers[kafka]==4.15.0"]

[build-system]
requires = ["hatchling==1.32.4"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/replayer"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["integration: needs Docker (testcontainers)"]
addopts = "-m 'not integration'"

[tool.ruff]
line-length = 110
target-version = "py312"

[tool.mypy]
strict = true
ignore_missing_imports = true
```
Run: `cd services/replayer && mkdir -p src/replayer tests && touch src/replayer/__init__.py && uv lock && uv sync`. Expected: `.venv` is created.

- [ ] **Step 2: Test fixtures**

`services/replayer/tests/conftest.py`:
```python
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "contracts"))

START = datetime(2025, 10, 7, 7, 0, 0)


def make_replay_parquet(path: Path, n: int = 20, start: datetime = START, step_s: int = 30,
                        row_group_size: int = 7) -> Path:
    """A tiny prepared replay file (same columns as scripts/prepare_data.py output): one trip every step_s."""
    pickups = [start + timedelta(seconds=i * step_s) for i in range(n)]
    table = pa.table({
        "row_idx": pa.array(range(n), pa.int64()),
        "vendor_id": pa.array([2] * n, pa.int32()),
        "pickup_ts": pa.array(pickups, pa.timestamp("us")),
        "dropoff_ts": pa.array([p + timedelta(minutes=12) for p in pickups], pa.timestamp("us")),
        "passenger_count": pa.array([1] * n, pa.int32()),
        "trip_distance": pa.array([2.5] * n),
        "ratecode_id": pa.array([1] * n, pa.int32()),
        "store_and_fwd_flag": pa.array(["N"] * n),
        "pu_location_id": pa.array([161] * n, pa.int32()),
        "do_location_id": pa.array([237] * n, pa.int32()),
        "payment_type": pa.array([1] * n, pa.int32()),
        "fare_amount": pa.array([15.0] * n),
        "extra": pa.array([0.0] * n),
        "mta_tax": pa.array([0.5] * n),
        "tip_amount": pa.array([3.0] * n),
        "tolls_amount": pa.array([0.0] * n),
        "improvement_surcharge": pa.array([1.0] * n),
        "congestion_surcharge": pa.array([2.5] * n),
        "airport_fee": pa.array([None] * n, pa.float64()),
        "cbd_congestion_fee": pa.array([0.75] * n),
        "total_amount": pa.array([22.75] * n),
    })
    pq.write_table(table, path, row_group_size=row_group_size)
    return path


@pytest.fixture
def replay_parquet(tmp_path: Path) -> Path:
    return make_replay_parquet(tmp_path / "trips.parquet")


class FakeMono:
    """A controllable time.monotonic()."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def mono() -> FakeMono:
    return FakeMono()
```

- [ ] **Step 3: Write the failing tests for config, clock, messages, state, and source**

`services/replayer/tests/test_config.py`:
```python
from datetime import datetime
from pathlib import Path

import pytest

from replayer.config import Config


def test_defaults():
    c = Config.from_env({})
    assert c.speed == 60.0 and c.mode == "live"
    assert c.parquet_path == Path("/data/replay/trips_2025-10.parquet")
    assert c.start_ts == datetime(2025, 10, 7, 7, 0) and c.until_ts is None
    assert (c.loop_offset_days, c.trace_sample_rate, c.control_port) == (35, 0.001, 8001)
    assert c.state_path == Path("/data/state/replayer.json") and c.brokers == "redpanda:9092"


def test_backfill_mode_requires_until():
    with pytest.raises(ValueError, match="REPLAY_UNTIL_TS"):
        Config.from_env({"REPLAY_SPEED": "max"})
    c = Config.from_env({"REPLAY_SPEED": "MAX", "REPLAY_UNTIL_TS": "2025-10-14T07:00:00"})
    assert c.speed is None and c.mode == "backfill" and c.until_ts == datetime(2025, 10, 14, 7)


@pytest.mark.parametrize("bad", ["0", "-5", "fast"])
def test_invalid_speed(bad):
    with pytest.raises(ValueError):
        Config.from_env({"REPLAY_SPEED": bad})


def test_parquet_path_follows_month_and_data_dir():
    c = Config.from_env({"TLC_MONTH": "2026-06", "DATA_DIR": "/x"})
    assert c.parquet_path == Path("/x/replay/trips_2026-06.parquet")
    assert Config.from_env({"REPLAY_PARQUET_PATH": "/y/t.parquet"}).parquet_path == Path("/y/t.parquet")
```

`services/replayer/tests/test_clock.py`:
```python
from datetime import datetime

import pytest

from replayer.clock import ManualClock, SimClock

T0 = datetime(2025, 10, 7, 7, 0, 0)


def test_advances_at_speed(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 1.0
    assert c.now() == datetime(2025, 10, 7, 7, 1, 0)


def test_pause_freezes_and_resume_does_not_jump(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 10.0
    c.pause()
    mono.t = 100.0
    assert c.paused and c.now() == datetime(2025, 10, 7, 7, 10)
    c.resume()
    mono.t = 101.0
    assert c.now() == datetime(2025, 10, 7, 7, 11)


def test_speed_change_does_not_jump(mono):
    c = SimClock(T0, 60, mono)
    mono.t = 5.0
    c.set_speed(120)
    assert c.now() == datetime(2025, 10, 7, 7, 5)
    mono.t = 6.0
    assert c.now() == datetime(2025, 10, 7, 7, 7) and c.speed == 120


def test_wall_gap_is_not_replayed(mono):
    c = SimClock(T0, 60, mono, max_gap_s=10)
    mono.t = 1.0
    assert c.tick() == datetime(2025, 10, 7, 7, 1)
    mono.t = 61.0  # process frozen for 60 s
    assert c.tick() == datetime(2025, 10, 7, 7, 1)
    mono.t = 62.0
    assert c.tick() == datetime(2025, 10, 7, 7, 2)


@pytest.mark.parametrize("speed", [0, -1])
def test_invalid_speed(mono, speed):
    with pytest.raises(ValueError):
        SimClock(T0, speed, mono)
    with pytest.raises(ValueError):
        SimClock(T0, 60, mono).set_speed(speed)


def test_manual_clock_only_moves_forward():
    c = ManualClock(T0)
    c.advance_to(datetime(2025, 10, 7, 8))
    c.advance_to(datetime(2025, 10, 7, 7, 30))
    assert c.now() == c.tick() == datetime(2025, 10, 7, 8) and c.paused is False
```

`services/replayer/tests/test_messages.py`:
```python
import math
from datetime import datetime, timezone

from contract_validator import assert_valid

from replayer.messages import (build_clock, build_ingested_checkpoint, build_trip, format_wall,
                               sampled_trace_id, trip_id)

WALL = datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=timezone.utc)
ROW = {
    "row_idx": 123456, "vendor_id": 2, "pickup_ts": datetime(2025, 10, 7, 8, 14, 3),
    "dropoff_ts": datetime(2025, 10, 7, 8, 29, 41), "passenger_count": 1, "trip_distance": 2.87,
    "ratecode_id": 1, "store_and_fwd_flag": "N", "pu_location_id": 161, "do_location_id": 237,
    "payment_type": 1, "fare_amount": 17.0, "extra": 0.0, "mta_tax": 0.5, "tip_amount": 4.4,
    "tolls_amount": 0.0, "improvement_surcharge": 1.0, "congestion_surcharge": 2.5, "airport_fee": None,
    "cbd_congestion_fee": 0.75, "total_amount": 26.15,
}


def build(row=ROW, loop=0, rate=0.0):
    return build_trip(row, loop=loop, offset_days=35, trace_sample_rate=rate, produced_at=WALL)


def test_trip_id_and_wall_format():
    assert trip_id(3, 42) == "r3-000000042"
    assert format_wall(WALL) == "2026-10-16T08:02:11.482Z"


def test_trip_matches_contract():
    msg = build()
    assert_valid("trip", msg)
    assert msg["trip_id"] == "r0-000123456" and msg["pickup_ts"] == "2025-10-07T08:14:03"
    assert msg["produced_at"] == "2026-10-16T08:02:11.482Z" and msg["airport_fee"] is None


def test_loop_offset_shifts_timestamps():
    msg = build(loop=1)
    assert msg["trip_id"] == "r1-000123456" and msg["loop"] == 1
    assert msg["pickup_ts"] == "2025-11-11T08:14:03" and msg["dropoff_ts"] == "2025-11-11T08:29:41"


def test_nan_and_missing_values():
    row = dict(ROW, extra=math.nan, passenger_count=None, trip_distance=None, pu_location_id=None,
               payment_type=None, store_and_fwd_flag="?", dropoff_ts=None)
    msg = build(row)
    assert_valid("trip", msg)
    assert msg["extra"] is None and msg["passenger_count"] is None and msg["store_and_fwd_flag"] is None
    # required numbers fall back to values the processor will reject with a clear reason
    assert msg["trip_distance"] == 0.0 and msg["pu_location_id"] == 0 and msg["payment_type"] == 5
    assert msg["dropoff_ts"] == msg["pickup_ts"]


def test_sampling_is_deterministic_and_close_to_rate():
    assert sampled_trace_id("r0-000000001", 0.0) is None
    assert sampled_trace_id("r0-000000001", 1.0) == sampled_trace_id("r0-000000001", 1.0) is not None
    hits = sum(sampled_trace_id(trip_id(0, i), 0.001) is not None for i in range(100_000))
    assert 50 <= hits <= 150


def test_clock_messages_match_contract():
    live = build_clock(sim_ts=datetime(2025, 10, 9, 17, 42, 0, 250_000), at=WALL, speed=60.0, loop=0,
                       row_idx=812344, paused=False)
    assert_valid("sim_clock", live)
    assert live["sim_ts"] == "2025-10-09T17:42:00.250" and live["mode"] == "live"
    backfill = build_clock(sim_ts=datetime(2025, 10, 9), at=WALL, speed=None, loop=0, row_idx=1, paused=False)
    assert_valid("sim_clock", backfill)
    assert backfill["speed"] == "max" and backfill["mode"] == "backfill"


def test_ingested_checkpoint_matches_contract():
    trip = build(rate=1.0)
    cp = build_ingested_checkpoint(trip, topic="trips.raw", partition=1, offset=482113, at=WALL)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "ingested" and cp["seq"] == 2 and cp["service"] == "replayer"
    assert cp["detail"] == {"topic": "trips.raw", "partition": 1, "offset": 482113}
```

`services/replayer/tests/test_state.py`:
```python
from datetime import datetime

from replayer.state import ReplayState, load_state, save_state


def test_missing_state_is_none(tmp_path):
    assert load_state(tmp_path / "nope.json") is None


def test_roundtrip(tmp_path):
    path = tmp_path / "state" / "replayer.json"
    save_state(path, ReplayState(loop=2, row_idx=812344, sim_ts=datetime(2025, 10, 14, 7, 0, 0, 250_000)))
    assert load_state(path) == ReplayState(loop=2, row_idx=812344, sim_ts=datetime(2025, 10, 14, 7, 0, 0, 250_000))


def test_save_is_atomic(tmp_path):
    path = tmp_path / "replayer.json"
    save_state(path, ReplayState(0, 1, datetime(2025, 10, 7)))
    save_state(path, ReplayState(0, 2, datetime(2025, 10, 7)))
    assert [p.name for p in tmp_path.iterdir()] == ["replayer.json"]  # no temp files left behind
    assert load_state(path).row_idx == 2
```

`services/replayer/tests/test_source.py`:
```python
from datetime import datetime

import pytest

from replayer.source import ReplaySource


def test_num_rows_and_seek(replay_parquet):
    src = ReplaySource(replay_parquet)
    assert src.num_rows == 20
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 6, 0)) == 0
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 7, 2, 0)) == 4
    assert src.first_row_at_or_after(datetime(2025, 10, 7, 7, 2, 1)) == 5
    assert src.first_row_at_or_after(datetime(2025, 10, 8)) == 20


def test_rows_from_crosses_row_groups(replay_parquet):
    rows = list(ReplaySource(replay_parquet).rows_from(4))
    assert [r["row_idx"] for r in rows] == list(range(4, 20))
    assert rows[0]["pickup_ts"] == datetime(2025, 10, 7, 7, 2, 0)


def test_rows_from_end_is_empty(replay_parquet):
    assert list(ReplaySource(replay_parquet).rows_from(20)) == []


def test_missing_file_explains_what_to_do(tmp_path):
    with pytest.raises(FileNotFoundError, match="make data"):
        ReplaySource(tmp_path / "missing.parquet")
```
Run: `uv run pytest -q`. Expected: FAIL with `ModuleNotFoundError` for `replayer.config` (and the other modules).

- [ ] **Step 4: Implement `config.py`**

```python
"""Replayer configuration from environment variables (spec 01 §Config)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Config:
    brokers: str
    parquet_path: Path
    speed: float | None  # None = "max" (backfill)
    start_ts: datetime
    until_ts: datetime | None
    loop_offset_days: int
    trace_sample_rate: float
    clock_interval_s: float
    state_path: Path
    control_port: int
    admin_token: str

    @property
    def mode(self) -> str:
        return "backfill" if self.speed is None else "live"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        e = os.environ if env is None else env
        speed_raw = e.get("REPLAY_SPEED", "60").strip().lower()
        speed = None if speed_raw == "max" else float(speed_raw)
        if speed is not None and speed <= 0:
            raise ValueError(f"REPLAY_SPEED must be > 0 or 'max', got {speed_raw!r}")
        until_raw = e.get("REPLAY_UNTIL_TS", "").strip()
        until = datetime.fromisoformat(until_raw) if until_raw else None
        if speed is None and until is None:
            raise ValueError("REPLAY_UNTIL_TS is required when REPLAY_SPEED=max (backfill mode)")
        month = e.get("TLC_MONTH", "2025-10")
        data_dir = Path(e.get("DATA_DIR", "/data"))
        default_parquet = data_dir / "replay" / f"trips_{month}.parquet"
        return cls(
            brokers=e.get("KAFKA_BROKERS", "redpanda:9092"),
            parquet_path=Path(e.get("REPLAY_PARQUET_PATH") or default_parquet),
            speed=speed,
            start_ts=datetime.fromisoformat(e.get("REPLAY_START_TS", "2025-10-07T07:00:00")),
            until_ts=until,
            loop_offset_days=int(e.get("REPLAY_LOOP_OFFSET_DAYS", "35")),
            trace_sample_rate=float(e.get("REPLAY_TRACE_SAMPLE_RATE", "0.001")),
            clock_interval_s=float(e.get("REPLAY_CLOCK_INTERVAL_S", "1")),
            state_path=Path(e.get("REPLAY_STATE_PATH", "/data/state/replayer.json")),
            control_port=int(e.get("REPLAY_CONTROL_PORT", "8001")),
            admin_token=e.get("ADMIN_TOKEN", ""),
        )
```

- [ ] **Step 5: Implement `clock.py`**

```python
"""Simulated clocks (spec 01 §Replayer pacing).

SimClock:    sim_now = sim_anchor + (monotonic - wall_anchor) * speed. It re-anchors on pause, resume and
             speed change, so none of them jump. A wall gap > max_gap_s is not replayed as a burst.
ManualClock: backfill mode. "now" is the pickup time of the last row produced.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timedelta


class SimClock:
    def __init__(self, sim_start: datetime, speed: float, monotonic: Callable[[], float] = time.monotonic,
                 max_gap_s: float = 10.0) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        self._mono = monotonic
        self._speed = float(speed)
        self._sim_anchor = sim_start
        self._wall_anchor = monotonic()
        self._last_tick = self._wall_anchor
        self._paused = False
        self._max_gap = max_gap_s

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    def now(self) -> datetime:
        if self._paused:
            return self._sim_anchor
        return self._sim_anchor + timedelta(seconds=(self._mono() - self._wall_anchor) * self._speed)

    def _reanchor(self) -> None:
        self._sim_anchor = self.now()
        self._wall_anchor = self._mono()

    def pause(self) -> None:
        if not self._paused:
            self._reanchor()
            self._paused = True

    def resume(self) -> None:
        if self._paused:
            self._wall_anchor = self._last_tick = self._mono()
            self._paused = False

    def set_speed(self, speed: float) -> None:
        if speed <= 0:
            raise ValueError("speed must be > 0")
        if not self._paused:
            self._reanchor()
        self._speed = float(speed)

    def tick(self) -> datetime:
        """Call once per loop iteration."""
        mono = self._mono()
        if not self._paused and mono - self._last_tick > self._max_gap:
            # Freeze sim time at the last tick instead of replaying the gap.
            self._sim_anchor += timedelta(seconds=(self._last_tick - self._wall_anchor) * self._speed)
            self._wall_anchor = mono
        self._last_tick = mono
        return self.now()


class ManualClock:
    paused = False

    def __init__(self, sim_start: datetime) -> None:
        self._now = sim_start

    def now(self) -> datetime:
        return self._now

    def tick(self) -> datetime:
        return self._now

    def advance_to(self, ts: datetime) -> None:
        if ts > self._now:
            self._now = ts
```

- [ ] **Step 6: Implement `messages.py`**

```python
"""Build messages that follow contracts/events/{trip,sim_clock,trace_checkpoint}.schema.json."""

from __future__ import annotations

import hashlib
import math
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

TRACE_NAMESPACE = uuid.UUID("6f1c2d3e-4b5a-4c6d-8e7f-9a0b1c2d3e4f")


def format_sim(dt: datetime, *, millis: bool = False) -> str:
    return dt.replace(tzinfo=None).isoformat(timespec="milliseconds" if millis else "seconds")


def format_wall(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def trip_id(loop: int, row_idx: int) -> str:
    return f"r{loop}-{row_idx:09d}"


def sampled_trace_id(tid: str, rate: float) -> str | None:
    """Deterministic: the same trip_id always gets the same decision and the same trace_id."""
    bucket = int(hashlib.sha1(tid.encode()).hexdigest()[:8], 16) % 10_000
    return str(uuid.uuid5(TRACE_NAMESPACE, tid)) if bucket < round(rate * 10_000) else None


def _num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return round(float(v), 2)


def _int(v: Any) -> int | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return int(v)


def build_trip(row: Mapping[str, Any], *, loop: int, offset_days: int, trace_sample_rate: float,
               produced_at: datetime) -> dict[str, Any]:
    shift = timedelta(days=loop * offset_days)
    tid = trip_id(loop, int(row["row_idx"]))
    pickup: datetime = row["pickup_ts"] + shift
    dropoff = row["dropoff_ts"] + shift if row.get("dropoff_ts") is not None else pickup
    flag = row.get("store_and_fwd_flag")
    payment = _int(row.get("payment_type"))
    return {
        "trip_id": tid,
        "source": "replay",
        "trace_id": sampled_trace_id(tid, trace_sample_rate),
        "loop": loop,
        "vendor_id": _int(row.get("vendor_id")),
        "pickup_ts": format_sim(pickup),
        "dropoff_ts": format_sim(dropoff),
        "passenger_count": _int(row.get("passenger_count")),
        "trip_distance": _num(row.get("trip_distance")) or 0.0,
        "ratecode_id": _int(row.get("ratecode_id")),
        "store_and_fwd_flag": flag if flag in ("Y", "N") else None,
        "pu_location_id": _int(row.get("pu_location_id")) or 0,
        "do_location_id": _int(row.get("do_location_id")) or 0,
        "payment_type": 5 if payment is None else payment,
        "fare_amount": _num(row.get("fare_amount")) or 0.0,
        "extra": _num(row.get("extra")),
        "mta_tax": _num(row.get("mta_tax")),
        "tip_amount": _num(row.get("tip_amount")) or 0.0,
        "tolls_amount": _num(row.get("tolls_amount")),
        "improvement_surcharge": _num(row.get("improvement_surcharge")),
        "congestion_surcharge": _num(row.get("congestion_surcharge")),
        "airport_fee": _num(row.get("airport_fee")),
        "cbd_congestion_fee": _num(row.get("cbd_congestion_fee")),
        "total_amount": _num(row.get("total_amount")) or 0.0,
        "produced_at": format_wall(produced_at),
    }


def build_clock(*, sim_ts: datetime, at: datetime, speed: float | None, loop: int, row_idx: int,
                paused: bool) -> dict[str, Any]:
    return {
        "sim_ts": format_sim(sim_ts, millis=True),
        "at": format_wall(at),
        "speed": "max" if speed is None else speed,
        "loop": loop,
        "row_idx": row_idx,
        "paused": paused,
        "mode": "backfill" if speed is None else "live",
    }


def build_ingested_checkpoint(trip: Mapping[str, Any], *, topic: str, partition: int, offset: int,
                              at: datetime) -> dict[str, Any]:
    return {
        "trace_id": trip["trace_id"],
        "trip_id": trip["trip_id"],
        "source": trip["source"],
        "stage": "ingested",
        "seq": 2,
        "service": "replayer",
        "at": format_wall(at),
        "detail": {"topic": topic, "partition": partition, "offset": offset},
    }
```

- [ ] **Step 7: Implement `state.py`**

```python
"""Replay position persisted atomically, so a restart continues exactly where it stopped (spec 01)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ReplayState:
    loop: int
    row_idx: int  # next row to produce
    sim_ts: datetime


def load_state(path: Path) -> ReplayState | None:
    if not path.exists():
        return None
    d = json.loads(path.read_text())
    return ReplayState(loop=int(d["loop"]), row_idx=int(d["row_idx"]), sim_ts=datetime.fromisoformat(d["sim_ts"]))


def save_state(path: Path, state: ReplayState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({"loop": state.loop, "row_idx": state.row_idx, "sim_ts": state.sim_ts.isoformat()}))
    os.replace(tmp, path)
```

- [ ] **Step 8: Implement `source.py`**

```python
"""Row access to the prepared, pickup-sorted replay parquet (spec 01 §Data prep)."""

from __future__ import annotations

import bisect
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq


class ReplaySource:
    def __init__(self, path: Path) -> None:
        if not path.exists():
            raise FileNotFoundError(f"{path} not found — run `make data` first")
        self._file = pq.ParquetFile(path)
        self.num_rows: int = self._file.metadata.num_rows
        self._pickups = self._file.read(columns=["pickup_ts"]).column("pickup_ts").to_numpy()
        starts, acc = [], 0
        for i in range(self._file.metadata.num_row_groups):
            starts.append(acc)
            acc += self._file.metadata.row_group(i).num_rows
        self._group_starts = starts

    def first_row_at_or_after(self, ts: datetime) -> int:
        return int(np.searchsorted(self._pickups, np.datetime64(ts, "us"), side="left"))

    def rows_from(self, row_idx: int, batch_size: int = 10_000) -> Iterator[dict[str, Any]]:
        if row_idx >= self.num_rows:
            return
        group = bisect.bisect_right(self._group_starts, row_idx) - 1
        skip = row_idx - self._group_starts[group]
        for g in range(group, len(self._group_starts)):
            table = self._file.read_row_group(g)
            if skip:
                table, skip = table.slice(skip), 0
            for batch in table.to_batches(max_chunksize=batch_size):
                yield from batch.to_pylist()
```

- [ ] **Step 9: Run the tests**

Run: `cd services/replayer && uv run pytest -q`. Expected: `27 passed`.

- [ ] **Step 10: Commit**

```bash
git add services/replayer
git commit -m "feat(replayer): config, sim clock, contract messages, atomic state, parquet source"
```

---

### Task 6: Replayer engine: pacing, loop, backfill, backpressure, control API

**Files:**
- Create: `services/replayer/src/replayer/{log,engine,control}.py`
- Test: `services/replayer/tests/{test_engine,test_control}.py`

**Interfaces:**
- Consumes: everything from Task 5.
- Produces:
  - `engine.Replayer(cfg, source, out, *, monotonic=time.monotonic, wall=utc_now, heartbeat_path: Path | None = None)` with:
    - `step(max_rows=5000) -> int`
    - `housekeeping(force=False)`
    - `relieve_backpressure()`
    - `run(stop: threading.Event)`
    - `pause()`, `resume()`, `set_speed(s)` (raises `ValueError` in backfill), `status() -> dict`
    - attributes `state: ReplayState`, `finished: bool`, `paused: bool`, `speed: float | None`
  - `engine.Out`, a Protocol: `send(topic, key, value: dict, on_delivery: Callable[[int, int], None] | None = None)`, `poll(timeout=0.0)`, `pending() -> int`, `flush(timeout=30.0) -> int`. `send` may raise `BufferError`.
  - Topic constants `TRIPS_TOPIC="trips.raw"`, `CLOCK_TOPIC="sim.clock"`, `TRACE_TOPIC="trace.checkpoints"`.
  - `control.make_server(replayer, port, token) -> ThreadingHTTPServer` with `GET /status` (open) and `POST /pause|/resume|/speed` (`X-Admin-Token` required; an empty token means every POST is refused).
  - `log.event(name: str, **fields)` prints one JSON line.

- [ ] **Step 1: Write the failing engine tests**

`services/replayer/tests/test_engine.py`:
```python
import json
import threading
from datetime import datetime, timezone

import pytest
from contract_validator import assert_valid

from replayer.config import Config
from replayer.engine import Replayer
from replayer.messages import trip_id
from replayer.source import ReplaySource
from replayer.state import load_state


def wall():
    return datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc)


class FakeOut:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.sent: list[tuple[str, str, dict]] = []
        self.fail_on = set(fail_on or ())
        self.pending_n = 0
        self._offset = 0

    def send(self, topic, key, value, on_delivery=None):
        if len(self.sent) in self.fail_on:
            self.fail_on.discard(len(self.sent))
            raise BufferError("Local: Queue full")
        self.sent.append((topic, key, value))
        if on_delivery:
            self._offset += 1
            on_delivery(0, self._offset)

    def poll(self, timeout=0.0):
        pass

    def pending(self):
        return self.pending_n

    def flush(self, timeout=30.0):
        return 0

    def on(self, topic):
        return [v for t, _, v in self.sent if t == topic]


def make(tmp_path, parquet, mono, **env):
    base = {"REPLAY_START_TS": "2025-10-07T07:00:00", "REPLAY_STATE_PATH": str(tmp_path / "state.json"),
            "REPLAY_PARQUET_PATH": str(parquet), "REPLAY_TRACE_SAMPLE_RATE": "0"}
    out = FakeOut()
    rep = Replayer(Config.from_env(base | env), ReplaySource(parquet), out, monotonic=mono, wall=wall)
    return rep, out


def ids(out):
    return [t["trip_id"] for t in out.on("trips.raw")]


def test_only_due_rows_are_produced(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    assert rep.step() == 1  # sim 07:00:00 → only the 07:00:00 trip
    mono.t = 1.0            # sim 07:01:00 → 07:00:30 and 07:01:00 are due
    assert rep.step() == 2
    assert ids(out) == [trip_id(0, i) for i in range(3)]
    for t in out.on("trips.raw"):
        assert_valid("trip", t)


def test_loops_with_offset_after_end_of_file(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="100000")
    mono.t = 1.0
    assert rep.step(max_rows=100) == 20
    assert rep.state.loop == 1 and rep.state.row_idx == 0
    for _ in range(8):  # small wall steps: a jump > 10 s would be treated as a freeze (see test_clock)
        mono.t += 5.0
        rep.step(max_rows=100)
    loop1 = out.on("trips.raw")[20]
    assert loop1["trip_id"] == "r1-000000000" and loop1["pickup_ts"] == "2025-11-11T07:00:00"


def test_backfill_stops_at_until_and_live_resumes_there(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="max", REPLAY_UNTIL_TS="2025-10-07T07:04:00")
    rep.run(threading.Event())
    assert rep.finished and ids(out) == [trip_id(0, i) for i in range(9)]  # 07:00:00 … 07:04:00
    saved = load_state(tmp_path / "state.json")
    assert saved.row_idx == 9 and saved.sim_ts == datetime(2025, 10, 7, 7, 4)
    clocks = out.on("sim.clock")
    assert clocks and clocks[-1]["mode"] == "backfill"
    assert_valid("sim_clock", clocks[-1])

    live, out2 = make(tmp_path, replay_parquet, mono)
    assert live.state.row_idx == 9 and live.clock.now() == datetime(2025, 10, 7, 7, 4)
    assert live.step() == 0
    mono.t += 0.5  # +30 sim seconds
    assert live.step() == 1 and ids(out2) == [trip_id(0, 9)]


def test_traced_trips_emit_ingested_checkpoint(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono, REPLAY_TRACE_SAMPLE_RATE="1")
    rep.step()
    (t1, k1, trip), (t2, k2, cp) = out.sent
    assert (t1, t2) == ("trips.raw", "trace.checkpoints") and k1 == trip["trip_id"] and k2 == trip["trace_id"]
    assert_valid("trace_checkpoint", cp)
    assert cp["trip_id"] == trip["trip_id"] and cp["detail"]["offset"] == 1


def test_pause_and_resume(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    rep.step()
    rep.pause()
    mono.t = 10.0
    assert rep.step() == 0 and rep.status()["paused"] is True
    rep.resume()
    mono.t = 11.0  # only 1 wall second since resume → sim 07:01:00
    rep.step()
    assert ids(out) == [trip_id(0, i) for i in range(3)]


def test_speed_cannot_change_in_backfill(tmp_path, replay_parquet, mono):
    rep, _ = make(tmp_path, replay_parquet, mono, REPLAY_SPEED="max", REPLAY_UNTIL_TS="2025-10-07T07:04:00")
    with pytest.raises(ValueError):
        rep.set_speed(120)


def test_backpressure_keeps_the_row(tmp_path, replay_parquet, mono):
    rep, out = make(tmp_path, replay_parquet, mono)
    out.fail_on = {1}
    mono.t = 1.0
    assert rep.step() == 1 and rep.paused
    assert rep.step() == 0
    out.pending_n = 0
    rep.relieve_backpressure()
    assert not rep.paused
    mono.t = 2.0  # clock resumed at t=1 (sim 07:01) → now 07:02
    rep.step()
    assert ids(out) == [trip_id(0, i) for i in range(5)]


def test_housekeeping_heartbeat_state_and_clock(tmp_path, replay_parquet, mono):
    heartbeat = tmp_path / "healthy"
    rep, out = make(tmp_path, replay_parquet, mono)
    rep._heartbeat = heartbeat
    rep.housekeeping(force=True)
    (clock,) = out.on("sim.clock")
    assert_valid("sim_clock", clock)
    assert [k for t, k, _ in out.sent] == ["clock"] and heartbeat.exists()
    assert json.loads((tmp_path / "state.json").read_text())["row_idx"] == 0


def test_fresh_start_seeks_to_start_ts(tmp_path, replay_parquet, mono):
    rep, _ = make(tmp_path, replay_parquet, mono, REPLAY_START_TS="2025-10-07T07:05:00")
    assert rep.state.row_idx == 10
```
Run: `uv run pytest tests/test_engine.py -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'replayer.engine'`.

- [ ] **Step 2: Implement `log.py` and `engine.py`**

`services/replayer/src/replayer/log.py`:
```python
"""Structured JSON logs on stdout (CLAUDE.md conventions)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

SERVICE = "replayer"


def event(name: str, **fields: Any) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "service": SERVICE,
              "event": name, **fields}
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)
```

`services/replayer/src/replayer/engine.py`:
```python
"""Replay engine: paces rows by the sim clock, loops at EOF, backfills, survives backpressure (spec 01)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from replayer.clock import ManualClock, SimClock
from replayer.config import Config
from replayer.log import event
from replayer.messages import build_clock, build_ingested_checkpoint, build_trip, format_sim
from replayer.source import ReplaySource
from replayer.state import ReplayState, load_state, save_state

TRIPS_TOPIC = "trips.raw"
CLOCK_TOPIC = "sim.clock"
TRACE_TOPIC = "trace.checkpoints"
STATE_SAVE_INTERVAL_S = 5.0
PROGRESS_INTERVAL_S = 10.0
RESUME_BELOW_PENDING = 50_000
IDLE_SLEEP_S = 0.02


class Out(Protocol):
    def send(self, topic: str, key: str, value: dict[str, Any],
             on_delivery: Callable[[int, int], None] | None = None) -> None: ...
    def poll(self, timeout: float = 0.0) -> None: ...
    def pending(self) -> int: ...
    def flush(self, timeout: float = 30.0) -> int: ...


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Replayer:
    def __init__(self, cfg: Config, source: ReplaySource, out: Out, *,
                 monotonic: Callable[[], float] = time.monotonic, wall: Callable[[], datetime] = utc_now,
                 heartbeat_path: Path | None = None) -> None:
        self.cfg, self.source, self.out = cfg, source, out
        self._mono, self._wall, self._heartbeat = monotonic, wall, heartbeat_path
        state = load_state(cfg.state_path)
        if state is None:
            state = ReplayState(loop=0, row_idx=source.first_row_at_or_after(cfg.start_ts), sim_ts=cfg.start_ts)
        self.state = state
        self.clock: SimClock | ManualClock = (
            ManualClock(state.sim_ts) if cfg.speed is None else SimClock(state.sim_ts, cfg.speed, monotonic))
        self._rows: Iterator[dict[str, Any]] = source.rows_from(state.row_idx)
        self._next: dict[str, Any] | None = next(self._rows, None)
        self._lock = threading.RLock()
        self._user_paused = False
        self._backpressure = False
        self.finished = False
        self.sent_total = 0
        self._sent_since_progress = 0
        now = monotonic()
        self._last_clock = float("-inf")
        self._last_save = now
        self._last_progress = now

    # ── pacing ────────────────────────────────────────────────────────────
    def step(self, max_rows: int = 5_000) -> int:
        """Produce every row whose (loop-shifted) pickup_ts is due. Returns the number produced."""
        with self._lock:
            if self.finished or self._backpressure:
                return 0
            now = self.clock.tick()
            produced = 0
            while produced < max_rows:
                row = self._current_row()
                if row is None:
                    break
                due = row["pickup_ts"] + timedelta(days=self.state.loop * self.cfg.loop_offset_days)
                if self.cfg.speed is None:
                    if self.cfg.until_ts is not None and due > self.cfg.until_ts:
                        self._finish()
                        break
                elif self.clock.paused or due > now:
                    break
                try:
                    self._produce(row)
                except BufferError:
                    self._enter_backpressure()
                    break
                if isinstance(self.clock, ManualClock):
                    self.clock.advance_to(due)
                produced += 1
            self.sent_total += produced
            self._sent_since_progress += produced
            return produced

    def _current_row(self) -> dict[str, Any] | None:
        if self._next is None:
            if self.source.num_rows == 0:
                return None
            self.state.loop += 1
            self.state.row_idx = 0
            self._rows = self.source.rows_from(0)
            self._next = next(self._rows, None)
            event("loop", loop=self.state.loop)
        return self._next

    def _produce(self, row: dict[str, Any]) -> None:
        msg = build_trip(row, loop=self.state.loop, offset_days=self.cfg.loop_offset_days,
                         trace_sample_rate=self.cfg.trace_sample_rate, produced_at=self._wall())
        on_delivery = self._checkpoint_on_delivery(msg) if msg["trace_id"] else None
        self.out.send(TRIPS_TOPIC, msg["trip_id"], msg, on_delivery)  # may raise BufferError: row not consumed
        self.state.row_idx = int(row["row_idx"]) + 1
        self._next = next(self._rows, None)

    def _checkpoint_on_delivery(self, msg: dict[str, Any]) -> Callable[[int, int], None]:
        def on_delivery(partition: int, offset: int) -> None:
            cp = build_ingested_checkpoint(msg, topic=TRIPS_TOPIC, partition=partition, offset=offset,
                                           at=self._wall())
            try:
                self.out.send(TRACE_TOPIC, msg["trace_id"], cp)
            except BufferError:
                event("checkpoint_dropped", trace_id=msg["trace_id"])
        return on_delivery

    def _finish(self) -> None:
        self.finished = True
        if self.cfg.until_ts is not None and isinstance(self.clock, ManualClock):
            self.clock.advance_to(self.cfg.until_ts)
        event("backfill_done", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx)

    # ── backpressure ──────────────────────────────────────────────────────
    def _enter_backpressure(self) -> None:
        self._backpressure = True
        if isinstance(self.clock, SimClock):
            self.clock.pause()
        event("broker_unavailable", pending=self.out.pending())

    def relieve_backpressure(self) -> None:
        with self._lock:
            if not self._backpressure:
                return
            self.out.poll(0.5)
            if self.out.pending() < RESUME_BELOW_PENDING:
                self._backpressure = False
                if isinstance(self.clock, SimClock) and not self._user_paused:
                    self.clock.resume()
                event("broker_available")

    # ── control ───────────────────────────────────────────────────────────
    @property
    def paused(self) -> bool:
        return self._user_paused or self._backpressure

    @property
    def speed(self) -> float | None:
        return self.clock.speed if isinstance(self.clock, SimClock) else None

    def pause(self) -> None:
        with self._lock:
            self._user_paused = True
            if isinstance(self.clock, SimClock):
                self.clock.pause()

    def resume(self) -> None:
        with self._lock:
            self._user_paused = False
            if isinstance(self.clock, SimClock) and not self._backpressure:
                self.clock.resume()

    def set_speed(self, speed: float) -> None:
        with self._lock:
            if not isinstance(self.clock, SimClock):
                raise ValueError("speed can't change in backfill mode")
            self.clock.set_speed(speed)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"mode": self.cfg.mode, "sim_ts": format_sim(self.clock.now(), millis=True),
                    "speed": "max" if self.speed is None else self.speed, "paused": self.paused,
                    "backpressure": self._backpressure, "loop": self.state.loop, "row_idx": self.state.row_idx,
                    "sent_total": self.sent_total, "producer_queue": self.out.pending(),
                    "finished": self.finished}

    # ── housekeeping & main loop ──────────────────────────────────────────
    def housekeeping(self, force: bool = False) -> None:
        with self._lock:
            mono = self._mono()
            if force or mono - self._last_clock >= self.cfg.clock_interval_s:
                self._last_clock = mono
                clock = build_clock(sim_ts=self.clock.now(), at=self._wall(), speed=self.speed,
                                    loop=self.state.loop, row_idx=self.state.row_idx, paused=self.paused)
                try:
                    self.out.send(CLOCK_TOPIC, "clock", clock)
                except BufferError:
                    pass
                if self._heartbeat is not None:
                    self._heartbeat.touch()
            if force or mono - self._last_save >= STATE_SAVE_INTERVAL_S:
                self._last_save = mono
                self.state.sim_ts = self.clock.now()
                save_state(self.cfg.state_path, self.state)
            if mono - self._last_progress >= PROGRESS_INTERVAL_S:
                elapsed = mono - self._last_progress
                event("progress", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx,
                      rows_sent_10s=self._sent_since_progress,
                      rate_per_s=round(self._sent_since_progress / elapsed, 1),
                      producer_queue=self.out.pending(), paused=self.paused)
                self._last_progress, self._sent_since_progress = mono, 0

    def run(self, stop: threading.Event) -> None:
        event("start", mode=self.cfg.mode, sim_ts=self.clock.now(), loop=self.state.loop,
              row_idx=self.state.row_idx, speed=self.speed or "max")
        while not stop.is_set() and not self.finished:
            produced = self.step()
            self.out.poll(0)
            self.housekeeping()
            if self._backpressure:
                self.relieve_backpressure()
            elif produced == 0:
                time.sleep(IDLE_SLEEP_S)
        self.out.flush(30)
        self.housekeeping(force=True)
        event("stop", sim_ts=self.clock.now(), loop=self.state.loop, row_idx=self.state.row_idx,
              sent_total=self.sent_total)
```
Run: `uv run pytest tests/test_engine.py -q`. Expected: `9 passed`.

- [ ] **Step 3: Write the failing control API test**

`services/replayer/tests/test_control.py`:
```python
import json
import threading
import urllib.error
import urllib.request

import pytest

from replayer.control import make_server


class FakeReplayer:
    def __init__(self):
        self.calls = []

    def pause(self): self.calls.append("pause")
    def resume(self): self.calls.append("resume")

    def set_speed(self, s):
        if s <= 0:
            raise ValueError("speed must be > 0")
        self.calls.append(("speed", s))

    def status(self): return {"paused": "pause" in self.calls}


@pytest.fixture
def server():
    rep = FakeReplayer()
    srv = make_server(rep, 0, "s3cret")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield rep, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def call(url, method="GET", token=None, body=None):
    headers = {"X-Admin-Token": token} if token else {}
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def test_status_is_open(server):
    _, base = server
    assert call(f"{base}/status") == (200, {"paused": False})


def test_post_requires_token(server):
    rep, base = server
    assert call(f"{base}/pause", "POST")[0] == 401
    assert call(f"{base}/pause", "POST", token="wrong")[0] == 401
    assert rep.calls == []


def test_pause_resume_speed(server):
    rep, base = server
    assert call(f"{base}/pause", "POST", "s3cret") == (200, {"paused": True})
    call(f"{base}/resume", "POST", "s3cret")
    assert call(f"{base}/speed", "POST", "s3cret", {"speed": 120})[0] == 200
    assert rep.calls == ["pause", "resume", ("speed", 120.0)]


@pytest.mark.parametrize("body", [{}, {"speed": "fast"}, {"speed": -1}])
def test_bad_speed_is_400(server, body):
    _, base = server
    assert call(f"{base}/speed", "POST", "s3cret", body)[0] == 400


def test_empty_token_refuses_everything():
    srv = make_server(FakeReplayer(), 0, "")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        assert call(f"http://127.0.0.1:{srv.server_address[1]}/pause", "POST", token="")[0] == 401
    finally:
        srv.shutdown()


def test_unknown_path_is_404(server):
    _, base = server
    assert call(f"{base}/nope")[0] == 404
```
Run: `uv run pytest tests/test_control.py -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'replayer.control'`.

- [ ] **Step 4: Implement `control.py`**

```python
"""Operator control API on 127.0.0.1:8001 (spec 01): GET /status, POST /pause|/resume|/speed."""

from __future__ import annotations

import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol


class Controllable(Protocol):
    def pause(self) -> None: ...
    def resume(self) -> None: ...
    def set_speed(self, speed: float) -> None: ...
    def status(self) -> dict[str, Any]: ...


def make_server(replayer: Controllable, port: int, token: str) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authorized(self) -> bool:
            supplied = self.headers.get("X-Admin-Token", "")
            return bool(token) and hmac.compare_digest(supplied, token)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/status":
                self._json(200, replayer.status())
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in ("/pause", "/resume", "/speed"):
                self._json(404, {"error": "not found"})
                return
            if not self._authorized():
                self._json(401, {"error": "missing or wrong X-Admin-Token"})
                return
            if self.path == "/pause":
                replayer.pause()
            elif self.path == "/resume":
                replayer.resume()
            else:
                length = int(self.headers.get("Content-Length", "0"))
                try:
                    body = json.loads(self.rfile.read(length) or b"{}")
                    replayer.set_speed(float(body["speed"]))
                except (KeyError, TypeError, ValueError) as e:
                    self._json(400, {"error": f"expected {{\"speed\": <number > 0>}}: {e}"})
                    return
            self._json(200, replayer.status())

        def log_message(self, format: str, *args: Any) -> None:  # silence default stderr logging
            pass

    return ThreadingHTTPServer(("0.0.0.0", port), Handler)
```
Run: `uv run pytest -q`. Expected: `44 passed` (27 + 9 + 8).

- [ ] **Step 5: Commit**

```bash
git add services/replayer
git commit -m "feat(replayer): pacing engine (loop, backfill, backpressure) + control API"
```

---

### Task 7: Replayer service: Kafka producer, entrypoint, image, compose, integration test

**Files:**
- Create: `services/replayer/src/replayer/{producer,health,main}.py`, `services/replayer/Dockerfile`, `services/replayer/.dockerignore`, `services/replayer/tests/test_integration.py`
- Modify: `docker-compose.yml` (add the `replayer` service)

**Interfaces:**
- Consumes: `Replayer`, `Out`, `make_server`, `Config`, `ReplaySource` (Tasks 5–6).
- Produces: `KafkaOut(brokers)`, which implements `Out` over confluent-kafka (`acks=all`, idempotent, `linger.ms=20`, lz4, 100 k queue). Also `python -m replayer.main` and `python -m replayer.health [path] [max_age_s]`.

- [ ] **Step 1: Write the failing integration test**

`services/replayer/tests/test_integration.py`:
```python
"""Backfill a tiny file into a real Redpanda and check what landed (needs Docker)."""

import json
import threading
import time

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from contract_validator import assert_valid
from testcontainers.kafka import RedpandaContainer

from conftest import make_replay_parquet
from replayer.config import Config
from replayer.engine import Replayer
from replayer.producer import KafkaOut
from replayer.source import ReplaySource

pytestmark = pytest.mark.integration


def consume_all(bootstrap: str, topic: str, expected: int, timeout_s: float = 30) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"it-{topic}", "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    out: list[dict] = []
    deadline = time.monotonic() + timeout_s
    while len(out) < expected and time.monotonic() < deadline:
        msg = c.poll(1.0)
        if msg is not None and msg.error() is None:
            out.append(json.loads(msg.value()))
    c.close()
    return out


def test_backfill_into_redpanda(tmp_path):
    with RedpandaContainer("redpandadata/redpanda:v26.2.3") as rp:
        bootstrap = rp.get_bootstrap_server()
        topics = [NewTopic(t, 3, 1) for t in ("trips.raw", "trace.checkpoints")] + [NewTopic("sim.clock", 1, 1)]
        for f in AdminClient({"bootstrap.servers": bootstrap}).create_topics(topics).values():
            f.result(30)
        parquet = make_replay_parquet(tmp_path / "trips.parquet", n=40)
        cfg = Config.from_env({"KAFKA_BROKERS": bootstrap, "REPLAY_PARQUET_PATH": str(parquet),
                               "REPLAY_SPEED": "max", "REPLAY_UNTIL_TS": "2025-10-07T08:00:00",
                               "REPLAY_START_TS": "2025-10-07T07:00:00", "REPLAY_TRACE_SAMPLE_RATE": "0.25",
                               "REPLAY_STATE_PATH": str(tmp_path / "state.json")})
        rep = Replayer(cfg, ReplaySource(parquet), KafkaOut(bootstrap))
        rep.run(threading.Event())

        trips = consume_all(bootstrap, "trips.raw", 40)
        assert sorted(t["trip_id"] for t in trips) == [f"r0-{i:09d}" for i in range(40)]
        for t in trips:
            assert_valid("trip", t)
        traced = [t for t in trips if t["trace_id"]]
        checkpoints = consume_all(bootstrap, "trace.checkpoints", len(traced))
        assert {c["trace_id"] for c in checkpoints} == {t["trace_id"] for t in traced}
        for c in checkpoints:
            assert_valid("trace_checkpoint", c)
        clocks = consume_all(bootstrap, "sim.clock", 1)
        assert clocks and clocks[0]["mode"] == "backfill"
```
Run: `uv run pytest -m integration -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'replayer.producer'`.

- [ ] **Step 2: Implement `producer.py`, `health.py`, `main.py`**

`services/replayer/src/replayer/producer.py`:
```python
"""confluent-kafka producer behind the engine's Out protocol."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from replayer.log import event


class KafkaOut:
    def __init__(self, brokers: str) -> None:
        self._p = Producer({
            "bootstrap.servers": brokers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 20,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 100_000,
        })

    def send(self, topic: str, key: str, value: dict[str, Any],
             on_delivery: Callable[[int, int], None] | None = None) -> None:
        def callback(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                event("delivery_failed", topic=topic, key=key, error=str(err))
            elif on_delivery is not None:
                on_delivery(msg.partition(), msg.offset())

        self._p.produce(topic, key=key.encode(), value=json.dumps(value, separators=(",", ":")).encode(),
                        on_delivery=callback)

    def poll(self, timeout: float = 0.0) -> None:
        self._p.poll(timeout)

    def pending(self) -> int:
        return len(self._p)

    def flush(self, timeout: float = 30.0) -> int:
        return int(self._p.flush(timeout))
```

`services/replayer/src/replayer/health.py`:
```python
"""Container healthcheck: exit 0 if the heartbeat file was touched recently (python -m replayer.health)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

HEARTBEAT = Path("/tmp/healthy")


def main(path: str = str(HEARTBEAT), max_age_s: str = "30") -> int:
    p = Path(path)
    return 0 if p.exists() and time.time() - p.stat().st_mtime < float(max_age_s) else 1


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
```

`services/replayer/src/replayer/main.py`:
```python
"""Entrypoint: python -m replayer.main (spec 01)."""

from __future__ import annotations

import signal
import threading

from replayer.config import Config
from replayer.control import make_server
from replayer.engine import Replayer
from replayer.health import HEARTBEAT
from replayer.log import event
from replayer.producer import KafkaOut
from replayer.source import ReplaySource


def main() -> int:
    try:
        cfg = Config.from_env()
        source = ReplaySource(cfg.parquet_path)
    except (ValueError, FileNotFoundError) as e:
        event("startup_failed", error=str(e))
        return 2
    rep = Replayer(cfg, source, KafkaOut(cfg.brokers), heartbeat_path=HEARTBEAT)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    server = None
    if cfg.mode == "live":
        server = make_server(rep, cfg.control_port, cfg.admin_token)
        threading.Thread(target=server.serve_forever, name="control", daemon=True).start()
    rep.run(stop)
    if server is not None:
        server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```
Run: `uv run pytest -m integration -q`. Expected: `1 passed` (the first run pulls the Redpanda image).
Run: `uv run pytest -q`. Expected: `44 passed`.

- [ ] **Step 3: Image and compose service**

`services/replayer/.dockerignore`:
```
.venv
__pycache__
.pytest_cache
.ruff_cache
.mypy_cache
tests
```

`services/replayer/Dockerfile`:
```Dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 TZ=UTC PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev
RUN useradd --system --uid 10001 app && mkdir -p /data/state && chown app /data/state
USER app
CMD ["python", "-m", "replayer.main"]
```

Add to `docker-compose.yml` under `services:`. Put it after `caddy`, before `volumes:`.
```yaml
  replayer:
    build: services/replayer
    env_file: [.env]
    environment:
      DATA_DIR: /data
      KAFKA_BROKERS: redpanda:9092
      TZ: UTC
    depends_on:
      redpanda-init: { condition: service_completed_successfully }
    ports: ["127.0.0.1:8001:8001"]
    volumes:
      - ./data/replay:/data/replay:ro
      - replayer-state:/data/state
    healthcheck:
      test: ["CMD", "python", "-m", "replayer.health"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 30s
    mem_limit: 512m
    restart: unless-stopped
    logging: *logging
```
Run: `docker compose up -d --build --wait replayer && sleep 15 && curl -s 127.0.0.1:8001/status && docker compose exec -T redpanda rpk topic consume sim.clock -n 1 -o -1`
Expected:
- The status JSON shows `"mode": "live"`, `sim_ts` moving at about 1 sim minute per wall second, and `sent_total` > 0.
- One sim.clock message.

Then `make pause && make resume && make speed S=120 && make speed S=60`. Expected: each returns the status JSON with the change applied.

- [ ] **Step 4: Commit**

```bash
git add services/replayer docker-compose.yml
git commit -m "feat(replayer): kafka producer, entrypoint, image, compose service"
```

---
### Task 8: Processor core (pure): times, rules, zones, enrich, aggregate, checkpoints, rows, config

**Files:**
- Create: `services/processor/pyproject.toml`, `services/processor/src/processor/{__init__,config,times,rules,zones,enrich,aggregate,checkpoints,rows}.py`, `services/processor/tests/fixtures/zones.csv`
- Test: `services/processor/tests/{conftest,test_config,test_times,test_rules,test_zones,test_enrich,test_aggregate,test_checkpoints,test_rows}.py`
- Modify: `contracts/events/enriched_trip.schema.json` (example `avg_speed_mph` 11.02 → 11.01), `docs/specs/03-stream-processor.md` (`avg_speed_mph` "1 decimal" → "2 decimals")

**Interfaces:**
- Produces:
  - `times`: `parse_sim(v) -> datetime | None` (naive), `format_sim(dt) -> str`, `format_wall(dt_aware) -> str`, `parse_wall(s) -> datetime` (aware UTC), `wall_now_str() -> str`, `sim_epoch_ms(dt) -> int`, `sim_from_epoch_ms(ms) -> datetime`.
  - `rules` (stdlib-only):
    - `REASON_CODES: tuple[str, ...]` (11), `RULES_CHECKED = 11`
    - `check_trip(t, tip_max_abs=100.0) -> list[str]`
    - `annotate(raw: bytes | str | Mapping, tip_max_abs=100.0) -> dict`, which always has a `_reasons` key. Unparseable input yields `{"_reasons": ["SCHEMA_INVALID"], "_raw": text}`.
    - `is_uuid(v) -> bool`, `is_traced(v) -> bool`, `to_rejected(v, rejected_at) -> dict` (rejected_trip contract).
  - `zones`: `Zone(zone, borough, service_zone)` NamedTuple, `UNKNOWN`, `load_zones(path) -> dict[int, Zone]`, `zone_for(zones, location_id) -> Zone`.
  - `enrich`: `TRIP_FIELDS`, `PAYMENT_NAMES`, `AIRPORT_IDS`, `enrich(trip, zones, processed_at) -> dict` (enriched_trip contract, no `_` keys).
  - `aggregate`: `init_bucket(trip) -> dict`, `add_trip(bucket, trip) -> dict`, `to_agg_record(window: {"start": ms, "end": ms, "value": bucket}, borough, emitted_at) -> dict` (borough_minute_agg contract).
  - `checkpoints`: `SEQ`, `checkpoint(trip, stage, detail, at)`, `validated_or_rejected(v, at) -> dict`, `enriched_and_aggregated(v, at) -> list[dict]`, `stored_checkpoint(v, table, batch_rows, at) -> dict`.
  - `rows`: `TRIPS_COLUMNS`, `REJECTED_COLUMNS`, `AGG_COLUMNS`, `trip_row(v) -> list`, `rejected_row(v) -> list`, `agg_row(v) -> list`. `*_ts` values become **aware UTC** datetimes with the same wall values, and null surcharges become `0.0`.
  - `config.Config.from_env(env=None)` with `brokers, consumer_group, commit_interval_s, window_grace_s, tip_max_abs, zones_path, state_dir, ch_host, ch_port, ch_db, ch_user, ch_password`.

- [ ] **Step 1: Project, fixtures, and doc fix**

`services/processor/pyproject.toml`:
```toml
[project]
name = "nyctaxi-processor"
version = "0.1.0"
description = "Quix Streams processor: validate, enrich, aggregate, sink to ClickHouse (spec 03)"
requires-python = ">=3.12,<3.13"
dependencies = ["quixstreams==3.26.0", "clickhouse-connect==1.9.0"]

[dependency-groups]
dev = ["pytest==9.1.1", "jsonschema==4.26.0", "referencing==0.37.0", "ruff==0.16.9", "mypy==2.3.1",
       "testcontainers[kafka]==4.15.0"]

[build-system]
requires = ["hatchling==1.32.4"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/processor"]

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["integration: needs Docker (testcontainers)"]
addopts = "-m 'not integration'"

[tool.ruff]
line-length = 110
target-version = "py312"

[tool.mypy]
strict = true
ignore_missing_imports = true
```
Run: `cd services/processor && mkdir -p src/processor tests/fixtures && touch src/processor/__init__.py && uv lock && uv sync`

`services/processor/tests/fixtures/zones.csv`:
```
location_id,borough,zone,service_zone,lat,lon
1,EWR,Newark Airport,EWR,40.6895,-74.1745
5,Staten Island,Arden Heights,Boro Zone,40.5504,-74.1860
7,Queens,Astoria,Boro Zone,40.7610,-73.9196
69,Bronx,East Concourse/Concourse Village,Boro Zone,40.8288,-73.9197
132,Queens,JFK Airport,Airports,40.6413,-73.7781
138,Queens,LaGuardia Airport,Airports,40.7769,-73.8740
161,Manhattan,Midtown Center,Yellow Zone,40.7580,-73.9776
230,Manhattan,Times Sq/Theatre District,Yellow Zone,40.7590,-73.9845
237,Manhattan,Upper East Side South,Yellow Zone,40.7680,-73.9620
255,Brooklyn,Williamsburg (North Side),Boro Zone,40.7180,-73.9570
264,Unknown,NV,N/A,,
265,N/A,Outside of NYC,N/A,,
```

`services/processor/tests/conftest.py`:
```python
import copy
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(REPO / "contracts"))

import contract_validator as cv  # noqa: E402


@pytest.fixture
def trip_example() -> dict:
    return copy.deepcopy(cv.schema("trip")["examples"][0])


@pytest.fixture
def enriched_example() -> dict:
    return copy.deepcopy(cv.schema("enriched_trip")["examples"][0])


@pytest.fixture
def zones():
    from processor.zones import load_zones
    return load_zones(FIXTURES / "zones.csv")
```

Doc fix (one-off, from the repo root):
```bash
cd /Users/vasilebordei/Desktop/learning/aod_dataEngineering
uv run python - <<'EOF'
import json, pathlib
p = pathlib.Path("contracts/events/enriched_trip.schema.json")
s = json.loads(p.read_text()); s["examples"][0]["avg_speed_mph"] = 11.01
p.write_text(json.dumps(s, indent=2) + "\n")
q = pathlib.Path("docs/specs/03-stream-processor.md"); t = q.read_text()
t = t.replace("if `duration_min ≥ 1`, else `null`. 1 decimal |", "if `duration_min ≥ 1`, else `null`. 2 decimals, computed from the unrounded duration |")
q.write_text(t)
EOF
uv run pytest -q contracts/tests
```
Expected: `20 passed`.

- [ ] **Step 2: Write the failing tests for times, config, rules, and zones**

`services/processor/tests/test_times.py`:
```python
from datetime import datetime, timezone

from processor.times import format_sim, format_wall, parse_sim, parse_wall, sim_epoch_ms, sim_from_epoch_ms


def test_parse_sim_variants():
    assert parse_sim("2025-10-07T08:14:03") == datetime(2025, 10, 7, 8, 14, 3)
    assert parse_sim("2025-10-07T08:14:03.250") == datetime(2025, 10, 7, 8, 14, 3, 250_000)
    assert parse_sim(datetime(2025, 10, 7, tzinfo=timezone.utc)) == datetime(2025, 10, 7)
    assert parse_sim("yesterday") is None and parse_sim(None) is None and parse_sim(12) is None


def test_format_sim():
    assert format_sim(datetime(2025, 10, 7, 8, 14, 3)) == "2025-10-07T08:14:03"
    assert format_sim(datetime(2025, 10, 7, 8, 14, 3, 250_000)) == "2025-10-07T08:14:03.250"


def test_wall_roundtrip():
    dt = datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=timezone.utc)
    assert format_wall(dt) == "2026-10-16T08:02:11.482Z"
    assert parse_wall("2026-10-16T08:02:11.482Z") == dt


def test_sim_epoch_is_naive_as_utc():
    ms = sim_epoch_ms(datetime(2025, 10, 7, 8, 14))
    assert ms == 1759824840000
    assert sim_from_epoch_ms(ms) == datetime(2025, 10, 7, 8, 14)
```

`services/processor/tests/test_config.py`:
```python
from pathlib import Path

from processor.config import Config


def test_defaults():
    c = Config.from_env({})
    assert (c.brokers, c.consumer_group, c.commit_interval_s, c.window_grace_s) == ("redpanda:9092", "processor-v1", 1.0, 180)
    assert c.tip_max_abs == 100.0 and c.zones_path == Path("/data/zones.csv") and c.state_dir == Path("/state")
    assert (c.ch_host, c.ch_port, c.ch_db, c.ch_user) == ("clickhouse", 8123, "taxi", "ingest_writer")


def test_overrides():
    c = Config.from_env({"PROCESSOR_WINDOW_GRACE_S": "0", "CLICKHOUSE_HTTP_PORT": "18123",
                         "CLICKHOUSE_INGEST_PASSWORD": "pw", "PROCESSOR_TIP_MAX_ABS": "50"})
    assert (c.window_grace_s, c.ch_port, c.ch_password, c.tip_max_abs) == (0, 18123, "pw", 50.0)
```

`services/processor/tests/test_rules.py`:
```python
import json

import pytest
from contract_validator import assert_valid

from processor.rules import REASON_CODES, annotate, check_trip, is_traced, to_rejected

BASE = {"trip_id": "r0-000000001", "source": "replay", "trace_id": None, "loop": 0,
        "pickup_ts": "2025-10-07T08:14:03", "dropoff_ts": "2025-10-07T08:29:41", "passenger_count": 1,
        "trip_distance": 2.87, "pu_location_id": 161, "do_location_id": 237, "payment_type": 1,
        "fare_amount": 17.0, "tip_amount": 4.4, "total_amount": 26.15}
AT = "2026-10-16T08:03:01.200Z"

CASES = [  # (code, makes it fail, boundary that still passes)
    ("FARE_NON_POSITIVE", {"fare_amount": 0.0}, {"fare_amount": 0.01}),
    ("TOTAL_NEGATIVE", {"total_amount": -0.01}, {"total_amount": 0.0}),
    ("DISTANCE_NON_POSITIVE", {"trip_distance": 0.0}, {"trip_distance": 0.01}),
    ("DISTANCE_TOO_LONG", {"trip_distance": 200.01}, {"trip_distance": 200.0}),
    ("DROPOFF_NOT_AFTER_PICKUP", {"dropoff_ts": "2025-10-07T08:14:03"}, {"dropoff_ts": "2025-10-07T08:14:04"}),
    ("DURATION_TOO_LONG", {"dropoff_ts": "2025-10-07T14:14:04"}, {"dropoff_ts": "2025-10-07T14:14:03"}),
    ("LOCATION_INVALID", {"pu_location_id": 266}, {"pu_location_id": 265}),
    ("LOCATION_INVALID", {"do_location_id": 0}, {"do_location_id": 1}),
    ("PASSENGER_COUNT_INVALID", {"passenger_count": 10}, {"passenger_count": 0}),
    ("TIP_NEGATIVE", {"tip_amount": -1.0}, {"tip_amount": 0.0}),
    ("TIP_UNREALISTIC", {"tip_amount": 100.01}, {"tip_amount": 100.0}),
    ("TIP_UNREALISTIC", {"fare_amount": 50.0, "tip_amount": 150.01}, {"fare_amount": 50.0, "tip_amount": 150.0}),
]


def test_base_trip_is_valid():
    assert check_trip(BASE) == []
    assert check_trip(dict(BASE, passenger_count=None)) == []


@pytest.mark.parametrize("code,bad,ok", CASES)
def test_each_rule(code, bad, ok):
    assert code in check_trip(dict(BASE, **bad))
    assert code not in check_trip(dict(BASE, **ok))


def test_all_failing_rules_are_reported():
    assert check_trip(dict(BASE, fare_amount=-12.5, total_amount=-16.0)) == ["FARE_NON_POSITIVE", "TOTAL_NEGATIVE"]


@pytest.mark.parametrize("bad", [
    {"pickup_ts": None}, {"pickup_ts": "noon"}, {"fare_amount": "17"}, {"pu_location_id": True},
    {"source": "taxi"}, {"trip_id": 5}, {"payment_type": 1.5}, {"total_amount": float("nan")},
])
def test_schema_invalid(bad):
    assert check_trip(dict(BASE, **bad)) == ["SCHEMA_INVALID"]


def test_reason_codes_match_contract():
    import contract_validator as cv
    assert list(REASON_CODES) == cv.schema("rejected_trip")["$defs"]["reason_code"]["enum"]


def test_annotate_valid_json():
    v = annotate(json.dumps(BASE).encode())
    assert v["_reasons"] == [] and v["trip_id"] == BASE["trip_id"]


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b"\xff\xfe", "{broken"])
def test_annotate_garbage(raw):
    v = annotate(raw)
    assert v["_reasons"] == ["SCHEMA_INVALID"] and isinstance(v["_raw"], str)


def test_to_rejected_keeps_original_payload():
    r = to_rejected(annotate(json.dumps(dict(BASE, fare_amount=-12.5)).encode()), AT)
    assert_valid("rejected_trip", r)
    assert r["trip_id"] == "r0-000000001" and r["reasons"] == ["FARE_NON_POSITIVE"]
    assert json.loads(r["payload"])["fare_amount"] == -12.5 and "_reasons" not in r["payload"]


def test_to_rejected_garbage_gets_a_stable_id():
    r1, r2 = (to_rejected(annotate(b"not json"), AT) for _ in range(2))
    assert_valid("rejected_trip", r1)
    assert r1["trip_id"] == r2["trip_id"] and r1["trip_id"].startswith("invalid-")
    assert r1["payload"] == "not json" and r1["pickup_ts"] is None and r1["trace_id"] is None


def test_is_traced():
    assert is_traced(dict(BASE, trace_id="3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"))
    assert not is_traced(BASE) and not is_traced(dict(BASE, trace_id="nope"))
    assert not is_traced({"_reasons": ["SCHEMA_INVALID"], "_raw": "x"})
```

`services/processor/tests/test_zones.py`:
```python
import pytest

from processor.zones import UNKNOWN, Zone, load_zones, zone_for


def test_loads_fixture(zones):
    assert zones[161] == Zone("Midtown Center", "Manhattan", "Yellow Zone")
    assert zone_for(zones, 132).service_zone == "Airports"


def test_unknown_ids(zones):
    assert zone_for(zones, 264) == UNKNOWN == zone_for(zones, 265) == zone_for(zones, 999)
    assert UNKNOWN == Zone("Unknown", "Unknown", "Unknown")


def test_unexpected_values_are_normalised(tmp_path):
    p = tmp_path / "z.csv"
    p.write_text("location_id,borough,zone,service_zone,lat,lon\n9,Atlantis,,Somewhere,,\n")
    assert load_zones(p)[9] == Zone("Unknown", "Unknown", "Unknown")


def test_empty_file_fails_fast(tmp_path):
    p = tmp_path / "z.csv"
    p.write_text("location_id,borough,zone,service_zone,lat,lon\n")
    with pytest.raises(ValueError, match="no zones"):
        load_zones(p)
```
Run: `uv run pytest -q`. Expected: FAIL with `ModuleNotFoundError` (processor.times, processor.config, …).

- [ ] **Step 3: Implement `times.py` and `config.py`**

`services/processor/src/processor/times.py`:
```python
"""Sim time (naive NYC local, *_ts) and wall time (UTC, *_at) helpers. Stdlib-only (see rules.py)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_sim(v: Any) -> datetime | None:
    """A sim timestamp as a naive datetime, or None if it can't be parsed."""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v).replace(tzinfo=None)
    except ValueError:
        return None


def format_sim(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds" if dt.microsecond == 0 else "milliseconds")


def format_wall(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def parse_wall(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def wall_now_str() -> str:
    return format_wall(datetime.now(timezone.utc))


def sim_epoch_ms(dt: datetime) -> int:
    """Event time for Quix windows: the naive sim time read as if it were UTC."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def sim_from_epoch_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)
```

`services/processor/src/processor/config.py`:
```python
"""Processor configuration from environment variables (spec 03 §Config)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    brokers: str
    consumer_group: str
    commit_interval_s: float
    window_grace_s: int
    tip_max_abs: float
    zones_path: Path
    state_dir: Path
    ch_host: str
    ch_port: int
    ch_db: str
    ch_user: str
    ch_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        e = os.environ if env is None else env
        return cls(
            brokers=e.get("KAFKA_BROKERS", "redpanda:9092"),
            consumer_group=e.get("PROCESSOR_CONSUMER_GROUP", "processor-v1"),
            commit_interval_s=float(e.get("PROCESSOR_COMMIT_INTERVAL_S", "1")),
            window_grace_s=int(e.get("PROCESSOR_WINDOW_GRACE_S", "180")),
            tip_max_abs=float(e.get("PROCESSOR_TIP_MAX_ABS", "100")),
            zones_path=Path(e.get("PROCESSOR_ZONES_PATH", "/data/zones.csv")),
            state_dir=Path(e.get("PROCESSOR_STATE_DIR", "/state")),
            ch_host=e.get("CLICKHOUSE_HOST", "clickhouse"),
            ch_port=int(e.get("CLICKHOUSE_HTTP_PORT", "8123")),
            ch_db=e.get("CLICKHOUSE_DB", "taxi"),
            ch_user=e.get("CLICKHOUSE_INGEST_USER", "ingest_writer"),
            ch_password=e.get("CLICKHOUSE_INGEST_PASSWORD", ""),
        )
```

- [ ] **Step 4: Implement `rules.py`**

```python
"""Validation rules (spec 03 §Validation rules).

Stdlib-only on purpose: scripts/prepare_data.py imports this module to compute the data-quality report with
exactly the rules the stream processor applies.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from processor.times import format_sim, parse_sim

REASON_CODES = (
    "SCHEMA_INVALID", "FARE_NON_POSITIVE", "TOTAL_NEGATIVE", "DISTANCE_NON_POSITIVE", "DISTANCE_TOO_LONG",
    "DROPOFF_NOT_AFTER_PICKUP", "DURATION_TOO_LONG", "LOCATION_INVALID", "PASSENGER_COUNT_INVALID",
    "TIP_NEGATIVE", "TIP_UNREALISTIC",
)
RULES_CHECKED = len(REASON_CODES)
MAX_DISTANCE_MILES = 200.0
MAX_DURATION = timedelta(hours=6)
REQUIRED_INT = ("pu_location_id", "do_location_id", "payment_type")
REQUIRED_NUM = ("trip_distance", "fare_amount", "tip_amount", "total_amount")
SOURCES = ("replay", "student")


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _is_int(v: Any) -> bool:
    return (isinstance(v, int) and not isinstance(v, bool)) or (isinstance(v, float) and v.is_integer())


def is_uuid(v: Any) -> bool:
    if not isinstance(v, str):
        return False
    try:
        uuid.UUID(v)
    except ValueError:
        return False
    return True


def check_trip(t: Mapping[str, Any], tip_max_abs: float = 100.0) -> list[str]:
    """Every reason code the trip fails, in REASON_CODES order. Empty list = valid."""
    pickup, dropoff = parse_sim(t.get("pickup_ts")), parse_sim(t.get("dropoff_ts"))
    if (not isinstance(t.get("trip_id"), str) or t.get("source") not in SOURCES
            or pickup is None or dropoff is None
            or not all(_is_int(t.get(k)) for k in REQUIRED_INT)
            or not all(_is_num(t.get(k)) for k in REQUIRED_NUM)):
        return ["SCHEMA_INVALID"]
    fare, total = float(t["fare_amount"]), float(t["total_amount"])
    dist, tip = float(t["trip_distance"]), float(t["tip_amount"])
    reasons = []
    if fare <= 0:
        reasons.append("FARE_NON_POSITIVE")
    if total < 0:
        reasons.append("TOTAL_NEGATIVE")
    if dist <= 0:
        reasons.append("DISTANCE_NON_POSITIVE")
    if dist > MAX_DISTANCE_MILES:
        reasons.append("DISTANCE_TOO_LONG")
    if dropoff <= pickup:
        reasons.append("DROPOFF_NOT_AFTER_PICKUP")
    elif dropoff - pickup > MAX_DURATION:
        reasons.append("DURATION_TOO_LONG")
    if not all(1 <= int(t[k]) <= 265 for k in ("pu_location_id", "do_location_id")):
        reasons.append("LOCATION_INVALID")
    pc = t.get("passenger_count")
    if pc is not None and (not _is_int(pc) or not 0 <= int(pc) <= 9):
        reasons.append("PASSENGER_COUNT_INVALID")
    if tip < 0:
        reasons.append("TIP_NEGATIVE")
    elif tip > max(tip_max_abs, 3 * fare):
        reasons.append("TIP_UNREALISTIC")
    return reasons


def annotate(raw: bytes | str | Mapping[str, Any], tip_max_abs: float = 100.0) -> dict[str, Any]:
    """Parse a trips.raw message and attach `_reasons`. Never raises on bad input."""
    if isinstance(raw, Mapping):
        value: Any = dict(raw)
    else:
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        try:
            value = json.loads(text)
        except ValueError:
            return {"_reasons": ["SCHEMA_INVALID"], "_raw": text}
        if not isinstance(value, dict):
            return {"_reasons": ["SCHEMA_INVALID"], "_raw": text}
    value["_reasons"] = check_trip(value, tip_max_abs)
    return value


def is_traced(v: Mapping[str, Any]) -> bool:
    return is_uuid(v.get("trace_id")) and isinstance(v.get("trip_id"), str) and v.get("source") in SOURCES


def to_rejected(v: Mapping[str, Any], rejected_at: str) -> dict[str, Any]:
    """Dead-letter record (rejected_trip contract). The original message is kept verbatim in `payload`."""
    if "_raw" in v:
        payload = str(v["_raw"])
    else:
        payload = json.dumps({k: x for k, x in v.items() if not k.startswith("_")}, default=str,
                             separators=(",", ":"))
    tid = v.get("trip_id")
    if not isinstance(tid, str) or not tid:
        tid = "invalid-" + hashlib.sha1(payload.encode()).hexdigest()[:12]
    pickup = parse_sim(v.get("pickup_ts"))
    return {
        "trip_id": tid,
        "source": v.get("source") if v.get("source") in SOURCES else "replay",
        "trace_id": v.get("trace_id") if is_uuid(v.get("trace_id")) else None,
        "pickup_ts": format_sim(pickup) if pickup else None,
        "reasons": list(v["_reasons"]),
        "payload": payload,
        "rejected_at": rejected_at,
    }
```

- [ ] **Step 5: Implement `zones.py`**

```python
"""Taxi zone lookup loaded once at startup from data/zones.csv (spec 01 output)."""

from __future__ import annotations

import csv
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple

BOROUGHS = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR", "Unknown", "N/A"}
SERVICE_ZONES = {"Yellow Zone", "Boro Zone", "Airports", "EWR", "N/A", "Unknown"}


class Zone(NamedTuple):
    zone: str
    borough: str
    service_zone: str


UNKNOWN = Zone("Unknown", "Unknown", "Unknown")


def load_zones(path: Path) -> dict[int, Zone]:
    zones: dict[int, Zone] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            location_id = int(r["location_id"])
            if location_id >= 264:  # 264 "Unknown" and 265 "Outside of NYC" enrich as UNKNOWN (spec 03)
                continue
            borough = r["borough"] if r["borough"] in BOROUGHS else "Unknown"
            service = r["service_zone"] if r["service_zone"] in SERVICE_ZONES else "Unknown"
            zones[location_id] = Zone(r["zone"] or "Unknown", borough, service)
    if not zones:
        raise ValueError(f"{path}: no zones loaded")
    return zones


def zone_for(zones: Mapping[int, Zone], location_id: int) -> Zone:
    return zones.get(location_id, UNKNOWN)
```
In `test_unexpected_values_are_normalised`, an empty `zone` value becomes `"Unknown"` because of `r["zone"] or "Unknown"`, which the test expects.

Run: `uv run pytest tests/test_times.py tests/test_config.py tests/test_rules.py tests/test_zones.py -q`. Expected: all pass (0 failed).

- [ ] **Step 6: Write the failing tests for enrich, aggregate, checkpoints, and rows**

`services/processor/tests/test_enrich.py`:
```python
from contract_validator import assert_valid

from processor.enrich import enrich

PROCESSED_AT = "2026-10-16T08:02:11.731Z"


def test_example_is_enriched_exactly(trip_example, enriched_example, zones):
    assert enrich(trip_example, zones, PROCESSED_AT) == enriched_example


def test_output_is_contract_valid_and_drops_internal_and_unknown_keys(trip_example, zones):
    out = enrich(dict(trip_example, _reasons=[], surprise="x"), zones, PROCESSED_AT)
    assert_valid("enriched_trip", out)
    assert "_reasons" not in out and "surprise" not in out


def test_cash_trip_has_no_tip_pct(trip_example, zones):
    out = enrich(dict(trip_example, payment_type=2, tip_amount=0.0), zones, PROCESSED_AT)
    assert out["is_card"] is False and out["tip_pct"] is None and out["payment_type_name"] == "Cash"


def test_unknown_zone_short_trip_and_airport(trip_example, zones):
    out = enrich(dict(trip_example, pu_location_id=264, do_location_id=132,
                      dropoff_ts="2025-10-07T08:14:40"), zones, PROCESSED_AT)
    assert (out["pu_zone"], out["pu_borough"], out["pu_service_zone"]) == ("Unknown", "Unknown", "Unknown")
    assert out["do_zone"] == "JFK Airport" and out["is_airport"] is True
    assert out["duration_min"] == 0.62 and out["avg_speed_mph"] is None
    assert_valid("enriched_trip", out)


def test_numbers_are_normalised(trip_example, zones):
    out = enrich(dict(trip_example, pu_location_id=161.0, fare_amount=17, produced_at="bad"), zones, PROCESSED_AT)
    assert out["pu_location_id"] == 161 and isinstance(out["pu_location_id"], int)
    assert out["fare_amount"] == 17.0 and isinstance(out["fare_amount"], float)
    assert out["produced_at"] == PROCESSED_AT
```

`services/processor/tests/test_aggregate.py`:
```python
from datetime import datetime

from contract_validator import assert_valid

from processor.aggregate import add_trip, init_bucket, to_agg_record
from processor.times import sim_epoch_ms

T1 = {"is_card": True, "tip_pct": 20.0, "fare_amount": 10.0, "total_amount": 15.0, "trip_distance": 1.0, "source": "replay"}
T2 = {"is_card": True, "tip_pct": 30.0, "fare_amount": 20.0, "total_amount": 30.0, "trip_distance": 3.0, "source": "student"}
T3 = {"is_card": False, "tip_pct": None, "fare_amount": 30.0, "total_amount": 35.0, "trip_distance": 5.0, "source": "replay"}
START = sim_epoch_ms(datetime(2025, 10, 7, 8, 14))
AT = "2026-10-16T08:02:15.004Z"


def test_reduce_and_record():
    bucket = add_trip(add_trip(init_bucket(T1), T2), T3)
    record = to_agg_record({"start": START, "end": START + 60_000, "value": bucket}, "Manhattan", AT)
    assert record == {
        "window_start": "2025-10-07T08:14:00", "window_end": "2025-10-07T08:15:00", "borough": "Manhattan",
        "trips": 3, "revenue": 80.0, "avg_fare": 20.0, "avg_distance": 3.0, "card_trips": 2,
        "avg_card_tip_pct": 25.0, "student_trips": 1, "emitted_at": AT,
    }
    assert_valid("borough_minute_agg", record)


def test_no_card_trips_means_null_tip():
    record = to_agg_record({"start": START, "end": START + 60_000, "value": init_bucket(T3)}, "Bronx", AT)
    assert record["avg_card_tip_pct"] is None and record["card_trips"] == 0
    assert_valid("borough_minute_agg", record)


def test_add_trip_does_not_mutate():
    bucket = init_bucket(T1)
    add_trip(bucket, T2)
    assert bucket["trips"] == 1
```

`services/processor/tests/test_checkpoints.py`:
```python
from contract_validator import assert_valid

from processor.checkpoints import enriched_and_aggregated, stored_checkpoint, validated_or_rejected
from processor.enrich import enrich
from processor.rules import annotate

TRACE = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"
AT = "2026-10-16T10:31:07.120Z"


def test_validated(trip_example):
    cp = validated_or_rejected(annotate(dict(trip_example, trace_id=TRACE)), AT)
    assert_valid("trace_checkpoint", cp)
    assert (cp["stage"], cp["seq"], cp["service"], cp["detail"]) == ("validated", 3, "processor", {"rules_checked": 11})


def test_rejected(trip_example):
    cp = validated_or_rejected(annotate(dict(trip_example, trace_id=TRACE, tip_amount=999.0)), AT)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "rejected" and cp["seq"] == 3 and cp["detail"] == {"reasons": ["TIP_UNREALISTIC"]}


def test_enriched_and_aggregated(trip_example, zones):
    enriched = enrich(dict(trip_example, trace_id=TRACE), zones, AT)
    e, a = enriched_and_aggregated(enriched, AT)
    for cp in (e, a):
        assert_valid("trace_checkpoint", cp)
    assert (e["stage"], e["seq"], a["stage"], a["seq"]) == ("enriched", 4, "aggregated", 5)
    assert e["detail"]["pu_zone"] == "Midtown Center" and e["detail"]["tip_pct"] == 25.88
    assert a["detail"] == {"window_start": "2025-10-07T08:14:00", "borough": "Manhattan"}


def test_stored(trip_example, zones):
    cp = stored_checkpoint(enrich(dict(trip_example, trace_id=TRACE), zones, AT), "taxi.trips", 143, AT)
    assert_valid("trace_checkpoint", cp)
    assert cp["stage"] == "stored" and cp["seq"] == 6 and cp["detail"] == {"table": "taxi.trips", "batch_rows": 143}
```

`services/processor/tests/test_rows.py`:
```python
import re
import uuid
from datetime import datetime, timedelta, timezone

from processor.rows import AGG_COLUMNS, REJECTED_COLUMNS, TRIPS_COLUMNS, agg_row, rejected_row, trip_row

from conftest import REPO

INIT = REPO / "infra" / "clickhouse" / "init"


def ddl_columns(file: str) -> list[str]:
    return re.findall(r"^    ([a-z_]+)\s", (INIT / file).read_text(), re.M)


def test_columns_match_ddl():
    assert list(TRIPS_COLUMNS) + ["stored_at"] == ddl_columns("20_trips.sql")
    assert list(REJECTED_COLUMNS) == ddl_columns("30_trips_rejected.sql")
    assert list(AGG_COLUMNS) == ddl_columns("40_agg_borough_minute.sql")


def test_sim_timestamps_are_utc_aware_and_unshifted(enriched_example):
    row = dict(zip(TRIPS_COLUMNS, trip_row(enriched_example)))
    assert row["pickup_ts"] == datetime(2025, 10, 7, 8, 14, 3, tzinfo=timezone.utc)
    assert row["pickup_ts"].utcoffset() == timedelta(0)
    assert row["produced_at"] == datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=timezone.utc)


def test_nulls_and_types(enriched_example):
    ex = dict(enriched_example, airport_fee=None, extra=None, trace_id="3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f")
    row = dict(zip(TRIPS_COLUMNS, trip_row(ex)))
    assert row["airport_fee"] == 0.0 and row["extra"] == 0.0
    assert row["trace_id"] == uuid.UUID("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f")
    assert row["tip_pct"] == 25.88 and row["is_card"] is True
    assert len(trip_row(enriched_example)) == len(TRIPS_COLUMNS)


def test_rejected_and_agg_rows():
    import contract_validator as cv
    rej = dict(zip(REJECTED_COLUMNS, rejected_row(cv.schema("rejected_trip")["examples"][1])))
    assert rej["pickup_ts"] == datetime(2025, 10, 9, 17, 42, tzinfo=timezone.utc)
    assert rej["reasons"] == ["TIP_UNREALISTIC"] and isinstance(rej["trace_id"], uuid.UUID)
    assert dict(zip(REJECTED_COLUMNS, rejected_row(cv.schema("rejected_trip")["examples"][0])))["trace_id"] is None
    agg = dict(zip(AGG_COLUMNS, agg_row(cv.schema("borough_minute_agg")["examples"][0])))
    assert agg["window_start"] == datetime(2025, 10, 7, 8, 14, tzinfo=timezone.utc) and agg["trips"] == 142
```
Run: `uv run pytest -q`. Expected: FAIL with `ModuleNotFoundError` for `processor.enrich`, `processor.aggregate`, `processor.checkpoints`, `processor.rows`.

- [ ] **Step 7: Implement `enrich.py`, `aggregate.py`, `checkpoints.py`, `rows.py`**

`services/processor/src/processor/enrich.py`:
```python
"""Enrichment: zone names and derived fields (spec 03 §Enrichment)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from processor.times import format_sim, parse_sim
from processor.zones import UNKNOWN, Zone, zone_for

TRIP_FIELDS = (
    "trip_id", "source", "trace_id", "loop", "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count",
    "trip_distance", "ratecode_id", "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount", "produced_at",
)
INT_FIELDS = ("loop", "vendor_id", "passenger_count", "ratecode_id", "pu_location_id", "do_location_id", "payment_type")
FLOAT_FIELDS = ("trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount")
PAYMENT_NAMES = {0: "Flex Fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown",
                 6: "Voided trip"}
AIRPORT_IDS = frozenset({1, 132, 138})
_WALL = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def enrich(trip: Mapping[str, Any], zones: Mapping[int, Zone], processed_at: str) -> dict[str, Any]:
    """A validated trip → enriched_trip contract. Unknown and internal (`_`) keys are dropped."""
    out: dict[str, Any] = {f: trip.get(f) for f in TRIP_FIELDS}
    for f in INT_FIELDS:
        if out[f] is not None:
            out[f] = int(out[f])
    for f in FLOAT_FIELDS:
        if out[f] is not None:
            out[f] = float(out[f])
    out["loop"] = out["loop"] or 0
    if not (isinstance(out["produced_at"], str) and _WALL.fullmatch(out["produced_at"])):
        out["produced_at"] = processed_at
    pickup, dropoff = parse_sim(trip["pickup_ts"]), parse_sim(trip["dropoff_ts"])
    assert pickup is not None and dropoff is not None  # guaranteed by check_trip
    out["pickup_ts"], out["dropoff_ts"] = format_sim(pickup), format_sim(dropoff)

    pu = zone_for(zones, out["pu_location_id"]) if out["pu_location_id"] < 264 else UNKNOWN
    do = zone_for(zones, out["do_location_id"]) if out["do_location_id"] < 264 else UNKNOWN
    seconds = (dropoff - pickup).total_seconds()
    duration_min = round(seconds / 60, 2)
    is_card = out["payment_type"] == 1
    fare = out["fare_amount"]
    out.update(
        pu_zone=pu.zone, pu_borough=pu.borough, pu_service_zone=pu.service_zone,
        do_zone=do.zone, do_borough=do.borough, do_service_zone=do.service_zone,
        payment_type_name=PAYMENT_NAMES.get(out["payment_type"], "Unknown"),
        is_card=is_card,
        is_airport=out["pu_location_id"] in AIRPORT_IDS or out["do_location_id"] in AIRPORT_IDS,
        duration_min=duration_min,
        avg_speed_mph=round(out["trip_distance"] / (seconds / 3600), 2) if seconds >= 60 else None,
        tip_pct=round(100 * out["tip_amount"] / fare, 2) if is_card and fare > 0 else None,
        pickup_hour=pickup.hour,
        pickup_dow=pickup.isoweekday(),
        processed_at=processed_at,
    )
    return out
```

`services/processor/src/processor/aggregate.py`:
```python
"""Tumbling-window aggregation per pickup borough (spec 03 §Aggregation)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from processor.times import format_sim, sim_from_epoch_ms


def _empty() -> dict[str, Any]:
    return {"trips": 0, "revenue": 0.0, "fare_sum": 0.0, "distance_sum": 0.0, "card_trips": 0,
            "card_tip_pct_sum": 0.0, "card_tip_pct_n": 0, "student_trips": 0}


def add_trip(bucket: Mapping[str, Any], trip: Mapping[str, Any]) -> dict[str, Any]:
    b = dict(bucket)
    b["trips"] += 1
    b["revenue"] += float(trip["total_amount"])
    b["fare_sum"] += float(trip["fare_amount"])
    b["distance_sum"] += float(trip["trip_distance"])
    if trip.get("is_card"):
        b["card_trips"] += 1
        if trip.get("tip_pct") is not None:
            b["card_tip_pct_sum"] += float(trip["tip_pct"])
            b["card_tip_pct_n"] += 1
    if trip.get("source") == "student":
        b["student_trips"] += 1
    return b


def init_bucket(trip: Mapping[str, Any]) -> dict[str, Any]:
    """Quix calls the initializer with the window's first value, so it must count it."""
    return add_trip(_empty(), trip)


def to_agg_record(window: Mapping[str, Any], borough: str, emitted_at: str) -> dict[str, Any]:
    b = window["value"]
    n = b["trips"]
    return {
        "window_start": format_sim(sim_from_epoch_ms(window["start"])),
        "window_end": format_sim(sim_from_epoch_ms(window["end"])),
        "borough": borough,
        "trips": n,
        "revenue": round(b["revenue"], 2),
        "avg_fare": round(b["fare_sum"] / n, 2),
        "avg_distance": round(b["distance_sum"] / n, 3),
        "card_trips": b["card_trips"],
        "avg_card_tip_pct": round(b["card_tip_pct_sum"] / b["card_tip_pct_n"], 2) if b["card_tip_pct_n"] else None,
        "student_trips": b["student_trips"],
        "emitted_at": emitted_at,
    }
```

`services/processor/src/processor/checkpoints.py`:
```python
"""Trace checkpoints emitted by the processor (spec 03 §Trace checkpoints, spec 07)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from processor.rules import RULES_CHECKED
from processor.times import format_sim, parse_sim

SEQ = {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
       "stored": 6, "queryable": 7}


def checkpoint(trip: Mapping[str, Any], stage: str, detail: dict[str, Any], at: str) -> dict[str, Any]:
    return {"trace_id": trip["trace_id"], "trip_id": trip["trip_id"], "source": trip["source"], "stage": stage,
            "seq": SEQ[stage], "service": "processor", "at": at, "detail": detail}


def validated_or_rejected(v: Mapping[str, Any], at: str) -> dict[str, Any]:
    if v["_reasons"]:
        return checkpoint(v, "rejected", {"reasons": list(v["_reasons"])}, at)
    return checkpoint(v, "validated", {"rules_checked": RULES_CHECKED}, at)


def enriched_and_aggregated(v: Mapping[str, Any], at: str) -> list[dict[str, Any]]:
    pickup = parse_sim(v["pickup_ts"])
    assert pickup is not None
    enriched = {k: v[k] for k in ("pu_zone", "pu_borough", "do_zone", "do_borough", "tip_pct", "duration_min")}
    window = {"window_start": format_sim(pickup.replace(second=0, microsecond=0)), "borough": v["pu_borough"]}
    return [checkpoint(v, "enriched", enriched, at), checkpoint(v, "aggregated", window, at)]


def stored_checkpoint(v: Mapping[str, Any], table: str, batch_rows: int, at: str) -> dict[str, Any]:
    return checkpoint(v, "stored", {"table": table, "batch_rows": batch_rows}, at)
```

`services/processor/src/processor/rows.py`:
```python
"""Message → ClickHouse row mapping. Column order equals infra/clickhouse/init DDL (tested)."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from processor.times import parse_sim, parse_wall

TRIPS_COLUMNS = (
    "trip_id", "source", "trace_id", "loop", "vendor_id", "pickup_ts", "dropoff_ts", "pickup_hour", "pickup_dow",
    "passenger_count", "trip_distance", "duration_min", "avg_speed_mph", "ratecode_id", "store_and_fwd_flag",
    "pu_location_id", "pu_zone", "pu_borough", "pu_service_zone", "do_location_id", "do_zone", "do_borough",
    "do_service_zone", "payment_type", "payment_type_name", "is_card", "is_airport", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tip_pct", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee", "total_amount", "produced_at", "processed_at",
)  # stored_at is filled by ClickHouse (DEFAULT now64(3))
REJECTED_COLUMNS = ("trip_id", "source", "trace_id", "pickup_ts", "reasons", "payload", "rejected_at")
AGG_COLUMNS = ("window_start", "window_end", "borough", "trips", "revenue", "avg_fare", "avg_distance",
               "card_trips", "avg_card_tip_pct", "student_trips", "emitted_at")

ZERO_IF_NULL = {"extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge",
                "airport_fee", "cbd_congestion_fee"}
SIM_COLUMNS = {"pickup_ts", "dropoff_ts", "window_start", "window_end"}
WALL_COLUMNS = {"produced_at", "processed_at", "rejected_at", "emitted_at"}


def _sim(v: Any) -> datetime | None:
    """Naive sim time → aware UTC with the same wall values, so the host timezone can never shift it."""
    dt = parse_sim(v)
    return dt.replace(tzinfo=timezone.utc) if dt is not None else None


def _row(v: Mapping[str, Any], columns: tuple[str, ...]) -> list[Any]:
    row: list[Any] = []
    for c in columns:
        x = v.get(c)
        if c in SIM_COLUMNS:
            x = _sim(x)
        elif c in WALL_COLUMNS:
            x = parse_wall(x)
        elif c == "trace_id":
            x = uuid.UUID(x) if x else None
        elif c in ZERO_IF_NULL and x is None:
            x = 0.0
        row.append(x)
    return row


def trip_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, TRIPS_COLUMNS)


def rejected_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, REJECTED_COLUMNS)


def agg_row(v: Mapping[str, Any]) -> list[Any]:
    return _row(v, AGG_COLUMNS)
```

- [ ] **Step 8: Run all processor unit tests**

Run: `cd services/processor && uv run pytest -q`. Expected: all pass (0 failed).
Also run: `cd ../.. && uv run pytest -q`. Expected: all root tests pass. Task 2's data-prep tests import `processor.rules` successfully.

- [ ] **Step 9: Commit**

```bash
git add services/processor contracts/events/enriched_trip.schema.json docs/specs/03-stream-processor.md
git commit -m "feat(processor): validation rules, enrichment, aggregation, checkpoints, row mapping"
```

---
### Task 9: Processor service: sinks, Quix app, image, compose, integration test

**Files:**
- Create: `services/processor/src/processor/{log,health,sinks,app}.py`, `services/processor/Dockerfile`, `services/processor/.dockerignore`
- Test: `services/processor/tests/{test_sinks,test_app,test_integration}.py`
- Modify: `docker-compose.yml` (add `processor`; `replayer` now depends on it)

**Interfaces:**
- Consumes: everything from Task 8. Topics from Task 4. The ClickHouse users from Task 3.
- Produces:
  - `ClickHouseSink(*, table, columns, to_row, client_factory, after_insert=None)` (a Quix `BatchingSink`) with `write_values(values: list[dict])`. On `OperationalError`/`OSError` it raises `SinkBackpressureError(retry_after=5.0)`.
  - `CheckpointProducer(brokers).emit(checkpoints: list[dict])`.
  - `app.pickup_ts_ms(value, headers, timestamp, timestamp_type) -> int`, `app.build(cfg, stats) -> Application`, and `python -m processor.app`.

- [ ] **Step 1: Write the failing sink and extractor tests**

`services/processor/tests/test_sinks.py`:
```python
import pytest
from clickhouse_connect.driver.exceptions import OperationalError
from quixstreams.sinks import SinkBackpressureError

from processor.rows import REJECTED_COLUMNS, rejected_row
from processor.sinks import ClickHouseSink

REJ = {"trip_id": "r0-000200017", "source": "replay", "trace_id": None, "pickup_ts": "2025-10-07T09:01:12",
       "reasons": ["FARE_NON_POSITIVE"], "payload": "{}", "rejected_at": "2026-10-16T08:03:01.200Z"}


class FakeClient:
    def __init__(self, fail: Exception | None = None):
        self.inserts = []
        self.fail = fail

    def insert(self, table, rows, column_names):
        if self.fail:
            raise self.fail
        self.inserts.append((table, rows, column_names))


def sink(client_factory, after=None):
    return ClickHouseSink(table="trips_rejected", columns=REJECTED_COLUMNS, to_row=rejected_row,
                          client_factory=client_factory, after_insert=after)


def test_inserts_rows_then_calls_after_insert():
    client, seen = FakeClient(), []
    sink(lambda: client, seen.extend).write_values([REJ])
    ((table, rows, cols),) = client.inserts
    assert table == "trips_rejected" and cols == list(REJECTED_COLUMNS) and len(rows) == 1
    assert seen == [REJ]


def test_connection_error_becomes_backpressure():
    seen = []
    s = sink(lambda: FakeClient(fail=OperationalError("connection refused")), seen.extend)
    with pytest.raises(SinkBackpressureError):
        s.write_values([REJ])
    assert seen == []  # no "stored" checkpoint for rows that were not stored


def test_unreachable_at_setup_is_retried_on_write():
    client, calls = FakeClient(), []

    def factory():
        calls.append(1)
        if len(calls) == 1:
            raise OperationalError("clickhouse not up yet")
        return client

    s = sink(factory)
    s.setup()  # must not raise
    s.write_values([REJ])
    assert len(calls) == 2 and len(client.inserts) == 1


def test_empty_batch_is_a_noop():
    client = FakeClient()
    sink(lambda: client).write_values([])
    assert client.inserts == []
```

`services/processor/tests/test_app.py`:
```python
from processor.app import pickup_ts_ms


def test_extractor_uses_pickup_ts_from_raw_bytes():
    assert pickup_ts_ms(b'{"trip_id":"x","pickup_ts":"2025-10-07T08:14:00"}', None, 42, None) == 1759824840000


def test_extractor_uses_pickup_ts_from_dict():
    assert pickup_ts_ms({"pickup_ts": "2025-10-07T08:14:00"}, None, 42, None) == 1759824840000


def test_extractor_falls_back_to_kafka_timestamp():
    assert pickup_ts_ms(b"not json", None, 42, None) == 42
    assert pickup_ts_ms({"pickup_ts": "later"}, None, 42, None) == 42
```
Run: `uv run pytest tests/test_sinks.py tests/test_app.py -q`. Expected: FAIL with `ModuleNotFoundError: No module named 'processor.sinks'`.

- [ ] **Step 2: Implement `log.py`, `health.py`, `sinks.py`**

`services/processor/src/processor/log.py`:
```python
"""Structured JSON logs on stdout (CLAUDE.md conventions)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any

SERVICE = "processor"


def event(name: str, **fields: Any) -> None:
    record = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "service": SERVICE,
              "event": name, **fields}
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)
```

`services/processor/src/processor/health.py`:
```python
"""Container healthcheck: exit 0 if the heartbeat file was touched recently (python -m processor.health)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

HEARTBEAT = Path("/tmp/healthy")


def main(path: str = str(HEARTBEAT), max_age_s: str = "30") -> int:
    p = Path(path)
    return 0 if p.exists() and time.time() - p.stat().st_mtime < float(max_age_s) else 1


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
```

`services/processor/src/processor/sinks.py`:
```python
"""ClickHouse sink + checkpoint producer (spec 03 §ClickHouse sinks, ADR-003)."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

from clickhouse_connect.driver.exceptions import OperationalError
from confluent_kafka import Producer
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

from processor.log import event

TRACE_TOPIC = "trace.checkpoints"


class ClickHouseSink(BatchingSink):
    """One INSERT per Quix checkpoint (commit_interval). A connection error pauses the partition and
    retries; the batch is never dropped, and after_insert only runs for rows that were stored."""

    def __init__(self, *, table: str, columns: Sequence[str], to_row: Callable[[dict[str, Any]], list[Any]],
                 client_factory: Callable[[], Any],
                 after_insert: Callable[[list[dict[str, Any]]], None] | None = None) -> None:
        super().__init__()
        self._table, self._columns, self._to_row = table, list(columns), to_row
        self._client_factory, self._after_insert = client_factory, after_insert
        self._client: Any = None

    def setup(self) -> None:
        try:
            self._client = self._client_factory()
        except Exception as e:  # noqa: BLE001 — ClickHouse may start after us; write() retries
            event("clickhouse_unavailable", table=self._table, error=str(e)[:200])

    def write(self, batch: SinkBatch) -> None:
        self.write_values([item.value for item in batch])

    def write_values(self, values: list[dict[str, Any]]) -> None:
        if not values:
            return
        rows = [self._to_row(v) for v in values]
        try:
            if self._client is None:
                self._client = self._client_factory()
            self._client.insert(self._table, rows, column_names=self._columns)
        except (OperationalError, OSError) as e:
            self._client = None
            event("sink_backpressure", table=self._table, rows=len(rows), error=str(e)[:200])
            raise SinkBackpressureError(retry_after=5.0) from e
        if self._after_insert is not None:
            self._after_insert(values)


class CheckpointProducer:
    """Separate producer for "stored" checkpoints, which exist only after the INSERT succeeded."""

    def __init__(self, brokers: str) -> None:
        self._p = Producer({"bootstrap.servers": brokers, "linger.ms": 5})

    def emit(self, checkpoints: list[dict[str, Any]]) -> None:
        for cp in checkpoints:
            self._p.produce(TRACE_TOPIC, key=cp["trace_id"].encode(), value=json.dumps(cp).encode())
        if checkpoints:
            self._p.flush(5)
```

- [ ] **Step 3: Implement `app.py`**

```python
"""Stream processor: python -m processor.app (spec 03).

  trips.raw ─annotate─┬─ traced ───────────────────► trace.checkpoints (validated | rejected)
                      ├─ rejected ─────────────────► trips.rejected + taxi.trips_rejected
                      └─ valid ─enrich─┬───────────► trips.enriched + taxi.trips (then "stored" checkpoints)
                                       └─ traced ──► trace.checkpoints (enriched, aggregated)
  trips.enriched ─group_by(pu_borough)─tumbling 1 min (sim time)─► agg.borough_minute + taxi.agg_borough_minute
"""

from __future__ import annotations

import re
import threading
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect
from quixstreams import Application

from processor.aggregate import add_trip, init_bucket, to_agg_record
from processor.checkpoints import enriched_and_aggregated, stored_checkpoint, validated_or_rejected
from processor.config import Config
from processor.enrich import enrich
from processor.health import HEARTBEAT
from processor.log import event
from processor.rows import AGG_COLUMNS, REJECTED_COLUMNS, TRIPS_COLUMNS, agg_row, rejected_row, trip_row
from processor.rules import annotate, is_traced, to_rejected
from processor.sinks import CheckpointProducer, ClickHouseSink
from processor.times import parse_sim, sim_epoch_ms, wall_now_str
from processor.zones import load_zones

_PICKUP = re.compile(rb'"pickup_ts"\s*:\s*"([^"]+)"')


def pickup_ts_ms(value: Any, headers: Any, timestamp: int, timestamp_type: Any) -> int:
    """Event time = pickup_ts, so windows run on the simulated clock. Falls back to the Kafka timestamp."""
    raw: Any = None
    if isinstance(value, (bytes, bytearray)):
        match = _PICKUP.search(value)
        raw = match.group(1).decode("utf-8", "replace") if match else None
    elif isinstance(value, dict):
        raw = value.get("pickup_ts")
    dt = parse_sim(raw)
    return sim_epoch_ms(dt) if dt is not None else timestamp


class Stats:
    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def add(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counts[key] += n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            snap = dict(self._counts)
            self._counts.clear()
            return snap

    def report_forever(self, heartbeat: Path, interval_s: float = 10.0) -> None:
        heartbeat.touch()
        while True:
            time.sleep(interval_s)
            heartbeat.touch()
            snap = self.snapshot()
            reasons = {k.split(":", 1)[1]: v for k, v in snap.items() if k.startswith("reason:")}
            event("progress", **{k: v for k, v in snap.items() if ":" not in k}, by_reason=reasons)


def build(cfg: Config, stats: Stats) -> Application:
    zones = load_zones(cfg.zones_path)
    app = Application(
        broker_address=cfg.brokers,
        consumer_group=cfg.consumer_group,
        auto_offset_reset="earliest",
        commit_interval=cfg.commit_interval_s,
        processing_guarantee="at-least-once",
        state_dir=str(cfg.state_dir),
        loglevel="WARNING",
    )
    raw_t = app.topic("trips.raw", value_deserializer="bytes", key_deserializer="str",
                      timestamp_extractor=pickup_ts_ms)
    enriched_t = app.topic("trips.enriched", key_serializer="str", key_deserializer="str",
                           timestamp_extractor=pickup_ts_ms)
    rejected_t = app.topic("trips.rejected", key_serializer="str")
    agg_t = app.topic("agg.borough_minute", key_serializer="str")
    trace_t = app.topic("trace.checkpoints", key_serializer="str")

    def clickhouse() -> Any:
        return clickhouse_connect.get_client(host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
                                             password=cfg.ch_password, database=cfg.ch_db)

    checkpoints = CheckpointProducer(cfg.brokers)

    def after_trips_insert(values: list[dict[str, Any]]) -> None:
        stats.add("sink_rows", len(values))
        at = wall_now_str()
        checkpoints.emit([stored_checkpoint(v, "taxi.trips", len(values), at) for v in values if is_traced(v)])

    trips_sink = ClickHouseSink(table="trips", columns=TRIPS_COLUMNS, to_row=trip_row,
                                client_factory=clickhouse, after_insert=after_trips_insert)
    rejects_sink = ClickHouseSink(table="trips_rejected", columns=REJECTED_COLUMNS, to_row=rejected_row,
                                  client_factory=clickhouse)
    agg_sink = ClickHouseSink(table="agg_borough_minute", columns=AGG_COLUMNS, to_row=agg_row,
                              client_factory=clickhouse)

    def annotate_counted(raw: bytes) -> dict[str, Any]:
        value = annotate(raw, cfg.tip_max_abs)
        stats.add("in")
        stats.add("rejected" if value["_reasons"] else "valid")
        for reason in value["_reasons"]:
            stats.add(f"reason:{reason}")
        return value

    def on_late(*args: Any, **kwargs: Any) -> bool:
        stats.add("late_records")
        return True  # keep Quix's own warning log line

    # 1 · clean
    sdf = app.dataframe(raw_t).apply(annotate_counted)
    sdf.filter(is_traced).apply(lambda v: validated_or_rejected(v, wall_now_str())) \
        .to_topic(trace_t, key=lambda c: c["trace_id"])
    sdf.filter(lambda v: bool(v["_reasons"])).apply(lambda v: to_rejected(v, wall_now_str())) \
        .to_topic(rejected_t, key=lambda r: r["trip_id"]).sink(rejects_sink)

    # 2 · enrich
    valid = sdf.filter(lambda v: not v["_reasons"]).apply(lambda v: enrich(v, zones, wall_now_str()))
    valid.filter(is_traced).apply(lambda v: enriched_and_aggregated(v, wall_now_str()), expand=True) \
        .to_topic(trace_t, key=lambda c: c["trace_id"])
    valid.to_topic(enriched_t, key=lambda v: v["trip_id"]).sink(trips_sink)

    # 3 · aggregate the enriched stream in 1-minute sim-time windows per pickup borough
    app.dataframe(enriched_t).group_by("pu_borough") \
        .tumbling_window(timedelta(minutes=1), grace_ms=timedelta(seconds=cfg.window_grace_s), on_late=on_late) \
        .reduce(add_trip, init_bucket).final() \
        .apply(lambda w, key, ts, headers: to_agg_record(w, key, wall_now_str()), metadata=True) \
        .update(lambda r: stats.add("windows_emitted")) \
        .to_topic(agg_t, key=lambda r: r["borough"]).sink(agg_sink)
    return app


def main() -> int:
    cfg = Config.from_env()
    stats = Stats()
    app = build(cfg, stats)
    threading.Thread(target=stats.report_forever, args=(HEARTBEAT,), name="progress", daemon=True).start()
    event("start", consumer_group=cfg.consumer_group, commit_interval_s=cfg.commit_interval_s,
          window_grace_s=cfg.window_grace_s)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```
Run: `uv run pytest -q`. Expected: all unit tests pass (0 failed).

- [ ] **Step 4: Write the integration test (real Redpanda + real ClickHouse with our DDL and users)**

`services/processor/tests/test_integration.py`:
```python
"""Processor end to end: testcontainers Redpanda + ClickHouse (infra/clickhouse init + users), the processor
running as a subprocess exactly like in Docker. Needs Docker. Run: uv run pytest -m integration."""

import json
import os
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import clickhouse_connect
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from contract_validator import assert_valid
from testcontainers.core.container import DockerContainer
from testcontainers.kafka import RedpandaContainer

from conftest import FIXTURES, REPO

pytestmark = pytest.mark.integration
RP_IMAGE = "redpandadata/redpanda:v26.2.3"
CH_IMAGE = "clickhouse/clickhouse-server:26.3.34"
PW = {"admin": "admin-pw", "ingest": "ingest-pw", "agent": "agent-pw", "grafana": "grafana-pw"}
ZONES = [161, 255, 7, 69, 5, 132]  # Manhattan, Brooklyn, Queens, Bronx, Staten Island, Queens (JFK)
START = datetime(2025, 10, 7, 8, 0, 0)
FLUSH = START + timedelta(hours=2)


def wait_for(fn, timeout=120.0, what="condition"):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(1)
    raise AssertionError(f"timed out waiting for {what} (last error: {last!r})")


def trip(i: int, pickup: datetime, **over) -> dict:
    card = i % 4 != 0
    return {"trip_id": f"r7-{i:09d}", "source": "replay", "trace_id": None, "loop": 7, "vendor_id": 2,
            "pickup_ts": pickup.isoformat(timespec="seconds"),
            "dropoff_ts": (pickup + timedelta(minutes=12)).isoformat(timespec="seconds"),
            "passenger_count": 1, "trip_distance": 2.5, "ratecode_id": 1, "store_and_fwd_flag": "N",
            "pu_location_id": ZONES[i % len(ZONES)], "do_location_id": 237, "payment_type": 1 if card else 2,
            "fare_amount": 15.0, "extra": 0.0, "mta_tax": 0.5, "tip_amount": 3.0 if card else 0.0,
            "tolls_amount": 0.0, "improvement_surcharge": 1.0, "congestion_surcharge": 2.5, "airport_fee": None,
            "cbd_congestion_fee": 0.75, "total_amount": 22.75, "produced_at": "2026-10-16T08:00:00.000Z"} | over


def fixture_messages(good_trace: str, bad_trace: str) -> list[tuple[str, bytes]]:
    msgs = []
    for i in range(1000):
        over: dict = {}
        if i % 20 == 19:  # 50 bad trips: 25 refunds, 25 zero-distance
            over = {"fare_amount": -5.0, "total_amount": -5.0} if (i // 20) % 2 == 0 else {"trip_distance": 0.0}
        if i == 100:
            over["trace_id"] = good_trace
        if i == 119:
            over["trace_id"] = bad_trace
        msgs.append((f"r7-{i:09d}", json.dumps(trip(i, START + timedelta(seconds=3 * i), **over)).encode()))
    msgs.append(("garbage", b"not json"))
    for j, zone in enumerate(ZONES):  # later pickups in every borough close all open windows
        msgs.append((f"r7-{2000 + j:09d}", json.dumps(trip(2000 + j, FLUSH, pu_location_id=zone)).encode()))
    return msgs


def one(client, sql):
    return client.query(sql).result_rows[0][0]


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    clickhouse = (
        DockerContainer(CH_IMAGE)
        .with_env("CLICKHOUSE_USER", "admin").with_env("CLICKHOUSE_PASSWORD", PW["admin"])
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_env("CLICKHOUSE_INGEST_PASSWORD", PW["ingest"]).with_env("CLICKHOUSE_AGENT_PASSWORD", PW["agent"])
        .with_env("CLICKHOUSE_GRAFANA_PASSWORD", PW["grafana"])
        .with_volume_mapping(str(REPO / "infra/clickhouse/init"), "/docker-entrypoint-initdb.d", "ro")
        .with_volume_mapping(str(REPO / "infra/clickhouse/config.d/stand.xml"),
                             "/etc/clickhouse-server/config.d/stand.xml", "ro")
        .with_volume_mapping(str(REPO / "infra/clickhouse/users.d/stand.xml"),
                             "/etc/clickhouse-server/users.d/stand.xml", "ro")
        .with_volume_mapping(str(FIXTURES / "zones.csv"), "/data/zones.csv", "ro")
        .with_exposed_ports(8123)
    )
    with RedpandaContainer(RP_IMAGE) as rp, clickhouse:
        bootstrap = rp.get_bootstrap_server()
        topics = [NewTopic(t, 3, 1) for t in
                  ("trips.raw", "trips.enriched", "trips.rejected", "agg.borough_minute", "trace.checkpoints")]
        for f in AdminClient({"bootstrap.servers": bootstrap}).create_topics(topics).values():
            f.result(30)
        host, port = clickhouse.get_container_host_ip(), int(clickhouse.get_exposed_port(8123))

        def admin_ready():
            c = clickhouse_connect.get_client(host=host, port=port, username="admin", password=PW["admin"],
                                              database="taxi")
            return c if one(c, "SELECT count() FROM taxi.zones") > 0 else None

        admin = wait_for(admin_ready, what="ClickHouse init scripts")
        env = {**os.environ, "TZ": "UTC", "KAFKA_BROKERS": bootstrap, "CLICKHOUSE_HOST": host,
               "CLICKHOUSE_HTTP_PORT": str(port), "CLICKHOUSE_DB": "taxi", "CLICKHOUSE_INGEST_USER": "ingest_writer",
               "CLICKHOUSE_INGEST_PASSWORD": PW["ingest"], "PROCESSOR_ZONES_PATH": str(FIXTURES / "zones.csv"),
               "PROCESSOR_STATE_DIR": str(tmp_path_factory.mktemp("state")),
               "PROCESSOR_CONSUMER_GROUP": f"it-{uuid.uuid4().hex[:8]}", "PROCESSOR_WINDOW_GRACE_S": "0"}
        log_path = tmp_path_factory.mktemp("logs") / "processor.log"
        log = log_path.open("w")
        proc = subprocess.Popen([sys.executable, "-m", "processor.app"], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            good, bad = str(uuid.uuid4()), str(uuid.uuid4())
            producer = Producer({"bootstrap.servers": bootstrap})
            for key, value in fixture_messages(good, bad):
                producer.produce("trips.raw", key=key, value=value)
            producer.flush(30)
            wait_for(lambda: one(admin, "SELECT count() FROM taxi.trips") >= 956
                     and one(admin, "SELECT count() FROM taxi.trips_rejected") >= 51
                     and (one(admin, "SELECT sum(trips) FROM taxi.agg_borough_minute "
                                     "WHERE window_start < '2025-10-07 10:00:00'") or 0) >= 950,
                     what=f"rows in ClickHouse (processor log: {log_path})")
            yield {"bootstrap": bootstrap, "admin": admin, "good": good, "bad": bad, "proc": proc}
        finally:
            proc.terminate()
            proc.wait(30)
            log.close()


def consume(bootstrap, topic, want, timeout=30.0):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"check-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    out, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline and not want(out):
        msg = c.poll(1.0)
        if msg is not None and msg.error() is None:
            out.append(json.loads(msg.value()))
    c.close()
    return out


def test_processor_is_still_running(stack):
    assert stack["proc"].poll() is None


def test_valid_and_rejected_counts(stack):
    a = stack["admin"]
    assert one(a, "SELECT count() FROM taxi.trips FINAL") == 956
    reasons = dict(a.query("SELECT reason, count() FROM taxi.trips_rejected FINAL ARRAY JOIN reasons AS reason "
                           "GROUP BY reason").result_rows)
    assert reasons == {"FARE_NON_POSITIVE": 25, "TOTAL_NEGATIVE": 25, "DISTANCE_NON_POSITIVE": 25, "SCHEMA_INVALID": 1}


def test_aggregates_sum_to_valid_trips(stack):
    assert one(stack["admin"], "SELECT sum(trips) FROM taxi.agg_borough_minute FINAL "
                               "WHERE window_start < '2025-10-07 10:00:00'") == 950


def test_sim_timestamps_are_stored_unshifted(stack):
    rows = stack["admin"].query("SELECT toString(pickup_ts), pu_zone, pu_borough, tip_pct FROM taxi.trips "
                                "WHERE trip_id = 'r7-000000001'").result_rows
    assert rows == [("2025-10-07 08:00:03.000", "Williamsburg (North Side)", "Brooklyn", 20.0)]


def test_trace_checkpoints_in_order(stack):
    good, bad = stack["good"], stack["bad"]

    def complete(msgs):
        by = defaultdict(list)
        for m in msgs:
            by[m["trace_id"]].append(m["stage"])
        return len(by[good]) >= 4 and len(by[bad]) >= 1

    msgs = consume(stack["bootstrap"], "trace.checkpoints", complete)
    for m in msgs:
        assert_valid("trace_checkpoint", m)
    assert [m["stage"] for m in msgs if m["trace_id"] == good] == ["validated", "enriched", "aggregated", "stored"]
    assert [m["stage"] for m in msgs if m["trace_id"] == bad] == ["rejected"]


@pytest.mark.parametrize("topic,schema", [("trips.enriched", "enriched_trip"), ("trips.rejected", "rejected_trip"),
                                          ("agg.borough_minute", "borough_minute_agg")])
def test_output_topics_follow_contracts(stack, topic, schema):
    msgs = consume(stack["bootstrap"], topic, lambda m: len(m) >= 20)
    assert msgs
    for m in msgs:
        assert_valid(schema, m)
```
Run: `uv run pytest -m integration -q`. Expected: `8 passed` (the first run pulls the images, so allow a few minutes).

If `group_by` or windows behave unexpectedly, read the processor log path printed in the timeout message. The known knobs are `PROCESSOR_WINDOW_GRACE_S` and the flush trips.

- [ ] **Step 5: Image and compose service**

`services/processor/.dockerignore`:
```
.venv
__pycache__
.pytest_cache
.ruff_cache
.mypy_cache
tests
```

`services/processor/Dockerfile`:
```Dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 TZ=UTC PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev
RUN useradd --system --uid 10001 app && mkdir -p /state && chown app /state
USER app
CMD ["python", "-m", "processor.app"]
```

Add to `docker-compose.yml` under `services:`, before `replayer`:
```yaml
  processor:
    build: services/processor
    env_file: [.env]
    environment:
      KAFKA_BROKERS: redpanda:9092
      CLICKHOUSE_HOST: clickhouse
      PROCESSOR_STATE_DIR: /state
      PROCESSOR_ZONES_PATH: /data/zones.csv
      TZ: UTC
    depends_on:
      redpanda-init: { condition: service_completed_successfully }
      clickhouse: { condition: service_healthy }
    volumes:
      - processor-state:/state
      - ./data/zones.csv:/data/zones.csv:ro
    healthcheck:
      test: ["CMD", "python", "-m", "processor.health"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 30s
    mem_limit: 768m
    restart: unless-stopped
    logging: *logging
```
In the `replayer` service, change `depends_on` to:
```yaml
    depends_on:
      redpanda-init: { condition: service_completed_successfully }
      processor: { condition: service_healthy }
```
Run: `make up && sleep 20 && docker compose logs --tail=5 processor`. Expected: `progress` lines with `in`, `valid`, `rejected`, and `sink_rows` > 0.

- [ ] **Step 6: Commit**

```bash
git add services/processor docker-compose.yml
git commit -m "feat(processor): ClickHouse sinks, Quix app (clean/enrich/window), image, compose"
```

---

### Task 10: M1 exit: smoke check, first backfill, docs and decisions

**Files:**
- Create: `scripts/smoke_pipeline.py`
- Modify: `docs/specs/03-stream-processor.md` (pipeline sketch), `docs/DIAGRAMS.md` §2 + §12 (+ `make diagrams-sync`), `docs/DECISIONS.md` (ADR-013), `docs/RUNBOOK.md` (backfill duration), `docs/specs/10-testing-soak-and-evals.md` (integration approach), `docs/IMPLEMENTATION_PLAN.md` (tick M0/M1)

**Interfaces:**
- Consumes: the running stack from Tasks 4, 7, 9.
- Produces: `make smoke`, which exits 0 only when every M1 exit criterion holds.

- [ ] **Step 1: Smoke script**

`scripts/smoke_pipeline.py`:
```python
"""M1 exit check against the running stack (make smoke). Exit 1 if any check fails."""

from __future__ import annotations

import time
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parent.parent
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if "=" in line and not line.lstrip().startswith("#"))


def main() -> int:
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="admin",
                                       password=ENV["CLICKHOUSE_ADMIN_PASSWORD"], database="taxi")

    def one(sql: str):
        return ch.query(sql).result_rows[0][0]

    before = one("SELECT count() FROM taxi.trips")
    time.sleep(10)
    rate = (one("SELECT count() FROM taxi.trips") - before) / 10
    fresh_ms = one("SELECT dateDiff('millisecond', max(stored_at), now64(3)) FROM taxi.trips")
    p95_ms = one("SELECT quantile(0.95)(dateDiff('millisecond', produced_at, stored_at)) FROM taxi.trips "
                 "WHERE stored_at >= now64(3) - INTERVAL 1 MINUTE")
    rejected = one("SELECT count() FROM taxi.trips_rejected")
    distinct_reasons = one("SELECT uniqExact(reason) FROM taxi.trips_rejected ARRAY JOIN reasons AS reason")
    windows = one("SELECT count() FROM taxi.agg_borough_minute")
    print("sim clock:", one("SELECT toString(now_ts) FROM taxi.sim_now"))
    checks = [
        ("rows flowing", rate >= 5, f"{rate:.1f} rows/s (≈80–200 in daytime at 60×)"),
        ("freshness < 3 s", fresh_ms < 3000, f"{fresh_ms} ms"),
        ("p95 produced→stored < 2 s (last minute)", p95_ms == p95_ms and p95_ms < 2000, f"{p95_ms:.0f} ms"),
        ("rejected rows with reasons", rejected > 0 and distinct_reasons >= 2,
         f"{rejected} rows, {distinct_reasons} distinct reasons"),
        ("borough-minute aggregates", windows > 0, f"{windows} windows"),
    ]
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name:<42} {detail}")
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
```
(`p95_ms == p95_ms` is False for NaN, which ClickHouse returns when there are no rows in the last minute.)

Run: `make smoke`. Expected: 5 × PASS, exit 0.

- [ ] **Step 2: First backfill (measures the night-before duration)**

Run: `time make backfill UNTIL=<suggested_backfill_until from data/replay/prep_report.json>`. For example, `UNTIL=2025-10-14T07:00:00`.
Expected: the replayer logs `backfill_done`, lag drains to 0, the live replayer restarts, and `curl -s 127.0.0.1:8001/status` shows `sim_ts` just after UNTIL.
Then run: `make smoke`. Expected: 5 × PASS. Also check `SELECT count() FROM taxi.trips` ≈ 7 days of trips (≈ 800 k–1 M).

Record the wall duration in `docs/RUNBOOK.md` §2: replace `note the duration: ____ min` with the measured value.

- [ ] **Step 3: Update docs for the implementation decisions**

```bash
uv run python - <<'EOF'
import pathlib
spec = pathlib.Path("docs/specs/03-stream-processor.md"); s = spec.read_text()
s = s.replace('''agg = (valid.group_by("pu_borough")''', '''agg = (app.dataframe(enriched_topic).group_by("pu_borough")   # second input: the enriched stream''')
s = s.replace("This is a sketch: adapt it to the pinned Quix version's branching API.",
              "This is a sketch; the implementation is `services/processor/src/processor/app.py`. The aggregation reads "
              "`trips.enriched` as a second StreamingDataFrame (ADR-013).")
spec.write_text(s)
d = pathlib.Path("docs/DIAGRAMS.md"); t = d.read_text()
t = t.replace("        V[validate] --> E[enrich] --> W[1-min tumbling window<br/>per pu_borough]",
              "        V[validate] --> E[enrich]\n        W[1-min tumbling window<br/>per pu_borough]")
t = t.replace("    E --> enr\n", "    E --> enr\n    enr --> W\n")
t = t.replace('    enr --> gb["group_by pu_borough"]', '    enrt --> gb["group_by pu_borough"]')
d.write_text(t)
t10 = pathlib.Path("docs/specs/10-testing-soak-and-evals.md"); u = t10.read_text()
u = u.replace("| Integration (testcontainers: Redpanda + ClickHouse) |",
              "| Integration (testcontainers: Redpanda + ClickHouse with the real init SQL and users; the processor runs as a subprocess) |")
t10.write_text(u)
EOF
make diagrams-sync && make docs-check
```
Expected: the diagrams are updated, and `docs-check` prints only `ok` lines.

Append to `docs/DECISIONS.md`:
```markdown

## ADR-013: Aggregation reads `trips.enriched`; implementation notes from M1
**Status:** accepted · 2026-09-28

**Context:** Building M1 with Quix Streams 3.26.

**Decision:**
- The windowed aggregation is a second StreamingDataFrame on `trips.enriched`, not a `group_by` branch off the validated stream. Branching, `group_by`, and windows then never interact.
- `avg_speed_mph` is rounded to 2 decimals.
- The processor integration test runs the processor as a subprocess against testcontainers Redpanda and ClickHouse, the latter with the real init SQL and users.
- *(If Task 4 needed it:)* `agent_ro` uses `readonly=2` with `<readonly/>` constraints, because clickhouse-connect sends session settings.

**Consequences:** The aggregates are computed from exactly what is stored in `taxi.trips`, which is easier to explain. There is one extra consumer of `trips.enriched`, which is negligible.
```

In `docs/IMPLEMENTATION_PLAN.md`, tick every M0 and M1 checkbox that is done (`- [ ]` → `- [x]`) **except** "Install Ollama… candidate models", "Ask IT…", "Order or borrow…" (people tasks) and the M1 api-skeleton item (moved to the M3 plan).

- [ ] **Step 4: Full verification**

Run each and confirm:
- `make test`: 0 failed (root, replayer, processor).
- `make test-integration`: 0 failed.
- `make lint`: ruff clean, mypy clean. Fix any type errors in `src/` only, without changing behaviour. Re-run `make test` after fixing.
- `make test-stack`: 18 passed.
- `make smoke`: 5 × PASS.
- `docker compose ps`: every service `healthy`, with 0 restarts (`docker inspect -f '{{.RestartCount}}' $(docker compose ps -q)` all `0`).

- [ ] **Step 5: Commit**

```bash
git add scripts/smoke_pipeline.py docs
git commit -m "chore: M1 exit — smoke check, first backfill, ADR-013, docs synced"
```

---

## Self-review notes (for the executor)
- **Order:** Task 1 → Task 8 → Task 2 → 3 → 4 → 5 → 6 → 7 → 9 → 10. Task 2's report imports `processor.rules`, so Task 8 comes early.
- **Out of scope here** (next plans):
  - the API skeleton (`/api/healthz`, `/api/zones`, `/api/sim/clock`, moved to the M3 plan)
  - Grafana dashboards (M2)
  - the agent (M3) and trace relay / rides (M4)
  - Caddy `/api` + web routes (M3)
- **Chaos drill** "ClickHouse outage 60 s" (spec 03 acceptance 6) is exercised in M5. The sink behaviour it relies on is unit-tested here (`test_connection_error_becomes_backpressure`).
