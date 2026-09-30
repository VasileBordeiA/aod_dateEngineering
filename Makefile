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
	cd services/api && uv run pytest -q
	cd web && pnpm test
	cd web && pnpm test:e2e  # Playwright against a mocked api (spec 10), no stack needed

test-integration: ## testcontainers integration tests (needs Docker running)
	cd services/replayer && uv run pytest -q -m integration
	cd services/processor && uv run pytest -q -m integration

test-stack: ## Checks against the running stack (after make up)
	uv run pytest -q -m stack

test-e2e: ## Trace your ride end to end on the running stack (spec 07 acceptance 3-6)
	uv run pytest -q -m e2e

burst: ## 50 rides in 10 s from 10 simulated phones (spec 07 acceptance 7). Book no other rides in the minute before.
	uv run python scripts/burst_rides.py

reset-rides: ## Delete every visitor ride (trips, rejects, trace events). Asks first.
	@read -p "Delete all student rides? [y/N] " a && [ "$$a" = y ]
	$(COMPOSE) exec -T clickhouse clickhouse-client --user admin --password "$(CLICKHOUSE_ADMIN_PASSWORD)" --multiquery -q \
	  "DELETE FROM taxi.trips WHERE source = 'student'; DELETE FROM taxi.trips_rejected WHERE source = 'student'; DELETE FROM taxi.trace_events WHERE source = 'student'"

lint: ## ruff + mypy
	uv run ruff check scripts contracts infra
	cd services/replayer && uv run ruff check src tests && uv run mypy src
	cd services/processor && uv run ruff check src tests && uv run mypy src
	cd services/api && uv run ruff check src tests && uv run mypy src
	cd web && pnpm lint && pnpm typecheck

smoke: ## M1 exit check: rows flowing, rejects with reasons, freshness
	uv run python scripts/smoke_pipeline.py

backfill: ## Replay at max speed through the real pipeline: make backfill UNTIL=2025-10-14T07:00:00
	@test -n "$(UNTIL)" || (echo "usage: make backfill UNTIL=2025-10-14T07:00:00" && exit 1)
	$(COMPOSE) stop replayer
	$(COMPOSE) run --rm --no-deps -e REPLAY_SPEED=max -e REPLAY_UNTIL_TS=$(UNTIL) replayer
	@echo "waiting for processor lag to reach 0…"
	@# only the pipeline topics: Quix's changelog topic always shows "lag" (it is not consumed by the group)
	@until [ "$$($(COMPOSE) exec -T redpanda rpk group describe $(PROCESSOR_CONSUMER_GROUP) | awk '$$1=="trips.raw"||$$1=="trips.enriched"{s+=$$6} END{print s+0}')" = "0" ]; do sleep 5; done
	$(COMPOSE) start replayer

pause: ## Pause the replayer (sim clock freezes)
	@curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" $(CTRL)/pause && echo
resume: ## Resume the replayer
	@curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" $(CTRL)/resume && echo
speed: ## Change replay speed: make speed S=120
	@curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" -d '{"speed": $(S)}' $(CTRL)/speed && echo

migrate: ## Re-apply the idempotent ClickHouse DDL
	for f in infra/clickhouse/init/*.sql; do echo "$$f"; $(COMPOSE) exec -T clickhouse clickhouse-client --user admin --password "$(CLICKHOUSE_ADMIN_PASSWORD)" --multiquery < $$f || exit 1; done

diagrams-sync: ## Copy diagrams from docs/DIAGRAMS.md into the specs
	python3 scripts/sync_diagrams.py

docs-check: ## Diagrams in sync + all Mermaid renders + contracts valid
	python3 scripts/sync_diagrams.py --check
	bash scripts/check_mermaid.sh
	uv run pytest -q contracts/tests

eval: ## Golden-set eval on the running api: make eval MODEL=taxi-qwen2.5-coder-14b [MODE=tools|oneshot] [PROVIDER=local] [ONLY=id,id]
	LLM_LOCAL_MODEL="$(or $(MODEL),$(LLM_LOCAL_MODEL))" AGENT_MODE="$(or $(MODE),$(AGENT_MODE))" \
	  LLM_ORDER="$(or $(PROVIDER),$(LLM_ORDER))" $(COMPOSE) up -d --wait api
	uv run python scripts/eval_agent.py --model "$(or $(MODEL),$(LLM_LOCAL_MODEL))" \
	  --mode "$(or $(MODE),$(AGENT_MODE))" --provider "$(or $(PROVIDER),$(LLM_ORDER))" $(if $(ONLY),--only $(ONLY))
	@echo "api still runs the eval settings; 'make up' restores .env"

llm: ## Switch the LLM provider order live: make llm ORDER=internal,local (ORDER= means cached answers only)
	@curl -fsS -X POST -H "X-Admin-Token: $(ADMIN_TOKEN)" -H 'Content-Type: application/json' \
	  -d "$$(python3 -c 'import json,sys; print(json.dumps({"order": [n for n in sys.argv[1].split(",") if n]}))' '$(ORDER)')" \
	  http://127.0.0.1:8000/api/admin/llm/order && echo

ollama-model: ## Derive a stand model with a 16 k context: make ollama-model BASE=qwen2.5-coder:14b
	@bash scripts/ollama_model.sh "$(BASE)"

.PHONY: help secrets data up down reset status logs images test test-integration test-stack test-e2e burst reset-rides lint smoke backfill pause resume speed migrate diagrams-sync docs-check eval llm ollama-model
