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
