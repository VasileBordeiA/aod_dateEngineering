"""Every api setting comes from env vars. .env.example documents each key (CLAUDE.md conventions)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

PROVIDER_NAMES = ("local", "internal")


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    base_url: str
    api_key: str
    model: str


@dataclass(frozen=True)
class Config:
    brokers: str
    ch_host: str
    ch_port: int
    ch_db: str
    agent_user: str
    agent_password: str
    ingest_user: str
    ingest_password: str
    providers: tuple[ProviderConfig, ...]
    llm_order: tuple[str, ...]
    llm_timeout_s: float
    llm_temperature: float
    llm_health_interval_s: float
    agent_mode: str
    max_tool_iterations: int
    max_repairs: int
    max_question_chars: int
    total_timeout_s: float
    rate_limit_per_min: int
    max_concurrency: int
    max_queue: int
    admin_token: str
    golden_path: Path
    trace_user: str
    trace_password: str
    rides_rate_limit_per_min: int
    rides_global_per_min: int
    trace_history_ttl_s: float
    trace_queryable_poll_ms: int
    trace_read_rate_limit_per_min: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        e = os.environ if env is None else env

        def get(key: str, default: str | None = None) -> str:
            value = e.get(key, default)
            if value is None:
                raise ValueError(f"missing env var {key}")
            return value

        providers = tuple(
            ProviderConfig(name, get(f"LLM_{name.upper()}_BASE_URL", ""), get(f"LLM_{name.upper()}_API_KEY", ""),
                           get(f"LLM_{name.upper()}_MODEL", ""))
            for name in PROVIDER_NAMES
            if get(f"LLM_{name.upper()}_BASE_URL", "").strip()
        )
        mode = get("AGENT_MODE", "tools")
        if mode not in ("tools", "oneshot"):
            raise ValueError(f"AGENT_MODE must be 'tools' or 'oneshot', got {mode!r}")
        return cls(
            brokers=get("KAFKA_BROKERS", "redpanda:9092"),
            ch_host=get("CLICKHOUSE_HOST", "clickhouse"),
            ch_port=int(get("CLICKHOUSE_HTTP_PORT", "8123")),
            ch_db=get("CLICKHOUSE_DB", "taxi"),
            agent_user=get("CLICKHOUSE_AGENT_USER", "agent_ro"),
            agent_password=get("CLICKHOUSE_AGENT_PASSWORD"),
            ingest_user=get("CLICKHOUSE_INGEST_USER", "ingest_writer"),
            ingest_password=get("CLICKHOUSE_INGEST_PASSWORD"),
            providers=providers,
            llm_order=tuple(n.strip() for n in get("LLM_ORDER", "local,internal").split(",") if n.strip()),
            llm_timeout_s=float(get("LLM_TIMEOUT_S", "30")),
            llm_temperature=float(get("LLM_TEMPERATURE", "0")),
            llm_health_interval_s=float(get("LLM_HEALTH_INTERVAL_S", "15")),
            agent_mode=mode,
            max_tool_iterations=int(get("AGENT_MAX_TOOL_ITERATIONS", "6")),
            max_repairs=int(get("AGENT_MAX_REPAIRS", "2")),
            max_question_chars=int(get("AGENT_MAX_QUESTION_CHARS", "300")),
            total_timeout_s=float(get("AGENT_TOTAL_TIMEOUT_S", "45")),
            rate_limit_per_min=int(get("AGENT_RATE_LIMIT_PER_MIN", "10")),
            max_concurrency=int(get("AGENT_MAX_CONCURRENCY", "2")),
            max_queue=int(get("AGENT_MAX_QUEUE", "6")),
            admin_token=get("ADMIN_TOKEN"),
            golden_path=Path(get("AGENT_GOLDEN_PATH", "/app/evals/agent_golden_questions.yaml")),
            trace_user=get("CLICKHOUSE_TRACE_USER", "trace_ro"),
            trace_password=get("CLICKHOUSE_TRACE_PASSWORD"),
            rides_rate_limit_per_min=int(get("RIDES_RATE_LIMIT_PER_MIN", "3")),
            rides_global_per_min=int(get("RIDES_GLOBAL_PER_MIN", "30")),
            trace_history_ttl_s=float(get("TRACE_HISTORY_TTL_S", "3600")),
            trace_queryable_poll_ms=int(get("TRACE_QUERYABLE_POLL_MS", "250")),
            trace_read_rate_limit_per_min=int(get("TRACE_READ_RATE_LIMIT_PER_MIN", "120")),
        )
