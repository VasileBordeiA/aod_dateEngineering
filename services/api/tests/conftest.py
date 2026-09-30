import sys
from pathlib import Path
from typing import Any

import pytest

from api.db import Result
from api.envconfig import Config
from api.kafka import Delivery, KafkaDeliveryError

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "contracts"))
GOLDEN = REPO / "evals" / "agent_golden_questions.yaml"

BASE_ENV = {
    "CLICKHOUSE_AGENT_PASSWORD": "a", "CLICKHOUSE_INGEST_PASSWORD": "i", "CLICKHOUSE_TRACE_PASSWORD": "t",
    "ADMIN_TOKEN": "t0ken",
    "LLM_LOCAL_BASE_URL": "http://fake-local/v1", "LLM_LOCAL_API_KEY": "ollama", "LLM_LOCAL_MODEL": "fake-local",
    "LLM_INTERNAL_BASE_URL": "", "AGENT_GOLDEN_PATH": str(GOLDEN),
}


def make_config(**env: str) -> Config:
    return Config.from_env({**BASE_ENV, **env})


class FakeDb:
    """Answers select() by the first `results` key that is a substring of the SQL; records everything."""

    def __init__(self, results: dict[str, Result] | None = None) -> None:
        self.results = results or {}
        self.queries: list[str] = []
        self.logged: list[dict[str, Any]] = []
        self.fail: Exception | None = None

    async def select(self, sql: str) -> Result:
        self.queries.append(sql)
        if self.fail is not None:
            raise self.fail
        for needle, result in self.results.items():
            if needle in sql:
                return result
        return Result(columns=[("x", "UInt8")], rows=[[1]], elapsed_ms=1)

    async def insert_agent_log(self, row: dict[str, Any]) -> None:
        self.logged.append(row)


class FakeTraceDb:
    """query() answers by the first `results` key that is a substring of the SQL; records (sql, params)."""

    def __init__(self, results: dict[str, Result] | None = None) -> None:
        self.results = results or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.fail: Exception | None = None

    async def query(self, sql: str, params: dict[str, Any]) -> Result:
        self.calls.append((sql, params))
        if self.fail is not None:
            raise self.fail
        for needle, result in self.results.items():
            if needle in sql:
                return result
        return Result(columns=[], rows=[], elapsed_ms=1)  # "nothing found", like an empty table


class FakeOut:
    """Records every produced message in order; `fail` makes send() raise like a broker that never acked."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self.fail = False
        self.offset = 482112

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> Delivery:
        if self.fail:
            raise KafkaDeliveryError("Local: Message timed out")
        self.sent.append((topic, key, value))
        self.offset += 1
        return Delivery(0, self.offset)

    def send_nowait(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.sent.append((topic, key, value))


class FakeRideClock:
    def __init__(self, sim_ts: str | None = "2025-10-09T17:42:00.250") -> None:
        self.sim_ts = sim_ts
        self.latest: dict[str, Any] | None = {"sim_ts": sim_ts} if sim_ts else None

    def fresh_sim_ts(self, max_age_s: float = 10.0) -> str | None:
        return self.sim_ts


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
