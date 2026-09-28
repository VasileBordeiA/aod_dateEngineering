import sys
from pathlib import Path
from typing import Any

import pytest

from api.config import Config
from api.db import Result

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "contracts"))
GOLDEN = REPO / "evals" / "agent_golden_questions.yaml"

BASE_ENV = {
    "CLICKHOUSE_AGENT_PASSWORD": "a", "CLICKHOUSE_INGEST_PASSWORD": "i", "ADMIN_TOKEN": "t0ken",
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


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
