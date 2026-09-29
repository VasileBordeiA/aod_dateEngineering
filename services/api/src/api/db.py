"""ClickHouse access (spec 04): agent_ro for everything the agent reads, ingest_writer only for taxi.agent_log."""

from __future__ import annotations

import asyncio
import math
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from api import log
from api.config import Config

AGENT_LOG_COLUMNS = (
    "asked_at", "request_id", "origin", "suggestion_id", "trace_id", "question", "provider", "model", "mode",
    "sql_attempts", "final_sql", "success", "refused", "error", "result_rows", "tool_calls", "latency_ms",
    "first_event_ms",
)


class DbError(Exception):
    """ClickHouse rejected the query. The message is the server's first line and is safe to show."""


class DbUnavailable(Exception):
    """ClickHouse could not be reached."""


@dataclass(frozen=True)
class Result:
    columns: list[tuple[str, str]]  # (name, ClickHouse type)
    rows: list[list[Any]]           # JSON-safe values (see jsonable)
    elapsed_ms: int

    @property
    def names(self) -> list[str]:
        return [name for name, _ in self.columns]


class Db(Protocol):
    async def select(self, sql: str) -> Result: ...

    async def insert_agent_log(self, row: dict[str, Any]) -> None: ...


def jsonable(value: Any) -> Any:
    """Make a ClickHouse value JSON-safe. NaN/inf → None, Decimal → float, dates → ISO text, UUID → text."""
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return jsonable(float(value))
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds") if value.microsecond else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    return str(value)


def clickhouse_message(exc: BaseException) -> str:
    """The first line of the server's message, at most 300 chars (spec 06: run_query errors)."""
    text = str(exc)
    if "server response:" in text:
        text = text.split("server response:", 1)[1]
    lines = text.strip().splitlines()
    return (lines[0] if lines else "ClickHouse error").strip()[:300]


class ClickHouseDb:
    """Two lazily created clickhouse-connect clients. No session ids, so each client is safe to share across threads."""

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._agent: Any = None
        self._ingest: Any = None
        self._lock = threading.Lock()

    def _connect(self, user: str, password: str) -> Any:
        return clickhouse_connect.get_client(host=self._cfg.ch_host, port=self._cfg.ch_port, username=user,
                                             password=password, database=self._cfg.ch_db,
                                             autogenerate_session_id=False, connect_timeout=3)

    def _select_sync(self, sql: str) -> Result:
        started = time.perf_counter()
        try:
            with self._lock:
                if self._agent is None:
                    self._agent = self._connect(self._cfg.agent_user, self._cfg.agent_password)
                client = self._agent
            r = client.query(sql)
        except OperationalError as exc:
            with self._lock:
                self._agent = None
            raise DbUnavailable(clickhouse_message(exc)) from exc
        except DatabaseError as exc:
            raise DbError(clickhouse_message(exc)) from exc
        return Result(columns=list(zip(r.column_names, [t.name for t in r.column_types], strict=True)),
                      rows=[[jsonable(v) for v in row] for row in r.result_rows],
                      elapsed_ms=int((time.perf_counter() - started) * 1000))

    async def select(self, sql: str) -> Result:
        return await asyncio.to_thread(self._select_sync, sql)

    def _insert_sync(self, row: dict[str, Any]) -> None:
        try:
            with self._lock:
                if self._ingest is None:
                    self._ingest = self._connect(self._cfg.ingest_user, self._cfg.ingest_password)
                client = self._ingest
            client.insert("taxi.agent_log", [[row[c] for c in AGENT_LOG_COLUMNS]], column_names=list(AGENT_LOG_COLUMNS))
        except Exception as exc:  # logging must never break an answer
            with self._lock:
                self._ingest = None
            log.event("agent_log_failed", error=clickhouse_message(exc))

    async def insert_agent_log(self, row: dict[str, Any]) -> None:
        await asyncio.to_thread(self._insert_sync, row)
