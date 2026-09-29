"""What one question did: built by observing the SSE events. It feeds the done event and the agent_log row (spec 04).
It never holds the client IP."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass
class RunRecord:
    question: str
    origin: str
    suggestion_id: str
    trace_id: str | None
    started: float
    asked_at: datetime
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    provider: str = ""
    model: str = ""
    mode: str = ""
    cached: bool = False
    sql_attempts: list[str] = field(default_factory=list)
    final_sql: str = ""
    result_rows: int = 0
    tool_calls: int = 0
    invalid_tool_calls: int = 0
    repairs: int = 0
    iterations: int = 0
    answer: str | None = None
    error: str = ""
    first_event_ms: int | None = None

    def _ms(self, now: float) -> int:
        return max(0, int((now - self.started) * 1000))

    def observe(self, event: str, data: dict[str, Any], now: float) -> None:
        if self.first_event_ms is None:
            self.first_event_ms = self._ms(now)
        if event == "sql":
            self.sql_attempts.append(str(data["sql"]))
        elif event == "rows":
            self.final_sql = self.sql_attempts[-1] if self.sql_attempts else ""
            self.result_rows = int(data["row_count"])
        elif event == "tool_call":
            self.tool_calls += 1
        elif event == "answer":
            self.answer = str(data["text"])
        elif event == "error":
            self.error = str(data["code"])

    @property
    def success(self) -> bool:
        return self.answer is not None and not self.error

    @property
    def refused(self) -> bool:
        """It answered without trying any SQL (off-topic, injection)."""
        return self.answer is not None and not self.sql_attempts and not self.cached

    def done_data(self, now: float) -> dict[str, Any]:
        return {"request_id": self.request_id, "provider": self.provider, "model": self.model, "mode": self.mode,
                "cached": self.cached, "latency_ms": self._ms(now), "tool_calls": self.tool_calls,
                "invalid_tool_calls": self.invalid_tool_calls, "repairs": self.repairs}

    def log_row(self, now: float) -> dict[str, Any]:
        return {
            "asked_at": self.asked_at, "request_id": UUID(self.request_id), "origin": self.origin,
            "suggestion_id": self.suggestion_id, "trace_id": UUID(self.trace_id) if self.trace_id else None,
            "question": self.question, "provider": self.provider, "model": self.model, "mode": self.mode,
            "sql_attempts": self.sql_attempts, "final_sql": self.final_sql, "success": self.success,
            "refused": self.refused, "error": self.error, "result_rows": self.result_rows,
            "tool_calls": min(self.tool_calls, 255), "latency_ms": self._ms(now),
            "first_event_ms": self.first_event_ms or 0,
        }
