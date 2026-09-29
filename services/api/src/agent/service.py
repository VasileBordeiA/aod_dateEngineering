"""One question in, one SSE stream out (spec 06). This is the only place that emits answer/error/done.

A producer task runs the question and fills a queue, and ask() drains it. If the visitor goes away, the producer is
cancelled, and the finally blocks still release the LLM slot and log the row.
"""

from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from agent.cache import CACHE_STATUS, TIMEOUT_CACHE_STATUS, cached_answer
from agent.catalog import Catalog
from agent.golden import GoldenQuestion
from agent.loop import LoopResult, ProviderFailed, run_tools
from agent.oneshot import run_oneshot
from agent.prompt import build_messages, ride_context
from agent.providers import ProviderPool
from agent.record import RunRecord
from agent.sse import encode
from agent.tools import Emit, Toolbox
from api import log
from api.config import Config
from api.db import Db, DbError, DbUnavailable
from api.ratelimit import TokenBucket

QUEUE_STATUS_EVERY_S = 3.0
READING = "Reading the question…"
NEXT_IN_LINE = "You're next in line…"
SWITCHING = "Switching to the backup model…"
DB_RESTARTING = "The database is restarting, try again in a moment."
LLM_UNAVAILABLE = "The AI is taking a short break. Try one of the suggested questions!"


@dataclass(frozen=True)
class AskRequest:
    question: str
    origin: str = "typed"
    suggestion_id: str | None = None
    trace_id: str | None = None


class AgentService:
    def __init__(self, cfg: Config, db: Db, pool: ProviderPool, catalog: Catalog, golden: list[GoldenQuestion],
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.cfg, self.db, self.pool, self.catalog = cfg, db, pool, catalog
        self.suggested = {q.id: q for q in golden if q.suggested}
        self.clock = clock
        self.limiter = TokenBucket(cfg.rate_limit_per_min, clock=clock)
        self._slots = asyncio.Semaphore(cfg.max_concurrency)
        self._waiting = 0
        self._background: set[asyncio.Task[None]] = set()

    async def ask(self, req: AskRequest, client_ip: str) -> AsyncIterator[bytes]:
        record = RunRecord(question=req.question.strip(), origin=req.origin, suggestion_id=req.suggestion_id or "",
                           trace_id=None, started=self.clock(), asked_at=datetime.now(UTC))
        queue: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def emit(event: str, data: dict[str, Any]) -> None:
            record.observe(event, data, self.clock())
            await queue.put(encode(event, data))

        producer = asyncio.create_task(self._produce(req, client_ip, record, emit))
        producer.add_done_callback(lambda _: queue.put_nowait(None))
        try:
            while (frame := await queue.get()) is not None:
                yield frame
        finally:
            if not producer.done():
                record.error = record.error or "CLIENT_GONE"
                producer.cancel()

    async def _produce(self, req: AskRequest, client_ip: str, record: RunRecord,
                       emit: Emit) -> None:
        try:
            await self._answer(req, client_ip, record, emit)
        except DbUnavailable:
            await self._error(emit, "INTERNAL", DB_RESTARTING)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.event("agent_crash", request_id=record.request_id, error=log.error_label(exc),
                      where=log.error_where(exc))
            await self._error(emit, "INTERNAL", "Something went wrong on our side. Try again!")
        finally:
            if record.error != "CLIENT_GONE":
                await emit("done", record.done_data(self.clock()))
            self._log(record)

    @staticmethod
    async def _error(emit: Emit, code: str, message: str, **extra: Any) -> None:
        await emit("error", {"code": code, "message": message, **extra})

    async def _answer(self, req: AskRequest, client_ip: str, record: RunRecord,
                      emit: Emit) -> None:
        question = record.question
        if len(question) > self.cfg.max_question_chars:
            await self._error(emit, "TOO_LONG",
                              f"Questions can be up to {self.cfg.max_question_chars} characters. Try a shorter one!")
            return
        allowed, retry_after = self.limiter.allow(client_ip)
        if not allowed:
            wait = math.ceil(retry_after)
            await self._error(emit, "RATE_LIMITED", f"Lots of questions right now! Try again in {wait} s.",
                              retry_after_s=wait)
            return
        suggestion = self.suggested.get(req.suggestion_id or "") if req.origin == "suggested" else None
        ride_line = await self._ride_context(req.trace_id, record)
        await emit("status", {"text": READING})
        if not await self._acquire_slot(emit):
            return
        answered, timed_out = False, False
        try:
            async with asyncio.timeout(self.cfg.total_timeout_s):
                answered = await self._with_providers(question, ride_line, record, emit)
        except TimeoutError:
            timed_out = True
        finally:
            self._slots.release()
        if answered:
            return
        if suggestion is not None:
            record.provider, record.model, record.mode, record.cached = "cache", "", "cached", True
            await cached_answer(suggestion, self.db, emit, TIMEOUT_CACHE_STATUS if timed_out else CACHE_STATUS)
        elif timed_out:
            await self._error(emit, "TIMEOUT", "That took too long. Try a suggested question!")
        else:
            await self._error(emit, "LLM_UNAVAILABLE", LLM_UNAVAILABLE)

    async def _acquire_slot(self, emit: Emit) -> bool:
        if self._waiting >= self.cfg.max_queue:
            await self._error(emit, "RATE_LIMITED", "The AI is busy with other visitors. Try again in 10 s.",
                              retry_after_s=10)
            return False
        self._waiting += 1
        try:
            while True:
                try:
                    await asyncio.wait_for(self._slots.acquire(), timeout=QUEUE_STATUS_EVERY_S)
                    return True
                except TimeoutError:
                    await emit("status", {"text": NEXT_IN_LINE})
        finally:
            self._waiting -= 1

    async def _with_providers(self, question: str, ride_line: str | None, record: RunRecord,
                              emit: Emit) -> bool:
        for i, llm in enumerate(self.pool.candidates()):
            if i:
                await emit("status", {"text": SWITCHING})
            record.provider, record.model, record.mode = llm.name, llm.model, self.cfg.agent_mode
            toolbox = Toolbox(self.db, self.catalog)
            started = self.clock()
            try:
                if self.cfg.agent_mode == "oneshot":
                    result = await run_oneshot(llm, question, ride_line, toolbox, self.catalog, emit,
                                               temperature=self.cfg.llm_temperature)
                else:
                    result = await run_tools(llm, build_messages(question, ride_line), toolbox, emit,
                                             max_iterations=self.cfg.max_tool_iterations,
                                             max_repairs=self.cfg.max_repairs, temperature=self.cfg.llm_temperature)
            except ProviderFailed as exc:
                self.pool.record(llm.name, ok=False)
                log.event("provider_failed", request_id=record.request_id, provider=llm.name, reason=str(exc)[:200])
                continue
            self.pool.record(llm.name, ok=True, latency_ms=int((self.clock() - started) * 1000))
            self._absorb(record, result)
            if result.error:
                await self._error(emit, *result.error)
            else:
                await emit("answer", {"text": result.answer or ""})
            return True
        return False

    @staticmethod
    def _absorb(record: RunRecord, result: LoopResult) -> None:
        record.invalid_tool_calls, record.repairs = result.invalid_tool_calls, result.repairs
        record.iterations = result.iterations

    async def _ride_context(self, trace_id: str | None, record: RunRecord) -> str | None:
        """Only a well-formed UUID that exists in trips or trips_rejected gets into the prompt (spec 06)."""
        if not trace_id:
            return None
        try:
            tid = str(uuid.UUID(trace_id))
        except ValueError:
            return None
        try:
            r = await self.db.select(f"SELECT (SELECT count() FROM taxi.trips WHERE trace_id = toUUID('{tid}')) + "
                                     f"(SELECT count() FROM taxi.trips_rejected WHERE trace_id = toUUID('{tid}')) AS n")
        except DbError:
            return None
        if not r.rows or not r.rows[0][0]:
            return None
        record.trace_id = tid
        return ride_context(tid)

    def _log(self, record: RunRecord) -> None:
        row = record.log_row(self.clock())
        task = asyncio.get_running_loop().create_task(self.db.insert_agent_log(row))
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        log.event("agent_done", request_id=record.request_id, provider=record.provider, model=record.model,
                  mode=record.mode, iterations=record.iterations, repairs=record.repairs, latency_ms=row["latency_ms"],
                  success=record.success,
                  error=record.error)
