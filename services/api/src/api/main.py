"""FastAPI app (spec 06, 07). create_app() is pure wiring, and tests pass fakes through Deps."""

from __future__ import annotations

import asyncio
import math
import secrets
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from agent.catalog import Catalog
from agent.golden import load_golden, suggestions
from agent.providers import ProviderPool, openai_client
from agent.service import AgentService, AskRequest
from api.db import ClickHouseDb, ClickHouseTraceDb, Db, DbError, DbUnavailable
from api.envconfig import Config
from api.kafka import KafkaOut
from api.simclock import SimClockFollower
from rides.checkpoints import TRACE_TOPIC
from rides.history import TraceHistory
from rides.hub import Hub
from rides.queryable import QueryableDetector
from rides.relay import TraceRelay
from rides.service import RideRejected, RideRequest, RideService
from rides.trace_service import TraceService, normalise_trace_id, serve


class SimClockSource(Protocol):
    latest: dict[str, Any] | None


@dataclass
class Deps:
    cfg: Config
    db: Db
    sim_clock: SimClockSource
    pool: Any = None  # agent.providers.ProviderPool (Task 5)
    agent: Any = None  # agent.service.AgentService (Task 7)
    golden: tuple[Any, ...] = ()  # agent.golden.GoldenQuestion (Task 3)
    rides: Any = None  # rides.service.RideService (Task 3)
    trace: Any = None  # rides.trace_service.TraceService


class OrderBody(BaseModel):
    order: list[str]


class AskBody(BaseModel):
    question: str = Field(min_length=1, max_length=2000)  # the friendly limit (TOO_LONG) is AGENT_MAX_QUESTION_CHARS
    origin: Literal["typed", "suggested", "ride"] = "typed"
    suggestion_id: str | None = Field(default=None, max_length=64)
    trace_id: str | None = Field(default=None, max_length=64)

    @field_validator("question")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question is blank")
        return v


class RideBody(BaseModel):
    pickup_location_id: int = Field(ge=1, le=263)
    dropoff_location_id: int = Field(ge=1, le=263)
    payment: Literal["card", "cash"]
    tip_amount: float = Field(ge=0, le=999)  # absurd tips are allowed on purpose: the processor rejects them
    passenger_count: int = Field(ge=1, le=6)

    @field_validator("pickup_location_id", "dropoff_location_id", "passenger_count", mode="before")
    @classmethod
    def whole_number(cls, v: Any, info: ValidationInfo) -> Any:
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError(f"{info.field_name} must be a whole number")
        return v

    @field_validator("tip_amount", mode="before")
    @classmethod
    def money(cls, v: Any) -> Any:
        if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
            raise ValueError("tip_amount must be a number")
        if abs(round(v, 2) - v) > 1e-9:
            raise ValueError("tip_amount has at most 2 decimals")
        return v


Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_app(deps: Deps, lifespan: Lifespan | None = None) -> FastAPI:
    app = FastAPI(title="NYC Taxi Live api", lifespan=lifespan, docs_url=None, redoc_url=None)
    zones_cache: list[dict[str, Any]] = []

    @app.exception_handler(DbUnavailable)
    async def db_down(_: Request, __: DbUnavailable) -> JSONResponse:
        return JSONResponse({"detail": "The database is restarting, try again in a moment."}, status_code=503)

    @app.exception_handler(RequestValidationError)
    async def invalid_body(_: Request, exc: RequestValidationError) -> JSONResponse:
        # A NaN/Infinity in the body reaches here as a raw non-finite float in `errors()[i]["input"]`, and the
        # standard-library json encoder rejects it (allow_nan=False), so it must be sanitised before it is encoded.
        sane = jsonable_encoder(exc.errors(), custom_encoder={float: lambda v: v if math.isfinite(v) else None})
        return JSONResponse({"detail": sane}, status_code=422)

    @app.exception_handler(RideRejected)
    async def ride_rejected(_: Request, exc: RideRejected) -> JSONResponse:
        body: dict[str, Any] = {"detail": exc.message}
        headers: dict[str, str] = {}
        if exc.retry_after_s is not None:
            body["retry_after_s"] = exc.retry_after_s
            headers["Retry-After"] = str(exc.retry_after_s)
        return JSONResponse(body, status_code=exc.status, headers=headers)

    @app.get("/api/healthz")
    async def healthz() -> dict[str, Any]:
        try:
            await deps.db.select("SELECT 1")
            clickhouse = True
        except (DbError, DbUnavailable):
            clickhouse = False
        return {"ok": True, "clickhouse": clickhouse, "sim_clock": deps.sim_clock.latest is not None}

    @app.get("/api/zones")
    async def zones() -> list[dict[str, Any]]:
        if not zones_cache:
            r = await deps.db.select(
                "SELECT location_id, zone, borough FROM taxi.zones "
                "WHERE location_id BETWEEN 1 AND 263 ORDER BY borough, zone"
            )
            zones_cache.extend(dict(zip(r.names, row, strict=True)) for row in r.rows)
        return zones_cache

    @app.get("/api/sim/clock")
    async def sim_clock() -> dict[str, Any]:
        if deps.sim_clock.latest is None:
            raise HTTPException(503, "The sim clock hasn't been received yet.")
        return deps.sim_clock.latest

    @app.get("/api/rides/estimate")
    async def ride_estimate(pickup_location_id: int = Query(ge=1, le=263),
                            dropoff_location_id: int = Query(ge=1, le=263)) -> dict[str, Any]:
        est = await deps.rides.estimate(pickup_location_id, dropoff_location_id)
        return {"trip_distance": est.trip_distance, "duration_min": est.duration_min,
                "fare_amount": est.fare_amount, "method": est.method}

    @app.post("/api/rides", status_code=201)
    async def create_ride(body: RideBody, request: Request) -> dict[str, Any]:
        # uvicorn --proxy-headers: the host comes from X-Forwarded-For. It's only a rate-limit key, never logged.
        client_ip = request.client.host if request.client else "unknown"
        req = RideRequest(body.pickup_location_id, body.dropoff_location_id, body.payment, round(body.tip_amount, 2),
                          body.passenger_count)
        result: dict[str, Any] = await deps.rides.create(req, client_ip)
        return result

    # /api/trace/live must be registered before /api/trace/{trace_id}, or "live" is parsed as an id
    @app.websocket("/api/trace/live")
    async def trace_live(ws: WebSocket) -> None:
        await ws.accept()
        await serve(ws, deps.trace.live(ws))

    @app.websocket("/api/trace/{trace_id}")
    async def trace_follow(ws: WebSocket, trace_id: str) -> None:
        tid = normalise_trace_id(trace_id)
        if tid is None:
            await ws.close(code=1008)
            return
        await ws.accept()
        await serve(ws, deps.trace.follow(ws, tid))

    @app.get("/api/trace/{trace_id}")
    async def trace_history(trace_id: str) -> dict[str, Any]:
        tid = normalise_trace_id(trace_id)
        cps = await deps.trace.history(tid) if tid else []
        if not cps:
            raise HTTPException(404, "No such ride.")
        return {"trace_id": tid, "checkpoints": cps}

    @app.get("/api/llm/status")
    async def llm_status() -> dict[str, Any]:
        return dict(deps.pool.status()) | {"mode": deps.cfg.agent_mode}

    @app.post("/api/admin/llm/order")
    async def llm_order(body: OrderBody, request: Request) -> dict[str, Any]:
        token = request.headers.get("X-Admin-Token", "")
        if not deps.cfg.admin_token or not secrets.compare_digest(token, deps.cfg.admin_token):
            raise HTTPException(401, "Admin token required.")
        try:
            deps.pool.set_order(body.order)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return dict(deps.pool.status()) | {"mode": deps.cfg.agent_mode}

    @app.get("/api/suggestions")
    async def get_suggestions() -> list[dict[str, str]]:
        return [{"id": q.id, "question": q.question} for q in deps.golden if q.suggested]

    @app.post("/api/agent/ask")
    async def ask(body: AskBody, request: Request) -> StreamingResponse:
        # uvicorn --proxy-headers: the host comes from X-Forwarded-For. It's only a rate-limit key, never logged.
        client_ip = request.client.host if request.client else "unknown"
        stream = deps.agent.ask(AskRequest(body.question, body.origin, body.suggestion_id, body.trace_id), client_ip)
        return StreamingResponse(stream, media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


def app_from_env() -> FastAPI:
    """uvicorn --factory entry point."""
    cfg = Config.from_env()
    follower = SimClockFollower(cfg.brokers)
    pool = ProviderPool(cfg.providers, cfg.llm_order, lambda pc: openai_client(pc, cfg.llm_timeout_s))
    golden = load_golden(cfg.golden_path)
    suggestions(golden)  # fail fast: the file must mark exactly 5
    db = ClickHouseDb(cfg)
    catalog = Catalog(db)
    agent = AgentService(cfg, db, pool, catalog, golden)
    out = KafkaOut(cfg.brokers)
    trace_db = ClickHouseTraceDb(cfg)
    hub = Hub(TraceHistory(cfg.trace_history_ttl_s))

    def emit(cp: dict[str, Any]) -> None:
        out.send_nowait(TRACE_TOPIC, cp["trace_id"], cp)  # → taxi.trace_events (Kafka engine) and back via the relay
        hub.publish(cp)

    detector = QueryableDetector(trace_db, emit, cfg.trace_queryable_poll_ms)
    hub.on_stored = detector.on_stored
    rides = RideService(cfg, trace_db, out, follower, publish=hub.publish)
    trace = TraceService(hub, trace_db, follower)
    relay = TraceRelay(cfg.brokers)
    deps = Deps(cfg=cfg, db=db, sim_clock=follower, pool=pool, agent=agent, golden=tuple(golden),
                rides=rides, trace=trace)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()

        def deliver(cp: dict[str, Any]) -> None:  # relay thread → event loop (the Hub is event-loop only)
            loop.call_soon_threadsafe(hub.publish, cp)

        follower.start()
        out.start()
        relay.start(deliver)
        tasks = [asyncio.create_task(pool.health_loop(cfg.llm_health_interval_s)),
                 asyncio.create_task(detector.run()), asyncio.create_task(rides.warm_up())]
        yield
        for t in tasks:
            t.cancel()
        relay.stop()
        out.stop()
        follower.stop()

    return create_app(deps, lifespan)
