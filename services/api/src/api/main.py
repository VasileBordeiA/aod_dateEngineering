"""FastAPI app (spec 06, 07). create_app() is pure wiring, and tests pass fakes through Deps."""

from __future__ import annotations

import asyncio
import math
import secrets
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from agent.catalog import Catalog
from agent.golden import load_golden, suggestions
from agent.providers import ProviderPool, openai_client
from agent.service import AgentService, AskRequest
from api.db import ClickHouseDb, Db, DbError, DbUnavailable
from api.envconfig import Config
from api.simclock import SimClockFollower
from rides.service import RideRejected, RideRequest


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
    trace: Any = None  # rides.hub.Hub (Task 4)


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
    deps = Deps(cfg=cfg, db=db, sim_clock=follower, pool=pool, agent=agent, golden=tuple(golden))

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        follower.start()
        health = asyncio.create_task(pool.health_loop(cfg.llm_health_interval_s))
        yield
        health.cancel()
        follower.stop()

    return create_app(deps, lifespan)
