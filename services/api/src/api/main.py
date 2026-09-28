"""FastAPI app (spec 06, 07). create_app() is pure wiring, and tests pass fakes through Deps."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from api.config import Config
from api.db import ClickHouseDb, Db, DbError, DbUnavailable
from api.simclock import SimClockFollower


class SimClockSource(Protocol):
    latest: dict[str, Any] | None


@dataclass
class Deps:
    cfg: Config
    db: Db
    sim_clock: SimClockSource
    pool: Any = None    # agent.providers.ProviderPool (Task 5)
    agent: Any = None   # agent.service.AgentService (Task 7)
    golden: tuple[Any, ...] = ()  # agent.golden.GoldenQuestion (Task 3)


Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


def create_app(deps: Deps, lifespan: Lifespan | None = None) -> FastAPI:
    app = FastAPI(title="NYC Taxi Live api", lifespan=lifespan, docs_url=None, redoc_url=None)
    zones_cache: list[dict[str, Any]] = []

    @app.exception_handler(DbUnavailable)
    async def db_down(_: Request, __: DbUnavailable) -> JSONResponse:
        return JSONResponse({"detail": "The database is restarting, try again in a moment."}, status_code=503)

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
            r = await deps.db.select("SELECT location_id, zone, borough FROM taxi.zones "
                                     "WHERE location_id BETWEEN 1 AND 263 ORDER BY borough, zone")
            zones_cache.extend(dict(zip(r.names, row, strict=True)) for row in r.rows)
        return zones_cache

    @app.get("/api/sim/clock")
    async def sim_clock() -> dict[str, Any]:
        if deps.sim_clock.latest is None:
            raise HTTPException(503, "The sim clock hasn't been received yet.")
        return deps.sim_clock.latest

    return app


def app_from_env() -> FastAPI:
    """uvicorn --factory entry point. Later tasks add the agent pieces here."""
    cfg = Config.from_env()
    follower = SimClockFollower(cfg.brokers)
    deps = Deps(cfg=cfg, db=ClickHouseDb(cfg), sim_clock=follower)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        follower.start()
        yield
        follower.stop()

    return create_app(deps, lifespan)
