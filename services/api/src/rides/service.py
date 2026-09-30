"""Ride creation (spec 07 §Ride creation): validate → estimate → sim-clock timestamp → trips.raw → checkpoints."""

from __future__ import annotations

import math
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from api import log
from api.db import DbError, DbUnavailable, TraceDb
from api.envconfig import Config
from api.kafka import KafkaDeliveryError, Out
from api.ratelimit import TokenBucket
from rides.checkpoints import TRACE_TOPIC, checkpoint, wall_str
from rides.estimate import Estimate, Zone, amounts, from_centroids, from_median, ride_notes
from rides.trip import build_trip

ZONES_SQL = "SELECT location_id, zone, borough, service_zone, lat, lon FROM taxi.zones"
MEDIAN_SQL = """
SELECT count() AS n,
       quantile(0.5)(trip_distance) AS distance,
       quantile(0.5)(duration_min)  AS duration,
       quantile(0.5)(fare_amount)   AS fare
FROM taxi.trips
WHERE source = 'replay' AND pu_location_id = {pickup:UInt16} AND do_location_id = {dropoff:UInt16}
"""
SIM_NOW_SQL = "SELECT now_ts FROM taxi.sim_now"
ESTIMATE_CACHE = 2000
BROKER_DOWN = "The pipeline is catching its breath, try again in a moment"
BUSY = "The dispatcher is busy, try again in a few seconds"
ZONES_LOADING = "The zone list is still loading, try again in a moment"


class RideClock(Protocol):
    def fresh_sim_ts(self, max_age_s: float = 10.0) -> str | None: ...


@dataclass(frozen=True)
class RideRequest:
    pickup_location_id: int
    dropoff_location_id: int
    payment: str
    tip_amount: float
    passenger_count: int


class RideRejected(Exception):
    def __init__(self, status: int, message: str, retry_after_s: int | None = None) -> None:
        super().__init__(message)
        self.status, self.message, self.retry_after_s = status, message, retry_after_s


class RideService:
    def __init__(self, cfg: Config, db: TraceDb, out: Out, sim_clock: RideClock,
                 publish: Callable[[dict[str, Any]], None] | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 wall: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._cfg, self._db, self._out, self._sim, self._publish, self._wall = cfg, db, out, sim_clock, publish, wall
        self._per_ip = TokenBucket(cfg.rides_rate_limit_per_min, clock)
        self._global = TokenBucket(cfg.rides_global_per_min, clock)
        self._zones: dict[int, Zone] = {}
        self._estimates: OrderedDict[tuple[int, int], Estimate] = OrderedDict()

    async def warm_up(self) -> None:
        try:
            await self.zones()
        except (RideRejected, DbUnavailable) as exc:
            log.event("zones_warmup_failed", error=str(exc.__cause__ or exc)[:200])

    async def zones(self) -> dict[int, Zone]:
        if not self._zones:
            try:
                r = await self._db.query(ZONES_SQL, {})
            except DbError as exc:  # e.g. the trace_ro quota: a 503 the phone retries, not a 500
                raise RideRejected(503, ZONES_LOADING) from exc
            self._zones = {int(row[0]): Zone(int(row[0]), str(row[1]), str(row[2]), str(row[3]), row[4], row[5])
                           for row in r.rows}
        return self._zones

    async def estimate(self, pu: int, do: int) -> Estimate:
        key = (pu, do)
        if key in self._estimates:
            self._estimates.move_to_end(key)
            return self._estimates[key]
        zones = await self.zones()
        if pu not in zones or do not in zones:
            raise RideRejected(503, ZONES_LOADING)
        try:
            r = await self._db.query(MEDIAN_SQL, {"pickup": pu, "dropoff": do})
        except (DbError, DbUnavailable):
            return from_centroids(zones[pu], zones[do])  # not cached: try the median again next time
        n, distance, duration, fare = r.rows[0] if r.rows else (0, None, None, None)
        est = from_median(int(n), distance, duration, fare) or from_centroids(zones[pu], zones[do])
        self._estimates[key] = est
        while len(self._estimates) > ESTIMATE_CACHE:
            self._estimates.popitem(last=False)
        return est

    async def _pickup_ts(self) -> datetime:
        ts = self._sim.fresh_sim_ts()
        if ts is None:
            try:
                r = await self._db.query(SIM_NOW_SQL, {})
                ts = str(r.rows[0][0]) if r.rows and r.rows[0][0] is not None else None
            except (DbError, DbUnavailable):
                ts = None
        if ts is None:
            raise RideRejected(503, "The sim clock hasn't started yet, try again in a moment")
        return datetime.fromisoformat(ts[:19])  # whole seconds

    def _limit(self, client_ip: str) -> None:
        ok, retry = self._per_ip.allow(client_ip)
        if not ok:
            wait = math.ceil(retry)
            raise RideRejected(429, f"You've booked {self._cfg.rides_rate_limit_per_min} rides this minute. "
                                    f"Try again in {wait} s.", wait)
        ok, retry = self._global.allow("all")
        if not ok:
            raise RideRejected(429, BUSY, math.ceil(retry))

    async def create(self, req: RideRequest, client_ip: str) -> dict[str, Any]:
        self._limit(client_ip)
        accepted_at = self._wall()
        zones = await self.zones()
        est = await self.estimate(req.pickup_location_id, req.dropoff_location_id)
        pu, do = zones[req.pickup_location_id], zones[req.dropoff_location_id]
        amt = amounts(est, pu, do, req.payment, req.tip_amount)
        trace_id = str(uuid.uuid4())
        trip = build_trip(trace_id=trace_id, pickup_ts=await self._pickup_ts(), est=est, amt=amt,
                          pu_location_id=pu.location_id, do_location_id=do.location_id,
                          passenger_count=req.passenger_count, produced_at=wall_str(self._wall()))
        try:
            ack = await self._out.send("trips.raw", trip["trip_id"], trip)
        except KafkaDeliveryError as exc:
            log.event("ride_broker_down", trace_id=trace_id, error=str(exc)[:200])
            raise RideRejected(503, BROKER_DOWN) from exc
        # `submitted` goes out only after the ack, so a failed ride leaves nothing behind. Its `at` is still the
        # moment the request was accepted, and the UI orders by seq, so latencies stay honest.
        submitted = checkpoint(trace_id, trip["trip_id"], "student", "submitted",
                               {"pickup_zone": pu.zone, "dropoff_zone": do.zone, "payment": req.payment,
                                "tip_amount": req.tip_amount, "fare_amount": est.fare_amount,
                                "estimate_method": est.method}, wall_str(accepted_at))
        ingested = checkpoint(trace_id, trip["trip_id"], "student", "ingested",
                              {"topic": "trips.raw", "partition": ack.partition, "offset": ack.offset},
                              wall_str(self._wall()))
        for cp in (submitted, ingested):
            self._out.send_nowait(TRACE_TOPIC, trace_id, cp)
            if self._publish is not None:
                self._publish(cp)
        log.event("ride_created", trace_id=trace_id, method=est.method, payment=req.payment)
        return {"trace_id": trace_id, "trip_id": trip["trip_id"], "trace_url": f"/trace/{trace_id}",
                "pickup_ts": trip["pickup_ts"],
                "estimate": {"trip_distance": est.trip_distance, "duration_min": est.duration_min,
                             "fare_amount": est.fare_amount, "total_amount": amt.total_amount, "method": est.method},
                "notes": ride_notes(req.payment)}
