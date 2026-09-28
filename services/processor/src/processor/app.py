"""Stream processor: python -m processor.app (spec 03).

  trips.raw ─annotate─┬─ traced ───────────────────► trace.checkpoints (validated | rejected)
                      ├─ rejected ─────────────────► trips.rejected + taxi.trips_rejected
                      └─ valid ─enrich─┬───────────► trips.enriched + taxi.trips (then "stored" checkpoints)
                                       └─ traced ──► trace.checkpoints (enriched, aggregated)
  trips.enriched ─group_by(pu_borough)─tumbling 1 min (sim time)─► agg.borough_minute + taxi.agg_borough_minute
"""

from __future__ import annotations

import re
import threading
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect
from quixstreams import Application

from processor.aggregate import add_trip, init_bucket, to_agg_record
from processor.checkpoints import enriched_and_aggregated, stored_checkpoint, validated_or_rejected
from processor.config import Config
from processor.enrich import enrich
from processor.health import HEARTBEAT
from processor.log import event
from processor.rows import AGG_COLUMNS, REJECTED_COLUMNS, TRIPS_COLUMNS, agg_row, rejected_row, trip_row
from processor.rules import annotate, is_traced, to_rejected
from processor.sinks import CheckpointProducer, ClickHouseSink
from processor.times import parse_sim, sim_epoch_ms, wall_now_str
from processor.zones import load_zones

_PICKUP = re.compile(rb'"pickup_ts"\s*:\s*"([^"]+)"')


def pickup_ts_ms(value: Any, headers: Any, timestamp: int, timestamp_type: Any) -> int:
    """Event time = pickup_ts, so windows run on the simulated clock. Falls back to the Kafka timestamp."""
    raw: Any = None
    if isinstance(value, (bytes, bytearray)):
        match = _PICKUP.search(value)
        raw = match.group(1).decode("utf-8", "replace") if match else None
    elif isinstance(value, dict):
        raw = value.get("pickup_ts")
    dt = parse_sim(raw)
    return sim_epoch_ms(dt) if dt is not None else timestamp


class Stats:
    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def add(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counts[key] += n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            snap = dict(self._counts)
            self._counts.clear()
            return snap

    def report_forever(self, heartbeat: Path, interval_s: float = 10.0) -> None:
        heartbeat.touch()
        while True:
            time.sleep(interval_s)
            heartbeat.touch()
            snap = self.snapshot()
            reasons = {k.split(":", 1)[1]: v for k, v in snap.items() if k.startswith("reason:")}
            event("progress", **{k: v for k, v in snap.items() if ":" not in k}, by_reason=reasons)


def build(cfg: Config, stats: Stats) -> Application:
    zones = load_zones(cfg.zones_path)
    app = Application(
        broker_address=cfg.brokers,
        consumer_group=cfg.consumer_group,
        auto_offset_reset="earliest",
        commit_interval=cfg.commit_interval_s,
        processing_guarantee="at-least-once",
        state_dir=str(cfg.state_dir),
        loglevel="WARNING",
    )
    raw_t = app.topic("trips.raw", value_deserializer="bytes", key_deserializer="str",
                      timestamp_extractor=pickup_ts_ms)
    enriched_t = app.topic("trips.enriched", key_serializer="str", key_deserializer="str",
                           timestamp_extractor=pickup_ts_ms)
    rejected_t = app.topic("trips.rejected", key_serializer="str")
    agg_t = app.topic("agg.borough_minute", key_serializer="str")
    trace_t = app.topic("trace.checkpoints", key_serializer="str")

    def clickhouse() -> Any:
        return clickhouse_connect.get_client(host=cfg.ch_host, port=cfg.ch_port, username=cfg.ch_user,
                                             password=cfg.ch_password, database=cfg.ch_db)

    checkpoints = CheckpointProducer(cfg.brokers)

    def after_trips_insert(values: list[dict[str, Any]]) -> None:
        stats.add("sink_rows", len(values))
        at = wall_now_str()
        checkpoints.emit([stored_checkpoint(v, "taxi.trips", len(values), at) for v in values if is_traced(v)])

    trips_sink = ClickHouseSink(table="trips", columns=TRIPS_COLUMNS, to_row=trip_row,
                                client_factory=clickhouse, after_insert=after_trips_insert)
    rejects_sink = ClickHouseSink(table="trips_rejected", columns=REJECTED_COLUMNS, to_row=rejected_row,
                                  client_factory=clickhouse)
    agg_sink = ClickHouseSink(table="agg_borough_minute", columns=AGG_COLUMNS, to_row=agg_row,
                              client_factory=clickhouse)

    def annotate_counted(raw: bytes) -> dict[str, Any]:
        value = annotate(raw, cfg.tip_max_abs)
        stats.add("in")
        stats.add("rejected" if value["_reasons"] else "valid")
        for reason in value["_reasons"]:
            stats.add(f"reason:{reason}")
        return value

    def on_late(*args: Any, **kwargs: Any) -> bool:
        stats.add("late_records")
        return True  # keep Quix's own warning log line

    # 1 · clean
    sdf = app.dataframe(raw_t).apply(annotate_counted)
    sdf.filter(is_traced).apply(lambda v: validated_or_rejected(v, wall_now_str())) \
        .to_topic(trace_t, key=lambda c: c["trace_id"])
    sdf.filter(lambda v: bool(v["_reasons"])).apply(lambda v: to_rejected(v, wall_now_str())) \
        .to_topic(rejected_t, key=lambda r: r["trip_id"]).sink(rejects_sink)

    # 2 · enrich
    valid = sdf.filter(lambda v: not v["_reasons"]).apply(lambda v: enrich(v, zones, wall_now_str()))
    valid.filter(is_traced).apply(lambda v: enriched_and_aggregated(v, wall_now_str()), expand=True) \
        .to_topic(trace_t, key=lambda c: c["trace_id"])
    valid.to_topic(enriched_t, key=lambda v: v["trip_id"]).sink(trips_sink)

    # 3 · aggregate the enriched stream in 1-minute sim-time windows per pickup borough
    app.dataframe(enriched_t).group_by("pu_borough") \
        .tumbling_window(timedelta(minutes=1), grace_ms=timedelta(seconds=cfg.window_grace_s), on_late=on_late) \
        .reduce(add_trip, init_bucket).final() \
        .apply(lambda w, key, ts, headers: to_agg_record(w, key, wall_now_str()), metadata=True) \
        .update(lambda r: stats.add("windows_emitted")) \
        .to_topic(agg_t, key=lambda r: r["borough"]).sink(agg_sink)
    return app


def main() -> int:
    cfg = Config.from_env()
    stats = Stats()
    app = build(cfg, stats)
    threading.Thread(target=stats.report_forever, args=(HEARTBEAT,), name="progress", daemon=True).start()
    event("start", consumer_group=cfg.consumer_group, commit_interval_s=cfg.commit_interval_s,
          window_grace_s=cfg.window_grace_s)
    app.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
