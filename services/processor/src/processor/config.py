"""Processor configuration from environment variables (spec 03 §Config)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    brokers: str
    consumer_group: str
    commit_interval_s: float
    window_grace_s: int
    tip_max_abs: float
    zones_path: Path
    state_dir: Path
    ch_host: str
    ch_port: int
    ch_db: str
    ch_user: str
    ch_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        e = os.environ if env is None else env
        return cls(
            brokers=e.get("KAFKA_BROKERS", "redpanda:9092"),
            consumer_group=e.get("PROCESSOR_CONSUMER_GROUP", "processor-v1"),
            commit_interval_s=float(e.get("PROCESSOR_COMMIT_INTERVAL_S", "1")),
            window_grace_s=int(e.get("PROCESSOR_WINDOW_GRACE_S", "180")),
            tip_max_abs=float(e.get("PROCESSOR_TIP_MAX_ABS", "100")),
            zones_path=Path(e.get("PROCESSOR_ZONES_PATH", "/data/zones.csv")),
            state_dir=Path(e.get("PROCESSOR_STATE_DIR", "/state")),
            ch_host=e.get("CLICKHOUSE_HOST", "clickhouse"),
            ch_port=int(e.get("CLICKHOUSE_HTTP_PORT", "8123")),
            ch_db=e.get("CLICKHOUSE_DB", "taxi"),
            ch_user=e.get("CLICKHOUSE_INGEST_USER", "ingest_writer"),
            ch_password=e.get("CLICKHOUSE_INGEST_PASSWORD", ""),
        )
