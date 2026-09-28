"""Replayer configuration from environment variables (spec 01 §Config)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class Config:
    brokers: str
    parquet_path: Path
    speed: float | None  # None = "max" (backfill)
    start_ts: datetime
    until_ts: datetime | None
    loop_offset_days: int
    trace_sample_rate: float
    clock_interval_s: float
    state_path: Path
    control_port: int
    admin_token: str

    @property
    def mode(self) -> str:
        return "backfill" if self.speed is None else "live"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        e = os.environ if env is None else env
        speed_raw = e.get("REPLAY_SPEED", "60").strip().lower()
        speed = None if speed_raw == "max" else float(speed_raw)
        if speed is not None and speed <= 0:
            raise ValueError(f"REPLAY_SPEED must be > 0 or 'max', got {speed_raw!r}")
        until_raw = e.get("REPLAY_UNTIL_TS", "").strip()
        until = datetime.fromisoformat(until_raw) if until_raw else None
        if speed is None and until is None:
            raise ValueError("REPLAY_UNTIL_TS is required when REPLAY_SPEED=max (backfill mode)")
        month = e.get("TLC_MONTH", "2025-10")
        data_dir = Path(e.get("DATA_DIR", "/data"))
        default_parquet = data_dir / "replay" / f"trips_{month}.parquet"
        return cls(
            brokers=e.get("KAFKA_BROKERS", "redpanda:9092"),
            parquet_path=Path(e.get("REPLAY_PARQUET_PATH") or default_parquet),
            speed=speed,
            start_ts=datetime.fromisoformat(e.get("REPLAY_START_TS", "2025-10-07T07:00:00")),
            until_ts=until,
            loop_offset_days=int(e.get("REPLAY_LOOP_OFFSET_DAYS", "35")),
            trace_sample_rate=float(e.get("REPLAY_TRACE_SAMPLE_RATE", "0.001")),
            clock_interval_s=float(e.get("REPLAY_CLOCK_INTERVAL_S", "1")),
            state_path=Path(e.get("REPLAY_STATE_PATH", "/data/state/replayer.json")),
            control_port=int(e.get("REPLAY_CONTROL_PORT", "8001")),
            admin_token=e.get("ADMIN_TOKEN", ""),
        )
