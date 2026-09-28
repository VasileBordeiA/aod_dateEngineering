"""Sim time (naive NYC local, *_ts) and wall time (UTC, *_at) helpers. Stdlib-only (see rules.py)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def parse_sim(v: Any) -> datetime | None:
    """A sim timestamp as a naive datetime, or None if it can't be parsed."""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v).replace(tzinfo=None)
    except ValueError:
        return None


def format_sim(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds" if dt.microsecond == 0 else "milliseconds")


def format_wall(dt: datetime) -> str:
    utc = dt.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def parse_wall(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


def wall_now_str() -> str:
    return format_wall(datetime.now(UTC))


def sim_epoch_ms(dt: datetime) -> int:
    """Event time for Quix windows: the naive sim time read as if it were UTC."""
    return int(dt.replace(tzinfo=UTC).timestamp() * 1000)


def sim_from_epoch_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).replace(tzinfo=None)
