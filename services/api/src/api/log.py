"""Structured JSON logs on stdout (CLAUDE.md conventions). Never pass keys, passwords or client IPs."""

from __future__ import annotations

import json
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SERVICE = "api"


def event(name: str, **fields: Any) -> None:
    record = {"ts": datetime.now(UTC).isoformat(timespec="milliseconds"), "service": SERVICE,
              "event": name, **fields}
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)


def error_label(exc: BaseException) -> str:
    """A log-safe name for an exception: its class, plus the HTTP status when it has one ("AuthenticationError 401").
    Never the message: an SDK error's text can quote a provider's response body, which may echo a key."""
    status = getattr(exc, "status_code", None)
    return f"{type(exc).__name__} {status}" if isinstance(status, int) else type(exc).__name__


def error_where(exc: BaseException) -> str:
    """Where an exception was raised, as "file.py:123" (the innermost frame). Safe to log, and enough to debug."""
    frames = traceback.extract_tb(exc.__traceback__)
    return f"{Path(frames[-1].filename).name}:{frames[-1].lineno}" if frames else ""
