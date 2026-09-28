"""Structured JSON logs on stdout (CLAUDE.md conventions)."""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any

SERVICE = "processor"


def event(name: str, **fields: Any) -> None:
    record = {"ts": datetime.now(UTC).isoformat(timespec="milliseconds"), "service": SERVICE,
              "event": name, **fields}
    print(json.dumps(record, default=str), file=sys.stdout, flush=True)
