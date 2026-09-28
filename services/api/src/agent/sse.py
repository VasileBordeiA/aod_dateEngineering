"""Server-Sent Events framing (spec 06 §SSE event contract). One JSON line per event."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def encode(event: str, data: Mapping[str, Any]) -> bytes:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return f"event: {event}\ndata: {payload}\n\n".encode()
