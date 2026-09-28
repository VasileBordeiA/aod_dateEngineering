"""Container healthcheck: exit 0 if the heartbeat file was touched recently (python -m processor.health)."""

from __future__ import annotations

import sys
import time
from pathlib import Path

HEARTBEAT = Path("/tmp/healthy")


def main(path: str = str(HEARTBEAT), max_age_s: str = "30") -> int:
    p = Path(path)
    return 0 if p.exists() and time.time() - p.stat().st_mtime < float(max_age_s) else 1


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
