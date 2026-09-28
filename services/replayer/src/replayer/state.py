"""Replay position persisted atomically, so a restart continues exactly where it stopped (spec 01)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ReplayState:
    loop: int
    row_idx: int  # next row to produce
    sim_ts: datetime


def load_state(path: Path) -> ReplayState | None:
    if not path.exists():
        return None
    d = json.loads(path.read_text())
    return ReplayState(loop=int(d["loop"]), row_idx=int(d["row_idx"]), sim_ts=datetime.fromisoformat(d["sim_ts"]))


def save_state(path: Path, state: ReplayState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({"loop": state.loop, "row_idx": state.row_idx, "sim_ts": state.sim_ts.isoformat()}))
    os.replace(tmp, path)
