"""Row access to the prepared, pickup-sorted replay parquet (spec 01 §Data prep)."""

from __future__ import annotations

import bisect
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq


class ReplaySource:
    def __init__(self, path: Path) -> None:
        if not path.exists():
            raise FileNotFoundError(f"{path} not found — run `make data` first")
        self._file = pq.ParquetFile(path)
        self.num_rows: int = self._file.metadata.num_rows
        self._pickups = self._file.read(columns=["pickup_ts"]).column("pickup_ts").to_numpy()
        starts, acc = [], 0
        for i in range(self._file.metadata.num_row_groups):
            starts.append(acc)
            acc += self._file.metadata.row_group(i).num_rows
        self._group_starts = starts

    def first_row_at_or_after(self, ts: datetime) -> int:
        return int(np.searchsorted(self._pickups, np.datetime64(ts, "us"), side="left"))

    def rows_from(self, row_idx: int, batch_size: int = 10_000) -> Iterator[dict[str, Any]]:
        if row_idx >= self.num_rows:
            return
        group = bisect.bisect_right(self._group_starts, row_idx) - 1
        skip = row_idx - self._group_starts[group]
        for g in range(group, len(self._group_starts)):
            table = self._file.read_row_group(g)
            if skip:
                table, skip = table.slice(skip), 0
            for batch in table.to_batches(max_chunksize=batch_size):
                yield from batch.to_pylist()
