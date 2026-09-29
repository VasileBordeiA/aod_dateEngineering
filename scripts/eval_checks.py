"""The golden-set check types (spec 10 §Agent eval harness, step 4). Pure, so the scoring is unit-tested."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

Rows = Sequence[Sequence[Any]]


def as_number(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int | float):
        return float(v) if math.isfinite(v) else None
    if isinstance(v, str):
        try:
            return float(v.strip().rstrip("%").replace(",", ""))
        except ValueError:
            return None
    return None


def close(a: float, b: float, tol: float) -> bool:
    if tol == 0:
        return abs(a - b) < 1e-9
    return abs(a - b) <= tol * max(abs(b), 1e-9)


def matches(agent_value: Any, ref_value: Any, tol: float, pct: bool = False) -> bool:
    if ref_value is None or isinstance(ref_value, bool | str):
        return agent_value is not None and str(agent_value).strip().lower() == str(ref_value).strip().lower()
    ref, got = as_number(ref_value), as_number(agent_value)
    if ref is None or got is None:
        return False
    if close(got, ref, tol):
        return True
    return pct and (close(got * 100, ref, tol) or close(got, ref * 100, tol))


def _row_has(row: Sequence[Any], ref_value: Any, tol: float, pct: bool) -> bool:
    return any(matches(v, ref_value, tol, pct) for v in row)


def _norm(v: Any) -> str:
    n = None if isinstance(v, str) else as_number(v)
    return f"{n:.6g}" if n is not None else str(v).strip().lower()


def check_result(check: dict[str, Any], *, agent_rows: Rows | None, ref_columns: Sequence[str], ref_rows: Rows,
                 answer: str | None, had_rows: bool, answer_mentions: Sequence[str] = (),
                 cached: bool = False) -> tuple[bool, str]:
    kind = check["type"]
    tol = float(check.get("tolerance", 0.02))
    pct = bool(check.get("pct", False))
    if cached:
        return False, "answered from the cache, not by the model"
    if kind == "refusal":
        if not answer:
            return False, "no answer"
        return (not had_rows), ("" if not had_rows else "returned data rows for a refusal question")
    if kind != "answer_only":
        if not agent_rows:
            return False, "the agent returned no rows"
        if not ref_rows:
            return False, "the reference query returned no rows"
        col = {name: i for i, name in enumerate(ref_columns)}
        if kind == "top1":
            ok = _row_has(agent_rows[0], ref_rows[0][0], tol, pct)
        elif kind == "top_k_set":
            k = int(check.get("k", 5))
            want = {_norm(r[0]) for r in ref_rows[:k]}
            top = agent_rows[:k]
            ok = any({_norm(r[j]) for r in top if j < len(r)} == want for j in range(len(top[0])))
        elif kind == "scalar":
            ok = all(_row_has(agent_rows[0], ref_rows[0][col[c]], tol, pct) for c in check["value_columns"])
        elif kind == "contains_value":
            want_all = check.get("match", "all") == "all"
            ok = True
            for ref_row in ref_rows:
                hits = [any(_row_has(r, ref_row[col[c]], tol, pct) for r in agent_rows) for c in check["value_columns"]]
                ok = ok and (all(hits) if want_all else any(hits))
        elif kind == "mapping":
            key = col[check["key_column"]]
            ok = all(any(_row_has(r, ref_row[key], 0.0, False)
                         and all(_row_has(r, ref_row[col[c]], tol, pct) for c in check["value_columns"])
                         for r in agent_rows)
                     for ref_row in ref_rows)
        else:
            raise ValueError(f"unknown check type {kind!r}")
        if not ok:
            return False, f"{kind} mismatch"
    if answer_mentions and not any(m.lower() in (answer or "").lower() for m in answer_mentions):
        return False, f"the answer mentions none of {list(answer_mentions)}"
    return True, ""
