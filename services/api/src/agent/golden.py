"""The golden set (evals/agent_golden_questions.yaml) drives both the suggestion buttons and make eval (spec 06)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PLACEHOLDER = re.compile(r"\{\{\s*(?:rows\.(\d+)\.)?(\w+)\s*\}\}")


@dataclass(frozen=True)
class GoldenQuestion:
    id: str
    question: str
    category: str
    suggested: bool
    origin: str
    reference_sql: str
    check: dict[str, Any]
    answer_mentions: tuple[str, ...]
    chart: dict[str, Any] | None
    answer_template: str | None
    ride: str | None


def load_golden(path: Path) -> list[GoldenQuestion]:
    data = yaml.safe_load(path.read_text())
    return [
        GoldenQuestion(
            id=q["id"],
            question=q["question"],
            category=q["category"],
            suggested=bool(q.get("suggested", False)),
            origin=q.get("origin", "typed"),
            reference_sql=q["reference_sql"],
            check=dict(q["check"]),
            answer_mentions=tuple(str(m) for m in q.get("answer_mentions", [])),
            chart=q.get("chart"),
            answer_template=q.get("answer_template"),
            ride=q.get("ride"),
        )
        for q in data["questions"]
    ]


def suggestions(golden: list[GoldenQuestion]) -> list[GoldenQuestion]:
    picked = [q for q in golden if q.suggested]
    if len(picked) != 5:
        raise ValueError(f"the golden set must mark exactly 5 questions suggested: true, found {len(picked)}")
    return picked


def fill_template(template: str, columns: list[str], rows: list[list[Any]]) -> str:
    """{{column}} = row 0, {{rows.N.column}} = row N. Missing values become '?' (a live-data gap, not a crash)."""

    def value(match: re.Match[str]) -> str:
        row_idx = int(match.group(1) or 0)
        column = match.group(2)
        if column not in columns or row_idx >= len(rows):
            return "?"
        v = rows[row_idx][columns.index(column)]
        return "?" if v is None else str(v)

    return " ".join(PLACEHOLDER.sub(value, template).split())
