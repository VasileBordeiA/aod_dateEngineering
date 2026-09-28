"""Validate messages against contracts/events/*.schema.json. Shared by the tests of every service."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

EVENTS_DIR = Path(__file__).resolve().parent / "events"


@lru_cache(maxsize=1)
def _schemas() -> dict[str, dict[str, Any]]:
    return {
        p.name.removesuffix(".schema.json"): json.loads(p.read_text())
        for p in sorted(EVENTS_DIR.glob("*.schema.json"))
    }


@lru_cache(maxsize=1)
def _registry() -> Registry:
    return Registry().with_resources((s["$id"], Resource.from_contents(s)) for s in _schemas().values())


def schema_names() -> list[str]:
    return list(_schemas())


def schema(name: str) -> dict[str, Any]:
    return _schemas()[name]


def errors(name: str, message: Any) -> list[str]:
    validator = Draft202012Validator(_schemas()[name], registry=_registry(), format_checker=FormatChecker())
    return [f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in validator.iter_errors(message)]


def assert_valid(name: str, message: Any) -> None:
    problems = errors(name, message)
    if problems:
        raise AssertionError(f"{name} contract violated:\n  " + "\n  ".join(problems))
