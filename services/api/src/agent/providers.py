"""LLM providers (spec 06): an ordered, health-checked list of OpenAI-compatible endpoints.

2 failures (health check or request) = unhealthy, 1 success = healthy again. The order can be switched live.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from openai import AsyncOpenAI

from api import log
from api.envconfig import ProviderConfig

FAILS_TO_MARK_DOWN = 2
HEALTH_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class LLM:
    name: str
    model: str
    client: AsyncOpenAI


def openai_client(pc: ProviderConfig, timeout_s: float) -> AsyncOpenAI:
    return AsyncOpenAI(base_url=pc.base_url, api_key=pc.api_key or "none", timeout=timeout_s, max_retries=0)


@dataclass
class _State:
    llm: LLM
    healthy: bool = True
    failures: int = 0
    last_ok_at: str | None = None
    latencies_ms: deque[int] = field(default_factory=lambda: deque(maxlen=50))


class ProviderPool:
    def __init__(
        self,
        configs: Sequence[ProviderConfig],
        order: Sequence[str],
        factory: Callable[[ProviderConfig], AsyncOpenAI],
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._states = {pc.name: _State(LLM(pc.name, pc.model, factory(pc))) for pc in configs}
        self._order = [name for name in order if name in self._states]
        self._clock = clock

    @property
    def order(self) -> list[str]:
        return list(self._order)

    def set_order(self, names: Sequence[str]) -> None:
        unknown = [n for n in names if n not in self._states]
        if unknown:
            raise ValueError(f"unknown or disabled provider(s): {', '.join(unknown)}")
        if len(set(names)) != len(names):
            raise ValueError("duplicate provider in order")
        self._order = list(names)
        log.event("llm_order_changed", order=self._order)

    def candidates(self) -> list[LLM]:
        return [self._states[n].llm for n in self._order if self._states[n].healthy]

    def record(self, name: str, ok: bool, latency_ms: int | None = None) -> None:
        state = self._states[name]
        if ok:
            if not state.healthy:
                log.event("llm_provider_up", provider=name)
            state.healthy, state.failures = True, 0
            state.last_ok_at = datetime.fromtimestamp(self._clock(), UTC).isoformat(timespec="seconds")
            if latency_ms is not None:
                state.latencies_ms.append(latency_ms)
            return
        state.failures += 1
        if state.healthy and state.failures >= FAILS_TO_MARK_DOWN:
            state.healthy = False
            log.event("llm_provider_down", provider=name)

    def status(self) -> dict[str, Any]:
        names = self._order + [n for n in self._states if n not in self._order]
        candidates = self.candidates()
        return {
            "providers": [
                {
                    "name": n,
                    "model": s.llm.model,
                    "healthy": s.healthy,
                    "last_ok_at": s.last_ok_at,
                    "p50_ms": int(statistics.median(s.latencies_ms)) if s.latencies_ms else None,
                }
                for n in names
                for s in [self._states[n]]
            ],
            "order": self.order,
            "active": candidates[0].name if candidates else "cache",
        }

    async def check(self) -> None:
        for name, state in self._states.items():
            try:
                await state.llm.client.with_options(timeout=HEALTH_TIMEOUT_S).models.list()
            except Exception:  # any failure to list models = not healthy (connection, 5xx, auth)
                self.record(name, ok=False)
            else:
                self.record(name, ok=True)

    async def health_loop(self, interval_s: float) -> None:
        while True:
            await self.check()
            await asyncio.sleep(interval_s)
