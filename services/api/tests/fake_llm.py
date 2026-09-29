"""A scripted OpenAI-compatible chat server (spec 10). The real agent loop runs against it in-process."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from openai import AsyncOpenAI


def tool(name: str, args: dict[str, Any] | str | None = None) -> dict[str, Any]:
    raw = args if isinstance(args, str) else json.dumps(args or {})
    return {"tool_calls": [(name, raw)]}


def tools(*calls: dict[str, Any]) -> dict[str, Any]:
    return {"tool_calls": [c for step in calls for c in step["tool_calls"]]}


def say(text: str | None) -> dict[str, Any]:
    return {"content": text}


def http(status: int) -> dict[str, Any]:
    return {"status": status}


def slow(seconds: float, step: dict[str, Any]) -> dict[str, Any]:
    return {**step, "delay": seconds}


def raw(body: str, status: int = 200) -> dict[str, Any]:
    """Return this exact body verbatim, with an `application/json` content-type. For malformed-body tests."""
    return {"raw_body": body, "status": status}


class _TimeoutTransport(httpx.AsyncBaseTransport):
    """httpx.ASGITransport ignores client timeouts. Enforce one so the SDK raises APITimeoutError as it would for real."""

    def __init__(self, inner: httpx.AsyncBaseTransport, timeout: float) -> None:
        self._inner = inner
        self._timeout = timeout

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            return await asyncio.wait_for(self._inner.handle_async_request(request), self._timeout)
        except TimeoutError as exc:
            raise httpx.ReadTimeout("fake LLM timed out", request=request) from exc


class FakeLLM:
    def __init__(self, script: list[dict[str, Any]] | None = None) -> None:
        self.script = list(script or [])
        self.requests: list[dict[str, Any]] = []
        self.down = False
        self.app = FastAPI()

        @self.app.get("/v1/models")
        async def models() -> JSONResponse:
            if self.down:
                return JSONResponse({"error": {"message": "down"}}, status_code=503)
            return JSONResponse(
                {"object": "list", "data": [{"id": "fake", "object": "model", "created": 0, "owned_by": "test"}]}
            )

        @self.app.post("/v1/chat/completions")
        async def chat(request: Request) -> Response:
            self.requests.append(await request.json())
            step = self.script.pop(0) if self.script else say("Out of script.")
            if step.get("delay"):
                await asyncio.sleep(step["delay"])
            if "raw_body" in step:
                return Response(content=step["raw_body"], status_code=step.get("status", 200),
                                 media_type="application/json")
            if "status" in step:
                return JSONResponse({"error": {"message": f"scripted {step['status']}"}}, status_code=step["status"])
            calls = [
                {
                    "id": f"call_{len(self.requests)}_{i}",
                    "type": "function",
                    "function": {"name": name, "arguments": raw},
                }
                for i, (name, raw) in enumerate(step.get("tool_calls", []))
            ]
            message: dict[str, Any] = {"role": "assistant", "content": step.get("content")}
            if calls:
                message["tool_calls"] = calls
            return JSONResponse(
                {
                    "id": "chatcmpl-fake",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "fake",
                    "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if calls else "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                }
            )

    def client(self, timeout: float = 5.0) -> AsyncOpenAI:
        transport = _TimeoutTransport(httpx.ASGITransport(app=self.app), timeout)
        return AsyncOpenAI(
            base_url="http://fake/v1",
            api_key="k",
            max_retries=0,
            timeout=timeout,
            http_client=httpx.AsyncClient(transport=transport, base_url="http://fake"),
        )
