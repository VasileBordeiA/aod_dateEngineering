"""Agent loop, AGENT_MODE=tools (spec 06). Streams each step through `emit`. The service owns answer/error/done."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, cast

from openai import OpenAIError
from openai.types.chat import ChatCompletion, ChatCompletionMessage, ChatCompletionMessageFunctionToolCall

from agent.providers import LLM
from agent.tools import TOOL_DEFS, Emit, Toolbox, ToolOutcome
from api.log import error_label

NO_WORKING_QUERY = ("NO_WORKING_QUERY", "I couldn't write a working query for that. Try rephrasing, or pick a "
                                        "suggested question!")
FORCED_ANSWER_PROMPT = "Answer now with what you have."
FIXING = "Fixing its own query…"
THINK = re.compile(r"<think>.*?(</think>|$)", re.S | re.I)


class ProviderFailed(Exception):
    """This provider can't serve the request (network, HTTP error, empty or unusable replies). Try the next."""


@dataclass
class LoopResult:
    answer: str | None = None
    error: tuple[str, str] | None = None
    iterations: int = 0
    tool_calls: int = 0
    invalid_tool_calls: int = 0
    repairs: int = 0


def clean_answer(text: str | None) -> str:
    """Drop reasoning blocks (qwen3 and other thinking models) and surrounding whitespace."""
    return THINK.sub("", text or "").strip()


async def chat(llm: LLM, messages: list[dict[str, Any]], *, temperature: float, tools: bool = False,
               tool_choice: str = "auto", response_format: dict[str, str] | None = None) -> ChatCompletionMessage:
    kwargs: dict[str, Any] = {"model": llm.model, "messages": messages, "temperature": temperature}
    if tools:
        kwargs |= {"tools": TOOL_DEFS, "tool_choice": tool_choice}
    if response_format is not None:
        kwargs["response_format"] = response_format
    try:
        response = cast(ChatCompletion, await llm.client.chat.completions.create(**kwargs))
    except (OpenAIError, ValueError) as exc:  # connection, timeout, 4xx/5xx incl. 429, or a body that
        # isn't valid JSON despite an application/json content-type (json.JSONDecodeError is a ValueError).
        # Only the class name and HTTP status: the SDK's message can quote the provider's response body.
        raise ProviderFailed(error_label(exc)) from exc
    if not response.choices:
        raise ProviderFailed("the response had no choices")
    return response.choices[0].message


def _arguments_preview(raw: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or "{}")
    except ValueError:
        return {"raw": (raw or "")[:200]}
    return parsed if isinstance(parsed, dict) else {"raw": (raw or "")[:200]}


async def traced_call(toolbox: Toolbox, call_id: str, name: str, raw: str | None, emit: Emit) -> ToolOutcome:
    """One tool step as the UI sees it: tool_call, the tool's own events (sql/rows/chart), then tool_result."""
    await emit("tool_call", {"id": call_id, "name": name, "arguments": _arguments_preview(raw)})
    outcome = await toolbox.call(name, raw, emit)
    await emit("tool_result", {"id": call_id, "name": name, "ok": outcome.ok, "summary": outcome.summary})
    return outcome


async def run_tools(llm: LLM, messages: list[dict[str, Any]], toolbox: Toolbox, emit: Emit, *, max_iterations: int,
                    max_repairs: int, temperature: float) -> LoopResult:
    result = LoopResult()
    invalid_in_a_row = 0
    for _ in range(max_iterations):
        result.iterations += 1
        message = await chat(llm, messages, temperature=temperature, tools=True)
        raw_calls = message.tool_calls or []
        if not raw_calls:
            answer = clean_answer(message.content)
            if not answer:
                raise ProviderFailed("an empty reply with no tool calls")
            result.answer = answer
            return result
        # Our TOOL_DEFS only declare function-type tools; a compliant model never returns any other kind,
        # but a provider that also supports "custom" tools (spec: any provider problem fails over) might.
        calls: list[ChatCompletionMessageFunctionToolCall] = []
        for raw_call in raw_calls:
            if not isinstance(raw_call, ChatCompletionMessageFunctionToolCall):
                raise ProviderFailed("the model returned a non-function tool call")
            calls.append(raw_call)
        ids = [call.id or f"call_{result.tool_calls + i}" for i, call in enumerate(calls)]
        messages.append({"role": "assistant", "content": message.content or "", "tool_calls": [
            {"id": call_id, "type": "function",
             "function": {"name": c.function.name, "arguments": c.function.arguments}}
            for call_id, c in zip(ids, calls, strict=True)]})
        for call_id, call in zip(ids, calls, strict=True):
            result.tool_calls += 1
            name, raw = call.function.name, call.function.arguments
            outcome = await traced_call(toolbox, call_id, name, raw, emit)
            messages.append({"role": "tool", "tool_call_id": call_id, "content": outcome.content})
            if outcome.invalid:
                result.invalid_tool_calls += 1
                invalid_in_a_row += 1
                if invalid_in_a_row >= 2:
                    raise ProviderFailed("two invalid tool calls in a row")
            else:
                invalid_in_a_row = 0
            if outcome.query_failed:
                result.repairs += 1
                if result.repairs > max_repairs:
                    result.error = NO_WORKING_QUERY
                    return result
                await emit("status", {"text": FIXING})
    messages.append({"role": "user", "content": FORCED_ANSWER_PROMPT})
    message = await chat(llm, messages, temperature=temperature, tools=True, tool_choice="none")
    result.answer = (clean_answer(message.content)
                      or "I ran out of steps before finding an answer. Try a suggested question!")
    return result
