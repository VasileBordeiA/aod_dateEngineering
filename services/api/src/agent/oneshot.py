"""AGENT_MODE=oneshot (spec 06, the stand's mode per ADR-018). The schema is in the prompt, and one JSON plan comes
back. The SQL still goes through the guard and runs as agent_ro via Toolbox.run_query.

Each run_query / render_chart step is wrapped in tool_call / tool_result like in tools mode, so the UI shows the same
steps (and a failed attempt's error). They are our steps, not the model's: LoopResult.tool_calls stays 0."""

from __future__ import annotations

import itertools
import json
import re
from typing import Any

from agent.catalog import Catalog
from agent.loop import FIXING, NO_WORKING_QUERY, LoopResult, ProviderFailed, chat, clean_answer, traced_call
from agent.prompt import ONESHOT_ANSWER, oneshot_system, system_message
from agent.providers import LLM
from agent.tools import LLM_ROWS, Emit, Toolbox

ONESHOT_STATUS = "Using the schema cheat-sheet"
FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.I)


async def _plan(llm: LLM, messages: list[dict[str, Any]], temperature: float) -> dict[str, Any]:
    # JSON mode is per provider: some gateways corrupt it on reasoning models, and the prompt asks for JSON anyway.
    response_format = {"type": "json_object"} if llm.json_mode else None
    message = await chat(llm, messages, temperature=temperature, response_format=response_format)
    text = FENCE.sub("", clean_answer(message.content))
    try:
        plan = json.loads(text)
    except ValueError as exc:
        raise ProviderFailed("the oneshot reply was not JSON") from exc
    if not isinstance(plan, dict):
        raise ProviderFailed("the oneshot reply was not a JSON object")
    return plan


async def run_oneshot(llm: LLM, question: str, ride_line: str | None, toolbox: Toolbox, catalog: Catalog, emit: Emit, *,
                      temperature: float) -> LoopResult:
    await emit("status", {"text": ONESHOT_STATUS})
    messages: list[dict[str, Any]] = [system_message(oneshot_system(await catalog.schema_text()), ride_line),
                                      {"role": "user", "content": question}]
    result = LoopResult(iterations=1)
    step_ids = (f"oneshot_{n}" for n in itertools.count(1))
    plan = await _plan(llm, messages, temperature)
    if "sql" not in plan:
        answer = clean_answer(str(plan.get("answer") or ""))
        if not answer:
            raise ProviderFailed("the oneshot reply had neither sql nor answer")
        result.answer = answer
        return result
    for attempt in range(2):  # the first try + one repair
        query_args = json.dumps({"sql": plan.get("sql"), "purpose": plan.get("purpose", "")})
        outcome = await traced_call(toolbox, next(step_ids), "run_query", query_args, emit)
        if outcome.ok:
            break
        if outcome.invalid:
            raise ProviderFailed("the oneshot reply had no usable sql")
        result.repairs += 1
        if attempt == 1:
            result.error = NO_WORKING_QUERY
            return result
        await emit("status", {"text": FIXING})
        error = json.loads(outcome.content)["error"]
        messages += [{"role": "assistant", "content": json.dumps(plan)},
                     {"role": "user", "content": f"That query failed: {error}\nReply with the corrected JSON object."}]
        plan = await _plan(llm, messages, temperature)
        result.iterations += 1
    if isinstance(plan.get("chart"), dict):
        # an invalid chart is simply skipped (its tool_result says why)
        await traced_call(toolbox, next(step_ids), "render_chart", json.dumps(plan["chart"]), emit)
    rows = toolbox.last_result
    assert rows is not None
    messages += [{"role": "assistant", "content": json.dumps(plan)},
                 {"role": "user", "content": f"{ONESHOT_ANSWER}\nColumns: {rows.names}\n"
                                             f"Rows: {json.dumps(rows.rows[:LLM_ROWS], default=str)}"}]
    answer = clean_answer((await chat(llm, messages, temperature=temperature)).content)
    if not answer:
        raise ProviderFailed("an empty answer")
    result.answer = answer
    return result
