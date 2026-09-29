"""Offline fallback for the 5 suggested questions (spec 06): run the reference SQL on LIVE data, still guarded and
still as agent_ro, and fill the answer template from the result."""

from __future__ import annotations

from agent.golden import GoldenQuestion, fill_template
from agent.sql_guard import check_sql
from agent.tools import Emit, rows_event
from api.db import Db

CACHE_STATUS = "AI is offline, so I'm using a pre-written query on live data"
TIMEOUT_CACHE_STATUS = "That took too long, so I'm using a pre-written query on live data"


async def cached_answer(q: GoldenQuestion, db: Db, emit: Emit, status: str = CACHE_STATUS) -> None:
    await emit("status", {"text": status})
    sql = q.reference_sql.strip()
    await emit("sql", {"attempt": 1, "sql": sql, "purpose": "A pre-written reference query, run on live data"})
    verdict = check_sql(sql)
    if not verdict.ok:  # test_sql_guard pins that every reference_sql passes, so this is a bug if it happens
        raise RuntimeError(f"reference SQL for {q.id} fails the guard: {verdict.reason}")
    result = await db.select(verdict.safe_sql)
    await emit("rows", rows_event(result))
    if q.chart:
        await emit("chart", {"unit": "none", **q.chart})
    await emit("answer", {"text": fill_template(q.answer_template or "", result.names, result.rows)})
