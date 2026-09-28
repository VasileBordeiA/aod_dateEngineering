"""SQL guard (spec 06). Pure and independent of the LLM.

Defence in depth: the agent_ro user is the real wall. This gives fast, friendly errors the model can repair,
and stops obvious abuse. Never add a flag that skips it (CLAUDE.md invariant 4).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

logging.getLogger("sqlglot").setLevel(logging.ERROR)  # "falling back to Command" warnings are expected here

MAX_SQL_CHARS = 5000
SAFETY_LIMIT = 200
ALLOWED_TABLES = frozenset({"trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"})
PARSE_ERROR = "The SQL checker couldn't parse this. Simplify the query."
DENIED_FUNCTIONS = frozenset({"sleep", "sleepeachrow", "getsetting", "currentuser", "current_user", "throwif"})
DENIED_PREFIXES = ("url", "file", "s3", "remote", "dictget")
TABLE_LIST = ", ".join(sorted(ALLOWED_TABLES))


@dataclass(frozen=True)
class GuardResult:
    ok: bool
    reason: str = ""
    safe_sql: str = ""


def _reject(reason: str) -> GuardResult:
    return GuardResult(ok=False, reason=reason)


def _function_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def _visible_cte_names(table: exp.Table) -> set[str]:
    """CTE names an enclosing WITH makes visible to `table`, honouring scope.

    Walk `table`'s ancestors. Each ancestor Select/SetOperation that carries a "with" arg
    contributes its CTE names, EXCEPT a CTE that is itself an ancestor of `table` (i.e. `table`
    sits inside that very CTE's own defining subquery). A non-recursive CTE's self-reference
    resolves to the real table of that name, not to the CTE — so it must not be exempted here.
    """
    ancestors: list[object] = []
    node = table.parent
    while node is not None:
        ancestors.append(node)
        node = node.parent
    in_scope = {id(a) for a in ancestors}
    names: set[str] = set()
    for ancestor in ancestors:
        if not isinstance(ancestor, exp.Select | exp.SetOperation):
            continue
        with_ = ancestor.args.get("with_")
        if with_ is None:
            continue
        for cte in with_.expressions:
            if id(cte) in in_scope:
                continue  # table is inside this CTE's own body: self-reference, not a mention
            names.add(cte.alias_or_name.lower())
    return names


def check_sql(sql: str) -> GuardResult:
    text = sql.strip().rstrip(";").strip()
    if not text:
        return _reject("The query is empty.")
    if len(text) > MAX_SQL_CHARS:
        return _reject(f"The query is too long (max {MAX_SQL_CHARS} characters). Aggregate instead.")
    try:
        statements = [s for s in sqlglot.parse(text, read="clickhouse") if s is not None]
    except (SqlglotError, RecursionError):
        # ParseError and TokenError (unterminated string/comment, bad token) both derive from
        # SqlglotError. A pathologically nested query can blow the interpreter's recursion limit
        # during parsing; either way this is a repairable "can't parse it" for the LLM (rule 2).
        return _reject(PARSE_ERROR)
    if len(statements) != 1:
        return _reject("Only one statement is allowed.")
    root = statements[0]
    # Rule 3: the root must be a query. SET, INSERT, DDL, SHOW, EXPLAIN, SYSTEM… are exp.Command or other nodes.
    if not isinstance(root, exp.Select | exp.SetOperation):
        return _reject(f"Only SELECT queries are allowed, not {root.key.upper()}.")
    # Rule 7: no SETTINGS, FORMAT or INTO on any (sub)query.
    for node in root.find_all(exp.Select, exp.SetOperation):
        for clause in ("settings", "format"):
            if node.args.get(clause):
                return _reject(f"{clause.upper()} clauses are not allowed.")
    if root.find(exp.Into) is not None:
        return _reject("INTO is not allowed.")
    # Rules 4 + 5: every table is a plain whitelisted name (optionally taxi.), or a CTE name
    # visible at that point (scope-aware: a CTE's own self-reference is the real table, not itself).
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            return _reject(f"Table functions like {table.this.sql('clickhouse')[:40]} are not allowed.")
        name, db = table.name.lower(), table.db.lower()
        if table.catalog or db not in ("", "taxi"):
            return _reject(f"Table {table.sql('clickhouse')} is not available. Use: {TABLE_LIST}.")
        if not db and name in _visible_cte_names(table):
            continue
        if name not in ALLOWED_TABLES:
            return _reject(f"Table {table.sql('clickhouse')} is not available. Use: {TABLE_LIST}.")
    # Rule 6: function denylist, anywhere in the tree.
    for func in root.find_all(exp.Func):
        fname = _function_name(func)
        if fname in DENIED_FUNCTIONS or fname.startswith(DENIED_PREFIXES):
            return _reject(f"The function {fname}() is not allowed.")
    return GuardResult(ok=True, safe_sql=f"SELECT * FROM (\n{text}\n) LIMIT {SAFETY_LIMIT}")
