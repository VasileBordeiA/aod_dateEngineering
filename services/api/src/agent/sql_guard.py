"""SQL guard (spec 06). Pure and independent of the LLM.

Defence in depth: the agent_ro user is the real wall. This gives fast, friendly errors the model can repair,
and on its own stops the agent reading agent_log or system.* (CLAUDE.md invariant 4). Never add a flag that
skips it.

Table references (rules 4 + 5). Every FROM/JOIN table, and every bare name on the right of IN (`x IN t`,
`x IN (t)`, `x IN taxi.t`: ClickHouse reads that name as a table), must be either
  - a whitelisted table, bare or `taxi.`-prefixed, spelled exactly as in ALLOWED_TABLES (ClickHouse names
    are case-sensitive, so `TAXI.TRIPS` would never run anyway), or
  - a WITH name (CTE) visible at that point.

WITH names (CTEs) follow four plain rules:
  1. A WITH name may not reuse a whitelisted table name (trips, zones, ...), in any letter case.
  2. Each WITH name (table CTE or scalar alias) is defined at most once in the whole statement,
     at any nesting level.
  3. A WITH name is visible only to its own query body (including subqueries inside it) and to *later*
     CTEs of the same WITH (backward-only), and must match exactly, letter case included, as ClickHouse does.
  4. A scalar WITH (`WITH <expr> AS name`) is a value, never a table: it makes no table name visible.
Why this is enough: a name the guard accepts either is a whitelisted table (never a WITH name, by rule 1)
or names exactly one CTE (rule 2) declared before the reference (rule 3), so the references between CTEs
only point backwards and can never form a cycle that makes ClickHouse fall back to a real table.
This assumes ClickHouse sees a CTE wherever the guard does (verified for UNION branches, FROM subqueries
and inner CTE bodies; ClickHouse also sees later siblings, which the guard doesn't).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

logging.getLogger("sqlglot").setLevel(logging.ERROR)  # "falling back to Command" warnings are expected here

MAX_SQL_CHARS = 5000
SAFETY_LIMIT = 200
ALLOWED_TABLES = frozenset({"trips", "trips_rejected", "agg_borough_minute", "zones", "sim_now", "trace_events"})
ALLOWED_DATABASE = "taxi"
PARSE_ERROR = "The SQL checker couldn't parse this. Simplify the query."
DENIED_FUNCTIONS = frozenset(
    {
        "sleep",
        "sleepeachrow",
        "getsetting",
        "getserversetting",
        "getmergetreesetting",
        "currentuser",
        "current_user",
        "throwif",
        # These take a table name as a string, so they would look at tables the whitelist can't see.
        "hascolumnintable",
        "hasdictionary",
        "joinget",
        "joingetornull",
    }
)
# "dict" covers dictGet* (spec rule 6) and the other dictionary readers (dictHas, dictIsIn, ...).
DENIED_PREFIXES = ("url", "file", "s3", "remote", "dict", "hastable")
# The function spellings of IN (in, notIn, globalNullIn, ...) also read a bare name as a table. The IN
# operator is checked like a table reference; these spellings are simply refused.
IN_FUNCTION = re.compile(r"(global)?(not)?(null)?in(ignoreset)?")
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


def _visible_cte_names(ref: exp.Expression) -> set[str]:
    """CTE names an enclosing WITH makes visible at `ref` (WITH rules 3 and 4).

    Names are returned exactly as written, not lowercased: ClickHouse matches CTE names
    case-sensitively (verified: `WITH AGENT_LOG AS (SELECT 1 AS x) SELECT * FROM agent_log` reads
    the real agent_log table), so a differently-cased CTE must not count as a shadow here either.

    Inside the body of CTE i of a WITH, only CTEs 0..i-1 of that same WITH are visible: never
    CTE i itself and never a later sibling. The query body that owns the WITH (`ref` is not
    nested inside any of that WITH's own CTEs) sees all of that WITH's table CTEs. Outer WITHs
    apply the same rule at their own level, so we walk `ref`'s full ancestor chain.

    This is stricter than ClickHouse, which also looks up later siblings (verified:
    `WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS x FROM trips) SELECT * FROM a` works there).
    The guard rejects such a forward reference; the model gets a repairable error instead.

    A scalar CTE (`WITH <expr> AS name`, `cte.args["scalar"]`) is a ClickHouse expression alias,
    not a relation (verified: `WITH 1 AS x SELECT * FROM x` errors "unknown table x"), so its
    name is never included.
    """
    ancestors: list[object] = []
    node = ref.parent
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
        ctes = with_.expressions
        owning_index = next((i for i, cte in enumerate(ctes) if id(cte) in in_scope), None)
        # None: `ref` isn't nested inside any of this WITH's own CTEs, so it's in the body that
        # owns the WITH, which sees all of them. Otherwise: only the strictly earlier siblings.
        visible = ctes if owning_index is None else ctes[:owning_index]
        names.update(cte.alias_or_name for cte in visible if not cte.args.get("scalar"))
    return names


def _table_ref_error(ref: exp.Expression, name: str, db: str, too_many_parts: bool) -> str:
    """Why `[db.]name` at `ref` is not an allowed table, or "" if it is (rules 4 + 5)."""
    unavailable = f"Table {ref.sql('clickhouse')} is not available. Use: {TABLE_LIST}."
    if too_many_parts or db not in ("", ALLOWED_DATABASE):
        return unavailable
    if not db and name in _visible_cte_names(ref):
        return ""
    return "" if name in ALLOWED_TABLES else unavailable


def _in_target(node: exp.In) -> exp.Column | exp.Identifier | None:
    """The bare name on the right of IN (`x IN t`, `x IN (t)`, `x IN taxi.t`), or None.

    sqlglot parses such a name as a Column, never a Table, but ClickHouse reads it as a table.
    NOT IN and GLOBAL IN are the same exp.In node. A subquery (`query`), a tuple() (`field`
    is a Struct) or a list of two or more items is left alone: those aren't table names.
    """
    target = node.args.get("field")
    if target is None:
        items = node.expressions
        target = items[0] if len(items) == 1 else None
    while isinstance(target, exp.Paren):
        target = target.this
    return target if isinstance(target, exp.Column | exp.Identifier) else None


def _check_references(root: exp.Expression) -> str:
    """Rules 4 + 5 over every table and every IN target. Returns a reason, or "" when all are allowed."""
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            return f"Table functions like {table.this.sql('clickhouse')[:40]} are not allowed."
        error = _table_ref_error(table, table.name, table.db, bool(table.catalog))
        if error:
            return error
    for in_ in root.find_all(exp.In):
        target = _in_target(in_)
        if isinstance(target, exp.Identifier):
            error = _table_ref_error(target, target.name, "", False)
        elif isinstance(target, exp.Column):
            if not isinstance(target.this, exp.Identifier):
                return f"IN {target.sql('clickhouse')} is not allowed. Use IN (SELECT …) or a list of values."
            too_many_parts = bool(target.args.get("db") or target.args.get("catalog"))
            error = _table_ref_error(target, target.name, target.table, too_many_parts)
        else:
            continue
        if error:
            return error
    return ""


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
    # WITH rules 1 + 2 (see the module docstring): no whitelisted table name, and no name twice.
    seen: set[str] = set()
    for cte in root.find_all(exp.CTE):
        name = cte.alias_or_name
        if name.lower() in ALLOWED_TABLES:
            return _reject(f"A WITH name can't reuse the table name '{name}'. Pick another name.")
        if name in seen:
            return _reject(f"Each WITH name must be unique; '{name}' is defined twice. Rename one.")
        seen.add(name)
    # Rules 4 + 5: tables and IN targets are whitelisted tables or visible CTEs (WITH rules 3 + 4).
    error = _check_references(root)
    if error:
        return _reject(error)
    # Rule 6: function denylist, anywhere in the tree.
    for func in root.find_all(exp.Func):
        fname = _function_name(func)
        if fname in DENIED_FUNCTIONS or fname.startswith(DENIED_PREFIXES) or IN_FUNCTION.fullmatch(fname):
            return _reject(f"The function {fname}() is not allowed.")
    return GuardResult(ok=True, safe_sql=f"SELECT * FROM (\n{text}\n) LIMIT {SAFETY_LIMIT}")
