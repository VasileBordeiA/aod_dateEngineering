"""make eval (spec 10): run the golden set against the RUNNING api with the replayer paused, then write a report.

Usage: uv run python scripts/eval_agent.py --model taxi-qwen2.5-coder-14b --mode tools [--provider local] [--only a,b]
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import clickhouse_connect
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eval_checks import check_result

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "evals" / "agent_golden_questions.yaml"
REPORTS = ROOT / "evals" / "reports"
WARMUP = "How many trips are in the data?"


def read_env() -> dict[str, str]:
    return dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
                if "=" in line and not line.lstrip().startswith("#"))


def request(method: str, url: str, body: dict[str, Any] | None = None, headers: dict[str, str] | None = None,
            timeout: float = 10) -> Any:
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"null")


def ask(api: str, payload: dict[str, Any], ip: str, timeout: float) -> tuple[list[tuple[str, Any]], float, float]:
    """POST /api/agent/ask and read the SSE stream. Returns (events, first_event_s, total_s)."""
    req = urllib.request.Request(f"{api}/api/agent/ask", method="POST", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json", "X-Forwarded-For": ip})
    started, first = time.monotonic(), None
    events: list[tuple[str, Any]] = []
    event = ""
    with urllib.request.urlopen(req, timeout=timeout) as r:
        for raw in r:
            line = raw.decode().rstrip("\n")
            if line.startswith("event: "):
                event = line.removeprefix("event: ")
            elif line.startswith("data: "):
                first = first if first is not None else time.monotonic() - started
                events.append((event, json.loads(line.removeprefix("data: "))))
    return events, first or 0.0, time.monotonic() - started


def plain(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (list, tuple)):
        return [plain(x) for x in v]
    return v if v is None or isinstance(v, (int, float, str, bool)) else str(v)


def wait_until_frozen(ch: Any) -> str:
    """After /pause, trips already in flight still land. Wait until sim_now stops moving."""
    last = None
    for _ in range(20):
        now = ch.query("SELECT toString(now_ts) FROM taxi.sim_now").result_rows[0][0]
        if now == last:
            return str(now)
        last = now
        time.sleep(3)
    raise SystemExit("sim_now keeps moving although the replayer is paused. Is a backfill running?")


def pctl(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True)
    p.add_argument("--mode", default="tools")
    p.add_argument("--provider", default="local")
    p.add_argument("--api", default="http://127.0.0.1:8000")
    p.add_argument("--control", default="http://127.0.0.1:8001")
    p.add_argument("--only", default="")
    p.add_argument("--timeout", type=float, default=90)
    args = p.parse_args(argv)
    env = read_env()
    admin = {"X-Admin-Token": env["ADMIN_TOKEN"]}
    questions = yaml.safe_load(GOLDEN.read_text())["questions"]
    if args.only:
        wanted = set(args.only.split(","))
        questions = [q for q in questions if q["id"] in wanted]
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="agent_ro",
                                       password=env["CLICKHOUSE_AGENT_PASSWORD"], database="taxi")
    status = request("GET", f"{args.api}/api/llm/status")
    active = next((pv for pv in status["providers"] if pv["name"] == status["active"]), None)
    if active is None:
        raise SystemExit("no healthy LLM provider: the eval would only measure the cache")
    print(f"provider {active['name']} · model {active['model']} · mode {status['mode']}")

    request("POST", f"{args.control}/pause", headers=admin)
    results: list[dict[str, Any]] = []
    try:
        if not request("GET", f"{args.control}/status").get("paused"):
            raise SystemExit("the replayer did not pause: refusing to evaluate on moving data (spec 10)")
        sim_now = wait_until_frozen(ch)
        ask(args.api, {"question": WARMUP, "origin": "typed"}, "10.250.1.1", args.timeout)  # warm-up, not scored
        for i, q in enumerate(questions):
            if q.get("origin") == "ride":
                results.append({"id": q["id"], "skipped": "needs POST /api/rides (M4)", "suggested": False})
                continue
            payload: dict[str, Any] = {"question": q["question"], "origin": q.get("origin", "typed")}
            if q.get("suggested"):
                payload["suggestion_id"] = q["id"]
            events, first_s, total_s = ask(args.api, payload, f"10.250.0.{i + 2}", args.timeout)
            by = {}
            for e, d in events:
                by.setdefault(e, []).append(d)
            answer = by["answer"][-1]["text"] if "answer" in by else None
            rows = by["rows"][-1] if "rows" in by else None
            done = by.get("done", [{}])[-1]
            ref = ch.query(q["reference_sql"])
            ok, why = check_result(q["check"], agent_rows=rows["rows"] if rows else None,
                                   ref_columns=ref.column_names, ref_rows=[plain(r) for r in ref.result_rows],
                                   answer=answer, had_rows=rows is not None,
                                   answer_mentions=q.get("answer_mentions", ()), cached=bool(done.get("cached")))
            results.append({"id": q["id"], "suggested": bool(q.get("suggested")), "category": q["category"],
                            "ok": ok, "why": why, "first_s": round(first_s, 2), "total_s": round(total_s, 2),
                            "tool_calls": done.get("tool_calls", 0),
                            "invalid_tool_calls": done.get("invalid_tool_calls", 0),
                            "repairs": done.get("repairs", 0), "provider": done.get("provider"),
                            "mode": done.get("mode"), "error": by["error"][-1]["code"] if "error" in by else "",
                            "answer": answer, "agent_sql": by["sql"][-1]["sql"] if "sql" in by else "",
                            "reference_sql": q["reference_sql"]})
            print(f"{'PASS' if ok else 'FAIL'} {q['id']:<28} {total_s:5.1f} s  {why}")
    finally:
        request("POST", f"{args.control}/resume", headers=admin)

    scored = [r for r in results if "skipped" not in r]
    suggested = [r for r in scored if r["suggested"]]
    refusals = [r for r in scored if r["category"] == "refusal"]
    latencies = [r["total_s"] for r in scored]
    calls = sum(r["tool_calls"] for r in scored)
    summary = {
        "model": args.model, "mode": args.mode, "provider": args.provider, "sim_now": sim_now,
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "accuracy": round(sum(r["ok"] for r in scored) / len(scored), 3) if scored else 0.0,
        "suggested_accuracy": round(sum(r["ok"] for r in suggested) / len(suggested), 3) if suggested else 0.0,
        "refusals_pass": all(r["ok"] for r in refusals),
        "p50_s": round(statistics.median(latencies), 1) if latencies else 0.0, "p95_s": round(pctl(latencies, 0.95), 1),
        "tool_call_validity": round(1 - sum(r["invalid_tool_calls"] for r in scored) / calls, 3) if calls else 1.0,
        "scored": len(scored), "skipped": len(results) - len(scored),
    }
    summary["targets_met"] = (summary["accuracy"] >= 0.8 and summary["suggested_accuracy"] == 1.0
                              and summary["p95_s"] < 15 and summary["refusals_pass"])
    REPORTS.mkdir(parents=True, exist_ok=True)
    # Path.with_suffix() would truncate at the LAST dot in the name, and model ids routinely contain one
    # (qwen2.5-coder, llama3.1), so the two report paths are built by string concatenation instead.
    stem_name = f"{datetime.now(UTC):%Y-%m-%d}-{args.model.replace(':', '-').replace('/', '-')}-{args.mode}"
    json_path, md_path = REPORTS / f"{stem_name}.json", REPORTS / f"{stem_name}.md"
    json_path.write_text(json.dumps({"summary": summary, "results": results}, indent=2, default=str))
    accuracy_row = (f"| Accuracy | {summary['accuracy']:.0%} ({summary['scored']} scored, "
                    f"{summary['skipped']} skipped) | ≥ 80% |")
    lines = [f"# Agent eval: {args.model} ({args.mode}, {args.provider})", "",
             "| Metric | Value | Target |", "|---|---|---|",
             accuracy_row,
             f"| Suggested | {summary['suggested_accuracy']:.0%} | 100% |",
             f"| Refusals | {'all pass' if summary['refusals_pass'] else 'FAIL'} | all pass |",
             f"| Latency p50 / p95 | {summary['p50_s']} s / {summary['p95_s']} s | p95 < 15 s |",
             f"| Tool-call validity | {summary['tool_call_validity']:.0%} | n/a |", "",
             f"Sim clock frozen at {sim_now}. Targets met: **{summary['targets_met']}**.", "", "## Failures", ""]
    for r in scored:
        if not r["ok"]:
            lines += [f"### {r['id']} ({r['why']})", "", f"Answer: {r['answer']}", "", "Agent SQL:", "```sql",
                      r["agent_sql"], "```", "Reference SQL:", "```sql", r["reference_sql"].strip(), "```", ""]
    md_path.write_text("\n".join(lines))
    print(json.dumps(summary, indent=2))
    print(f"report: {md_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
