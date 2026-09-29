import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eval_agent import error_result


def test_error_result_is_a_scored_fail_with_the_normal_shape():
    q = {"id": "flaky_one", "category": "aggregation", "suggested": True, "reference_sql": "SELECT 1"}
    r = error_result(q, 12.345, TimeoutError("timed out"))
    assert r == {
        "id": "flaky_one", "suggested": True, "category": "aggregation", "ok": False,
        "why": "request failed: TimeoutError: timed out", "first_s": 0.0, "total_s": 12.35,
        "tool_calls": 0, "invalid_tool_calls": 0, "repairs": 0, "provider": None, "mode": None,
        "error": "", "answer": "", "agent_sql": "", "reference_sql": "SELECT 1",
    }


def test_error_result_truncates_long_messages_and_defaults_suggested():
    q = {"id": "x", "category": "refusal", "reference_sql": "SELECT 2"}
    r = error_result(q, 0.0, ValueError("x" * 500))
    assert r["why"].startswith("request failed: ValueError: ")
    assert len(r["why"]) < 160
    assert r["suggested"] is False
    assert r["ok"] is False
