import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eval_agent import error_result, ride_question


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


def test_ride_question_fills_both_placeholders_and_uses_the_right_ride():
    rides = {"card": "00000000-0000-0000-0000-00000000000c", "cash": "00000000-0000-0000-0000-00000000000a"}
    q = {"id": "ride_rank", "question": "Where does my ride rank for tips today?", "origin": "ride", "ride": "card",
         "reference_sql": "SELECT '{trace_id_card}', '{trace_id_cash}'"}
    sql, payload = ride_question(q, rides)
    assert sql == "SELECT '00000000-0000-0000-0000-00000000000c', '00000000-0000-0000-0000-00000000000a'"
    assert payload == {"question": q["question"], "origin": "ride", "trace_id": rides["card"]}
    assert ride_question({**q, "ride": "cash"}, rides)[1]["trace_id"] == rides["cash"]
