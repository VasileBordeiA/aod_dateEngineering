import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eval_checks import check_result, matches


def run(check, agent_rows, ref_columns, ref_rows, answer="ok", had_rows=True, **kw):
    return check_result(check, agent_rows=agent_rows, ref_columns=ref_columns, ref_rows=ref_rows, answer=answer,
                        had_rows=had_rows, **kw)[0]


def test_matches_numbers_strings_and_pct():
    assert matches(23.14, 23.1, 0.01) and not matches(25.0, 23.1, 0.01)
    assert matches("manhattan", "Manhattan", 0) and not matches("Brooklyn", "Manhattan", 0)
    assert matches(12345.0, 12345, 0.0) and not matches(12346, 12345, 0.0)
    assert matches(0.231, 23.1, 0.01, pct=True) and not matches(0.231, 23.1, 0.01)
    assert matches("23.1", 23.1, 0.01) and not matches(None, 23.1, 0.5)


def test_top1():
    ref_cols, ref = ["borough", "avg_tip_pct"], [["Manhattan", 23.1], ["Queens", 20.7]]
    assert run({"type": "top1"}, [["Manhattan", 23.0, 5000]], ref_cols, ref)
    assert not run({"type": "top1"}, [["Queens", 20.7]], ref_cols, ref)


def test_top_k_set_any_column():
    ref = [["A"], ["B"], ["C"], ["D"]]
    assert run({"type": "top_k_set", "k": 3}, [[10, "b"], [9, "a"], [8, "C"]], ["zone"], ref)
    assert not run({"type": "top_k_set", "k": 3}, [[10, "a"], [9, "b"], [8, "D"]], ["zone"], ref)


def test_scalar_first_row():
    assert run({"type": "scalar", "value_columns": ["trips"], "tolerance": 0.0}, [[4321]], ["trips"], [[4321]])
    assert not run({"type": "scalar", "value_columns": ["trips"], "tolerance": 0.0}, [[4320]], ["trips"], [[4321]])
    assert run({"type": "scalar", "value_columns": ["card_share_pct"], "tolerance": 0.01, "pct": True},
               [[0.712]], ["card_share_pct"], [[71.2]])


def test_contains_value_all_and_any():
    ref_cols, ref = ["trip_type", "avg_total_usd"], [["Airport trips", 72.5], ["Other trips", 24.1]]
    check = {"type": "contains_value", "value_columns": ["avg_total_usd"], "match": "all", "tolerance": 0.02}
    assert run(check, [["Other", 24.0], ["Airport", 72.4]], ref_cols, ref)
    assert not run(check, [["Airport", 72.4]], ref_cols, ref)
    any_check = {"type": "contains_value", "value_columns": ["a", "b"], "match": "any", "tolerance": 0.02}
    assert run(any_check, [[1.0, 99.0]], ["a", "b"], [[1.0, 2.0]])


def test_mapping():
    check = {"type": "mapping", "key_column": "hour", "value_columns": ["avg_distance_miles"], "tolerance": 0.03}
    ref_cols, ref = ["hour", "avg_distance_miles"], [[0, 3.5], [1, 3.2]]
    assert run(check, [[1, 3.21], [0, 3.49]], ref_cols, ref)
    assert not run(check, [[0, 3.5]], ref_cols, ref)
    assert not run(check, [[0, 3.5], [1, 4.0]], ref_cols, ref)


def test_refusal_and_answer_only():
    assert run({"type": "refusal"}, None, [], [], answer="I only know taxi data!", had_rows=False)
    assert not run({"type": "refusal"}, [[1]], ["x"], [[1]], answer="Here you go", had_rows=True)
    assert not run({"type": "refusal"}, None, [], [], answer="", had_rows=False)
    assert run({"type": "answer_only"}, None, [], [], answer="Tips are only recorded for card payments.",
               answer_mentions=("card",))
    assert not run({"type": "answer_only"}, None, [], [], answer="Cash is great.", answer_mentions=("card",))


def test_answer_mentions_apply_to_data_checks_too():
    ref_cols, ref = ["borough", "avg_tip_pct"], [["Manhattan", 23.1]]
    assert not run({"type": "top1"}, [["Manhattan", 23.1]], ref_cols, ref, answer="Manhattan!", answer_mentions=("card",))
    assert run({"type": "top1"}, [["Manhattan", 23.1]], ref_cols, ref, answer="Manhattan (card tips only)",
               answer_mentions=("card",))


def test_no_rows_and_cached_fail():
    assert not run({"type": "top1"}, None, ["b"], [["Manhattan"]], had_rows=False)
    assert not run({"type": "top1"}, [["Manhattan"]], ["b"], [["Manhattan"]], cached=True)
