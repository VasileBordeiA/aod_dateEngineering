import pytest
from conftest import GOLDEN

from agent.golden import fill_template, load_golden, suggestions


def test_loads_every_entry():
    golden = load_golden(GOLDEN)
    assert len(golden) == 32
    assert {q.origin for q in golden} == {"typed", "suggested", "ride"}
    assert all(q.reference_sql.strip() for q in golden)


def test_exactly_five_suggestions_in_file_order():
    ids = [q.id for q in suggestions(load_golden(GOLDEN))]
    assert ids == ["best_tippers", "rush_hour_distance", "busiest_zones_last_hour", "airport_vs_other", "rejected_why"]
    assert all(q.chart and q.answer_template for q in suggestions(load_golden(GOLDEN)))


def test_wrong_suggestion_count_is_an_error():
    golden = load_golden(GOLDEN)
    with pytest.raises(ValueError, match="exactly 5"):
        suggestions(golden[1:])


def test_fill_template_row0_and_row_n():
    cols = ["borough", "avg_tip_pct"]
    rows = [["Manhattan", 23.1], ["Brooklyn", 19.4]]
    assert (
        fill_template("{{borough}} tips {{avg_tip_pct}}%, then {{rows.1.borough}}", cols, rows)
        == "Manhattan tips 23.1%, then Brooklyn"
    )


def test_fill_template_missing_values_do_not_crash():
    assert fill_template("{{nope}} and {{rows.9.borough}}", ["borough"], [["Queens"]]) == "? and ?"
    assert fill_template("{{borough}}", ["borough"], []) == "?"
