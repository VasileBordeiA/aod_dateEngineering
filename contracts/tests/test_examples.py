import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import contract_validator as cv

EXPECTED = {"trip", "enriched_trip", "rejected_trip", "borough_minute_agg", "trace_checkpoint", "sim_clock"}


def test_expected_schema_set():
    assert set(cv.schema_names()) == EXPECTED


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_schema_is_valid_2020_12(name):
    Draft202012Validator.check_schema(cv.schema(name))


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_examples_validate(name):
    examples = cv.schema(name).get("examples", [])
    assert examples, f"{name} has no examples"
    for example in examples:
        cv.assert_valid(name, example)


@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_unknown_field_is_rejected(name):
    example = dict(cv.schema(name)["examples"][0])
    example["__unexpected"] = 1
    assert cv.errors(name, example)


def test_assert_valid_lists_the_problem():
    bad = dict(cv.schema("trip")["examples"][0], source="taxi")
    with pytest.raises(AssertionError, match="source"):
        cv.assert_valid("trip", bad)
