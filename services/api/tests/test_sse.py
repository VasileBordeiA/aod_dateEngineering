import json
import math

import pytest

from agent.sse import encode


def test_frame_format():
    frame = encode("status", {"text": "Reading the question…"}).decode()
    assert frame == 'event: status\ndata: {"text":"Reading the question…"}\n\n'


def test_data_is_single_line_even_with_newlines_in_values():
    frame = encode("sql", {"attempt": 1, "sql": "SELECT 1\nFROM trips", "purpose": "x"}).decode()
    assert frame.count("\n") == 3
    assert json.loads(frame.split("data: ", 1)[1])["sql"] == "SELECT 1\nFROM trips"


def test_nan_is_a_bug_not_silent():
    with pytest.raises(ValueError):
        encode("rows", {"rows": [[math.nan]]})
