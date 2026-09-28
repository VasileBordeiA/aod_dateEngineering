import copy
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(REPO / "contracts"))

import contract_validator as cv  # noqa: E402


@pytest.fixture
def trip_example() -> dict:
    return copy.deepcopy(cv.schema("trip")["examples"][0])


@pytest.fixture
def enriched_example() -> dict:
    return copy.deepcopy(cv.schema("enriched_trip")["examples"][0])


@pytest.fixture
def zones():
    from processor.zones import load_zones
    return load_zones(FIXTURES / "zones.csv")
