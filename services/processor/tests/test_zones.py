import pytest

from processor.zones import UNKNOWN, Zone, load_zones, zone_for


def test_loads_fixture(zones):
    assert zones[161] == Zone("Midtown Center", "Manhattan", "Yellow Zone")
    assert zone_for(zones, 132).service_zone == "Airports"


def test_unknown_ids(zones):
    assert zone_for(zones, 264) == UNKNOWN == zone_for(zones, 265) == zone_for(zones, 999)
    assert UNKNOWN == Zone("Unknown", "Unknown", "Unknown")


def test_unexpected_values_are_normalised(tmp_path):
    p = tmp_path / "z.csv"
    p.write_text("location_id,borough,zone,service_zone,lat,lon\n9,Atlantis,,Somewhere,,\n")
    assert load_zones(p)[9] == Zone("Unknown", "Unknown", "Unknown")


def test_empty_file_fails_fast(tmp_path):
    p = tmp_path / "z.csv"
    p.write_text("location_id,borough,zone,service_zone,lat,lon\n")
    with pytest.raises(ValueError, match="no zones"):
        load_zones(p)
