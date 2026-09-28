import re
import uuid
from datetime import UTC, datetime, timedelta

from conftest import REPO

from processor.rows import AGG_COLUMNS, REJECTED_COLUMNS, TRIPS_COLUMNS, agg_row, rejected_row, trip_row

INIT = REPO / "infra" / "clickhouse" / "init"


def ddl_columns(file: str) -> list[str]:
    return re.findall(r"^    ([a-z_]+)\s", (INIT / file).read_text(), re.M)


def test_columns_match_ddl():
    assert [*TRIPS_COLUMNS, "stored_at"] == ddl_columns("20_trips.sql")
    assert list(REJECTED_COLUMNS) == ddl_columns("30_trips_rejected.sql")
    assert list(AGG_COLUMNS) == ddl_columns("40_agg_borough_minute.sql")


def test_sim_timestamps_are_utc_aware_and_unshifted(enriched_example):
    row = dict(zip(TRIPS_COLUMNS, trip_row(enriched_example), strict=True))
    assert row["pickup_ts"] == datetime(2025, 10, 7, 8, 14, 3, tzinfo=UTC)
    assert row["pickup_ts"].utcoffset() == timedelta(0)
    assert row["produced_at"] == datetime(2026, 10, 16, 8, 2, 11, 482_000, tzinfo=UTC)


def test_nulls_and_types(enriched_example):
    ex = dict(enriched_example, airport_fee=None, extra=None, trace_id="3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f")
    row = dict(zip(TRIPS_COLUMNS, trip_row(ex), strict=True))
    assert row["airport_fee"] == 0.0 and row["extra"] == 0.0
    assert row["trace_id"] == uuid.UUID("3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f")
    assert row["tip_pct"] == 25.88 and row["is_card"] is True
    assert len(trip_row(enriched_example)) == len(TRIPS_COLUMNS)


def test_rejected_and_agg_rows():
    import contract_validator as cv
    rej = dict(zip(REJECTED_COLUMNS, rejected_row(cv.schema("rejected_trip")["examples"][1]), strict=True))
    assert rej["pickup_ts"] == datetime(2025, 10, 9, 17, 42, tzinfo=UTC)
    assert rej["reasons"] == ["TIP_UNREALISTIC"] and isinstance(rej["trace_id"], uuid.UUID)
    assert dict(zip(REJECTED_COLUMNS, rejected_row(cv.schema("rejected_trip")["examples"][0]), strict=True))["trace_id"] is None
    agg = dict(zip(AGG_COLUMNS, agg_row(cv.schema("borough_minute_agg")["examples"][0]), strict=True))
    assert agg["window_start"] == datetime(2025, 10, 7, 8, 14, tzinfo=UTC) and agg["trips"] == 142
