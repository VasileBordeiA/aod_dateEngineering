import json
from datetime import UTC, datetime

from contract_validator import assert_valid

from rides.checkpoints import SEQ, checkpoint, ordered, parse_checkpoint, parse_wall, wall_str

TID = "3f2c9a1e-8b7d-4c21-9e55-0a1b2c3d4e5f"


def test_wall_str_round_trips_with_milliseconds():
    dt = datetime(2026, 10, 16, 10, 31, 7, 41_999, tzinfo=UTC)
    assert wall_str(dt) == "2026-10-16T10:31:07.041Z"
    assert parse_wall("2026-10-16T10:31:07.041Z") == datetime(2026, 10, 16, 10, 31, 7, 41_000, tzinfo=UTC)


def test_checkpoint_follows_the_contract():
    cp = checkpoint(TID, f"s-{TID}", "student", "submitted", {"payment": "card"}, "2026-10-16T10:31:07.000Z")
    assert cp["seq"] == 1 and cp["service"] == "api"
    assert_valid("trace_checkpoint", cp)


def test_seq_matches_the_contract():
    assert SEQ == {"submitted": 1, "ingested": 2, "validated": 3, "rejected": 3, "enriched": 4, "aggregated": 5,
                   "stored": 6, "queryable": 7}


def test_ordered_by_seq_then_at():
    a = {"stage": "stored", "seq": 6, "at": "2026-10-16T10:31:08.000Z"}
    b = {"stage": "submitted", "seq": 1, "at": "2026-10-16T10:31:09.000Z"}
    assert ordered([a, b]) == [b, a]


def test_parse_checkpoint_accepts_good_and_ignores_garbage():
    good = checkpoint(TID, f"s-{TID}", "student", "ingested", {}, "2026-10-16T10:31:07.000Z")
    assert parse_checkpoint(json.dumps(good).encode()) == good
    for bad in [b"", b"not json", b"[1]", json.dumps({**good, "stage": "teleported"}).encode(),
                json.dumps({**good, "seq": "2"}).encode(), json.dumps({k: v for k, v in good.items() if k != "at"}).encode(),
                json.dumps({k: v for k, v in good.items() if k != "trip_id"}).encode(),
                json.dumps({**good, "trip_id": 7}).encode(),
                json.dumps({k: v for k, v in good.items() if k != "source"}).encode(),
                json.dumps({**good, "source": None}).encode(),
                json.dumps({k: v for k, v in good.items() if k != "service"}).encode(),
                json.dumps({**good, "service": ["api"]}).encode(),
                json.dumps({k: v for k, v in good.items() if k != "detail"}).encode(),
                json.dumps({**good, "detail": "{}"}).encode(),
                None]:
        assert parse_checkpoint(bad) is None
