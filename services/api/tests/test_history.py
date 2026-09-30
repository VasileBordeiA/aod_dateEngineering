from rides.history import TraceHistory


def cp(tid: str, stage: str, seq: int, at: str = "2026-10-16T10:31:07.000Z", source: str = "student") -> dict:
    return {"trace_id": tid, "trip_id": f"s-{tid}", "source": source, "stage": stage, "seq": seq, "service": "api",
            "at": at, "detail": {}}


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_add_dedups_by_stage_and_keeps_the_first():
    h = TraceHistory(3600)
    first = cp("a", "stored", 6, "2026-10-16T10:31:08.000Z")
    assert h.add(first) is True
    assert h.add(cp("a", "stored", 6, "2026-10-16T10:31:09.000Z")) is False  # reprocessed after a restart
    assert h.get("a") == [first]


def test_get_orders_by_seq_not_arrival():
    h = TraceHistory(3600)
    for c in [cp("a", "stored", 6), cp("a", "submitted", 1), cp("a", "rejected", 3)]:
        h.add(c)
    assert [c["stage"] for c in h.get("a")] == ["submitted", "rejected", "stored"]


def test_ttl_expires_untouched_traces():
    clock = Clock()
    h = TraceHistory(ttl_s=10, clock=clock)
    h.add(cp("a", "submitted", 1))
    clock.t = 9
    h.add(cp("b", "submitted", 1))
    clock.t = 11
    assert h.get("a") == [] and len(h.get("b")) == 1


def test_lru_bound():
    h = TraceHistory(3600, max_traces=2)
    for tid in ["a", "b", "c"]:
        h.add(cp(tid, "submitted", 1))
    assert h.get("a") == [] and h.get("c") != []


def test_merge_and_has_stage():
    h = TraceHistory(3600)
    h.add(cp("a", "validated", 3))
    merged = h.merge("a", [cp("a", "submitted", 1), cp("a", "validated", 3, "2026-10-16T10:31:09.000Z")])
    assert [c["stage"] for c in merged] == ["submitted", "validated"]
    assert h.has_stage("a", "submitted") and not h.has_stage("a", "stored") and not h.has_stage("x", "stored")


def test_recent_student_newest_first_and_only_students():
    h = TraceHistory(3600)
    h.add(cp("s1", "submitted", 1))
    h.add(cp("r1", "ingested", 2, source="replay"))
    h.add(cp("s2", "submitted", 1))
    h.add(cp("s1", "ingested", 2))  # touching s1 makes it the newest
    assert [t for t, _ in h.recent_student(20)] == ["s1", "s2"]
    assert [t for t, _ in h.recent_student(1)] == ["s1"]
