from api.ratelimit import TokenBucket


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_allows_burst_then_limits_per_key():
    clock = Clock()
    bucket = TokenBucket(per_min=10, clock=clock)
    assert all(bucket.allow("a")[0] for _ in range(10))
    allowed, retry = bucket.allow("a")
    assert not allowed and 5.9 < retry <= 6.0
    assert bucket.allow("b")[0]


def test_refills_over_time():
    clock = Clock()
    bucket = TokenBucket(per_min=10, clock=clock)
    for _ in range(10):
        bucket.allow("a")
    clock.t = 6.0
    assert bucket.allow("a")[0]
    assert not bucket.allow("a")[0]


def test_idle_keys_are_forgotten():
    clock = Clock()
    bucket = TokenBucket(per_min=10, clock=clock)
    for i in range(100):
        bucket.allow(f"ip{i}")
    clock.t = 3600
    bucket.allow("new")
    assert len(bucket._buckets) == 1
