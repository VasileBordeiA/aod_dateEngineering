import asyncio

from test_history import cp

from rides.history import TraceHistory
from rides.hub import Hub


def test_followers_get_new_checkpoints_once():
    hub = Hub(TraceHistory(3600))
    q = hub.follow("a")
    other = hub.follow("b")
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("a", "submitted", 1))  # duplicate
    assert q.qsize() == 1 and other.qsize() == 0
    hub.unfollow("a", q)
    assert hub.follower_count("a") == 0


def test_live_watchers_get_every_source():
    hub = Hub(TraceHistory(3600))
    live = hub.watch_live()
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("r", "ingested", 2, source="replay"))
    assert live.qsize() == 2
    hub.unwatch_live(live)
    assert hub.live_count() == 0


def test_on_stored_fires_once_per_trace():
    seen: list[str] = []
    hub = Hub(TraceHistory(3600), on_stored=lambda c: seen.append(c["trace_id"]))
    hub.publish(cp("a", "stored", 6))
    hub.publish(cp("a", "stored", 6))
    hub.publish(cp("a", "enriched", 4))
    assert seen == ["a"]


def test_a_slow_client_never_blocks_publish():
    hub = Hub(TraceHistory(3600), queue_size=1)
    q = hub.follow("a")
    hub.publish(cp("a", "submitted", 1))
    hub.publish(cp("a", "ingested", 2))  # queue full: dropped for this client, kept in history
    assert q.qsize() == 1 and len(hub.history.get("a")) == 2
    assert isinstance(q, asyncio.Queue)
