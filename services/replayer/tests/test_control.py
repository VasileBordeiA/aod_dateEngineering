import json
import threading
import urllib.error
import urllib.request

import pytest

from replayer.control import make_server


class FakeReplayer:
    def __init__(self):
        self.calls = []

    def pause(self): self.calls.append("pause")
    def resume(self): self.calls.append("resume")

    def set_speed(self, s):
        if s <= 0:
            raise ValueError("speed must be > 0")
        self.calls.append(("speed", s))

    def status(self): return {"paused": "pause" in self.calls}


@pytest.fixture
def server():
    rep = FakeReplayer()
    srv = make_server(rep, 0, "s3cret")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield rep, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def call(url, method="GET", token=None, body=None):
    headers = {"X-Admin-Token": token} if token else {}
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e)


def test_status_is_open(server):
    _, base = server
    assert call(f"{base}/status") == (200, {"paused": False})


def test_post_requires_token(server):
    rep, base = server
    assert call(f"{base}/pause", "POST")[0] == 401
    assert call(f"{base}/pause", "POST", token="wrong")[0] == 401
    assert rep.calls == []


def test_pause_resume_speed(server):
    rep, base = server
    assert call(f"{base}/pause", "POST", "s3cret") == (200, {"paused": True})
    call(f"{base}/resume", "POST", "s3cret")
    assert call(f"{base}/speed", "POST", "s3cret", {"speed": 120})[0] == 200
    assert rep.calls == ["pause", "resume", ("speed", 120.0)]


@pytest.mark.parametrize("body", [{}, {"speed": "fast"}, {"speed": -1}])
def test_bad_speed_is_400(server, body):
    _, base = server
    assert call(f"{base}/speed", "POST", "s3cret", body)[0] == 400


def test_empty_token_refuses_everything():
    srv = make_server(FakeReplayer(), 0, "")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        assert call(f"http://127.0.0.1:{srv.server_address[1]}/pause", "POST", token="")[0] == 401
    finally:
        srv.shutdown()


def test_unknown_path_is_404(server):
    _, base = server
    assert call(f"{base}/nope")[0] == 404
