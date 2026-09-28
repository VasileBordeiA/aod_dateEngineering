"""Operator control API on 127.0.0.1:8001 (spec 01): GET /status, POST /pause|/resume|/speed."""

from __future__ import annotations

import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol


class Controllable(Protocol):
    def pause(self) -> None: ...
    def resume(self) -> None: ...
    def set_speed(self, speed: float) -> None: ...
    def status(self) -> dict[str, Any]: ...


def make_server(replayer: Controllable, port: int, token: str) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _json(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authorized(self) -> bool:
            supplied = self.headers.get("X-Admin-Token", "")
            return bool(token) and hmac.compare_digest(supplied, token)

        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/status":
                self._json(200, replayer.status())
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path not in ("/pause", "/resume", "/speed"):
                self._json(404, {"error": "not found"})
                return
            if not self._authorized():
                self._json(401, {"error": "missing or wrong X-Admin-Token"})
                return
            if self.path == "/pause":
                replayer.pause()
            elif self.path == "/resume":
                replayer.resume()
            else:
                length = int(self.headers.get("Content-Length", "0"))
                try:
                    body = json.loads(self.rfile.read(length) or b"{}")
                    replayer.set_speed(float(body["speed"]))
                except (KeyError, TypeError, ValueError) as e:
                    self._json(400, {"error": f"expected {{\"speed\": <number > 0>}}: {e}"})
                    return
            self._json(200, replayer.status())

        def log_message(self, format: str, *args: Any) -> None:  # silence default stderr logging
            pass

    return ThreadingHTTPServer(("0.0.0.0", port), Handler)
