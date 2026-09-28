"""Entrypoint: python -m replayer.main (spec 01)."""

from __future__ import annotations

import signal
import threading

from replayer.config import Config
from replayer.control import make_server
from replayer.engine import Replayer
from replayer.health import HEARTBEAT
from replayer.log import event
from replayer.producer import KafkaOut
from replayer.source import ReplaySource


def main() -> int:
    try:
        cfg = Config.from_env()
        source = ReplaySource(cfg.parquet_path)
    except (ValueError, FileNotFoundError) as e:
        event("startup_failed", error=str(e))
        return 2
    rep = Replayer(cfg, source, KafkaOut(cfg.brokers), heartbeat_path=HEARTBEAT)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    server = None
    if cfg.mode == "live":
        server = make_server(rep, cfg.control_port, cfg.admin_token)
        threading.Thread(target=server.serve_forever, name="control", daemon=True).start()
    rep.run(stop)
    if server is not None:
        server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
