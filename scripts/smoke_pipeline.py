"""M1 exit check against the running stack (make smoke). Exit 1 if any check fails."""

from __future__ import annotations

import time
from pathlib import Path

import clickhouse_connect

ROOT = Path(__file__).resolve().parent.parent
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if "=" in line and not line.lstrip().startswith("#"))


def main() -> int:
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="admin",
                                       password=ENV["CLICKHOUSE_ADMIN_PASSWORD"], database="taxi")

    def one(sql: str):
        return ch.query(sql).result_rows[0][0]

    before = one("SELECT count() FROM taxi.trips")
    time.sleep(10)
    rate = (one("SELECT count() FROM taxi.trips") - before) / 10
    fresh_ms = one("SELECT dateDiff('millisecond', max(stored_at), now64(3)) FROM taxi.trips")
    p95_ms = one("SELECT quantile(0.95)(dateDiff('millisecond', produced_at, stored_at)) FROM taxi.trips "
                 "WHERE stored_at >= now64(3) - INTERVAL 1 MINUTE")
    rejected = one("SELECT count() FROM taxi.trips_rejected")
    distinct_reasons = one("SELECT uniqExact(reason) FROM taxi.trips_rejected ARRAY JOIN reasons AS reason")
    windows = one("SELECT count() FROM taxi.agg_borough_minute")
    print("sim clock:", one("SELECT toString(now_ts) FROM taxi.sim_now"))
    checks = [
        ("rows flowing", rate >= 5, f"{rate:.1f} rows/s (≈80–200 in daytime at 60×)"),
        ("freshness < 3 s", fresh_ms < 3000, f"{fresh_ms} ms"),
        ("p95 produced→stored < 2 s (last minute)", p95_ms == p95_ms and p95_ms < 2000, f"{p95_ms:.0f} ms"),
        ("rejected rows with reasons", rejected > 0 and distinct_reasons >= 2,
         f"{rejected} rows, {distinct_reasons} distinct reasons"),
        ("borough-minute aggregates", windows > 0, f"{windows} windows"),
    ]
    for name, ok, detail in checks:
        print(f"{'PASS' if ok else 'FAIL'}  {name:<42} {detail}")
    return 0 if all(ok for _, ok, _ in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
