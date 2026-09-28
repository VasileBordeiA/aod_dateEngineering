"""Download one month of NYC TLC yellow-taxi data and prepare it for replay (spec 01, `make data`).

Outputs (under DATA_DIR, default ./data):
  replay/trips_<month>.parquet   pickup-sorted, contiguous row_idx, renamed columns, only rows inside the month
  replay/prep_report.json        data-quality preview computed with the stream processor's own rules
  zones.csv                      location_id,borough,zone,service_zone,lat,lon
  taxi_zones.geojson             simplified zone polygons (EPSG:4326) for the Grafana geomap
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "processor" / "src"))
from processor.rules import check_trip  # noqa: E402  (stdlib-only module shared with the processor)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"

# (target column, candidate source columns, DuckDB cast)
COLUMNS: list[tuple[str, tuple[str, ...], str]] = [
    ("vendor_id", ("VendorID",), "INTEGER"),
    ("pickup_ts", ("tpep_pickup_datetime",), "TIMESTAMP"),
    ("dropoff_ts", ("tpep_dropoff_datetime",), "TIMESTAMP"),
    ("passenger_count", ("passenger_count",), "INTEGER"),
    ("trip_distance", ("trip_distance",), "DOUBLE"),
    ("ratecode_id", ("RatecodeID",), "INTEGER"),
    ("store_and_fwd_flag", ("store_and_fwd_flag",), "VARCHAR"),
    ("pu_location_id", ("PULocationID",), "INTEGER"),
    ("do_location_id", ("DOLocationID",), "INTEGER"),
    ("payment_type", ("payment_type",), "INTEGER"),
    ("fare_amount", ("fare_amount",), "DOUBLE"),
    ("extra", ("extra",), "DOUBLE"),
    ("mta_tax", ("mta_tax",), "DOUBLE"),
    ("tip_amount", ("tip_amount",), "DOUBLE"),
    ("tolls_amount", ("tolls_amount",), "DOUBLE"),
    ("improvement_surcharge", ("improvement_surcharge",), "DOUBLE"),
    ("congestion_surcharge", ("congestion_surcharge",), "DOUBLE"),
    ("airport_fee", ("Airport_fee", "airport_fee"), "DOUBLE"),
    ("cbd_congestion_fee", ("cbd_congestion_fee",), "DOUBLE"),
    ("total_amount", ("total_amount",), "DOUBLE"),
]
REQUIRED = {"pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "trip_distance",
            "payment_type", "fare_amount", "tip_amount", "total_amount"}


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = (int(x) for x in month.split("-"))
    start = date(year, mon, 1)
    nxt = date(year + (mon == 12), mon % 12 + 1, 1)
    return start.isoformat(), nxt.isoformat()


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as resp:
        size = int(resp.headers.get("Content-Length", -1))
    if dest.exists() and dest.stat().st_size == size:
        print(f"  cached  {dest}")
        return dest
    print(f"  get     {url}")
    part = dest.with_suffix(dest.suffix + ".part")
    urllib.request.urlretrieve(url, part)
    part.replace(dest)
    return dest


def select_list(source_columns: Iterable[str]) -> str:
    by_lower = {c.lower(): c for c in source_columns}
    parts = []
    for target, candidates, cast in COLUMNS:
        src = next((by_lower[c.lower()] for c in candidates if c.lower() in by_lower), None)
        if src is None:
            if target in REQUIRED:
                raise ValueError(f"required column for {target!r} missing (looked for {candidates})")
            parts.append(f"CAST(NULL AS {cast}) AS {target}")
        else:
            parts.append(f'CAST("{src}" AS {cast}) AS {target}')
    return ",\n               ".join(parts)


def prepare_replay(raw: Path, out: Path, month: str) -> int:
    start, nxt = month_bounds(month)
    con = duckdb.connect()
    cols = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{raw}')").fetchall()]
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          SELECT row_number() OVER (ORDER BY pickup_ts, dropoff_ts, pu_location_id) - 1 AS row_idx, *
          FROM (SELECT {select_list(cols)} FROM read_parquet('{raw}'))
          WHERE pickup_ts >= TIMESTAMP '{start}' AND pickup_ts < TIMESTAMP '{nxt}'
          ORDER BY row_idx
        ) TO '{out}' (FORMAT parquet, ROW_GROUP_SIZE 100000)
    """)
    return pq.ParquetFile(out).metadata.num_rows


def iter_rows(path: Path) -> Iterator[dict[str, Any]]:
    for batch in pq.ParquetFile(path).iter_batches(batch_size=100_000):
        yield from batch.to_pylist()


def count_reasons(rows: Iterable[dict[str, Any]], tip_max_abs: float = 100.0
                  ) -> tuple[Counter[str], dict[str, Counter[str]]]:
    total: Counter[str] = Counter()
    per_day: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        reasons = check_trip({**row, "trip_id": "prep", "source": "replay"}, tip_max_abs)
        day = per_day[row["pickup_ts"].date().isoformat()]
        total["rows"] += 1
        day["rows"] += 1
        if reasons:
            total["rejected_rows"] += 1
            day["rejected_rows"] += 1
        for reason in reasons:
            total[reason] += 1
            day[reason] += 1
    return total, per_day


def build_report(raw: Path, replay: Path, month: str) -> dict[str, Any]:
    start, nxt = month_bounds(month)
    con = duckdb.connect()
    raw_rows, dmin, dmax = con.execute(f"""
        SELECT (SELECT count(*) FROM read_parquet('{raw}')),
               min(tpep_pickup_datetime), max(tpep_pickup_datetime)
        FROM read_parquet('{raw}')
        WHERE tpep_pickup_datetime < TIMESTAMP '{start}' OR tpep_pickup_datetime >= TIMESTAMP '{nxt}'
    """).fetchone()
    total, per_day = count_reasons(iter_rows(replay))
    by_weekday: dict[str, list[int]] = defaultdict(list)
    for day, counts in per_day.items():
        by_weekday[date.fromisoformat(day).strftime("%A")].append(counts["rows"])
    avg = {wd: sum(v) / len(v) for wd, v in by_weekday.items()}
    busiest = max(avg, key=avg.__getitem__)
    first = date.fromisoformat(start) + timedelta(days=7)  # leave a week of history for the backfill
    while first.strftime("%A") != busiest:
        first += timedelta(days=1)
    return {
        "month": month,
        "raw_rows": raw_rows,
        "kept_rows": total["rows"],
        "dropped_out_of_month": raw_rows - total["rows"],
        "dropped_pickup_min": str(dmin) if dmin else None,
        "dropped_pickup_max": str(dmax) if dmax else None,
        "rejected_rows": total["rejected_rows"],
        "rejected_pct": round(100 * total["rejected_rows"] / max(total["rows"], 1), 2),
        "by_reason": {k: v for k, v in total.most_common() if k not in ("rows", "rejected_rows")},
        "per_day": {d: dict(c) for d, c in sorted(per_day.items())},
        "avg_trips_by_weekday": {k: round(v) for k, v in sorted(avg.items(), key=lambda kv: -kv[1])},
        "suggested_backfill_until": datetime.combine(first, datetime.min.time()).replace(hour=7).isoformat(),
    }


def zones_frames(lookup: pd.DataFrame, shapes: gpd.GeoDataFrame) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    shapes = shapes.to_crs(2263)
    dissolved = shapes[["LocationID", "geometry"]].dissolve(by="LocationID", as_index=False)
    centroids = dissolved.geometry.centroid.to_crs(4326)  # centroid in a projected CRS, then reproject
    cent = pd.DataFrame({"location_id": dissolved["LocationID"].astype(int),
                         "lat": centroids.y.round(6), "lon": centroids.x.round(6)})
    lk = lookup.rename(columns={"LocationID": "location_id", "Borough": "borough", "Zone": "zone"})
    lk = lk[["location_id", "borough", "zone", "service_zone"]].copy()
    lk["location_id"] = lk["location_id"].astype(int)
    lk[["borough", "zone", "service_zone"]] = lk[["borough", "zone", "service_zone"]].fillna("Unknown")
    zones = lk.merge(cent, on="location_id", how="left").sort_values("location_id").reset_index(drop=True)
    geo = dissolved.assign(geometry=dissolved.geometry.simplify(20)).to_crs(4326)
    geo = geo.rename(columns={"LocationID": "location_id"}).merge(
        lk[["location_id", "zone", "borough"]], on="location_id", how="left")
    return zones, geo


def build_zones(lookup_csv: Path, shapes_zip: Path, out_csv: Path, out_geojson: Path) -> dict[str, Any]:
    with zipfile.ZipFile(shapes_zip) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".shp"))
    shapes = gpd.read_file(f"zip://{shapes_zip}!{member}")
    zones, geo = zones_frames(pd.read_csv(lookup_csv), shapes)
    zones.to_csv(out_csv, index=False)
    out_geojson.unlink(missing_ok=True)
    geo.to_file(out_geojson, driver="GeoJSON")
    missing = zones.loc[zones["lat"].isna(), "location_id"].tolist()
    return {"zones": len(zones), "with_centroid": int(zones["lat"].notna().sum()), "without_geometry": missing,
            "geojson_features": len(geo), "geojson_bytes": out_geojson.stat().st_size}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Prepare NYC TLC data for replay (spec 01).")
    ap.add_argument("--month", default=os.environ.get("TLC_MONTH", "2025-10"))
    ap.add_argument("--data-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "./data")))
    ap.add_argument("--skip-report", action="store_true", help="skip the (slow) data-quality report")
    args = ap.parse_args(argv)
    data, raw_dir = args.data_dir, args.data_dir / "raw"

    print("1/4 download")
    trips_raw = download(f"{BASE_URL}/trip-data/yellow_tripdata_{args.month}.parquet",
                         raw_dir / f"yellow_tripdata_{args.month}.parquet")
    lookup = download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", raw_dir / "taxi_zone_lookup.csv")
    shapes = download(f"{BASE_URL}/misc/taxi_zones.zip", raw_dir / "taxi_zones.zip")

    print("2/4 replay file")
    replay = data / "replay" / f"trips_{args.month}.parquet"
    print(f"  {prepare_replay(trips_raw, replay, args.month):,} rows → {replay}")

    print("3/4 zones")
    zinfo = build_zones(lookup, shapes, data / "zones.csv", data / "taxi_zones.geojson")
    print(f"  {zinfo['zones']} zones, {zinfo['with_centroid']} with centroid, no geometry: {zinfo['without_geometry']}")

    if args.skip_report:
        return 0
    print("4/4 data-quality report (applies the processor's validation rules to every row)")
    report = build_report(trips_raw, replay, args.month) | {"zones": zinfo}
    (data / "replay" / "prep_report.json").write_text(json.dumps(report, indent=2))
    print(f"  rejected {report['rejected_rows']:,} of {report['kept_rows']:,} ({report['rejected_pct']}%)")
    print(f"  by reason: {report['by_reason']}")
    print(f"  suggested: REPLAY_START_TS / backfill UNTIL = {report['suggested_backfill_until']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
