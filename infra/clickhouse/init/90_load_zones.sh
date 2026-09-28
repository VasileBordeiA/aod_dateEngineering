#!/bin/bash
# Load the taxi zones once (spec 04 §Zones loader). Runs from /docker-entrypoint-initdb.d on the first start.
set -euo pipefail
ch() { clickhouse-client --host 127.0.0.1 --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"; }
if [ ! -s /data/zones.csv ]; then
  echo "90_load_zones: /data/zones.csv is missing or empty; run 'make data' first" >&2
  exit 1
fi
if [ "$(ch --query 'SELECT count() FROM taxi.zones')" = "0" ]; then
  ch --input_format_csv_empty_as_default=1 --query "INSERT INTO taxi.zones FORMAT CSVWithNames" < /data/zones.csv
fi
echo "90_load_zones: taxi.zones has $(ch --query 'SELECT count() FROM taxi.zones') rows"
