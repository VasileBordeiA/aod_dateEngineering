#!/bin/bash
# Create the six topics (spec 02). Idempotent: an existing topic is fine.
set -euo pipefail
RETENTION="${TOPIC_RETENTION_MS:-21600000}"

create() {
  local name="$1"; shift
  local out
  out="$(rpk topic create "$name" "$@" 2>&1)" || true
  if grep -qE "^${name//./\\.}\s+(OK|TOPIC_ALREADY_EXISTS)" <<<"$out"; then
    echo "topic ${name}: ready"
  else
    echo "topic ${name}: FAILED"; echo "$out"; exit 1
  fi
}

# Event-time streams are single-partition so pickup_ts stays ordered into the windows (ADR-014).
for t in trips.raw trips.enriched; do
  create "$t" -p 1 -r 1 -c "retention.ms=${RETENTION}" -c cleanup.policy=delete
done
for t in trips.rejected agg.borough_minute trace.checkpoints; do
  create "$t" -p 3 -r 1 -c "retention.ms=${RETENTION}" -c cleanup.policy=delete
done
create sim.clock -p 1 -r 1 -c cleanup.policy=compact
rpk topic list
