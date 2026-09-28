#!/bin/bash
# Create the six topics (spec 02). Idempotent, and converges config on topics that already exist.
#
# Retention is by SIZE, not time: the processor writes records with their event-time (sim, 2025) timestamps,
# so time-based retention would delete freshly written data immediately (ADR-015).
set -euo pipefail
RETENTION_BYTES="${TOPIC_RETENTION_BYTES:-1073741824}"

create() {
  local name="$1"; shift
  local out
  out="$(rpk topic create "$name" "$@" 2>&1)" || true
  if ! grep -qE "^${name//./\\.}\s+(OK|TOPIC_ALREADY_EXISTS)" <<<"$out"; then
    echo "topic ${name}: FAILED"; echo "$out"; exit 1
  fi
}

size_retention() {
  rpk topic alter-config "$1" --set retention.ms=-1 --set "retention.bytes=${RETENTION_BYTES}" \
    --set cleanup.policy=delete >/dev/null
  echo "topic $1: ready (retention.bytes=${RETENTION_BYTES})"
}

# Event-time streams are single-partition so pickup_ts stays ordered into the windows (ADR-014).
for t in trips.raw trips.enriched; do create "$t" -p 1 -r 1; size_retention "$t"; done
for t in trips.rejected agg.borough_minute trace.checkpoints; do create "$t" -p 3 -r 1; size_retention "$t"; done
create sim.clock -p 1 -r 1 -c cleanup.policy=compact
echo "topic sim.clock: ready (compacted)"
rpk topic list
