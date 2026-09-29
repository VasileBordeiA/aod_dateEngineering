#!/usr/bin/env bash
# Ollama's default context silently truncates our tool definitions + schema (spec 06), so each candidate model gets a
# derived "taxi-<base>" model with num_ctx 16384 and temperature 0. Usage: scripts/ollama_model.sh qwen2.5-coder:14b
set -euo pipefail
base="${1:?usage: $0 <base-model>}"
name="taxi-$(printf '%s' "$base" | tr ':/' '--')"
modelfile="$(mktemp)"
trap 'rm -f "$modelfile"' EXIT
printf 'FROM %s\nPARAMETER num_ctx 16384\nPARAMETER temperature 0\n' "$base" > "$modelfile"
ollama create "$name" -f "$modelfile" >/dev/null
echo "$name"
