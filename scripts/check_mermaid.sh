#!/bin/bash
# Render every ```mermaid block in the repo's Markdown; exit 1 if any fails (make docs-check).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$(mktemp -d)"
python3 - "$ROOT" "$OUT" <<'PY'
import os, re, sys, glob
root, out = sys.argv[1], sys.argv[2]
for f in glob.glob(f"{root}/**/*.md", recursive=True):
    if "/node_modules/" in f or "/.superpowers/" in f: continue
    for i, block in enumerate(re.findall(r"```mermaid\n(.*?)```", open(f).read(), re.S)):
        name = os.path.relpath(f, root).replace("/", "_")[:-3]
        open(f"{out}/{name}_{i + 1:02d}.mmd", "w").write(block)
PY
fail=0
for f in "$OUT"/*.mmd; do
  if npx -y -p @mermaid-js/mermaid-cli mmdc -q -i "$f" -o "${f%.mmd}.svg" >/dev/null 2>&1; then echo "ok   $(basename "$f")"
  else echo "FAIL $(basename "$f")"; fail=1; fi
done
exit $fail
