"""Fill empty generated secrets in .env (make secrets). Never overwrites a value that is already set.

Keys that exist in .env.example but not yet in .env are appended first, with the template's default.
User-provided secrets (LLM_INTERNAL_API_KEY, STAND_WIFI_PASSWORD) are left alone.
"""

from __future__ import annotations

import re
import secrets
import sys
from pathlib import Path

GENERATED = re.compile(r"^(CLICKHOUSE_[A-Z]+_PASSWORD|GRAFANA_ADMIN_PASSWORD|ADMIN_TOKEN)=\s*$")
KEY = re.compile(r"^([A-Z][A-Z0-9_]*)=")


def add_missing(text: str, template: str) -> tuple[str, list[str]]:
    present = {m.group(1) for line in text.splitlines() if (m := KEY.match(line))}
    new = [line for line in template.splitlines() if (m := KEY.match(line)) and m.group(1) not in present]
    if not new:
        return text, []
    return text.rstrip("\n") + "\n" + "\n".join(new) + "\n", [line.split("=", 1)[0] for line in new]


def fill(text: str) -> tuple[str, list[str]]:
    lines, filled = [], []
    for line in text.splitlines():
        match = GENERATED.match(line)
        if match:
            line = f"{match.group(1)}={secrets.token_hex(16)}"
            filled.append(match.group(1))
        lines.append(line)
    return "\n".join(lines) + "\n", filled


def main(path: str = ".env", template_path: str = ".env.example") -> int:
    env_file = Path(path)
    text, added = add_missing(env_file.read_text(), Path(template_path).read_text())
    text, filled = fill(text)
    env_file.write_text(text)
    if added:
        print("added from template:", ", ".join(added))
    print("generated:", ", ".join(filled) if filled else "nothing (all set)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
