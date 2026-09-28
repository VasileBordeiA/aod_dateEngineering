"""Fill empty generated secrets in .env (make secrets). Never overwrites a value that is already set.

User-provided secrets (LLM_INTERNAL_API_KEY, STAND_WIFI_PASSWORD) are left alone.
"""

from __future__ import annotations

import re
import secrets
import sys
from pathlib import Path

GENERATED = re.compile(r"^(CLICKHOUSE_[A-Z]+_PASSWORD|GRAFANA_ADMIN_PASSWORD|ADMIN_TOKEN)=\s*$")


def fill(text: str) -> tuple[str, list[str]]:
    lines, filled = [], []
    for line in text.splitlines():
        match = GENERATED.match(line)
        if match:
            line = f"{match.group(1)}={secrets.token_hex(16)}"
            filled.append(match.group(1))
        lines.append(line)
    return "\n".join(lines) + "\n", filled


def main(path: str = ".env") -> int:
    env_file = Path(path)
    text, filled = fill(env_file.read_text())
    env_file.write_text(text)
    print("generated:", ", ".join(filled) if filled else "nothing (all set)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:]))
