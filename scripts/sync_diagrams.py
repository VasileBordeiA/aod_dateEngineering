"""Copy Mermaid diagrams from docs/DIAGRAMS.md into the specs that embed them.

DIAGRAMS.md is the source of truth. Each target file gets one block between
<!-- diagrams:begin --> and <!-- diagrams:end -->, which this script rewrites.
Run with --check to fail (exit 1) when any embedded copy is out of date.

Usage: python scripts/sync_diagrams.py [--check]     (make diagrams-sync / make docs-check)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIAGRAMS = ROOT / "docs" / "DIAGRAMS.md"

# target file -> (diagram section numbers, heading to insert before on first run)
TARGETS: dict[str, tuple[list[int], str]] = {
    "docs/specs/01-data-and-replayer.md": ([11], "## Interfaces"),
    "docs/specs/02-topics-and-event-contracts.md": ([2], "## Topics"),
    "docs/specs/03-stream-processor.md": ([12], "## Interfaces"),
    "docs/specs/04-clickhouse-schema.md": ([6], "## Interfaces"),
    "docs/specs/06-ai-data-agent.md": ([3, 8], "## Interfaces"),
    "docs/specs/07-trace-your-ride.md": ([4, 5], "## Interfaces"),
    "docs/specs/09-infrastructure-and-networking.md": ([7], "## Interfaces"),
    "docs/IMPLEMENTATION_PLAN.md": ([10], "## Milestones"),
}
BEGIN, END = "<!-- diagrams:begin -->", "<!-- diagrams:end -->"


def diagram(source: str, number: int) -> tuple[str, str]:
    section = re.search(rf"\n## {number}\. (.*?)\n(.*?)(?=\n## \d+\. |\Z)", source, re.S)
    if not section:
        raise SystemExit(f"DIAGRAMS.md has no section {number}")
    block = re.search(r"```mermaid\n.*?```", section.group(2), re.S)
    if not block:
        raise SystemExit(f"DIAGRAMS.md section {number} has no mermaid block")
    return section.group(1), block.group(0)


def render(source: str, numbers: list[int], relative_link: str) -> str:
    lines = [BEGIN, "## Diagrams", f"*Copied from [DIAGRAMS.md]({relative_link}). Edit there, then run `make diagrams-sync`.*", ""]
    for n in numbers:
        title, block = diagram(source, n)
        lines += [f"**§{n} {title}**", "", block, ""]
    lines += [END]
    return "\n".join(lines)


def main() -> int:
    check = "--check" in sys.argv
    source = DIAGRAMS.read_text()
    stale = []
    for rel, (numbers, anchor) in TARGETS.items():
        path = ROOT / rel
        text = path.read_text()
        link = "../DIAGRAMS.md" if "/specs/" in rel else "DIAGRAMS.md"
        block = render(source, numbers, link)
        if BEGIN in text:
            new = re.sub(re.escape(BEGIN) + r".*?" + re.escape(END), lambda _: block, text, count=1, flags=re.S)
        elif anchor in text:
            new = text.replace(anchor, block + "\n\n" + anchor, 1)
        else:
            raise SystemExit(f"{rel}: neither a diagrams block nor the anchor '{anchor}' was found")
        if new != text:
            stale.append(rel)
            if not check:
                path.write_text(new)
    if check and stale:
        print("Out of date:", *stale, sep="\n  ")
        return 1
    print("updated:" if stale and not check else "up to date", *stale, sep="\n  ")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
