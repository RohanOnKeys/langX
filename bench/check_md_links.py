"""Check that relative markdown links in langX resolve to existing files.

Covers README.md and every markdown file under docs/, experiments/,
hpx_impl/, spikes/ and bench/. Links are resolved relative to the linking
file. http(s), mailto and pure #anchors are skipped.

Usage: python bench/check_md_links.py
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
LINK = re.compile(r"\]\(([^)\s]+)\)")


def markdown_files():
    yield ROOT / "README.md"
    for folder in ("docs", "experiments", "hpx_impl", "spikes", "bench"):
        yield from sorted((ROOT / folder).rglob("*.md"))


def main():
    broken = []
    for md in markdown_files():
        for target in LINK.findall(md.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path = (md.parent / target.split("#", 1)[0]).resolve()
            if not path.exists():
                broken.append(f"{md.relative_to(ROOT)}: {target}")
    for line in broken:
        print(f"broken link: {line}")
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
