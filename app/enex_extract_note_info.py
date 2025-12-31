#!/usr/bin/env python3
import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List


def parse_enex_titles(enex_path: Path) -> List[str]:
    """
    Parse an Evernote .enex export and return a list of note titles.
    Uses streaming XML parsing to handle large exports.
    """
    titles: List[str] = []

    context = ET.iterparse(str(enex_path), events=("end",))
    for _, elem in context:
        if elem.tag == "note":
            title_elem = elem.find("title")
            title = (title_elem.text or "").strip() if title_elem is not None else ""
            titles.append(title)
            elem.clear()  # free memory

    return titles


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract note count and note titles from an Evernote .enex export."
    )
    parser.add_argument("enex_file", help="Path to the .enex file")
    parser.add_argument("--print", action="store_true", help="Print note titles to stdout")
    parser.add_argument(
        "--fail-on-empty-title",
        action="store_true",
        help="Exit non-zero if any note has an empty title",
    )
    args = parser.parse_args()

    enex_path = Path(args.enex_file).expanduser().resolve()
    if not enex_path.exists():
        print(f"ERROR: File not found: {enex_path}", file=sys.stderr)
        sys.exit(2)

    titles = parse_enex_titles(enex_path)

    print(f"ENEX file: {enex_path}")
    print(f"Total exported notes: {len(titles)}")

    empty_titles = [i for i, t in enumerate(titles, start=1) if not t]
    print(f"Empty titles: {len(empty_titles)}")

    if args.print:
        for t in titles:
            print(t)

    if args.fail_on_empty_title and empty_titles:
        print(f"ERROR: Found {len(empty_titles)} notes with empty titles.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
