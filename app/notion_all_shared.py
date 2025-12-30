import os
import sys
import argparse
from typing import Optional, List, Dict, Any, Tuple
from notion_client import Client as NotionClient


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def title_from_rich_text_array(arr) -> str:
    if isinstance(arr, list):
        return "".join([p.get("plain_text", "") for p in arr]).strip()
    return ""


def object_display_name(obj: Dict[str, Any]) -> str:
    # pages have "properties" etc; search results usually include "title" for pages and data_sources
    if obj.get("object") == "page":
        # Many page search results include a "title" field; if not, fall back
        t = title_from_rich_text_array(obj.get("title"))
        return t or obj.get("id", "")
    if obj.get("object") == "data_source":
        t = title_from_rich_text_array(obj.get("title"))
        if t:
            return t
        name = obj.get("name")
        return name.strip() if isinstance(name, str) else obj.get("id", "")
    return obj.get("id", "")


def dump_shared(notion: NotionClient, print_names: bool) -> Tuple[int, int]:
    cursor: Optional[str] = None
    pages = 0
    data_sources = 0

    while True:
        resp = notion.search(
            start_cursor=cursor,
            page_size=100,
            # IMPORTANT: no query => everything shared with the integration
        )

        for item in resp.get("results", []):
            obj_type = item.get("object")
            name = object_display_name(item)

            if obj_type == "page":
                pages += 1
                if print_names:
                    print(f"PAGE       | {name}")
            elif obj_type == "data_source":
                data_sources += 1
                if print_names:
                    print(f"DATA_SOURCE| {name}")
            else:
                if print_names:
                    print(f"{str(obj_type).upper():<10} | {name}")

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    return pages, data_sources


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true", help="Print all shared page/data_source names.")
    args = parser.parse_args()

    notion = NotionClient(auth=require_env("NOTION_TOKEN"), notion_version="2025-09-03")

    pages, data_sources = dump_shared(notion, args.print)
    print(f"\nTotal shared pages: {pages}")
    print(f"Total shared data_sources: {data_sources}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)