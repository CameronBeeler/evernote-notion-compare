import os
<<<<<<< HEAD
import sys
import argparse
from typing import Optional, List, Dict, Any, Tuple, cast
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
=======
import httpx
from typing import Dict, Any, cast
from notion_client import Client as NotionClient

NOTION_VERSION = "2025-09-03"


def require_env(name: str) -> str:
    """Require an environment variable to be set."""
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val

def _rt_to_text(rt) -> str:
    if isinstance(rt, list):
        return "".join(x.get("plain_text", "") for x in rt).strip()
    return ""

def _ds_title_from_obj(obj: dict) -> str:
    # Common shapes we see
    t = _rt_to_text(obj.get("title"))
    if t:
        return t
    name = obj.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return ""

def retrieve_data_source_name_raw(token: str, data_source_id: str) -> str:
    """
    Fallback for when search results don't include a usable title/name.
    """
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
    }
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, headers=headers)
        r.raise_for_status()
        obj = r.json()
    return _ds_title_from_obj(obj)

def list_all_data_sources(notion: NotionClient) -> None:
    token = require_env("NOTION_TOKEN")
    cursor = None
    count = 0
    unnamed = 0
>>>>>>> 90bae8de14a548e94841fcafc9b6bf4cb65cf98d

    while True:
        resp = cast(Dict[str, Any], notion.search(
            start_cursor=cursor,
            page_size=100,
<<<<<<< HEAD
            # IMPORTANT: no query => everything shared with the integration
        ))

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
=======
            filter={"property": "object", "value": "data_source"},
        ))

        for item in resp.get("results", []):
            ds_id = item.get("id", "")
            title = _ds_title_from_obj(item)

            # If search didn't include a good name, retrieve it directly
            if not title and ds_id:
                try:
                    title = retrieve_data_source_name_raw(token, ds_id)
                except Exception:
                    title = ""

            if not title:
                unnamed += 1
                title = "(no title returned)"

            print(f"{ds_id} | {title}")
            count += 1
>>>>>>> 90bae8de14a548e94841fcafc9b6bf4cb65cf98d

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

<<<<<<< HEAD
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
=======
    print(f"\nTotal data sources visible to integration: {count}")
    print(f"Data sources with no title returned: {unnamed}")
>>>>>>> 90bae8de14a548e94841fcafc9b6bf4cb65cf98d
