import os
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

    while True:
        resp = cast(Dict[str, Any], notion.search(
            start_cursor=cursor,
            page_size=100,
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

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    print(f"\nTotal data sources visible to integration: {count}")
    print(f"Data sources with no title returned: {unnamed}")