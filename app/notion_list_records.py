"""List and query records from a Notion database.

This module provides functionality to search for Notion databases by name
and extract the titles of all records (rows) within them.
"""
import os
import re
import sys
import argparse
import httpx
from typing import Any, Dict, List, Optional, cast
from notion_client import Client as NotionClient

NOTION_VERSION = "2025-09-03"

def normalize_uuid(raw: str) -> str:
    """
    Accepts UUID with or without dashes, returns dashed UUID.
    """
    s = raw.strip().lower()
    s = re.sub(r"[^0-9a-f]", "", s)  # remove non-hex (incl dashes)
    if len(s) != 32:
        raise ValueError(f"Not a valid Notion UUID (need 32 hex chars): {raw}")
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"

def notion_url_from_uuid(raw: str) -> str:
    s = re.sub(r"[^0-9a-f]", "", raw.strip().lower())
    if len(s) != 32:
        raise ValueError(f"Not a valid Notion UUID (need 32 hex chars): {raw}")
    return f"https://www.notion.so/{s}"

def _rt_to_text(rt) -> str:
    if isinstance(rt, list):
        return "".join(x.get("plain_text", "") for x in rt).strip()
    return ""

def extract_page_title(page: dict) -> str:
    props = page.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            return _rt_to_text(prop.get("title"))
    # sometimes page search results include a top-level title
    t = _rt_to_text(page.get("title"))
    return t

def retrieve_page_by_id(token: str, page_id: str) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
    }
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

def retrieve_data_source_by_id(token: str, data_source_id: str) -> dict:
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
    }
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

def extract_data_source_title(ds: dict) -> str:
    t = _rt_to_text(ds.get("title"))
    if t:
        return t
    name = ds.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return ""

def require_env(name: str) -> str:
    """Require an environment variable to be set.

    Args:
        name: The name of the environment variable to retrieve.

    Returns:
        The value of the environment variable.

    Raises:
        RuntimeError: If the environment variable is not set.
    """
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val

def list_all_data_sources(notion: NotionClient) -> None:
    cursor = None
    count = 0

    while True:
        resp = cast(Dict[str, Any], notion.search(
            start_cursor=cursor,
            page_size=100,
            filter={"property": "object", "value": "data_source"},
        ))

        for item in resp.get("results", []):
            title_parts = item.get("title") or []
            title = "".join(p.get("plain_text", "") for p in title_parts).strip()
            print(title)
            count += 1

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    print(f"\nTotal data sources visible to integration: {count}")

def prompt_db_name_if_missing(db_name: Optional[str]) -> str:
    """Prompt the user for a Notion database name if not provided.

    Args:
        db_name: The name of the Notion database to search for.

    Returns:
        The name of the Notion database to search for.
    """
    if db_name and db_name.strip():
        return db_name.strip()
    entered = input("Enter Notion Database name: ").strip()
    if not entered:
        raise ValueError("Database name cannot be empty.")
    return entered


def find_data_source_id_by_name(notion: NotionClient, db_name: str) -> str:
    """
    Find a Notion *data source* whose title matches db_name exactly.
    Notion Search filter supports value='data_source' (NOT 'database').
    """
    cursor = None
    matches = []

    while True:
        resp = cast(Dict[str, Any], notion.search(
            query=db_name,
            start_cursor=cursor,
            page_size=50,
            filter={"property": "object", "value": "data_source"},
            sort={"direction": "descending", "timestamp": "last_edited_time"},
        ))

        for item in resp.get("results", []):
            if item.get("object") != "data_source":
                continue
            title = extract_data_source_title(item)
            if title == db_name:
                matches.append(item)

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    if not matches:
        raise LookupError(
            f"No Notion data_source found with exact name '{db_name}'. "
            f"Confirm the data source is shared with your integration and you're using the correct NOTION_TOKEN."
        )

    return matches[0]["id"]


def extract_title_from_page(page: dict) -> str:
    """
    Finds the 'title' property on the page (database row) and returns its plain text.
    This avoids assuming the title property is literally named "Name".
    """
    props = page.get("properties", {})
    for prop in props.values():
        if prop.get("type") == "title":
            title_items = prop.get("title", [])
            return "".join([t.get("plain_text", "") for t in title_items]).strip()
    return ""


def query_data_source_pages(notion: NotionClient, data_source_id: str) -> List[Dict[str, Any]]:
    """
    Query all pages (rows) in a Notion data source with pagination.
    Tries SDK endpoint if present, otherwise falls back to raw HTTP.
    """
    results: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    # Try SDK first (newer notion-client versions may provide this)
    data_sources_ep = getattr(notion, "data_sources", None)

    while True:
        if data_sources_ep is not None and hasattr(data_sources_ep, "query"):
            resp = cast(Dict[str, Any], data_sources_ep.query(  # type: ignore[attr-defined]
                data_source_id=data_source_id,
                start_cursor=cursor,
                page_size=100,
            ))
        else:
            # Fallback: direct REST call to Query a data source
            token = require_env("NOTION_TOKEN")
            notion_version = "2025-09-03"
            url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"

            headers = {
                "Authorization": f"Bearer {token}",
                "Notion-Version": notion_version,
                "Content-Type": "application/json",
            }
            payload: Dict[str, Any] = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor

            with httpx.Client(timeout=30.0) as client:
                r = client.post(url, headers=headers, json=payload)
                r.raise_for_status()
                resp = cast(Dict[str, Any], r.json())

        results.extend(cast(List[Dict[str, Any]], resp.get("results", [])))

        cursor = cast(Optional[str], resp.get("next_cursor"))
        if not resp.get("has_more"):
            break

    return results


def collect_row_titles_from_data_source(notion: NotionClient, data_source_id: str) -> List[str]:
    pages = query_data_source_pages(notion, data_source_id)
    titles: List[str] = []
    for page in pages:
        titles.append(extract_title_from_page(page))
    return titles

def _page_title_from_search_obj(obj: dict) -> str:
    # Search results often include a top-level "title" field for pages
    t = _rt_to_text(obj.get("title"))
    if t:
        return t
    # Fallback: try to derive title from properties (if present)
    props = obj.get("properties", {}) or {}
    for prop in props.values():
        if prop.get("type") == "title":
            return _rt_to_text(prop.get("title"))
    return ""

def _data_source_title_from_search_obj(obj: dict) -> str:
    t = _rt_to_text(obj.get("title"))
    if t:
        return t
    name = obj.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return ""


def list_visible_objects(notion: NotionClient, object_type: str) -> None:
    """
    object_type: 'page' or 'data_source'
    """
    cursor = None
    count = 0
    while True:
        resp = cast(Dict[str, Any], notion.search(
            start_cursor=cursor,
            page_size=100,
            filter={"property": "object", "value": object_type},
        ))

        for item in resp.get("results", []):
            oid = item.get("id", "")
            if object_type == "page":
                title = _page_title_from_search_obj(item) or "(no title returned)"
                print(f"{oid} | PAGE | {title}")
            else:
                title = _data_source_title_from_search_obj(item) or "(no title returned)"
                print(f"{oid} | DATA_SOURCE | {title}")
            count += 1

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    print(f"\nTotal {object_type}s visible to integration: {count}")


def main() -> None:
    """Main entry point for the module.

    Parses command-line arguments, initializes the Notion API client,
    prompts for a database name if needed, and collects and prints the row titles.
    """
    parser = argparse.ArgumentParser(description="List Notion database record names (row titles).")
    parser.add_argument("--db-name", help="Notion database name (exact match). If omitted, prompts.")
    parser.add_argument("--print", action="store_true", help="Print record names to stdout.")
    parser.add_argument( "--list-data-sources", action="store_true", help="List all Notion data sources visible to the integration and exit.")
    parser.add_argument("--list-pages", action="store_true", help="List all visible pages and exit.")
    parser.add_argument("--list-all", action="store_true", help="List all visible pages + data sources and exit.")
    parser.add_argument("--resolve-id", help="Resolve a Notion UUID (page or data_source) to its title/name.")
    parser.add_argument("--type", choices=["page", "data_source"], help="Type for --resolve-id.")
    parser.add_argument("--expect", help="Expected title/name to compare against (exact match).")
    args = parser.parse_args()

    notion_token = require_env("NOTION_TOKEN")
    notion = NotionClient(auth=notion_token, notion_version="2025-09-03")

    if args.list_data_sources:
        list_all_data_sources(notion)
        return

    if args.list_all:
        list_visible_objects(notion, "data_source")
        print("")  # spacer
        list_visible_objects(notion, "page")
        return
    
    if args.list_data_sources:
        list_visible_objects(notion, "data_source")
        return
    
    if args.list_pages:
        list_visible_objects(notion, "page")
        return

    if args.resolve_id:
        token = require_env("NOTION_TOKEN")
        obj_id = normalize_uuid(args.resolve_id)
        obj_type = args.type
        if not obj_type:
            raise ValueError("--type is required when using --resolve-id (page|data_source)")

        if obj_type == "page":
            obj = retrieve_page_by_id(token, obj_id)
            title = extract_page_title(obj)
        else:
            obj = retrieve_data_source_by_id(token, obj_id)
            title = extract_data_source_title(obj)

        print(f"Type: {obj_type}")
        print(f"ID:   {obj_id}")
        print(f"URL:  {notion_url_from_uuid(obj_id)}")
        print(f"Name: {title or '(no title returned)'}")

        if args.expect is not None:
            ok = (title == args.expect)
            print(f"Expected: {args.expect}")
            print(f"Match:    {ok}")
            if not ok:
                raise SystemExit(2)
        return

    db_name = prompt_db_name_if_missing(args.db_name)

    data_source_id = find_data_source_id_by_name(notion, db_name)
    titles = collect_row_titles_from_data_source(notion, data_source_id)

    print(f"Database: {db_name}")
    print(f"Total records: {len(titles)}")

    if args.print:
        for t in titles:
            print(t)
        # Always print count
    

if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, LookupError, KeyboardInterrupt) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
