"""List and query records from a Notion database.

This module provides functionality to search for Notion databases by name
and extract the titles of all records (rows) within them.
"""
import os
import sys
import argparse
import httpx
from typing import Any, Dict, List, Optional, cast

from notion_client import Client as NotionClient


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


def extract_data_source_title(obj: dict) -> str:
    """
    Robustly extract the display name for a data_source returned by search.
    """
    # In many responses, data_source objects still use a title-like array
    title_parts = obj.get("title")
    if isinstance(title_parts, list):
        return "".join([p.get("plain_text", "") for p in title_parts]).strip()

    # Some objects may use "name"
    name = obj.get("name")
    if isinstance(name, str):
        return name.strip()

    return ""


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


def main() -> None:
    """Main entry point for the module.

    Parses command-line arguments, initializes the Notion API client,
    prompts for a database name if needed, and collects and prints the row titles.
    """
    parser = argparse.ArgumentParser(description="List Notion database record names (row titles).")
    parser.add_argument("--db-name", help="Notion database name (exact match). If omitted, prompts.")
    parser.add_argument("--print", action="store_true", help="Print record names to stdout.")
    args = parser.parse_args()

    notion_token = require_env("NOTION_TOKEN")
    notion = NotionClient(auth=notion_token, notion_version="2025-09-03")

    db_name = prompt_db_name_if_missing(args.db_name)

    data_source_id = find_data_source_id_by_name(notion, db_name)
    titles = collect_row_titles_from_data_source(notion, data_source_id)

    # Always print count
    print(f"Database: {db_name}")
    print(f"Total records: {len(titles)}")

    if args.print:
        for t in titles:
            print(t)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, LookupError, KeyboardInterrupt) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
