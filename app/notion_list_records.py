"""List and query records from a Notion database.

This module provides functionality to search for Notion databases by name
and extract the titles of all records (rows) within them.
"""
import os
import sys
import argparse
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

def find_database_id_by_name(notion: NotionClient, db_name: str) -> str:
    """
    Uses Notion search to find a database whose title matches db_name exactly.
    (Search filter no longer supports value='database', so we filter client-side.)
    """
    cursor = None
    matches = []

    while True:
        # NOTE: No filter here; Notion validates filter.value as page|data_source
        resp = cast(Dict[str, Any], notion.search(
            query=db_name,
            start_cursor=cursor,
            page_size=50,
            sort={"direction": "descending", "timestamp": "last_edited_time"},
        ))

        for item in resp.get("results", []):
            if item.get("object") != "database":
                continue

            title_parts = item.get("title", [])
            title = "".join([p.get("plain_text", "") for p in title_parts]).strip()
            if title == db_name:
                matches.append(item)

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    if not matches:
        raise LookupError(
            f"No Notion database found with exact name '{db_name}'. "
            f"Confirm the database is shared with your integration."
        )

    # We sorted by last_edited_time desc, so first is most recent
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


def collect_database_row_titles(notion: NotionClient, database_id: str) -> List[str]:
    """Collect all row titles from a Notion database.

    Queries the database in pages and extracts the title property from each row,
    handling pagination automatically.

    Args:
        notion: The Notion API client instance.
        database_id: The ID of the Notion database to query.

    Returns:
        A list of title strings, one for each row in the database.
    """
    titles: List[str] = []
    cursor = None

    while True:
        resp = cast(Dict[str, Any], notion.databases.query(  # type: ignore[attr-defined]
            database_id=database_id,
            start_cursor=cursor,
            page_size=100,
        ))

        for page in resp.get("results", []):
            title = extract_title_from_page(page)
            titles.append(title)

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

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
    notion = NotionClient(auth=notion_token)

    db_name = prompt_db_name_if_missing(args.db_name)

    db_id = find_database_id_by_name(notion, db_name)
    titles = collect_database_row_titles(notion, db_id)

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
