import os
import sys
import argparse
from typing import List, Optional

from notion_client import Client as NotionClient


def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val


def prompt_db_name_if_missing(db_name: Optional[str]) -> str:
    if db_name and db_name.strip():
        return db_name.strip()
    entered = input("Enter Notion Database name: ").strip()
    if not entered:
        raise ValueError("Database name cannot be empty.")
    return entered


def find_database_id_by_name(notion: NotionClient, db_name: str) -> str:
    """
    Uses Notion's search endpoint to find a database whose title matches db_name exactly.
    """
    cursor = None
    matches = []

    while True:
        resp = notion.search(
            query=db_name,
            start_cursor=cursor,
            page_size=50,
            filter={"property": "object", "value": "database"},
            sort={"direction": "ascending", "timestamp": "last_edited_time"},
        )

        for item in resp.get("results", []):
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

    if len(matches) > 1:
        # If you have duplicates, choose the most recently edited (last in our ascending sort would be newest,
        # but we sorted ascending; easiest: pick the one with max last_edited_time).
        matches.sort(key=lambda d: d.get("last_edited_time", ""))
    return matches[-1]["id"]


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
    titles: List[str] = []
    cursor = None

    while True:
        resp = notion.databases.query(
            database_id=database_id,
            start_cursor=cursor,
            page_size=100,
        )

        for page in resp.get("results", []):
            title = extract_title_from_page(page)
            titles.append(title)

        cursor = resp.get("next_cursor")
        if not resp.get("has_more"):
            break

    return titles


def main() -> None:
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
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)