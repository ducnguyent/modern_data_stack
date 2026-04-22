"""
DEV.to Ingestion Module
=======================
Fetches articles from the DEV.to API, saves raw JSON,
and logs each run to a local SQLite database.
"""

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEVTO_API_URL = "https://dev.to/api/articles?per_page=1000&top=1"
REQUEST_TIMEOUT = 10  # seconds

# Project paths (relative to repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOGS_DIR = PROJECT_ROOT / "data" / "logs"
RUNS_DB = LOGS_DIR / "runs.db"


# ---------------------------------------------------------------------------
# Parser — pure function, unit‑testable
# ---------------------------------------------------------------------------
def parse_article(raw: dict) -> dict:
    """Extract and validate fields from a raw DEV.to API response.

    Returns:
        Dict with fields: id, title, positive_reactions_count,
        public_reactions_count, comments_count, author, published_at,
        tag_list, url, reading_time_minutes.
    """
    user = raw.get("user") or {}
    
    # Handle tag_list which can be a list or a string, store as comma-separated string
    raw_tag_list = raw.get("tag_list", [])
    if isinstance(raw_tag_list, list):
        tag_list_str = ",".join(str(tg) for tg in raw_tag_list)
    elif isinstance(raw_tag_list, str):
        tag_list_str = raw_tag_list
    else:
        tag_list_str = ""

    return {
        "id": int(raw["id"]) if "id" in raw else None,
        "title": str(raw.get("title", "")) or None,
        "positive_reactions_count": int(raw.get("positive_reactions_count", 0)),
        "public_reactions_count": int(raw.get("public_reactions_count", 0)),
        "comments_count": int(raw.get("comments_count", 0)),
        "author": str(user.get("username", "")) or None,
        "published_at": str(raw.get("published_at", "")) or None,
        "tag_list": tag_list_str or None,
        "url": str(raw.get("url", "")) or None,
        "reading_time_minutes": int(raw.get("reading_time_minutes", 0)),
    }


# ---------------------------------------------------------------------------
# Run logger (SQLite)
# ---------------------------------------------------------------------------
_CREATE_RUNS_TABLE = """
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   TEXT    NOT NULL,
    rows_fetched INTEGER NOT NULL,
    errors      INTEGER NOT NULL DEFAULT 0,
    file_path   TEXT
);
"""


def _init_runs_db() -> None:
    """Ensure the runs table exists in the SQLite database."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(RUNS_DB) as conn:
        conn.execute(_CREATE_RUNS_TABLE)


def _log_run(rows_fetched: int, errors: int, file_path: str | None) -> None:
    """Insert a run record into the SQLite log."""
    _init_runs_db()
    with sqlite3.connect(RUNS_DB) as conn:
        conn.execute(
            "INSERT INTO runs (timestamp, rows_fetched, errors, file_path) VALUES (?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), rows_fetched, errors, file_path),
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def fetch_and_save(date_str: str | None = None) -> Path:
    """Fetch articles, save JSON file, and log the run.

    Args:
        date_str: Override date string for the output filename (YYYY‑MM‑DD).
                  Defaults to today (UTC).

    Returns:
        Path to the saved JSON file.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / f"devto_{date_str}.json"

    logger.info("Fetching DEV.to articles …")
    start = time.monotonic()
    
    articles: list[dict] = []
    errors = 0
    try:
        resp = requests.get(DEVTO_API_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        raw_articles = resp.json()
        for raw in raw_articles:
            try:
                articles.append(parse_article(raw))
            except Exception as exc:
                logger.warning("Failed to parse article: %s", exc)
                errors += 1
    except requests.RequestException as exc:
        logger.error("Failed to fetch DEV.to articles: %s", exc)
        errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Fetched %d articles (%d parsing errors) in %.1f s", len(articles), errors, elapsed
    )

    # Save raw JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=2, ensure_ascii=False)
    logger.info("Saved → %s", output_path)

    # Log to SQLite
    _log_run(rows_fetched=len(articles), errors=errors, file_path=str(output_path))

    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    fetch_and_save()
