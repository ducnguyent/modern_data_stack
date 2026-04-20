"""
Hacker News Ingestion Module
=============================
Fetches top stories from the HN Firebase API, saves raw JSON,
and logs each run to a local SQLite database.
"""

import json
import logging
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
TOP_STORIES_URL = f"{HN_BASE_URL}/topstories.json"
ITEM_URL = f"{HN_BASE_URL}/item/{{item_id}}.json"

MAX_STORIES = 500
MAX_WORKERS = 20
REQUEST_TIMEOUT = 10  # seconds

# Project paths (relative to repo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LOGS_DIR = PROJECT_ROOT / "data" / "logs"
RUNS_DB = LOGS_DIR / "runs.db"


# ---------------------------------------------------------------------------
# Parser — pure function, unit‑testable
# ---------------------------------------------------------------------------
STORY_FIELDS = {"id", "title", "score", "by", "descendants", "time", "url"}


def parse_story(raw: dict) -> dict:
    """Extract and validate story fields from a raw HN API response.

    Args:
        raw: Raw JSON dict from the HN item endpoint.

    Returns:
        Dict with exactly 7 keys: id, title, score, by, descendants, time, url.
        Missing fields default to ``None`` (strings) or ``0`` (integers).
    """
    return {
        "id": int(raw["id"]) if "id" in raw else None,
        "title": str(raw.get("title", "")) or None,
        "score": int(raw.get("score", 0)),
        "by": str(raw.get("by", "")) or None,
        "descendants": int(raw.get("descendants", 0)),
        "time": int(raw.get("time", 0)),
        "url": str(raw.get("url", "")) or None,
    }


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def _fetch_top_story_ids(limit: int = MAX_STORIES) -> list[int]:
    """Return the first *limit* story IDs from the HN top‑stories endpoint."""
    resp = requests.get(TOP_STORIES_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    ids = resp.json()
    return ids[:limit]


def _fetch_item(item_id: int) -> dict | None:
    """Fetch a single HN item by ID. Returns ``None`` on failure."""
    try:
        resp = requests.get(
            ITEM_URL.format(item_id=item_id), timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Failed to fetch item %s: %s", item_id, exc)
        return None


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
    """Fetch top stories, save JSON file, and log the run.

    Args:
        date_str: Override date string for the output filename (YYYY‑MM‑DD).
                  Defaults to today (UTC).

    Returns:
        Path to the saved JSON file.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / f"hn_stories_{date_str}.json"

    logger.info("Fetching top %d story IDs …", MAX_STORIES)
    story_ids = _fetch_top_story_ids(MAX_STORIES)

    stories: list[dict] = []
    errors = 0

    logger.info("Fetching %d items with %d workers …", len(story_ids), MAX_WORKERS)
    start = time.monotonic()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {executor.submit(_fetch_item, sid): sid for sid in story_ids}
        for future in as_completed(future_to_id):
            result = future.result()
            if result is not None:
                stories.append(parse_story(result))
            else:
                errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Fetched %d stories (%d errors) in %.1f s", len(stories), errors, elapsed
    )

    # Save raw JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stories, f, indent=2, ensure_ascii=False)
    logger.info("Saved → %s", output_path)

    # Log to SQLite
    _log_run(rows_fetched=len(stories), errors=errors, file_path=str(output_path))

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
