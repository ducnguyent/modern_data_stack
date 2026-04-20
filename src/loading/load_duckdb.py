"""
DuckDB Loading Module
=====================
Reads raw JSON from the landing zone and loads it into DuckDB
as an idempotent daily append (delete‑then‑insert by date).
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
DB_PATH = WAREHOUSE_DIR / "hn.duckdb"

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.raw_stories (
    id              BIGINT,
    title           VARCHAR,
    score           INTEGER,
    "by"            VARCHAR,
    descendants     INTEGER,
    "time"          BIGINT,
    url             VARCHAR,
    ingested_date   DATE
);
"""

_DELETE_DAY = "DELETE FROM raw.raw_stories WHERE ingested_date = ?;"

_INSERT = """
INSERT INTO raw.raw_stories (id, title, score, "by", descendants, "time", url, ingested_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _latest_raw_file() -> Path:
    """Return the most recent JSON file in data/raw/."""
    files = sorted(RAW_DIR.glob("hn_stories_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError(f"No raw JSON files found in {RAW_DIR}")
    return files[0]


def _extract_date_from_filename(path: Path) -> str:
    """Extract YYYY-MM-DD from filename like hn_stories_2024-01-15.json."""
    stem = path.stem  # hn_stories_2024-01-15
    return stem.replace("hn_stories_", "")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def load_to_duckdb(json_path: Path | None = None) -> int:
    """Load a raw JSON file into the DuckDB warehouse.

    Args:
        json_path: Explicit path to a JSON file. Defaults to the latest
                   file in ``data/raw/``.

    Returns:
        Number of rows loaded.
    """
    if json_path is None:
        json_path = _latest_raw_file()

    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    ingested_date = _extract_date_from_filename(json_path)
    logger.info("Loading %s (date=%s) → %s", json_path.name, ingested_date, DB_PATH)

    with open(json_path, "r", encoding="utf-8") as f:
        stories = json.load(f)

    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.execute(_CREATE_SCHEMA)
        conn.execute(_CREATE_TABLE)

        # Idempotent: remove any existing rows for this date, then re‑insert
        conn.execute(_DELETE_DAY, [ingested_date])

        for story in stories:
            conn.execute(
                _INSERT,
                [
                    story.get("id"),
                    story.get("title"),
                    story.get("score"),
                    story.get("by"),
                    story.get("descendants"),
                    story.get("time"),
                    story.get("url"),
                    ingested_date,
                ],
            )

        row_count = conn.execute(
            "SELECT COUNT(*) FROM raw.raw_stories WHERE ingested_date = ?",
            [ingested_date],
        ).fetchone()[0]

        logger.info("Loaded %d rows for %s", row_count, ingested_date)
        return row_count
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    load_to_duckdb()
