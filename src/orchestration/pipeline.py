"""
Prefect Orchestration Pipeline
===============================
Daily pipeline that ingests HN stories, loads to DuckDB,
runs dbt transformations + tests, and alerts on failure.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv
from prefect import flow, task

# Load environment variables from .env at the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
@task(name="fetch-hn-stories", retries=2, retry_delay_seconds=30)
def fetch_stories() -> str:
    """Run the ingestion script and return the output file path."""
    from src.ingestion.fetch_hn import fetch_and_save

    path = fetch_and_save()
    logger.info("Ingestion complete → %s", path)
    return str(path)


@task(name="load-to-duckdb", retries=1, retry_delay_seconds=10)
def load_to_warehouse() -> int:
    """Load the latest raw JSON into DuckDB."""
    from src.loading.load_duckdb import load_to_duckdb

    row_count = load_to_duckdb()
    logger.info("Loaded %d rows into DuckDB", row_count)
    return row_count


@task(name="dbt-run")
def run_dbt_models() -> None:
    """Execute ``dbt run`` against the project."""
    _run_dbt_command("run")


@task(name="dbt-test")
def run_dbt_tests() -> bool:
    """Execute ``dbt test``. Returns True if all tests pass."""
    try:
        _run_dbt_command("test")
        return True
    except subprocess.CalledProcessError:
        return False


@task(name="slack-alert")
def send_slack_alert(message: str) -> None:
    """Post a message to Slack via webhook. Skips silently if URL not set."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or webhook_url.startswith("https://hooks.slack.com/services/XXX"):
        logger.warning("SLACK_WEBHOOK_URL not configured — skipping alert")
        return

    try:
        resp = requests.post(
            webhook_url,
            json={"text": f":rotating_light: *HN Pipeline Alert*\n{message}"},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("Slack alert sent")
    except requests.RequestException as exc:
        logger.error("Failed to send Slack alert: %s", exc)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _run_dbt_command(command: str) -> None:
    """Run a dbt CLI command as a subprocess."""
    # Use the dbt binary from the same virtualenv as the running Python
    venv_bin = Path(sys.executable).parent
    dbt_bin = venv_bin / "dbt"
    cmd = [
        str(dbt_bin), command,
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
    ]
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True, capture_output=True, text=True)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------
@flow(name="hn-daily-pipeline", log_prints=True)
def hn_pipeline() -> None:
    """End‑to‑end Hacker News data pipeline.

    Steps:
        1. Fetch top 500 stories from HN API
        2. Load raw JSON into DuckDB warehouse
        3. Run dbt models (staging + marts)
        4. Run dbt tests — alert via Slack on failure
    """
    print("🚀 Starting HN daily pipeline")

    # Step 1: Ingest
    fetch_stories()

    # Step 2: Load
    load_to_warehouse()

    # Step 3: Transform
    run_dbt_models()

    # Step 4: Test + Alert
    tests_passed = run_dbt_tests()
    if not tests_passed:
        send_slack_alert("dbt tests failed! Check the pipeline logs.")
        raise RuntimeError("dbt tests failed")

    print("✅ Pipeline completed successfully")


# ---------------------------------------------------------------------------
# CLI — run once or serve with schedule
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    if "--serve" in sys.argv:
        # Deploy with daily schedule at 8 AM UTC
        hn_pipeline.serve(
            name="hn-daily-deployment",
            cron="0 8 * * *",
            timezone="UTC",
        )
    else:
        # One‑off run
        hn_pipeline()
