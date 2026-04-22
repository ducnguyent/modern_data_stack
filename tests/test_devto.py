"""
Tests for the DEV.to ingestion parser.
========================================
Validates ``parse_article()`` field extraction logic.
"""

import pytest

from src.ingestion.fetch_devto import parse_article


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def complete_article():
    """A full raw article dict as might be returned by the DEV.to API."""
    return {
        "id": 12345,
        "title": "A cool DEV article",
        "positive_reactions_count": 100,
        "public_reactions_count": 120,
        "comments_count": 50,
        "user": {"username": "devuser"},
        "published_at": "2024-01-01T12:00:00Z",
        "tag_list": ["python", "data", "engineering"],
        "url": "https://dev.to/devuser/a-cool-dev-article",
        "reading_time_minutes": 5,
        "extra_field": "should_be_ignored"
    }


@pytest.fixture
def minimal_article():
    """A minimal article dict missing optional fields, tag_list, user."""
    return {"id": 99999}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestParseArticleValid:
    def test_returns_expected_keys(self, complete_article):
        result = parse_article(complete_article)
        expected_keys = {
            "id", "title", "positive_reactions_count", "public_reactions_count",
            "comments_count", "author", "published_at", "tag_list", "url",
            "reading_time_minutes"
        }
        assert set(result.keys()) == expected_keys

    def test_values_match(self, complete_article):
        result = parse_article(complete_article)
        assert result["id"] == 12345
        assert result["title"] == "A cool DEV article"
        assert result["positive_reactions_count"] == 100
        assert result["public_reactions_count"] == 120
        assert result["comments_count"] == 50
        assert result["author"] == "devuser"
        assert result["published_at"] == "2024-01-01T12:00:00Z"
        assert result["tag_list"] == "python,data,engineering"
        assert result["url"] == "https://dev.to/devuser/a-cool-dev-article"
        assert result["reading_time_minutes"] == 5

    def test_ignores_extra_fields(self, complete_article):
        result = parse_article(complete_article)
        assert "extra_field" not in result


class TestParseArticleMissingFields:
    def test_missing_fields_default_sensibly(self, minimal_article):
        result = parse_article(minimal_article)
        assert result["id"] == 99999
        assert result["title"] is None
        assert result["positive_reactions_count"] == 0
        assert result["public_reactions_count"] == 0
        assert result["comments_count"] == 0
        assert result["author"] is None
        assert result["published_at"] is None
        assert result["tag_list"] is None
        assert result["url"] is None
        assert result["reading_time_minutes"] == 0

    def test_user_field_null(self):
        article = {"id": 1, "user": None}
        result = parse_article(article)
        assert result["author"] is None

    def test_tag_list_string(self):
        article = {"id": 2, "tag_list": "python, data"}
        result = parse_article(article)
        assert result["tag_list"] == "python, data"

    def test_tag_list_missing(self):
        article = {"id": 2}
        result = parse_article(article)
        assert result["tag_list"] is None
