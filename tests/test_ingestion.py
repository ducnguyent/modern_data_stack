"""
Tests for the HN ingestion parser.
===================================
Validates ``parse_story()`` handles complete, partial, and extra‑field inputs.
"""

import pytest

from src.ingestion.fetch_hn import parse_story


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def complete_story():
    """A full raw story dict as returned by the HN API."""
    return {
        "id": 12345,
        "title": "Show HN: A cool project",
        "score": 142,
        "by": "pg",
        "descendants": 37,
        "time": 1700000000,
        "url": "https://example.com/cool",
        "type": "story",
        "kids": [111, 222, 333],
    }


@pytest.fixture
def minimal_story():
    """A story dict with only the required ``id`` field."""
    return {"id": 99999}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestParseStoryValid:
    """Tests for a complete, well‑formed story input."""

    def test_returns_exactly_seven_keys(self, complete_story):
        result = parse_story(complete_story)
        assert set(result.keys()) == {"id", "title", "score", "by", "descendants", "time", "url"}

    def test_values_match(self, complete_story):
        result = parse_story(complete_story)
        assert result["id"] == 12345
        assert result["title"] == "Show HN: A cool project"
        assert result["score"] == 142
        assert result["by"] == "pg"
        assert result["descendants"] == 37
        assert result["time"] == 1700000000
        assert result["url"] == "https://example.com/cool"


class TestParseStoryMissingFields:
    """Tests for stories with missing optional fields."""

    def test_missing_title_defaults_to_none(self, minimal_story):
        result = parse_story(minimal_story)
        assert result["title"] is None

    def test_missing_score_defaults_to_zero(self, minimal_story):
        result = parse_story(minimal_story)
        assert result["score"] == 0

    def test_missing_author_defaults_to_none(self, minimal_story):
        result = parse_story(minimal_story)
        assert result["by"] is None

    def test_missing_descendants_defaults_to_zero(self, minimal_story):
        result = parse_story(minimal_story)
        assert result["descendants"] == 0

    def test_missing_url_defaults_to_none(self, minimal_story):
        result = parse_story(minimal_story)
        assert result["url"] is None


class TestParseStoryFiltersExtraFields:
    """Tests that extra fields from the API are stripped out."""

    def test_kids_not_in_output(self, complete_story):
        result = parse_story(complete_story)
        assert "kids" not in result

    def test_type_not_in_output(self, complete_story):
        result = parse_story(complete_story)
        assert "type" not in result

    def test_arbitrary_extra_field_not_in_output(self):
        story = {"id": 1, "custom_field": "should_be_dropped", "foo": "bar"}
        result = parse_story(story)
        assert "custom_field" not in result
        assert "foo" not in result


class TestParseStoryTypeCasting:
    """Tests that output values have the correct Python types."""

    def test_id_is_int(self, complete_story):
        result = parse_story(complete_story)
        assert isinstance(result["id"], int)

    def test_score_is_int(self, complete_story):
        result = parse_story(complete_story)
        assert isinstance(result["score"], int)

    def test_descendants_is_int(self, complete_story):
        result = parse_story(complete_story)
        assert isinstance(result["descendants"], int)

    def test_time_is_int(self, complete_story):
        result = parse_story(complete_story)
        assert isinstance(result["time"], int)

    def test_title_is_str(self, complete_story):
        result = parse_story(complete_story)
        assert isinstance(result["title"], str)

    def test_string_id_gets_cast_to_int(self):
        story = {"id": "42"}
        result = parse_story(story)
        assert result["id"] == 42
        assert isinstance(result["id"], int)
