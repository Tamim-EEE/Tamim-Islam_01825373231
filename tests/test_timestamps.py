"""Unit tests for HL7 timestamp parsing."""

import pytest
from src.timestamps import parse_hl7_timestamp, parse_hl7_date, InvalidTimestampError


# Tests for HL7 Timestamp Parsing
class TestHL7TimestampParsing:
    """Tests for HL7 timestamp to ISO 8601 conversion."""

    def test_full_datetime_with_seconds(self):
        """Test parsing complete datetime with seconds."""
        result = parse_hl7_timestamp("20250502130000")
        assert result == "2025-05-02T13:00:00Z"

    def test_datetime_with_minutes_only(self):
        """Test parsing datetime without seconds."""
        result = parse_hl7_timestamp("202505021300")
        assert result == "2025-05-02T13:00:00Z"

    def test_datetime_with_hours_only(self):
        """Test parsing datetime with hours only."""
        result = parse_hl7_timestamp("2025050213")
        assert result == "2025-05-02T13:00:00Z"

    def test_date_only(self):
        """Test parsing date without time component."""
        result = parse_hl7_timestamp("20250502")
        assert result == "2025-05-02"

    def test_year_month_only(self):
        """Test parsing year and month only."""
        result = parse_hl7_timestamp("202505")
        assert result == "2025-05"

    def test_year_only(self):
        """Test parsing year only."""
        result = parse_hl7_timestamp("2025")
        assert result == "2025"

    def test_datetime_with_positive_timezone(self):
        """Test parsing datetime with positive UTC offset."""
        result = parse_hl7_timestamp("20250502130000+0500")
        assert result == "2025-05-02T13:00:00+05:00"

    def test_datetime_with_negative_timezone(self):
        """Test parsing datetime with negative UTC offset."""
        result = parse_hl7_timestamp("20250502130000-0800")
        assert result == "2025-05-02T13:00:00-08:00"

    def test_datetime_with_utc_offset(self):
        """Test parsing datetime with +0000 (UTC)."""
        result = parse_hl7_timestamp("20250502130000+0000")
        assert result == "2025-05-02T13:00:00Z"

    def test_datetime_with_fractional_seconds(self):
        """Test parsing datetime with milliseconds."""
        result = parse_hl7_timestamp("20250502130045.123")
        assert result == "2025-05-02T13:00:45.123Z"

    def test_datetime_with_fraction_and_timezone(self):
        """Test parsing datetime with both fraction and timezone."""
        result = parse_hl7_timestamp("20250502130045.1234+0500")
        assert result == "2025-05-02T13:00:45.123+05:00"

    def test_empty_string_returns_none(self):
        """Test that empty string returns None."""
        assert parse_hl7_timestamp("") is None
        assert parse_hl7_timestamp("   ") is None

    def test_none_returns_none(self):
        """Test that None input returns None."""
        assert parse_hl7_timestamp(None) is None

    def test_invalid_format_raises_error(self):
        """Test that invalid formats raise InvalidTimestampError."""
        with pytest.raises(InvalidTimestampError):
            parse_hl7_timestamp("not-a-timestamp")

    def test_invalid_month_raises_error(self):
        """Test that invalid month raises error."""
        with pytest.raises(InvalidTimestampError):
            parse_hl7_timestamp("20251302")  # Month 13

    def test_invalid_day_raises_error(self):
        """Test that invalid day raises error."""
        with pytest.raises(InvalidTimestampError):
            parse_hl7_timestamp("20250532")  # Day 32

    def test_invalid_hour_raises_error(self):
        """Test that invalid hour raises error."""
        with pytest.raises(InvalidTimestampError):
            parse_hl7_timestamp("2025050225")  # Hour 25

    def test_whitespace_trimmed(self):
        """Test that surrounding whitespace is handled."""
        result = parse_hl7_timestamp("  20250502130000  ")
        assert result == "2025-05-02T13:00:00Z"


# Tests for HL7 Date Parsing
class TestHL7DateParsing:
    """Tests for HL7 date only parsing."""

    def test_standard_date(self):
        """Test parsing standard YYYYMMDD date."""
        result = parse_hl7_date("19850210")
        assert result == "1985-02-10"

    def test_empty_returns_none(self):
        """Test empty input returns None."""
        assert parse_hl7_date("") is None
        assert parse_hl7_date(None) is None

    def test_invalid_date_raises_error(self):
        """Test invalid date raises error."""
        with pytest.raises(InvalidTimestampError):
            parse_hl7_date("19850230")  # Feb 30 doesn't exist

    def test_strips_time_from_datetime(self):
        """Test that time portion is stripped from datetime."""
        result = parse_hl7_date("19850210120000")
        assert result == "1985-02-10"
