"""
Timestamp utilities for HL7 date/time handling.

HL7 uses a specific timestamp format (DTM data type) that needs to be
converted to ISO 8601 for modern applications.
"""

import re
from datetime import datetime
from typing import Optional
from .exceptions import InvalidTimestampError


# HL7 timestamp format patterns
# Full: YYYYMMDDHHMMSS.SSSS+/-ZZZZ
# Partial formats are common and must be handled
HL7_TIMESTAMP_PATTERNS = [
    # With timezone offset
    (
        r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d+)([+-]\d{4})$",
        "full_with_tz_fraction",
    ),
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})([+-]\d{4})$", "full_with_tz"),
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})([+-]\d{4})$", "minutes_with_tz"),
    # Without timezone (assume UTC)
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d+)$", "full_with_fraction"),
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$", "full"),
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})$", "minutes"),
    (r"^(\d{4})(\d{2})(\d{2})(\d{2})$", "hours"),
    (r"^(\d{4})(\d{2})(\d{2})$", "date"),
    (r"^(\d{4})(\d{2})$", "month"),
    (r"^(\d{4})$", "year"),
]


# Convert HL7 Timestamp to ISO 8601
def parse_hl7_timestamp(value: str, assume_utc: bool = True) -> Optional[str]:
    """
    Convert HL7 timestamp to ISO 8601 format.

    HL7 timestamps come in various formats:
    - YYYY (year only)
    - YYYYMM (year and month)
    - YYYYMMDD (date)
    - YYYYMMDDHH (date with hour)
    - YYYYMMDDHHMM (date with minutes)
    - YYYYMMDDHHMMSS (full datetime)
    - YYYYMMDDHHMMSS.SSSS (with fractional seconds)
    - Any of the above with +/-ZZZZ timezone offset

    Args:
        value: HL7 timestamp string
        assume_utc: If True, treat timestamps without TZ as UTC

    Returns:
        ISO 8601 formatted string, or None if input is empty

    Raises:
        InvalidTimestampError: If timestamp format is invalid
    """
    if not value or not value.strip():
        return None

    value = value.strip()

    for pattern, format_type in HL7_TIMESTAMP_PATTERNS:
        match = re.match(pattern, value)
        if match:
            return _convert_matched_timestamp(match, format_type, assume_utc)

    raise InvalidTimestampError(value, "does not match any known HL7 timestamp format")


# Helper to convert regex match to ISO 8601
def _convert_matched_timestamp(
    match: re.Match, format_type: str, assume_utc: bool
) -> str:
    """
    Convert a regex match to ISO 8601 based on the matched format.
    """
    groups = match.groups()

    try:
        if format_type == "year":
            year = int(groups[0])
            return f"{year:04d}"

        elif format_type == "month":
            year, month = int(groups[0]), int(groups[1])
            return f"{year:04d}-{month:02d}"

        elif format_type == "date":
            year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
            _validate_date(year, month, day)
            return f"{year:04d}-{month:02d}-{day:02d}"

        elif format_type == "hours":
            year, month, day, hour = [int(g) for g in groups]
            _validate_date(year, month, day)
            _validate_time(hour, 0, 0)
            tz_suffix = "Z" if assume_utc else ""
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:00:00{tz_suffix}"

        elif format_type == "minutes":
            year, month, day, hour, minute = [int(g) for g in groups]
            _validate_date(year, month, day)
            _validate_time(hour, minute, 0)
            tz_suffix = "Z" if assume_utc else ""
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00{tz_suffix}"

        elif format_type == "full":
            year, month, day, hour, minute, second = [int(g) for g in groups]
            _validate_date(year, month, day)
            _validate_time(hour, minute, second)
            tz_suffix = "Z" if assume_utc else ""
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}{tz_suffix}"

        elif format_type == "full_with_fraction":
            year, month, day, hour, minute, second = [int(g) for g in groups[:6]]
            fraction = groups[6]
            _validate_date(year, month, day)
            _validate_time(hour, minute, second)
            tz_suffix = "Z" if assume_utc else ""
            # Normalize fraction to 3 digits (milliseconds)
            fraction = fraction[:3].ljust(3, "0")
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}.{fraction}{tz_suffix}"

        elif format_type == "minutes_with_tz":
            year, month, day, hour, minute = [int(g) for g in groups[:5]]
            tz_offset = groups[5]
            _validate_date(year, month, day)
            _validate_time(hour, minute, 0)
            iso_tz = _convert_tz_offset(tz_offset)
            return (
                f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00{iso_tz}"
            )

        elif format_type == "full_with_tz":
            year, month, day, hour, minute, second = [int(g) for g in groups[:6]]
            tz_offset = groups[6]
            _validate_date(year, month, day)
            _validate_time(hour, minute, second)
            iso_tz = _convert_tz_offset(tz_offset)
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}{iso_tz}"

        elif format_type == "full_with_tz_fraction":
            year, month, day, hour, minute, second = [int(g) for g in groups[:6]]
            fraction = groups[6]
            tz_offset = groups[7]
            _validate_date(year, month, day)
            _validate_time(hour, minute, second)
            iso_tz = _convert_tz_offset(tz_offset)
            fraction = fraction[:3].ljust(3, "0")
            return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}.{fraction}{iso_tz}"

    except ValueError as e:
        raise InvalidTimestampError(match.group(0), str(e))

    raise InvalidTimestampError(match.group(0), f"unhandled format type: {format_type}")


# Validate date components
def _validate_date(year: int, month: int, day: int) -> None:
    """Validate date components."""
    if not (1 <= month <= 12):
        raise ValueError(f"month must be 1-12, got {month}")
    if not (1 <= day <= 31):
        raise ValueError(f"day must be 1-31, got {day}")
    # Let datetime handle complex validation
    datetime(year, month, day)


# Validate time components
def _validate_time(hour: int, minute: int, second: int) -> None:
    """Validate time components."""
    if not (0 <= hour <= 23):
        raise ValueError(f"hour must be 0-23, got {hour}")
    if not (0 <= minute <= 59):
        raise ValueError(f"minute must be 0-59, got {minute}")
    if not (0 <= second <= 59):
        raise ValueError(f"second must be 0-59, got {second}")


# Convert HL7 timezone offset to ISO 8601
def _convert_tz_offset(hl7_offset: str) -> str:
    """
    Convert HL7 timezone offset (+/-HHMM) to ISO 8601 format (+/-HH:MM).

    Args:
        hl7_offset: HL7 format offset like "+0500" or "-0800"

    Returns:
        ISO 8601 format like "+05:00" or "-08:00"
    """
    if not hl7_offset:
        return "Z"

    if hl7_offset in ("Z", "+0000", "-0000"):
        return "Z"

    sign = hl7_offset[0]
    hours = hl7_offset[1:3]
    minutes = hl7_offset[3:5]

    return f"{sign}{hours}:{minutes}"


# Parse HL7 Date to ISO 8601 Date
def parse_hl7_date(value: str) -> Optional[str]:
    """
    Parse HL7 date (without time) to ISO 8601 date format.

    Args:
        value: HL7 date string (YYYYMMDD or partial)

    Returns:
        ISO 8601 date string (YYYY-MM-DD) or None
    """
    if not value or not value.strip():
        return None

    value = value.strip()

    # Handle date-only formats
    if len(value) == 8:
        year, month, day = value[:4], value[4:6], value[6:8]
        try:
            y, m, d = int(year), int(month), int(day)
            _validate_date(y, m, d)
            return f"{y:04d}-{m:02d}-{d:02d}"
        except (ValueError, InvalidTimestampError) as e:
            raise InvalidTimestampError(value, str(e))

    # Fall back to general timestamp parsing for other formats
    result = parse_hl7_timestamp(value, assume_utc=False)
    if result:
        # Extract just the date portion
        return result[:10] if len(result) >= 10 else result

    return None
