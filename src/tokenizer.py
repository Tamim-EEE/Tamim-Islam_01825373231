"""
HL7 message tokenizer and low-level parsing utilities.

This module handles the raw wire format of HL7 v2.x messages, providing
safe extraction of segments, fields, and components without making
assumptions about message structure.
"""

from dataclasses import dataclass
from typing import List, Optional

# Default HL7 delimiters (can be overridden from MSH)
DEFAULT_FIELD_SEPARATOR = "|"
DEFAULT_COMPONENT_SEPARATOR = "^"
DEFAULT_REPETITION_SEPARATOR = "~"
DEFAULT_ESCAPE_CHARACTER = "\\"
DEFAULT_SUBCOMPONENT_SEPARATOR = "&"


# Delimiters Dataclass
@dataclass
class Delimiters:
    """
    HL7 message delimiters extracted from MSH segment.

    The MSH segment always starts with "MSH|" and the encoding characters
    follow in position MSH-2, defining how the rest of the message is parsed.
    """

    field: str = DEFAULT_FIELD_SEPARATOR
    component: str = DEFAULT_COMPONENT_SEPARATOR
    repetition: str = DEFAULT_REPETITION_SEPARATOR
    escape: str = DEFAULT_ESCAPE_CHARACTER
    subcomponent: str = DEFAULT_SUBCOMPONENT_SEPARATOR


# Segment Dataclass
@dataclass
class Segment:
    """
    Represents a parsed HL7 segment with its type and fields.

    Fields are stored as raw strings; component splitting happens
    on demand to avoid unnecessary processing.
    """

    segment_type: str
    fields: List[str]
    raw_text: str

    def get_field(self, index: int, default: str = "") -> str:
        """
        Safely retrieve a field by index.

        HL7 field indices are 1-based in documentation, but this method
        uses 0-based indexing internally. The segment type counts as
        field 0.

        Args:
            index: 0-based field index
            default: Value to return if field doesn't exist

        Returns:
            Field value or default
        """
        if index < 0 or index >= len(self.fields):
            return default
        return self.fields[index] or default

    def get_component(
        self,
        field_index: int,
        component_index: int,
        separator: str = DEFAULT_COMPONENT_SEPARATOR,
        default: str = "",
    ) -> str:
        """
        Extract a specific component from a field.

        Args:
            field_index: 0-based field index
            component_index: 0-based component index within the field
            separator: Component separator character
            default: Value to return if component doesn't exist

        Returns:
            Component value or default
        """
        field_value = self.get_field(field_index)
        if not field_value:
            return default

        components = field_value.split(separator)
        if component_index < 0 or component_index >= len(components):
            return default
        return components[component_index] or default

    def get_subcomponent(
        self,
        field_index: int,
        component_index: int,
        subcomponent_index: int,
        component_sep: str = DEFAULT_COMPONENT_SEPARATOR,
        subcomponent_sep: str = DEFAULT_SUBCOMPONENT_SEPARATOR,
        default: str = "",
    ) -> str:
        """
        Extract a subcomponent from a field.

        Args:
            field_index: 0-based field index
            component_index: 0-based component index
            subcomponent_index: 0-based subcomponent index
            component_sep: Component separator
            subcomponent_sep: Subcomponent separator
            default: Value if not found

        Returns:
            Subcomponent value or default
        """
        component = self.get_component(field_index, component_index, component_sep, "")
        if not component:
            return default

        subcomponents = component.split(subcomponent_sep)
        if subcomponent_index < 0 or subcomponent_index >= len(subcomponents):
            return default
        return subcomponents[subcomponent_index] or default


# HL7 Tokenizer Class
class HL7Tokenizer:
    """
    Tokenizes raw HL7 message text into structured segments.

    This class handles the low-level parsing of HL7 wire format,
    including delimiter detection and segment splitting.
    """

    # Common segment terminators in HL7 files
    SEGMENT_TERMINATORS = ["\r\n", "\r", "\n"]

    def __init__(self):
        self.delimiters = Delimiters()

    def tokenize(self, message_text: str) -> List[Segment]:
        """
        Parse raw HL7 message text into a list of segments.

        Args:
            message_text: Raw HL7 message string

        Returns:
            List of Segment objects
        """
        # Normalize line endings
        normalized = self._normalize_line_endings(message_text)

        # Split into raw segment strings
        raw_segments = [s.strip() for s in normalized.split("\n") if s.strip()]

        if not raw_segments:
            return []

        # Extract delimiters from MSH if present
        self._detect_delimiters(raw_segments)

        # Parse each segment
        segments = []
        for raw in raw_segments:
            segment = self._parse_segment(raw)
            if segment:
                segments.append(segment)

        return segments

    def _normalize_line_endings(self, text: str) -> str:
        """Convert various line endings to simple newlines."""
        result = text.replace("\r\n", "\n").replace("\r", "\n")
        return result

    def _detect_delimiters(self, raw_segments: List[str]) -> None:
        """
        Extract encoding characters from MSH segment.

        The MSH segment structure is:
        MSH|^~\\&|...

        Where position 1 (after MSH) is the field separator,
        and position 2 contains the encoding characters.
        """
        for raw in raw_segments:
            if raw.startswith("MSH"):
                if len(raw) >= 4:
                    self.delimiters.field = raw[3]
                if len(raw) >= 8:
                    encoding = raw[4:8]
                    if len(encoding) >= 1:
                        self.delimiters.component = encoding[0]
                    if len(encoding) >= 2:
                        self.delimiters.repetition = encoding[1]
                    if len(encoding) >= 3:
                        self.delimiters.escape = encoding[2]
                    if len(encoding) >= 4:
                        self.delimiters.subcomponent = encoding[3]
                break

        # Validate detected delimiters
        self._validate_delimiters()

    def _validate_delimiters(self) -> None:
        """
        Validate that detected delimiters are reasonable.

        HL7 delimiters should be non-alphanumeric special characters
        and should not conflict with each other.
        """
        from .exceptions import MalformedSegmentError

        delimiters = [
            ("field", self.delimiters.field),
            ("component", self.delimiters.component),
            ("repetition", self.delimiters.repetition),
            ("escape", self.delimiters.escape),
            ("subcomponent", self.delimiters.subcomponent),
        ]

        # Check each delimiter is a non-alphanumeric special character
        for name, char in delimiters:
            if not char or char.isalnum() or char.isspace():
                raise MalformedSegmentError(
                    "MSH",
                    f"Invalid {name} delimiter '{char}': must be a non-alphanumeric, non-whitespace character",
                )

        # Check for conflicts (same character used for different purposes)
        chars_used = set()
        for name, char in delimiters:
            if char in chars_used:
                raise MalformedSegmentError(
                    "MSH", f"Conflicting delimiter: '{char}' used for multiple purposes"
                )
            chars_used.add(char)

    def _parse_segment(self, raw_text: str) -> Optional[Segment]:
        """
        Parse a raw segment string into a Segment object.

        Args:
            raw_text: Single segment string

        Returns:
            Segment object or None if segment is empty
        """
        if not raw_text:
            return None

        # Handle MSH specially - the field separator is part of the segment
        if raw_text.startswith("MSH"):
            return self._parse_msh_segment(raw_text)

        fields = raw_text.split(self.delimiters.field)
        if not fields:
            return None

        segment_type = fields[0]

        return Segment(segment_type=segment_type, fields=fields, raw_text=raw_text)

    def _parse_msh_segment(self, raw_text: str) -> Segment:
        """
        Parse MSH segment with special handling for field separator.

        MSH is unique because the field separator itself occupies MSH-1,
        so we need to handle it differently to maintain consistent
        field indexing.
        """
        # Field separator is character 4 (index 3)
        sep = raw_text[3] if len(raw_text) > 3 else self.delimiters.field

        # Split by separator, but MSH-1 is the separator itself
        parts = raw_text.split(sep)

        # Insert the field separator as MSH-1
        fields = [parts[0], sep] + parts[1:] if parts else ["MSH", sep]

        return Segment(segment_type="MSH", fields=fields, raw_text=raw_text)


# HL7 Message Splitter
def split_hl7_messages(content: str) -> List[str]:
    """
    Split file content containing multiple HL7 messages.

    Messages are separated by MSH segments. This function identifies
    message boundaries and returns individual message strings.

    Args:
        content: File content potentially containing multiple messages

    Returns:
        List of individual message strings
    """
    # Normalize line endings first
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")

    # Find all MSH positions
    lines = normalized.split("\n")
    messages = []
    current_message_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # New message starts with MSH
        if stripped.startswith("MSH"):
            # Save previous message if exists
            if current_message_lines:
                messages.append("\n".join(current_message_lines))
            current_message_lines = [stripped]
        else:
            # Continue current message
            if current_message_lines:
                current_message_lines.append(stripped)

    # Don't forget the last message
    if current_message_lines:
        messages.append("\n".join(current_message_lines))

    return messages
