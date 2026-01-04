"""
Custom exceptions for HL7 parsing operations.

Provides clear, actionable error messages for different failure modes
encountered during HL7 message processing.
"""

# Base Exception
class HL7ParseError(Exception):
    """Base exception for all HL7 parsing errors."""

    def __init__(
        self,
        message: str,
        segment: str = None,
        field_index: int = None,
        line_number: int = None,
    ):
        self.segment = segment
        self.field_index = field_index
        self.line_number = line_number

        details = []
        if segment:
            details.append(f"segment={segment}")
        if field_index is not None:
            details.append(f"field={field_index}")
        if line_number is not None:
            details.append(f"line={line_number}")

        full_message = message
        if details:
            full_message = f"{message} [{', '.join(details)}]"

        super().__init__(full_message)


# Invalid Message Type Error
class InvalidMessageTypeError(HL7ParseError):
    """Raised when message type is not SIU^S12."""

    def __init__(self, expected: str, actual: str):
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Invalid message type: expected '{expected}', got '{actual}'",
            segment="MSH",
        )


# Missing Segment Error
class MissingSegmentError(HL7ParseError):
    """Raised when a required segment is missing from the message."""

    def __init__(self, segment_type: str):
        self.segment_type = segment_type
        super().__init__(
            f"Required segment '{segment_type}' not found in message",
            segment=segment_type,
        )


# Malformed Segment Error
class MalformedSegmentError(HL7ParseError):
    """Raised when a segment cannot be parsed due to structural issues."""

    def __init__(self, segment_type: str, reason: str):
        self.reason = reason
        super().__init__(f"Malformed segment: {reason}", segment=segment_type)


# Invalid Timestamp Error
class InvalidTimestampError(HL7ParseError):
    """Raised when an HL7 timestamp cannot be parsed."""

    def __init__(self, value: str, reason: str = None):
        self.value = value
        msg = f"Invalid HL7 timestamp: '{value}'"
        if reason:
            msg += f" - {reason}"
        super().__init__(msg)


# File Read Error
class FileReadError(HL7ParseError):
    """Raised when the input file cannot be read."""

    def __init__(self, filepath: str, reason: str):
        self.filepath = filepath
        self.reason = reason
        super().__init__(f"Cannot read file '{filepath}': {reason}")
