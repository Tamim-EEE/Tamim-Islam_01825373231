"""
Main HL7 SIU message parser.

This module orchestrates the parsing process, combining tokenization,
segment extraction, and domain model construction into a cohesive
parsing pipeline.
"""

from typing import Dict, List
from .models import Appointment, ParseResult
from .tokenizer import HL7Tokenizer, Segment, split_hl7_messages
from .extractors import MSHExtractor, SCHExtractor, PIDExtractor, PV1Extractor
from .exceptions import (
    HL7ParseError,
    InvalidMessageTypeError,
    MissingSegmentError,
    MalformedSegmentError,
)


# Expected message type for SIU scheduling messages
EXPECTED_MESSAGE_TYPE = "SIU"
EXPECTED_TRIGGER_EVENT = "S12"


# SIU Parser Class
class SIUParser:
    """
    Parser for HL7 SIU^S12 (Schedule Information Unsolicited - New Appointment)
    messages.

    This parser handles:
    - Single or multiple messages per input
    - Validation of message type
    - Extraction of appointment data from MSH, SCH, PID, and PV1 segments
    - Graceful handling of missing optional data
    - Normalization of timestamps to ISO 8601

    Usage:
        parser = SIUParser()
        results = parser.parse(message_text)
        for result in results:
            print(result.appointment.to_json())
    """

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the parser.

        Args:
            strict_mode: If True, raise exceptions for missing optional fields. If False (default), collect warnings and continue.
        """
        self.strict_mode = strict_mode
        self.tokenizer = HL7Tokenizer()

    def parse(self, content: str) -> List[ParseResult]:
        """
        Parse HL7 content that may contain one or more SIU messages.

        Args:
            content: Raw HL7 message content

        Returns:
            List of ParseResult objects, one per valid message

        Raises:
            InvalidMessageTypeError: If a message is not SIU^S12
            MissingSegmentError: If required segment is missing (strict mode)
        """
        if not content or not content.strip():
            return []

        messages = split_hl7_messages(content)
        results = []

        for idx, message_text in enumerate(messages):
            try:
                result = self._parse_single_message(message_text, idx)
                results.append(result)
            except HL7ParseError:
                # Re-raise parsing errors as-is
                raise

        return results

    def _parse_single_message(
        self, message_text: str, message_index: int
    ) -> ParseResult:
        """
        Parse a single HL7 SIU message.

        Args:
            message_text: Single message content
            message_index: Index of this message in the file

        Returns:
            ParseResult with appointment data
        """
        warnings = []

        # Tokenize the message into segments
        segments = self.tokenizer.tokenize(message_text)

        if not segments:
            raise MalformedSegmentError("", "Message contains no valid segments")

        # Build a lookup map for segment access
        segment_map = self._build_segment_map(segments)

        # Validate message type first
        self._validate_message_type(segment_map)

        # Create extractors with message-specific delimiters
        delimiters = self.tokenizer.delimiters
        msh_extractor = MSHExtractor(delimiters)
        sch_extractor = SCHExtractor(delimiters)
        pid_extractor = PIDExtractor(delimiters)
        pv1_extractor = PV1Extractor(delimiters)

        # Extract appointment ID - required field
        appointment_id = ""
        appointment_datetime = None
        location = ""
        reason = ""

        sch_segment = segment_map.get("SCH")
        if sch_segment:
            appointment_id = sch_extractor.extract_appointment_id(sch_segment)
            appointment_datetime = sch_extractor.extract_appointment_datetime(
                sch_segment
            )
            reason = sch_extractor.extract_reason(sch_segment)
            location = sch_extractor.extract_location(sch_segment)
        else:
            if self.strict_mode:
                raise MissingSegmentError("SCH")
            warnings.append(
                "SCH segment not found - appointment details may be incomplete"
            )
            appointment_id = "UNKNOWN"

        # Extract patient information
        patient = None
        pid_segment = segment_map.get("PID")
        if pid_segment:
            patient = pid_extractor.extract_patient(pid_segment)
        else:
            if self.strict_mode:
                raise MissingSegmentError("PID")
            warnings.append("PID segment not found - patient information unavailable")

        # Extract provider information and location fallback
        provider = None
        pv1_segment = segment_map.get("PV1")
        if pv1_segment:
            provider = pv1_extractor.extract_provider(pv1_segment)
            # Use PV1 location if SCH didn't have one
            if not location:
                location = pv1_extractor.extract_location(pv1_segment)
        else:
            warnings.append("PV1 segment not found - provider information unavailable")

        # Extract message metadata from MSH
        message_control_id = None
        message_datetime = None
        msh_segment = segment_map.get("MSH")
        if msh_segment:
            message_control_id = msh_extractor.extract_control_id(msh_segment)
            message_datetime = msh_extractor.extract_timestamp(msh_segment)

        # Build the appointment object
        appointment = Appointment(
            appointment_id=appointment_id,
            appointment_datetime=appointment_datetime,
            patient=patient,
            provider=provider,
            location=location or None,
            reason=reason or None,
            message_control_id=message_control_id,
            message_datetime=message_datetime,
        )

        return ParseResult(
            appointment=appointment,
            warnings=warnings,
            source_message_index=message_index,
        )

    def _build_segment_map(self, segments: List[Segment]) -> Dict[str, Segment]:
        """
        Build a lookup map from segment type to segment.

        For segments that may repeat (like OBX), only the first is stored.
        This is sufficient for SIU messages where we need single instances
        of each segment type.
        """
        segment_map = {}
        for segment in segments:
            seg_type = segment.segment_type
            # Keep only the first occurrence of each segment type
            if seg_type not in segment_map:
                segment_map[seg_type] = segment
        return segment_map

    def _validate_message_type(self, segment_map: Dict[str, Segment]) -> None:
        """
        Validate that this is an SIU^S12 message.

        Raises:
            MissingSegmentError: If MSH segment is not found
            MalformedSegmentError: If MSH segment is structurally invalid
            InvalidMessageTypeError: If message type is not SIU^S12
        """
        msh_segment = segment_map.get("MSH")
        if not msh_segment:
            raise MissingSegmentError("MSH")

        # MSH must have at least encoding characters (field 2)
        # Fields: [0]=MSH, [1]=|, [2]=^~\&, [3]=sending app, ...
        # A valid MSH must have field[2] with encoding chars (at least ^~\&)
        if len(msh_segment.fields) < 3 or not msh_segment.fields[2]:
            raise MalformedSegmentError(
                "MSH", "MSH segment missing required encoding characters"
            )

        extractor = MSHExtractor(self.tokenizer.delimiters)
        message_type, trigger_event = extractor.extract_message_type(msh_segment)

        expected = f"{EXPECTED_MESSAGE_TYPE}^{EXPECTED_TRIGGER_EVENT}"
        actual = f"{message_type}^{trigger_event}" if trigger_event else message_type

        if message_type != EXPECTED_MESSAGE_TYPE:
            raise InvalidMessageTypeError(expected, actual)

        # S12 is the trigger for new appointment booking
        # Other triggers (S13, S14, etc.) might be handled differently
        if trigger_event and trigger_event != EXPECTED_TRIGGER_EVENT:
            raise InvalidMessageTypeError(expected, actual)


# Convenience function to parse HL7 messages
def parse_hl7_message(content: str, strict: bool = False) -> List[ParseResult]:
    """
    Convenience function to parse HL7 SIU messages.

    Args:
        content: Raw HL7 message content
        strict: Enable strict validation mode

    Returns:
        List of parse results
    """
    parser = SIUParser(strict_mode=strict)
    return parser.parse(content)
