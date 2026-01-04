"""
Segment extractors for specific HL7 segment types.

Each extractor is responsible for pulling relevant data from its
segment type and converting it to domain model objects.
"""

from typing import Optional, Tuple
from .models import Patient, Provider
from .tokenizer import Segment, Delimiters
from .timestamps import parse_hl7_timestamp, parse_hl7_date


# Base Segment Extractor
class SegmentExtractor:
    """Base class for segment extractors."""

    def __init__(self, delimiters: Delimiters = None):
        self.delimiters = delimiters or Delimiters()

    @property
    def component_sep(self) -> str:
        return self.delimiters.component

    @property
    def subcomponent_sep(self) -> str:
        return self.delimiters.subcomponent


# Message Header Extractor
class MSHExtractor(SegmentExtractor):
    """
    Extracts data from MSH (Message Header) segment.

    MSH contains message metadata including type, control ID, and timestamp.

    Key fields:
    - MSH-7: Message date/time
    - MSH-9: Message type (e.g., SIU^S12)
    - MSH-10: Message control ID
    """

    def extract_message_type(self, segment: Segment) -> Tuple[str, str]:
        """
        Extract message type and trigger event.

        Returns:
            Tuple of (message_type, trigger_event), e.g., ("SIU", "S12")
        """
        # MSH-9 is the message type field
        # After MSH special handling: fields[0]="MSH", fields[1]="|", fields[2]=encoding
        # So MSH-9 would be at index 9
        msg_type_field = segment.get_field(9, "")

        if not msg_type_field:
            return ("", "")

        components = msg_type_field.split(self.component_sep)
        message_type = components[0] if len(components) > 0 else ""
        trigger_event = components[1] if len(components) > 1 else ""

        return (message_type, trigger_event)

    def extract_control_id(self, segment: Segment) -> str:
        """Extract message control ID (MSH-10)."""
        return segment.get_field(10, "")

    def extract_timestamp(self, segment: Segment) -> Optional[str]:
        """Extract and normalize message timestamp (MSH-7)."""
        raw_ts = segment.get_field(7, "")
        if not raw_ts:
            return None
        try:
            return parse_hl7_timestamp(raw_ts)
        except Exception:
            return None


# Scheduling Segment Extractor
class SCHExtractor(SegmentExtractor):
    """
    Extracts scheduling data from SCH segment.

    SCH contains the core appointment information:
    - SCH-1: Placer appointment ID
    - SCH-2: Filler appointment ID
    - SCH-7: Appointment reason
    - SCH-11: Appointment timing (start date/time, duration)
    - SCH-23: Appointment location
    """

    def extract_appointment_id(self, segment: Segment) -> str:
        """
        Extract appointment identifier.

        Tries filler appointment ID (SCH-2) first, falls back to placer (SCH-1).
        """
        # SCH-2: Filler appointment ID - typically the authoritative ID
        filler_id = segment.get_component(2, 0, self.component_sep)
        if filler_id:
            return filler_id

        # Fall back to placer appointment ID (SCH-1)
        placer_id = segment.get_component(1, 0, self.component_sep)
        return placer_id or ""

    def extract_appointment_datetime(self, segment: Segment) -> Optional[str]:
        """
        Extract appointment start date/time from SCH timing fields.

        Different HL7 implementations place timing in SCH-10 or SCH-11.
        The timing quantity field typically has component 4 as the start time.
        Format: ^duration^units^start_datetime^end_datetime
        """
        # Try SCH-10 first (some implementations), then SCH-11
        for field_index in [10, 11]:
            timing_field = segment.get_field(field_index, "")

            if not timing_field:
                continue

            # The timing field may have multiple components
            # Component 4 (index 3) is typically the start datetime
            components = timing_field.split(self.component_sep)

            # Try component 4 first (start datetime in TQ format)
            if len(components) > 3 and components[3]:
                try:
                    return parse_hl7_timestamp(components[3])
                except Exception:
                    pass

            # Some implementations put datetime in first component
            if components and components[0]:
                try:
                    return parse_hl7_timestamp(components[0])
                except Exception:
                    pass

        return None

    def extract_reason(self, segment: Segment) -> str:
        """
        Extract appointment reason from SCH-6 or SCH-7.

        Different HL7 implementations use SCH-6 (Event Reason) or
        SCH-7 (Appointment Reason). We check both fields.
        """
        # Try SCH-6 first (Event Reason), then SCH-7 (Appointment Reason)
        for field_index in [6, 7]:
            reason_field = segment.get_field(field_index, "")
            if not reason_field:
                continue

            # May be a CE (Coded Element) - component 2 is the text
            components = reason_field.split(self.component_sep)

            # Try text component first, fall back to code
            if len(components) > 1 and components[1]:
                return components[1].strip()
            if components and components[0]:
                return components[0].strip()

        return ""

    def extract_location(self, segment: Segment) -> str:
        """
        Extract appointment location.

        Location can be in SCH-23 (filler contact location) or
        derived from other fields.
        """
        # SCH-23 is filler contact point
        location = segment.get_field(23, "")

        if location:
            # May be a composite field - extract readable part
            components = location.split(self.component_sep)
            # Build location from available components
            parts = [c.strip() for c in components[:3] if c.strip()]
            return " ".join(parts) if parts else location.strip()

        return ""


# Patient Identification Extractor
class PIDExtractor(SegmentExtractor):
    """
    Extracts patient demographic data from PID segment.

    Key fields:
    - PID-3: Patient identifier list
    - PID-5: Patient name
    - PID-7: Date of birth
    - PID-8: Administrative sex
    """

    def extract_patient(self, segment: Segment) -> Patient:
        """Extract complete patient information."""
        patient_id = self._extract_patient_id(segment)
        first_name, last_name = self._extract_name(segment)
        dob = self._extract_dob(segment)
        gender = self._extract_gender(segment)

        return Patient(
            id=patient_id,
            first_name=first_name or None,
            last_name=last_name or None,
            dob=dob,
            gender=gender or None,
        )

    def _extract_patient_id(self, segment: Segment) -> str:
        """
        Extract patient ID from PID-3.

        PID-3 is a repeating field of patient identifiers.
        We take the first (primary) identifier.
        """
        id_field = segment.get_field(3, "")
        if not id_field:
            return ""

        # Handle repetitions (separated by ~)
        first_id = id_field.split(self.delimiters.repetition)[0]

        # Extract ID from CX composite type - first component is the ID
        id_value = first_id.split(self.component_sep)[0]
        return id_value

    def _extract_name(self, segment: Segment) -> Tuple[str, str]:
        """
        Extract patient name from PID-5.

        PID-5 is XPN (Extended Person Name) type:
        - Component 1: Family name
        - Component 2: Given name
        """
        name_field = segment.get_field(5, "")
        if not name_field:
            return ("", "")

        # Handle repetitions - take first name
        first_name_entry = name_field.split(self.delimiters.repetition)[0]
        components = first_name_entry.split(self.component_sep)

        family_name = components[0] if len(components) > 0 else ""
        given_name = components[1] if len(components) > 1 else ""

        # Family name might have subcomponents (surname, prefix, etc.)
        # Extract just the surname
        if self.subcomponent_sep in family_name:
            family_name = family_name.split(self.subcomponent_sep)[0]

        return (given_name, family_name)

    def _extract_dob(self, segment: Segment) -> Optional[str]:
        """Extract date of birth from PID-7."""
        dob_field = segment.get_field(7, "")
        if not dob_field:
            return None

        try:
            return parse_hl7_date(dob_field)
        except Exception:
            return None

    def _extract_gender(self, segment: Segment) -> str:
        """
        Extract administrative sex from PID-8.

        Standard values: M, F, O, U, A, N
        """
        return segment.get_field(8, "")


# Provider/Visit Extractor
class PV1Extractor(SegmentExtractor):
    """
    Extracts visit/provider information from PV1 segment.

    Key fields:
    - PV1-3: Assigned patient location
    - PV1-7: Attending doctor
    - PV1-8: Referring doctor
    - PV1-17: Admitting doctor
    """

    def extract_provider(self, segment: Segment) -> Optional[Provider]:
        """
        Extract provider (attending physician) information.

        Tries multiple fields in priority order:
        1. PV1-7: Attending doctor
        2. PV1-17: Admitting doctor
        3. PV1-8: Referring doctor
        """
        # Try attending doctor first
        provider_id, provider_name = self._extract_xcn_field(segment, 7)

        # Fall back to admitting doctor
        if not provider_id:
            provider_id, provider_name = self._extract_xcn_field(segment, 17)

        # Fall back to referring doctor
        if not provider_id:
            provider_id, provider_name = self._extract_xcn_field(segment, 8)

        if not provider_id and not provider_name:
            return None

        return Provider(id=provider_id or "UNKNOWN", name=provider_name or None)

    def _extract_xcn_field(self, segment: Segment, field_index: int) -> Tuple[str, str]:
        """
        Extract ID and name from XCN (Extended Composite ID) field.

        XCN structure:
        - Component 1: ID number
        - Component 2: Family name
        - Component 3: Given name
        - Component 4: Middle name
        - Component 5: Suffix
        - Component 6: Prefix (Dr., etc.)
        - Component 7: Degree
        """
        field_value = segment.get_field(field_index, "")
        if not field_value:
            return ("", "")

        # Handle repetitions - take first
        first_entry = field_value.split(self.delimiters.repetition)[0]
        components = first_entry.split(self.component_sep)

        provider_id = components[0] if len(components) > 0 else ""
        family_name = components[1] if len(components) > 1 else ""
        given_name = components[2] if len(components) > 2 else ""
        middle_name = components[3] if len(components) > 3 else ""
        suffix = components[4] if len(components) > 4 else ""
        prefix = (
            components[5] if len(components) > 5 else ""
        )  # Fixed: prefix is component 6 (index 5)
        degree = components[6] if len(components) > 6 else ""

        # Build display name
        name_parts = []
        if prefix:
            name_parts.append(prefix)
        if degree:
            name_parts.append(degree)
        if given_name:
            name_parts.append(given_name)
        if middle_name:
            name_parts.append(middle_name)
        if family_name:
            name_parts.append(family_name)
        if suffix:
            name_parts.append(suffix)

        provider_name = " ".join(name_parts)

        return (provider_id, provider_name)

    def extract_location(self, segment: Segment) -> str:
        """
        Extract patient location from PV1-3.

        PV1-3 is PL (Person Location) type with multiple components.
        """
        location_field = segment.get_field(3, "")
        if not location_field:
            return ""

        components = location_field.split(self.component_sep)

        # Build readable location from components
        # PL: Point of care ^ Room ^ Bed ^ Facility ^ ...
        location_parts = []

        # Facility (component 4)
        if len(components) > 3 and components[3]:
            location_parts.append(components[3])

        # Point of care (component 1)
        if len(components) > 0 and components[0]:
            location_parts.append(components[0])

        # Room (component 2)
        if len(components) > 1 and components[1]:
            room_part = components[1].strip()
            if room_part.lower().startswith("room"):
                location_parts.append(room_part)
            else:
                location_parts.append(f"Room {room_part}")

        # Bed (component 3)
        if len(components) > 2 and components[2]:
            location_parts.append(f"Bed {components[2]}")

        return " ".join(location_parts)
