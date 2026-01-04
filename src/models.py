"""
Domain models for HL7 appointment data.

These dataclasses represent the normalized output structure for parsed
appointment information. They provide a clean separation between the
raw HL7 wire format and the application's domain model.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional
import json


# Patient Information
@dataclass
class Patient:
    """Patient demographic information extracted from PID segment."""

    id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    dob: Optional[str] = None  # ISO 8601 date format
    gender: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding None values for cleaner output."""
        return {k: v for k, v in asdict(self).items() if v is not None}


# Provider Information
@dataclass
class Provider:
    """Healthcare provider information extracted from PV1 segment."""

    id: str
    name: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


# Appointment Information
@dataclass
class Appointment:
    """
    Represents a scheduled appointment extracted from an SIU^S12 message.

    This is the primary output model of the parser. All timestamps are
    normalized to ISO 8601 format for downstream consumption.
    """

    appointment_id: str
    appointment_datetime: Optional[str] = None  # ISO 8601 datetime
    patient: Optional[Patient] = None
    provider: Optional[Provider] = None
    location: Optional[str] = None
    reason: Optional[str] = None

    # Additional metadata that may be useful
    message_control_id: Optional[str] = None
    message_datetime: Optional[str] = None

    def to_dict(self) -> dict:
        """
        Convert appointment to dictionary representation.

        Nested objects (patient, provider) are also converted.
        None values are excluded for cleaner JSON output.
        """
        result = {}

        if self.appointment_id:
            result["appointment_id"] = self.appointment_id
        if self.appointment_datetime:
            result["appointment_datetime"] = self.appointment_datetime
        if self.patient:
            result["patient"] = self.patient.to_dict()
        if self.provider:
            result["provider"] = self.provider.to_dict()
        if self.location:
            result["location"] = self.location
        if self.reason:
            result["reason"] = self.reason
        if self.message_control_id:
            result["message_control_id"] = self.message_control_id
        if self.message_datetime:
            result["message_datetime"] = self.message_datetime

        return result

    def to_json(self, indent: int = 2) -> str:
        """Serialize appointment to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


# Parse Result Container
@dataclass
class ParseResult:
    """
    Container for parsing results, including any warnings or issues
    encountered during parsing that didn't prevent extraction.
    """

    appointment: Appointment
    warnings: list = field(default_factory=list)
    source_message_index: int = 0

    def to_dict(self) -> dict:
        result = {
            "appointment": self.appointment.to_dict(),
            "message_index": self.source_message_index,
        }
        if self.warnings:
            result["warnings"] = self.warnings
        return result
