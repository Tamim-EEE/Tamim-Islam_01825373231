"""Integration tests for the SIU parser."""

import os
import pytest
import json
from src.parser import SIUParser, parse_hl7_message
from src.io_handler import stream_hl7_file
from src.io_handler import read_hl7_file
from src.io_handler import write_json_output
from src.io_handler import parse_hl7_file
import tempfile
from src.exceptions import (
    InvalidMessageTypeError,
    MissingSegmentError,
    MalformedSegmentError,
    InvalidTimestampError,
    FileReadError,
    HL7ParseError,
)


# Sample valid SIU S12 message
# SCH field positions: 1=placer, 2=filler, 7=reason, 11=timing, 23=location
VALID_SIU_MESSAGE = """MSH|^~\\&|SCHEDULING|HOSPITAL|RECEIVER|CLINIC|20250502130000||SIU^S12|MSG001|P|2.5
SCH|PL123|AP456|||||CONSULT^General Consultation||||20250502140000||||||||||||Clinic A^Room 203
PID|1||P12345^^^HOSP||Doe^John^M||19850210|M
PV1|1|O|CLINIC^101^^MainHospital||||D67890^Smith^Jane^^Dr."""


# Tests for the main SIU parser
class TestSIUParser:
    """Tests for the main SIU parser."""

    def test_parse_valid_message(self):
        """Test parsing a complete valid SIU S12 message."""
        parser = SIUParser()
        results = parser.parse(VALID_SIU_MESSAGE)

        assert len(results) == 1

        appt = results[0].appointment
        assert appt.appointment_id == "AP456"
        assert appt.appointment_datetime == "2025-05-02T14:00:00Z"
        assert appt.reason == "General Consultation"

        assert appt.patient is not None
        assert appt.patient.id == "P12345"
        assert appt.patient.first_name == "John"
        assert appt.patient.last_name == "Doe"
        assert appt.patient.dob == "1985-02-10"
        assert appt.patient.gender == "M"

        assert appt.provider is not None
        assert appt.provider.id == "D67890"
        assert "Smith" in appt.provider.name

    def test_parse_message_without_sch_segment(self):
        """Test parsing message missing SCH segment (non-strict mode)."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
PID|1||P999|||Patient^Test
PV1|1|O"""

        parser = SIUParser(strict_mode=False)
        results = parser.parse(message)

        assert len(results) == 1
        assert results[0].appointment.appointment_id == "UNKNOWN"
        assert "SCH segment not found" in results[0].warnings[0]

    def test_parse_message_without_sch_strict_mode(self):
        """Test that strict mode raises error for missing SCH."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
PID|1||P999"""

        parser = SIUParser(strict_mode=True)

        with pytest.raises(MissingSegmentError) as exc:
            parser.parse(message)

        assert "SCH" in str(exc.value)

    def test_parse_message_without_pid_segment(self):
        """Test parsing message missing PID segment."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
SCH|PL001|AP001"""

        parser = SIUParser()
        results = parser.parse(message)

        assert len(results) == 1
        assert results[0].appointment.patient is None
        assert any("PID segment not found" in w for w in results[0].warnings)

    def test_parse_message_without_pv1_segment(self):
        """Test parsing message missing PV1 segment."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
SCH|PL001|AP001
PID|1||P123|||Test^Patient"""

        parser = SIUParser()
        results = parser.parse(message)

        assert len(results) == 1
        assert results[0].appointment.provider is None
        assert any("PV1 segment not found" in w for w in results[0].warnings)

    def test_parse_invalid_message_type(self):
        """Test rejection of non-SIU messages."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||ADT^A01|1|P|2.5
PID|1||P123"""

        parser = SIUParser()

        with pytest.raises(InvalidMessageTypeError) as exc:
            parser.parse(message)

        assert "ADT^A01" in str(exc.value)
        assert "SIU^S12" in str(exc.value)

    def test_parse_wrong_trigger_event(self):
        """Test rejection of SIU with wrong trigger event."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S14|1|P|2.5
SCH|1|1"""

        parser = SIUParser()

        with pytest.raises(InvalidMessageTypeError):
            parser.parse(message)

    def test_parse_malformed_empty_message_type(self):
        """Test that message with empty/invalid message type raises error."""
        # MSH segment exists but message type field is empty
        message = "MSH|^~\\&|||||||||"

        parser = SIUParser()

        with pytest.raises(InvalidMessageTypeError) as exc:
            parser.parse(message)

        assert "SIU^S12" in str(exc.value)

    def test_parse_malformed_msh_segment(self):
        """Test that structurally malformed MSH segment raises error."""
        # MSH segment without encoding characters (too short)
        message = "MSH|"

        parser = SIUParser()

        with pytest.raises(MalformedSegmentError) as exc:
            parser.parse(message)

        assert "MSH" in str(exc.value)
        assert "encoding" in str(exc.value).lower()

    def test_parse_invalid_delimiters(self):
        """Test that invalid or conflicting delimiters raise error."""
        # MSH with alphanumeric delimiter (invalid)
        message = "MSH1^~\\&|"

        parser = SIUParser()

        with pytest.raises(MalformedSegmentError) as exc:
            parser.parse(message)

        assert "delimiter" in str(exc.value).lower()

    def test_parse_missing_msh_segment(self):
        """Test rejection of message without MSH."""
        # Message without MSH will result in empty results since
        # split_hl7_messages only recognizes messages starting with MSH
        message = """PID|1||P123
SCH|1|1"""

        parser = SIUParser()
        results = parser.parse(message)

        # No valid messages found (no MSH to start a message)
        assert len(results) == 0

    def test_parse_empty_content(self):
        """Test parsing empty content returns empty list."""
        parser = SIUParser()

        assert parser.parse("") == []
        assert parser.parse("   ") == []
        assert parser.parse("\n\n") == []

    def test_parse_multiple_messages(self):
        """Test parsing file with multiple messages."""
        content = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
SCH|PL001|AP001|||||||CHECKUP
PID|1||P001||Patient^First

MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|2|P|2.5
SCH|PL002|AP002|||||||FOLLOWUP
PID|1||P002||Patient^Second"""

        parser = SIUParser()
        results = parser.parse(content)

        assert len(results) == 2
        assert results[0].appointment.appointment_id == "AP001"
        assert results[0].appointment.patient.first_name == "First"
        assert results[0].source_message_index == 0

        assert results[1].appointment.appointment_id == "AP002"
        assert results[1].appointment.patient.id == "P002"
        assert results[1].source_message_index == 1

    def test_parse_message_with_extra_segments(self):
        """Test that extra/unknown segments are ignored."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
EVN|S12|20250502
SCH|PL001|AP001
PID|1||P123|||Test^Patient
PV1|1|O
OBX|1|TX|NOTE||This is a note
NTE|1||Additional information"""

        parser = SIUParser()
        results = parser.parse(message)

        # Should parse successfully, ignoring EVN, OBX, NTE
        assert len(results) == 1
        assert results[0].appointment.appointment_id == "AP001"

    def test_parse_message_with_empty_fields(self):
        """Test handling of empty fields throughout the message."""
        message = """MSH|^~\\&|||||20250502||SIU^S12|1|P|2.5
SCH||AP001
PID|1||P123|||||M"""

        parser = SIUParser()
        results = parser.parse(message)

        assert len(results) == 1
        appt = results[0].appointment
        assert appt.appointment_id == "AP001"
        assert appt.patient.id == "P123"
        assert appt.patient.first_name is None
        assert appt.patient.gender == "M"


# Tests for JSON serialization of appointments
class TestJSONOutput:
    """Tests for JSON serialization of appointments."""

    def test_appointment_to_json(self):
        """Test appointment serialization to JSON."""
        parser = SIUParser()
        results = parser.parse(VALID_SIU_MESSAGE)

        json_str = results[0].appointment.to_json()
        data = json.loads(json_str)

        assert data["appointment_id"] == "AP456"
        assert "patient" in data
        assert data["patient"]["id"] == "P12345"
        assert "provider" in data

    def test_appointment_dict_excludes_none(self):
        """Test that None values are excluded from dict output."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
SCH||AP001
PID|1||P123"""

        parser = SIUParser()
        results = parser.parse(message)

        data = results[0].appointment.to_dict()

        # Location should not be in dict since it's None
        assert "location" not in data or data.get("location") is not None
        # Provider should not be in dict since PV1 was missing
        assert "provider" not in data


# Tests for the convenience parsing function
class TestConvenienceFunction:
    """Tests for the convenience parsing function."""

    def test_parse_hl7_message_function(self):
        """Test the module-level parse function."""
        results = parse_hl7_message(VALID_SIU_MESSAGE)

        assert len(results) == 1
        assert results[0].appointment.appointment_id == "AP456"

    def test_parse_hl7_message_strict_mode(self):
        """Test strict mode via convenience function."""
        message = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
PID|1||P123"""

        with pytest.raises(MissingSegmentError):
            parse_hl7_message(message, strict=True)

    def test_stream_hl7_file(self):
        """Test streaming file parsing."""
        # Create a temporary file with multiple messages
        multi_message_content = (
            VALID_SIU_MESSAGE + "\n\n" + VALID_SIU_MESSAGE.replace("MSG001", "MSG002")
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".hl7", delete=False) as f:
            f.write(multi_message_content)
            temp_path = f.name

        try:
            # Stream parse the file
            results = list(stream_hl7_file(temp_path))

            assert len(results) == 2
            for result in results:
                assert result.appointment.appointment_id == "AP456"
        finally:
            os.unlink(temp_path)


# Tests for Exception detail formatting
class TestExceptionDetails:
    """Tests for exception detail formatting."""

    def test_hl7_parse_error_with_line_number(self):
        """Test HL7ParseError includes line number in message."""
        error = HL7ParseError("Test error", segment="MSH", line_number=42)
        assert "line=42" in str(error)
        assert error.line_number == 42

    def test_hl7_parse_error_without_line_number(self):
        """Test HL7ParseError works without line number."""
        error = HL7ParseError("Test error", segment="MSH", field_index=5)
        assert "line=" not in str(error)
        assert error.line_number is None

    def test_invalid_timestamp_error_details(self):
        """Test InvalidTimestampError includes value."""
        error = InvalidTimestampError("2025-99-99", "invalid month")
        assert "2025-99-99" in str(error)
        assert "invalid month" in str(error)
        assert error.value == "2025-99-99"

    def test_file_read_error_details(self):
        """Test FileReadError includes filepath and reason."""
        error = FileReadError("/path/to/file.hl7", "permission denied")
        assert "/path/to/file.hl7" in str(error)
        assert "permission denied" in str(error)
        assert error.filepath == "/path/to/file.hl7"
        assert error.reason == "permission denied"


# Tests for IO handler functionality
class TestIOHandler:
    """Tests for IO handler functionality."""

    def test_read_hl7_file_not_exists(self):
        """Test reading non-existent file raises error."""
        with pytest.raises(FileReadError) as exc:
            read_hl7_file("nonexistent_file.hl7")

        assert "does not exist" in str(exc.value)

    def test_read_hl7_file_is_directory(self):
        """Test reading directory raises error."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(FileReadError) as exc:
                read_hl7_file(temp_dir)

            assert "not a file" in str(exc.value)

    def test_read_hl7_file_encoding_fallback(self):
        """Test encoding fallback when primary encoding fails."""
        # Create a file with content that would fail UTF-8 but work with latin-1
        test_content = "MSH|^~\\&|Test|Test\r\nPID|1|Test"

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".hl7", delete=False) as f:
            # Write as bytes that are invalid UTF-8 but valid latin-1
            f.write(test_content.encode("latin-1"))
            temp_path = f.name

        try:
            content = read_hl7_file(temp_path)
            assert "MSH|^~\\&|Test|Test" in content
        finally:
            os.unlink(temp_path)

    def test_stream_hl7_file_not_exists(self):
        """Test streaming non-existent file raises error."""
        with pytest.raises(FileReadError) as exc:
            list(stream_hl7_file("nonexistent_file.hl7"))

        assert "does not exist" in str(exc.value)

    def test_parse_hl7_file_with_io_error(self):
        """Test parse_hl7_file handles IO errors."""

        with pytest.raises(FileReadError) as exc:
            parse_hl7_file("nonexistent_file.hl7")

        assert "does not exist" in str(exc.value)

    def test_write_json_output(self):
        """Test JSON output writing functionality."""
        # Parse a message to get results
        parser = SIUParser()
        results = parser.parse(VALID_SIU_MESSAGE)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            # Test pretty printing
            write_json_output(results, temp_path, pretty=True)

            with open(temp_path, "r") as f:
                content = f.read()
                assert '"appointment_id": "AP456"' in content
                assert "  " in content  # Pretty printing with indentation

            # Test compact output
            write_json_output(results, temp_path, pretty=False)

            with open(temp_path, "r") as f:
                content = f.read()
                assert "appointment_id" in content
                # Compact JSON still has some spacing but no indentation
                assert not content.startswith("  ")  # No leading indentation
        finally:
            os.unlink(temp_path)
