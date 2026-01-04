"""Unit tests for HL7 message tokenizer."""

from src.tokenizer import HL7Tokenizer, Segment, split_hl7_messages


# Tests for HL7 Tokenizer
class TestHL7Tokenizer:
    """Tests for the HL7 tokenizer."""

    def test_tokenize_simple_message(self):
        """Test tokenizing a basic HL7 message."""
        message = "MSH|^~\\&|SENDER|FACILITY|RECEIVER|DEST|20250502||SIU^S12|12345|P|2.5\r\nSCH|1234"

        tokenizer = HL7Tokenizer()
        segments = tokenizer.tokenize(message)

        assert len(segments) == 2
        assert segments[0].segment_type == "MSH"
        assert segments[1].segment_type == "SCH"

    def test_msh_field_separator_detection(self):
        """Test that field separator is correctly detected from MSH."""
        message = "MSH|^~\\&|TEST"

        tokenizer = HL7Tokenizer()
        tokenizer.tokenize(message)

        assert tokenizer.delimiters.field == "|"
        assert tokenizer.delimiters.component == "^"
        assert tokenizer.delimiters.repetition == "~"
        assert tokenizer.delimiters.escape == "\\"
        assert tokenizer.delimiters.subcomponent == "&"

    def test_custom_delimiters(self):
        """Test message with non-standard delimiters."""
        # Using # as field separator
        message = "MSH#!~\\&#SENDER#FACILITY"

        tokenizer = HL7Tokenizer()
        tokenizer.tokenize(message)

        assert tokenizer.delimiters.field == "#"
        assert tokenizer.delimiters.component == "!"

    def test_handles_crlf_endings(self):
        """Test handling of Windows-style line endings."""
        message = "MSH|^~\\&|TEST\r\nPID|1|P123\r\nPV1|1"

        tokenizer = HL7Tokenizer()
        segments = tokenizer.tokenize(message)

        assert len(segments) == 3

    def test_handles_cr_only_endings(self):
        """Test handling of Mac-style line endings."""
        message = "MSH|^~\\&|TEST\rPID|1|P123\rPV1|1"

        tokenizer = HL7Tokenizer()
        segments = tokenizer.tokenize(message)

        assert len(segments) == 3

    def test_handles_lf_only_endings(self):
        """Test handling of Unix-style line endings."""
        message = "MSH|^~\\&|TEST\nPID|1|P123\nPV1|1"

        tokenizer = HL7Tokenizer()
        segments = tokenizer.tokenize(message)

        assert len(segments) == 3

    def test_skips_empty_lines(self):
        """Test that empty lines are ignored."""
        message = "MSH|^~\\&|TEST\n\n\nPID|1|P123\n\nPV1|1"

        tokenizer = HL7Tokenizer()
        segments = tokenizer.tokenize(message)

        assert len(segments) == 3

    def test_empty_message_returns_empty_list(self):
        """Test empty input returns empty list."""
        tokenizer = HL7Tokenizer()

        assert tokenizer.tokenize("") == []
        assert tokenizer.tokenize("   ") == []
        assert tokenizer.tokenize("\n\n") == []


# Tests for Segment class
class TestSegment:
    """Tests for the Segment class."""

    def test_get_field_valid_index(self):
        """Test retrieving a valid field."""
        segment = Segment(
            segment_type="PID",
            fields=["PID", "1", "P12345", "ALT123", "", "Doe^John"],
            raw_text="PID|1|P12345|ALT123||Doe^John",
        )

        assert segment.get_field(2) == "P12345"
        assert segment.get_field(5) == "Doe^John"

    def test_get_field_out_of_range(self):
        """Test retrieving field beyond available fields."""
        segment = Segment(segment_type="PID", fields=["PID", "1"], raw_text="PID|1")

        assert segment.get_field(99) == ""
        assert segment.get_field(99, "default") == "default"

    def test_get_field_empty_value(self):
        """Test retrieving an empty field."""
        segment = Segment(
            segment_type="PID", fields=["PID", "1", "", "P123"], raw_text="PID|1||P123"
        )

        assert segment.get_field(2) == ""
        assert segment.get_field(2, "N/A") == "N/A"

    def test_get_component(self):
        """Test extracting component from a field."""
        segment = Segment(
            segment_type="PID",
            fields=["PID", "1", "P12345", "", "", "Doe^John^M"],
            raw_text="PID|1|P12345|||Doe^John^M",
        )

        assert segment.get_component(5, 0) == "Doe"
        assert segment.get_component(5, 1) == "John"
        assert segment.get_component(5, 2) == "M"

    def test_get_component_missing(self):
        """Test getting missing component returns default."""
        segment = Segment(
            segment_type="PID", fields=["PID", "1", "Simple"], raw_text="PID|1|Simple"
        )

        assert segment.get_component(2, 5) == ""
        assert segment.get_component(2, 5, default="N/A") == "N/A"

    def test_get_subcomponent(self):
        """Test extracting subcomponent from a field."""
        segment = Segment(
            segment_type="PID",
            fields=["PID", "1", "", "", "", "Doe&Jr^John"],
            raw_text="PID|1||||Doe&Jr^John",
        )

        assert segment.get_subcomponent(5, 0, 0) == "Doe"
        assert segment.get_subcomponent(5, 0, 1) == "Jr"


# Tests for splitting multiple messages
class TestSplitMessages:
    """Tests for splitting multiple messages from a file."""

    def test_split_single_message(self):
        """Test splitting content with one message."""
        content = "MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5\nPID|1"

        messages = split_hl7_messages(content)

        assert len(messages) == 1

    def test_split_multiple_messages(self):
        """Test splitting content with multiple messages."""
        content = """MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|1|P|2.5
PID|1|P001
SCH|1001

MSH|^~\\&|SND|FAC||RCV|20250502||SIU^S12|2|P|2.5
PID|1|P002
SCH|1002"""

        messages = split_hl7_messages(content)

        assert len(messages) == 2
        assert "P001" in messages[0]
        assert "P002" in messages[1]

    def test_split_handles_empty_lines(self):
        """Test that empty lines between messages are handled."""
        content = """MSH|^~\\&|TEST
PID|1


MSH|^~\\&|TEST2
PID|2"""

        messages = split_hl7_messages(content)

        assert len(messages) == 2

    def test_empty_content_returns_empty_list(self):
        """Test empty content returns empty list."""
        assert split_hl7_messages("") == []
        assert split_hl7_messages("   ") == []
