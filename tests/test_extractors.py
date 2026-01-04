"""Unit tests for segment extractors."""

from src.extractors import MSHExtractor, SCHExtractor, PIDExtractor, PV1Extractor
from src.tokenizer import Segment, Delimiters


# Helper to create a Segment from raw text
def make_segment(segment_type: str, raw_text: str, delimiter: str = "|") -> Segment:
    """Helper to create a Segment from raw text."""
    fields = raw_text.split(delimiter)
    return Segment(segment_type=segment_type, fields=fields, raw_text=raw_text)


# Tests for MSHExtractor
class TestMSHExtractor:
    """Tests for MSH segment extraction."""

    def setup_method(self):
        self.extractor = MSHExtractor(Delimiters())

    def test_extract_message_type(self):
        """Test extracting message type from MSH-9."""
        # MSH with proper field separator handling
        segment = Segment(
            segment_type="MSH",
            fields=[
                "MSH",
                "|",
                "^~\\&",
                "SENDER",
                "FAC",
                "",
                "RCV",
                "20250502130000",
                "",
                "SIU^S12",
                "12345",
            ],
            raw_text="MSH|^~\\&|SENDER|FAC||RCV|20250502130000||SIU^S12|12345",
        )

        msg_type, trigger = self.extractor.extract_message_type(segment)

        assert msg_type == "SIU"
        assert trigger == "S12"

    def test_extract_message_type_without_trigger(self):
        """Test extracting message type without trigger event."""
        segment = Segment(
            segment_type="MSH",
            fields=["MSH", "|", "^~\\&", "", "", "", "", "", "", "ACK"],
            raw_text="MSH|^~\\&|||||||ACK",
        )

        msg_type, trigger = self.extractor.extract_message_type(segment)

        assert msg_type == "ACK"
        assert trigger == ""

    def test_extract_control_id(self):
        """Test extracting message control ID from MSH-10."""
        segment = Segment(
            segment_type="MSH",
            fields=["MSH", "|", "^~\\&", "", "", "", "", "", "", "SIU^S12", "MSG12345"],
            raw_text="MSH|^~\\&|||||||SIU^S12|MSG12345",
        )

        control_id = self.extractor.extract_control_id(segment)

        assert control_id == "MSG12345"

    def test_extract_timestamp(self):
        """Test extracting and normalizing MSH-7 timestamp."""
        segment = Segment(
            segment_type="MSH",
            fields=["MSH", "|", "^~\\&", "", "", "", "", "20250502130000"],
            raw_text="MSH|^~\\&|||||20250502130000",
        )

        timestamp = self.extractor.extract_timestamp(segment)

        assert timestamp == "2025-05-02T13:00:00Z"


# Tests for SCHExtractor
class TestSCHExtractor:
    """Tests for SCH segment extraction."""

    def setup_method(self):
        self.extractor = SCHExtractor(Delimiters())

    def test_extract_appointment_id_filler(self):
        """Test extracting filler appointment ID from SCH-2."""
        segment = make_segment("SCH", "SCH|PLACER123|FILLER456")

        appt_id = self.extractor.extract_appointment_id(segment)

        assert appt_id == "FILLER456"

    def test_extract_appointment_id_fallback_to_placer(self):
        """Test fallback to placer ID when filler is empty."""
        segment = make_segment("SCH", "SCH|PLACER123|")

        appt_id = self.extractor.extract_appointment_id(segment)

        assert appt_id == "PLACER123"

    def test_extract_appointment_datetime(self):
        """Test extracting appointment datetime from SCH-11."""
        # SCH-11 with timing info in component 4
        # Field index 11 needs 10 empty fields between SCH and the datetime
        segment = make_segment("SCH", "SCH||APPT123|||||||||^30^MIN^20250502130000")

        dt = self.extractor.extract_appointment_datetime(segment)

        assert dt == "2025-05-02T13:00:00Z"

    def test_extract_appointment_datetime_sch10(self):
        """Test extracting appointment datetime from SCH-10."""
        # SCH-10 with timing info in component 4
        # Field index 10 needs 9 empty fields between SCH and the datetime
        segment = make_segment("SCH", "SCH||APPT123||||||||^30^MIN^20250502130000|")

        dt = self.extractor.extract_appointment_datetime(segment)

        assert dt == "2025-05-02T13:00:00Z"

    def test_extract_appointment_datetime_first_component(self):
        """Test datetime in first component of SCH-11."""

        segment = make_segment("SCH", "SCH||APPT123|||||||||20250502140000")

        dt = self.extractor.extract_appointment_datetime(segment)

        assert dt == "2025-05-02T14:00:00Z"

    def test_extract_reason(self):
        """Test extracting appointment reason from SCH-7."""
        segment = make_segment("SCH", "SCH||APPT123|||||CONSULT^General Consultation")

        reason = self.extractor.extract_reason(segment)

        assert reason == "General Consultation"

    def test_extract_reason_code_only(self):
        """Test reason extraction with code only."""
        segment = make_segment("SCH", "SCH||APPT123|||||FOLLOWUP")

        reason = self.extractor.extract_reason(segment)

        assert reason == "FOLLOWUP"

    def test_extract_location(self):
        """Test extracting location from SCH-23."""
        # Field index 23 needs 21 pipes after SCH|| to place data correctly
        segment = make_segment("SCH", "SCH||" + "|" * 21 + "Clinic A^Room 203^Bed 1")

        location = self.extractor.extract_location(segment)

        assert "Clinic A" in location


# Tests for PIDExtractor
class TestPIDExtractor:
    """Tests for PID segment extraction."""

    def setup_method(self):
        self.extractor = PIDExtractor(Delimiters())

    def test_extract_patient_complete(self):
        """Test extracting complete patient information."""
        segment = make_segment("PID", "PID|1||P12345^^^HOSP||Doe^John^M||19850210|M")

        patient = self.extractor.extract_patient(segment)

        assert patient.id == "P12345"
        assert patient.first_name == "John"
        assert patient.last_name == "Doe"
        assert patient.dob == "1985-02-10"
        assert patient.gender == "M"

    def test_extract_patient_minimal(self):
        """Test extracting patient with minimal data."""
        segment = make_segment("PID", "PID|1||P99999")

        patient = self.extractor.extract_patient(segment)

        assert patient.id == "P99999"
        assert patient.first_name is None
        assert patient.last_name is None

    def test_extract_patient_with_repetitions(self):
        """Test patient ID extraction with multiple IDs."""
        segment = make_segment("PID", "PID|1||PRIMARY123~SECONDARY456")

        patient = self.extractor.extract_patient(segment)

        # Should take the first (primary) ID
        assert patient.id == "PRIMARY123"

    def test_extract_patient_empty_name(self):
        """Test extraction when name field is empty."""
        segment = make_segment("PID", "PID|1||P12345||||19900101|F")

        patient = self.extractor.extract_patient(segment)

        assert patient.id == "P12345"
        assert patient.first_name is None
        assert patient.last_name is None
        assert patient.dob == "1990-01-01"
        assert patient.gender == "F"


# Tests for PV1Extractor
class TestPV1Extractor:
    """Tests for PV1 segment extraction."""

    def setup_method(self):
        self.extractor = PV1Extractor(Delimiters())

    def test_extract_provider_attending(self):
        """Test extracting attending physician from PV1-7."""
        segment = make_segment("PV1", "PV1|1|I|ROOM101||||D67890^Smith^Jane^^Dr.")

        provider = self.extractor.extract_provider(segment)

        assert provider is not None
        assert provider.id == "D67890"
        assert "Smith" in provider.name
        assert "Jane" in provider.name

    def test_extract_provider_fallback_to_admitting(self):
        """Test fallback to admitting doctor when attending is empty."""
        # PV1-7 empty, PV1-17 has data
        fields = ["PV1", "1", "I", ""] + [""] * 13 + ["D11111^Jones^Bob"]
        segment = Segment(segment_type="PV1", fields=fields, raw_text="|".join(fields))

        provider = self.extractor.extract_provider(segment)

        assert provider is not None
        assert provider.id == "D11111"

    def test_extract_provider_none_when_missing(self):
        """Test None returned when no provider data."""
        segment = make_segment("PV1", "PV1|1|I")

        provider = self.extractor.extract_provider(segment)

        assert provider is None

    def test_extract_location(self):
        """Test extracting location from PV1-3."""
        segment = make_segment("PV1", "PV1|1|I|ClinicA^201^A^MainHospital")

        location = self.extractor.extract_location(segment)

        assert "ClinicA" in location
        assert "201" in location
        assert "MainHospital" in location
