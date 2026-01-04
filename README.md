# HL7 SIU S12 Appointment Parser

A Python module for parsing HL7 v2.x SIU (Scheduling Information Unsolicited) S12 messages and converting them to structured JSON.

## Overview

Healthcare systems frequently exchange scheduling data using HL7 v2.x messages. This parser handles the conversion of legacy HL7 wire format into structured JSON suitable for modern APIs, analytics pipelines, or downstream services.

### Key Features

- **Manual HL7 parsing** - No external HL7 libraries; demonstrates understanding of the wire format
- **Robust error handling** - Graceful degradation for missing optional fields
- **Multiple message support** - Process files containing one or many SIU messages
- **ISO 8601 timestamps** - Automatic normalization of HL7 timestamps
- **Clean architecture** - Separation of I/O, parsing, and domain logic
- **Comprehensive tests** - Unit and integration test coverage
- **CLI interface** - Easy command-line usage

## Project Structure

```
Tamim-Islam_01825373231/
├── src/
│   ├── __init__.py          # Package initialization
│   ├── exceptions.py        # Custom exception classes
│   ├── models.py            # Domain models (Appointment, Patient, Provider)
│   ├── tokenizer.py         # HL7 message tokenization
│   ├── timestamps.py        # HL7 timestamp to ISO 8601 conversion
│   ├── extractors.py        # Segment-specific data extractors
│   ├── parser.py            # Main SIU parser orchestration
│   └── io_handler.py        # File I/O operations
├── tests/
│   ├── conftest.py          # Test configuration
│   ├── test_timestamps.py   # Timestamp parsing tests
│   ├── test_tokenizer.py    # Tokenizer tests
│   ├── test_extractors.py   # Extractor tests
│   └── test_parser.py       # Integration tests
├── samples/                  # Sample HL7 files
├── hl7_parser.py            # CLI entry point
├── Dockerfile               # Container build file
├── requirements.txt         # Python dependencies
└── README.md
```

## Installation

### Prerequisites

- Python 3.8 or newer
- pip (Python package manager)

### Setup

1. Clone or download this repository
2. Create a virtual environment (recommended):

```bash
# Create virtual environment:
python -m venv venv

# Activate venv on Mac/Linux:
source venv/bin/activate

# Activate venv on Windows: 
venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Command Line Interface

Parse a single file and output JSON to stdout:

```bash
python hl7_parser.py samples/single_appointment.hl7
```

Save output to a file:

```bash
python hl7_parser.py samples/single_appointment.hl7 -o result.json
```

Parse with strict validation (fail on missing optional segments):

```bash
python hl7_parser.py samples/single_appointment.hl7 --strict
```

Verbose output with warnings:

```bash
python hl7_parser.py samples/minimal.hl7 --verbose
```

Stream parse large files:

```bash
python hl7_parser.py samples/large_file.hl7 --stream
```

Stream parse and save to file:

```bash
python hl7_parser.py samples/large_file.hl7 --stream -o results.json
```

Stream parse with strict validation:

```bash
python hl7_parser.py samples/large_file.hl7 --stream --strict
```

### Streaming Output Formats

**File Output (`-o filename.json`)**: Produces standard JSON array format:
```json
[
  {"appointment_id": "1", ...},
  {"appointment_id": "2", ...}
]
```

**Stdout Output**: Uses JSON Lines format for memory efficiency:
```json
{"appointment_id": "1", ...}
{"appointment_id": "2", ...}
```

### Programmatic Usage

```python
from src.parser import SIUParser
from src.io_handler import parse_hl7_file

# Parse from file
results = parse_hl7_file("appointment.hl7")

for result in results:
    appointment = result.appointment
    print(f"Appointment ID: {appointment.appointment_id}")
    print(f"Patient: {appointment.patient.first_name} {appointment.patient.last_name}")
    print(appointment.to_json())

# Parse from string
parser = SIUParser()
results = parser.parse(hl7_message_string)

# Enable strict mode
parser = SIUParser(strict_mode=True)
```

### Example Output

```json
{
  "appointment_id": "123456",
  "appointment_datetime": "2025-05-02T14:00:00Z",
  "patient": {
    "id": "P12345",
    "first_name": "John",
    "last_name": "Doe",
    "dob": "1985-02-10",
    "gender": "M"
  },
  "provider": {
    "id": "D67890",
    "name": "Dr. Jane Smith"
  },
  "location": "Clinic A Room 203",
  "reason": "General Consultation",
  "message_control_id": "MSG123456",
  "message_datetime": "2025-05-02T13:00:00Z"
}
```

## Running Tests

Run all tests:

```bash
pytest tests/ -v
```

Run with coverage report:

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Run specific test file:

```bash
pytest tests/test_parser.py -v
```

## Docker

Build the container:

```bash
docker build -t hl7-parser .
```

Run the parser (choose based on your shell):

- **Unix-like systems (Linux/Mac)**:

```bash
docker run -v "$PWD/samples:/data" hl7-parser /data/single_appointment.hl7
```

- **Windows CMD**:

```cmd
docker run -v "%CD%/samples:/data" hl7-parser /data/single_appointment.hl7
```

## Design Decisions

### Architecture

The parser follows a layered architecture with clear separation of concerns:

1. **I/O Layer** (`io_handler.py`) - Handles file reading with encoding detection
2. **Tokenization Layer** (`tokenizer.py`) - Converts raw HL7 text to structured segments
3. **Extraction Layer** (`extractors.py`) - Segment-specific field extraction
4. **Domain Layer** (`models.py`) - Clean domain models independent of HL7 format
5. **Orchestration Layer** (`parser.py`) - Coordinates the parsing pipeline

### Why No HL7 Libraries?

The exercise specifically evaluates understanding of the HL7 wire format. Manual parsing demonstrates:

- Knowledge of HL7 delimiters and encoding characters
- Understanding of positional field access
- Handling of components and subcomponents
- Awareness of segment structure variations

### Error Handling Strategy

- **Strict mode**: Raises exceptions for missing required segments (SCH, PID)
- **Default mode**: Collects warnings and continues with available data
- **Invalid message types**: Always rejected with clear error messages
- **Malformed input**: Specific exception types with context information

### Timestamp Normalization

HL7 timestamps come in many formats (YYYYMMDD to YYYYMMDDHHMMSS.SSSS+ZZZZ). The parser:

- Accepts all valid HL7 timestamp formats
- Converts to ISO 8601 for consistency
- Assumes UTC when timezone is not specified
- Validates date/time component ranges

## Supported HL7 Segments

| Segment | Purpose | Required |
|---------|---------|----------|
| MSH | Message header, type validation | Yes |
| SCH | Scheduling information (appointment ID, time, reason) | Yes* |
| PID | Patient demographics | Yes* |
| PV1 | Provider/visit information | No |

\* Required in strict mode; optional in default mode

## Edge Cases Handled

- **Missing segments**: Graceful handling with warnings (default) or exceptions (strict)
- **Empty fields**: Safe extraction with default values
- **Multiple messages**: Proper splitting and individual parsing
- **Invalid message types**: Rejected with specific error message
- **Custom delimiters**: Detected from MSH encoding characters
- **Various line endings**: CR, LF, and CRLF all supported
- **Extra segments**: Ignored without error (EVN, OBX, NTE, etc.)

## Assumptions and Tradeoffs

### Assumptions

1. **Message type**: Only SIU^S12 messages are processed; other SIU triggers (S13, S14) are rejected
2. **Encoding**: Files are in UTF-8, Latin-1, or Windows-1252 encoding
3. **Segment order**: Segments can appear in any order within a message
4. **Single occurrence**: For repeating segments (like OBX), only the first is processed

### Tradeoffs

1. **Memory vs. streaming**: By default, entire file is loaded. Use `--stream` for large files
2. **Strictness**: Default permissive mode may mask data quality issues
3. **Field coverage**: Only essential appointment fields are extracted; extensions are possible but not implemented
4. **Timezone handling**: UTC assumed for timestamps without timezone (common in healthcare)

## Extending the Parser

### Extending the Parser with New HL7 Segments

The parser is designed with an extensible architecture that allows you to easily add support for additional HL7 segments. Follow these steps to add a new segment:

#### 1. Create an Extractor Class

Create a new extractor class in `src/extractors.py` that inherits from `SegmentExtractor`:

```python
class AIGExtractor(SegmentExtractor):
    """Extracts Appointment Information - General Resource data from AIG segments."""

    def extract_resource(self, segment: Segment) -> Optional[str]:
        """Extract resource identifier from AIG-3."""
        return segment.get_field(3, "")
```

#### 2. Add Field to the Appointment Model

Add the new field to the `Appointment` dataclass in `src/models.py`:

```python
@dataclass
class Appointment:
    # ... existing fields ...

    # Extension fields for additional segments
    resource: Optional[str] = None  # AIG segment resource identifier
```

#### 3. Update the to_dict() Method

Update the `to_dict()` method in the `Appointment` class to include the new field:

```python
def to_dict(self) -> dict:
    result = {}

    # ... existing field mappings ...

    if self.resource:
        result["resource"] = self.resource

    return result
```

#### 4. Integrate in the Parser

Update `src/parser.py` to use the new extractor:

```python
# Add import
from .extractors import MSHExtractor, SCHExtractor, PIDExtractor, PV1Extractor, AIGExtractor

# In _parse_single_message method:
# Create extractor instance
aig_extractor = AIGExtractor(delimiters)

# Extract data
resource = None
aig_segment = segment_map.get("AIG")
if aig_segment:
    resource = aig_extractor.extract_resource(aig_segment)

# Add to appointment constructor
appointment = Appointment(
    # ... existing fields ...
    resource=resource,
)
```

#### Example: Complete AIG Segment Support

Here's a complete example adding AIG (Appointment Information - General Resource) segment support:

**src/extractors.py:**
```python
class AIGExtractor(SegmentExtractor):
    def extract_resource(self, segment: Segment) -> Optional[str]:
        return segment.get_field(3, "")
```

**src/models.py:**
```python
@dataclass
class Appointment:
    # ... existing fields ...
    resource: Optional[str] = None

    def to_dict(self) -> dict:
        # ... existing code ...
        if self.resource:
            result["resource"] = self.resource
        return result
```

**src/parser.py:**
```python
# Add to imports
from .extractors import AIGExtractor

# In parser method:
aig_extractor = AIGExtractor(delimiters)

# Extract AIG data
resource = None
aig_segment = segment_map.get("AIG")
if aig_segment:
    resource = aig_extractor.extract_resource(aig_segment)

# Include in Appointment creation
appointment = Appointment(
    appointment_id=appointment_id,
    # ... other fields ...
    resource=resource,
)
```

#### Testing Your Extension

1. Create a test HL7 file with your new segment inside samples folder
2. Run the parser: `python hl7_parser.py samples/your_test_file.hl7`
3. Verify the new field appears in the JSON output
4. Add unit tests for your extractor in `tests/test_extractors.py`

This modular architecture ensures that new HL7 segments can be added without modifying the core parsing logic.

## License

This project is provided for assessment purposes.

## Author

Tamim Islam \
Project Engineer at Raktch Technology & Software
