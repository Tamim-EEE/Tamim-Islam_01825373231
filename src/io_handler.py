"""
File I/O operations for HL7 message processing.

Handles reading HL7 files from disk with proper encoding detection
and error handling.
"""

import os
from pathlib import Path
from typing import List, Union, Iterator

from .parser import SIUParser, ParseResult
from .exceptions import FileReadError


# Common encodings used in HL7 files
ENCODINGS_TO_TRY = ["utf-8", "latin-1", "cp1252", "ascii"]


# Read HL7 File
def read_hl7_file(filepath: Union[str, Path]) -> str:
    """
    Read an HL7 file from disk.

    Attempts multiple encodings to handle various file sources.

    Args:
        filepath: Path to the HL7 file

    Returns:
        File content as string

    Raises:
        FileReadError: If file cannot be read
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileReadError(str(filepath), "file does not exist")

    if not filepath.is_file():
        raise FileReadError(str(filepath), "path is not a file")

    # Try different encodings
    last_error = None
    for encoding in ENCODINGS_TO_TRY:
        try:
            with open(filepath, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError as e:
            last_error = e
            continue
        except IOError as e:
            raise FileReadError(str(filepath), str(e))

    # If all encodings failed
    raise FileReadError(
        str(filepath),
        f"could not decode file with any supported encoding: {last_error}",
    )


# Parse HL7 File
def parse_hl7_file(
    filepath: Union[str, Path], strict: bool = False
) -> List[ParseResult]:
    """
    Read and parse an HL7 file.

    Args:
        filepath: Path to the HL7 file
        strict: Enable strict validation mode

    Returns:
        List of parse results for each message in the file
    """
    content = read_hl7_file(filepath)
    parser = SIUParser(strict_mode=strict)
    return parser.parse(content)


# Stream Parse HL7 File
def stream_hl7_file(
    filepath: Union[str, Path], strict: bool = False
) -> Iterator[ParseResult]:
    """
    Stream parse an HL7 file, yielding results one at a time.

    Useful for large files with many messages where loading all
    results into memory at once is not desirable.

    Args:
        filepath: Path to the HL7 file
        strict: Enable strict validation mode

    Yields:
        ParseResult for each message
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileReadError(str(filepath), "file does not exist")

    if not filepath.is_file():
        raise FileReadError(str(filepath), "path is not a file")

    # Try to open with best encoding
    file_handle = None
    for encoding in ENCODINGS_TO_TRY:
        try:
            file_handle = open(filepath, "r", encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise FileReadError(
            str(filepath),
            "could not decode file with any supported encoding",
        )

    try:
        parser = SIUParser(strict_mode=strict)
        current_message_lines = []
        message_idx = 0

        for line_num, line in enumerate(file_handle, 1):
            stripped = line.rstrip("\r\n").strip()
            if not stripped:
                continue

            # New message starts with MSH
            if stripped.startswith("MSH"):
                # Process previous message if exists
                if current_message_lines:
                    message_text = "\n".join(current_message_lines)
                    try:
                        result = parser._parse_single_message(message_text, message_idx)
                        yield result
                        message_idx += 1
                    except Exception as e:
                        # For streaming, we can choose to skip or yield error
                        # For now, re-raise to maintain consistency
                        raise

                current_message_lines = [stripped]
            else:
                # Continue current message
                if current_message_lines:
                    current_message_lines.append(stripped)

        # Don't forget the last message
        if current_message_lines:
            message_text = "\n".join(current_message_lines)
            try:
                result = parser._parse_single_message(message_text, message_idx)
                yield result
            except Exception as e:
                raise

    finally:
        if file_handle:
            file_handle.close()


# Write JSON Output
def write_json_output(
    results: List[ParseResult], filepath: Union[str, Path], pretty: bool = True
) -> None:
    """
    Write parse results to a JSON file.

    Args:
        results: List of parse results
        filepath: Output file path
        pretty: If True, format JSON with indentation
    """
    import json

    output = [r.to_dict() for r in results]

    indent = 2 if pretty else None

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=indent)
