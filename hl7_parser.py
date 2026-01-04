#!/usr/bin/env python3
"""
HL7 SIU S12 Appointment Parser - Command Line Interface

A robust parser for HL7 v2.x SIU scheduling messages that converts
appointment data to structured JSON.

Usage:
    python hl7_parser.py input.hl7
    python hl7_parser.py input.hl7 -o output.json
    python hl7_parser.py input.hl7 --strict --verbose
"""

import argparse
import json
import sys
import itertools
from pathlib import Path
from typing import List

# Add src to path for direct execution
sys.path.insert(0, str(Path(__file__).parent))

from src.io_handler import parse_hl7_file, stream_hl7_file
from src.parser import ParseResult
from src.exceptions import HL7ParseError, FileReadError


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        prog="hl7_parser",
        description="Parse HL7 SIU S12 messages and convert to JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s appointment.hl7
      Parse file and output JSON to stdout
      
  %(prog)s appointment.hl7 -o result.json
      Parse file and save output to result.json
      
  %(prog)s appointment.hl7 --strict
      Parse with strict validation (fail on missing optional fields)
      
  %(prog)s appointment.hl7 --compact
      Output compact JSON without formatting
""",
    )

    parser.add_argument("input_file", type=Path, help="Path to the HL7 file to parse")

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        dest="output_file",
        help="Output file path (default: stdout)",
    )

    parser.add_argument(
        "-s",
        "--strict",
        action="store_true",
        help="Enable strict mode (fail on missing optional segments)",
    )

    parser.add_argument(
        "-c",
        "--compact",
        action="store_true",
        help="Output compact JSON (no indentation)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed parsing information and warnings",
    )

    parser.add_argument(
        "--stream",
        action="store_true",
        help="Stream parse large files (memory efficient)",
    )

    parser.add_argument("--version", action="version", version="%(prog)s 1.0.0")

    return parser


def format_output(
    results: List[ParseResult], compact: bool = False, verbose: bool = False
) -> str:
    """
    Format parse results as JSON string.

    Args:
        results: List of parse results
        compact: If True, output without indentation
        verbose: If True, include all metadata

    Returns:
        JSON string
    """
    indent = None if compact else 2

    if verbose:
        output = [r.to_dict() for r in results]
    else:
        # Simplified output - just appointments
        output = [r.appointment.to_dict() for r in results]

    # Single message: output just the object
    # Multiple messages: output as array
    if len(output) == 1:
        return json.dumps(output[0], indent=indent)

    return json.dumps(output, indent=indent)


# Streamed output for large files
def stream_format_output(
    results_iter, compact: bool = False, verbose: bool = False, output_file=None
) -> None:
    """
    Format and output parse results from a streaming iterator.

    For file output: writes JSON array incrementally
    For stdout: writes JSON Lines format (one object per line)

    Args:
        results_iter: Iterator of ParseResult objects
        compact: If True, output without indentation
        verbose: If True, include all metadata
        output_file: If provided, write to file as JSON array; otherwise use JSON Lines to stdout
    """
    indent = None if compact else 2
    first = True

    if output_file:
        # Write JSON array incrementally to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("[\n")
            for result in results_iter:
                if not first:
                    f.write(",\n")
                if verbose:
                    output = result.to_dict()
                else:
                    output = result.appointment.to_dict()
                json.dump(output, f, indent=indent)
                first = False
            f.write("\n]\n")
    else:
        # JSON Lines format for stdout (memory efficient)
        for result in results_iter:
            if verbose:
                output = result.to_dict()
            else:
                output = result.appointment.to_dict()
            print(json.dumps(output, indent=indent))


def print_warnings(results: List[ParseResult]) -> None:
    """Print any warnings from parsing to stderr."""
    for i, result in enumerate(results):
        if result.warnings:
            msg_label = f"Message {i + 1}" if len(results) > 1 else "Message"
            for warning in result.warnings:
                print(f"Warning ({msg_label}): {warning}", file=sys.stderr)


def main(args: List[str] = None) -> int:
    """
    Main entry point for the CLI.

    Args:
        args: Command line arguments (uses sys.argv if None)

    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    parser = create_argument_parser()
    parsed_args = parser.parse_args(args)

    try:
        # Parse the file
        if parsed_args.stream:
            # For streaming, process results incrementally for memory efficiency
            results_iter = stream_hl7_file(
                parsed_args.input_file, strict=parsed_args.strict
            )

            # Check if there are any results by trying to get the first one
            try:
                first_result = next(results_iter)
            except StopIteration:
                # No results
                print("No valid SIU messages found in file", file=sys.stderr)
                return 1

            # For verbose mode, we need to collect all results to show warnings
            # This reduces memory efficiency but is necessary for verbose output
            if parsed_args.verbose:
                results_list = [first_result]
                results_list.extend(results_iter)
                print_warnings(results_list)
                results_iter = iter(results_list)
                result_count = len(results_list)
            else:
                # Reconstruct iterator for non-verbose mode
                results_iter = itertools.chain([first_result], results_iter)
                result_count = None  # Don't count in streaming mode

            # Format and output results
            if parsed_args.output_file:
                # For file output, write incrementally as JSON array
                stream_format_output(
                    results_iter,
                    compact=parsed_args.compact,
                    verbose=parsed_args.verbose,
                    output_file=parsed_args.output_file,
                )
                if parsed_args.verbose:
                    print(
                        f"Output written to: {parsed_args.output_file}", file=sys.stderr
                    )
            else:
                # For stdout, use JSON Lines format (one JSON object per line)
                stream_format_output(
                    results_iter,
                    compact=parsed_args.compact,
                    verbose=parsed_args.verbose,
                )

            # Report summary in verbose mode
            if parsed_args.verbose and result_count is not None:
                msg = "message" if result_count == 1 else "messages"
                print(f"Successfully parsed {result_count} {msg}", file=sys.stderr)
            elif parsed_args.verbose:
                print("Streaming completed successfully", file=sys.stderr)

        else:
            # Regular parsing
            results = parse_hl7_file(parsed_args.input_file, strict=parsed_args.strict)

            if not results:
                print("No valid SIU messages found in file", file=sys.stderr)
                return 1

            # Show warnings if verbose
            if parsed_args.verbose:
                print_warnings(results)

            # Format output
            json_output = format_output(
                results, compact=parsed_args.compact, verbose=parsed_args.verbose
            )

            # Write output
            if parsed_args.output_file:
                with open(parsed_args.output_file, "w", encoding="utf-8") as f:
                    f.write(json_output)
                    f.write("\n")
                if parsed_args.verbose:
                    print(
                        f"Output written to: {parsed_args.output_file}", file=sys.stderr
                    )
            else:
                print(json_output)

            # Report summary in verbose mode
            if parsed_args.verbose:
                count = len(results)
                msg = "message" if count == 1 else "messages"
                print(f"Successfully parsed {count} {msg}", file=sys.stderr)

        return 0

    except FileReadError as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 1

    except HL7ParseError as e:
        print(f"Parse error: {e}", file=sys.stderr)
        return 2

    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if parsed_args.verbose:
            import traceback

            traceback.print_exc()
        return 3


if __name__ == "__main__":
    sys.exit(main())
