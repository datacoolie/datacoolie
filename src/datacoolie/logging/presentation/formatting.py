"""Shared formatting for optional execution correlation fields.

The persisted log contract stores ``dataflow_id``, ``dataflow_run_id`` and
``event_name`` as separate fields.  This module owns their human-readable
console representation without changing the original ``message`` or the
record shared by Python logging handlers.
"""

from __future__ import annotations

import copy
import logging
from typing import Any

DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s%(datacoolie_context)s - %(message)s"
)


def _optional_text(value: Any) -> str:
    """Return a non-empty string or an empty presentation value."""
    return value if isinstance(value, str) and value else ""


def _escape_presentation_text(value: str) -> str:
    """Keep control characters in correlation fields from changing the line."""
    escaped: list[str] = []
    for character in value:
        codepoint = ord(character)
        if character == "\n":
            escaped.append(r"\n")
        elif character == "\r":
            escaped.append(r"\r")
        elif character == "\t":
            escaped.append(r"\t")
        elif codepoint < 0x20 or codepoint == 0x7F:
            escaped.append(f"\\x{codepoint:02x}")
        else:
            escaped.append(character)
    return "".join(escaped)


def format_context_suffix(
    dataflow_id: Any = None,
    dataflow_run_id: Any = None,
    event_name: Any = None,
) -> str:
    """Render execution context as optional bracketed console segments.

    ``dataflow_id`` gets its own segment.  A run ID and event name share a
    segment and are joined with ``:`` when both are present.  Missing or
    malformed values do not produce empty brackets or dangling punctuation.
    """
    dataflow = _escape_presentation_text(_optional_text(dataflow_id))
    run_id = _escape_presentation_text(_optional_text(dataflow_run_id))
    event = _escape_presentation_text(_optional_text(event_name))

    parts: list[str] = []
    if dataflow:
        parts.append(f"[{dataflow}]")
    if run_id or event:
        run_event = f"{run_id}:{event}" if run_id and event else run_id or event
        parts.append(f"[{run_event}]")
    return " ".join(parts)


def prepare_record(record: logging.LogRecord) -> logging.LogRecord:
    """Copy *record* and add safe, presentation-only context attributes."""
    isolated = copy.copy(record)
    isolated.__dict__ = record.__dict__.copy()

    # These defaults make custom formats such as ``%(event_name)s`` safe for
    # ordinary records that do not carry an event.  Invalid values are omitted
    # consistently with the persisted LogRecord projection.
    dataflow_id = _optional_text(getattr(record, "dataflow_id", None))
    dataflow_run_id = _optional_text(getattr(record, "dataflow_run_id", None))
    event_name = _optional_text(getattr(record, "event_name", None))
    isolated.dataflow_id = _escape_presentation_text(dataflow_id)
    isolated.dataflow_run_id = _escape_presentation_text(dataflow_run_id)
    isolated.event_name = _escape_presentation_text(event_name)
    context = format_context_suffix(
        dataflow_id,
        dataflow_run_id,
        event_name,
    )
    # The default format places this optional value immediately after the
    # logger name.  Keep the leading space conditional so records without
    # context do not leave a trailing/double separator behind.
    isolated.datacoolie_context = f" {context}" if context else ""
    # Formatter.format caches traceback text on the record.  Keep that cache
    # on the copy so another handler never observes presentation side effects.
    isolated.exc_text = None
    return isolated


class ContextFormatter(logging.Formatter):
    """Plain formatter that safely renders execution context placeholders."""

    def format(self, record: logging.LogRecord) -> str:
        return super().format(prepare_record(record))


__all__ = [
    "DEFAULT_LOG_FORMAT",
    "ContextFormatter",
    "format_context_suffix",
    "prepare_record",
]
