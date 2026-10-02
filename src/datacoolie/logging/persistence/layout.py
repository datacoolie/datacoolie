"""Pure helpers for logging partitions and file names."""

from __future__ import annotations

import re
import string
from datetime import datetime
from typing import Optional
from urllib.parse import quote

from datacoolie.logging.configuration.constants import DEFAULT_PARTITION_PATTERN
from datacoolie.utils.time import utc_now
from datacoolie.utils.path_utils import normalize_path

_PARTITION_TOKENS = {"year", "month", "day", "hour"}


def validate_partition_pattern(pattern: str) -> None:
    """Validate a partition template without performing I/O."""
    if not isinstance(pattern, str) or not pattern.strip():
        raise ValueError("partition_pattern must be a non-empty string")

    # Use the same grammar that ``str.format`` uses.  A regex-only check lets
    # malformed/unescaped braces through and moves the failure to path
    # creation (or, worse, writes a literal ``{year}`` directory).
    fields: list[str] = []
    literal_parts: list[str] = []
    try:
        parsed = list(string.Formatter().parse(pattern))
    except ValueError as exc:
        raise ValueError("partition_pattern contains malformed braces") from exc
    for literal, field_name, format_spec, conversion in parsed:
        if "{" in literal or "}" in literal:
            raise ValueError("partition_pattern cannot contain escaped or literal braces")
        literal_parts.append(literal)
        if field_name is None:
            continue
        if format_spec or conversion or field_name not in _PARTITION_TOKENS:
            raise ValueError(
                "partition_pattern must contain only simple {year}, {month}, {day}, "
                "or {hour} placeholders"
            )
        fields.append(field_name)
    if not fields:
        raise ValueError(
            "partition_pattern must contain only {year}, {month}, {day}, or {hour} placeholders"
        )
    ordered_tokens = ("year", "month", "day", "hour")
    if tuple(fields) != ordered_tokens[: len(fields)]:
        raise ValueError(
            "partition_pattern placeholders must be an ordered prefix of "
            "{year}, {month}, {day}, and {hour}"
        )
    literal = "".join(literal_parts)
    if "%" in literal or any(character.isdigit() for character in literal):
        raise ValueError("partition_pattern literals cannot contain digits or '%' directives")

    # The syntax pass above guarantees every remaining brace is a real simple
    # field, so this segment check can stay deliberately small and readable.
    if any(
        not re.search(r"\{(?:year|month|day|hour)\}", segment)
        for segment in re.split(r"[/\\]", pattern)
    ):
        raise ValueError("each partition_pattern segment must contain a placeholder")


def format_partition_path(
    base_path: str,
    run_date: Optional[datetime] = None,
    pattern: str = DEFAULT_PARTITION_PATTERN,
) -> str:
    """Append a validated date partition to a base path."""
    validate_partition_pattern(pattern)
    timestamp = run_date or utc_now()
    partition = pattern.format(
        year=f"{timestamp.year:04d}",
        month=timestamp.strftime("%m"),
        day=timestamp.strftime("%d"),
        hour=timestamp.strftime("%H"),
    )
    return normalize_path(f"{base_path.rstrip('/')}/{partition}")


def safe_filename_token(value: object, *, fallback: str = "default") -> str:
    """Return a reversible path-safe token.

    Percent-encoding keeps distinct caller identifiers distinct while leaving
    ordinary UUID/job names readable. The raw identifier remains in the JSON
    record; this helper only determines the storage filename component.
    """
    raw = "" if value is None else str(value).strip()
    if not raw:
        return fallback
    token = quote(raw, safe="-_.~")
    # Leading/trailing dots are legal in many URI stores but problematic on
    # Windows filesystems; encode only those boundary characters.
    if raw.startswith(".") and token.startswith("."):
        token = "%2E" + token[1:]
    if raw.endswith(".") and token.endswith("."):
        token = token[:-1] + "%2E"
    return token or fallback


def build_job_stem(
    started_at: datetime,
    *,
    job_id: object,
    job_num: object,
    job_index: object,
) -> str:
    """Build the common job filename stem.

    The Driver's ``job_id`` is the identity of the complete lifecycle.  It is
    included once in the filename; no second per-logger or per-flush UUID is
    needed for either snapshot or batch output.
    """
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    return (
        f"{timestamp}_{job_num}_{job_index}_"
        f"{safe_filename_token(job_id)}"
    )
