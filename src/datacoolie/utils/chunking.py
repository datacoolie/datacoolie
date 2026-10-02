"""Date/time chunk boundary utilities for the DataCoolie framework."""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from dateutil.relativedelta import relativedelta

from datacoolie.core.exceptions import ConfigurationError


_ISO_FRACTION_RE = re.compile(r"[.,](\d+)")
_ISO_OFFSET_FRACTION_RE = re.compile(
    r"[+-](?P<hours>\d{2})(?::?(?P<minutes>\d{2}))?"
    r"(?::?(?P<seconds>\d{2}))?[.,](?P<fraction>\d+)(?:[Zz])?$"
)


def validate_iso_fraction_precision(value: str) -> None:
    """Reject ISO fractions that cannot survive ``datetime`` parsing exactly.

    Python's :meth:`datetime.fromisoformat` keeps six microsecond digits and
    silently discards any further digits.  Extra digits made entirely of
    zeroes do not change the represented instant, so they remain valid.  A
    non-zero digit beyond the sixth would change the bound and must be
    rejected before parsing.

    The helper raises ``ValueError`` so callers can translate the failure into
    their own configuration or source error type without coupling this shared
    utility to a reader layer.
    """

    text = value.strip()
    for fraction in _ISO_FRACTION_RE.findall(text):
        if len(fraction) > 6 and any(digit != "0" for digit in fraction[6:]):
            raise ValueError(
                "ISO datetime fractional precision beyond six microsecond digits "
                "cannot be represented losslessly"
            )
    offset_match = _ISO_OFFSET_FRACTION_RE.search(text)
    if offset_match is not None:
        offset_parts = (
            offset_match.group("hours"),
            offset_match.group("minutes") or "00",
            offset_match.group("seconds") or "00",
        )
        fraction = offset_match.group("fraction")
        if all(part == "00" for part in offset_parts) and any(
            digit != "0" for digit in fraction
        ):
            raise ValueError(
                "ISO fractional zero offset cannot be represented losslessly"
            )


# ============================================================================
# Calendar boundary helpers
# ============================================================================


def _floor_boundary(dt: datetime, delta_kwargs: Dict[str, int]) -> datetime:
    """Floor *dt* to the start of the current calendar period.

    The period is determined by the highest-order key in *delta_kwargs*:

    * years → Jan 1 of the aligned year
    * months → 1st of the aligned month
    * weeks → Monday 00:00 of the current week
    * days → midnight of the current day
    * hours → start of the aligned hour
    * minutes → start of the aligned minute
    """
    tz = dt.tzinfo
    if "years" in delta_kwargs:
        n = delta_kwargs["years"]
        year = dt.year - ((dt.year - 1) % n) if n > 1 else dt.year
        return datetime(year, 1, 1, tzinfo=tz)
    if "months" in delta_kwargs:
        n = delta_kwargs["months"]
        month_idx = dt.month - 1  # 0-based
        floored_idx = month_idx - (month_idx % n)
        return datetime(dt.year, floored_idx + 1, 1, tzinfo=tz)
    if "weeks" in delta_kwargs:
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        return datetime(monday.year, monday.month, monday.day, tzinfo=tz)
    if "days" in delta_kwargs:
        return datetime(dt.year, dt.month, dt.day, tzinfo=tz)
    if "hours" in delta_kwargs:
        n = delta_kwargs["hours"]
        floored_hour = dt.hour - (dt.hour % n) if n > 1 else dt.hour
        return datetime(dt.year, dt.month, dt.day, floored_hour, tzinfo=tz)
    if "minutes" in delta_kwargs:
        n = delta_kwargs["minutes"]
        floored_min = dt.minute - (dt.minute % n) if n > 1 else dt.minute
        return datetime(dt.year, dt.month, dt.day, dt.hour, floored_min, tzinfo=tz)
    return dt


def _ceil_boundary(dt: datetime, delta_kwargs: Dict[str, int]) -> datetime:
    """Find the first calendar boundary >= *dt*.

    If *dt* is already at a boundary, returns *dt* unchanged.
    Otherwise, returns the start of the next period.
    """
    floored = _floor_boundary(dt, delta_kwargs)
    if floored >= dt:
        return floored
    delta = relativedelta(**delta_kwargs)
    return floored + delta


def generate_chunk_boundaries(
    start: Any,
    end: Any,
    interval: Dict[str, int],
) -> List[Tuple[Any, Any]]:
    """Generate left-closed, right-open ``[lower, upper)`` chunk boundaries.

    Each returned tuple represents a chunk where the lower bound is
    **inclusive** and the upper bound is **exclusive**.  This produces
    whole calendar-aligned intervals (whole days, weeks, months, etc.)
    suitable for ``col >= lower AND col < upper`` filters.

    Args:
        start: Inclusive lower bound of the full range.
        end: Exclusive upper bound of the full range.
        interval: Chunking interval.  Time-based keys (``months``,
            ``days``, ``hours``, ``minutes``, ``weeks``, ``years``)
            create :class:`relativedelta` offsets.  The ``step`` key
            creates integer-based chunks.

    Returns:
        List of ``(lower, upper)`` tuples covering ``[start, end)``.
        The type of the bounds matches the input type:

        * ``date`` / ``datetime`` inputs → ``date`` / ``datetime`` bounds.
        * Date-only strings (``"2025-01-01"``) → :class:`date` bounds.
        * Datetime strings (``"2025-01-01T06:00:00"``) → :class:`datetime` bounds.
        * Integer inputs (``step`` mode) → ``int`` bounds.

    Raises:
        ConfigurationError: When *interval* is invalid or *start* >= *end*.
    """
    if not interval:
        raise ConfigurationError("chunk_interval must be a non-empty dict")

    normalized_start, normalized_end, value_family = normalize_chunk_range(start, end)

    # -- Integer step mode -------------------------------------------------
    if "step" in interval:
        if len(interval) != 1:
            raise ConfigurationError(
                "chunk_interval.step cannot be combined with time-based keys"
            )
        return _generate_integer_chunks(
            normalized_start, normalized_end, interval["step"]
        )

    # -- Time-based mode ---------------------------------------------------
    if value_family != "temporal":
        raise ConfigurationError(
            "time-based chunking requires date or datetime bounds; "
            f"got {type(normalized_start).__name__}"
        )
    return _generate_time_chunks(normalized_start, normalized_end, interval)


def validate_chunk_range(start: Any, end: Any) -> None:
    """Validate a bounded replay range without generating chunks."""

    normalize_chunk_range(start, end)


def normalize_chunk_range(start: Any, end: Any) -> Tuple[Any, Any, str]:
    """Return a canonical, lossless pair for a bounded replay range.

    Native and ISO representations of the same temporal family are normalized
    to native ``date`` or ``datetime`` values before the range is compared or
    forwarded to a reader.  A date is never silently promoted to midnight, and
    aware/naive datetimes cannot be mixed.  The third return value is either
    ``"integer"``, ``"decimal"``, ``"float"`` or ``"temporal"``.

    This helper deliberately does not decide whether a source supports the
    resulting scalar.  That is a reader capability decision; chunk generation
    separately rejects non-integer/non-temporal stepping.
    """

    if isinstance(start, bool) or isinstance(end, bool):
        raise ConfigurationError("Replay range bounds cannot be bool values")

    if type(start) is int or type(end) is int:
        if type(start) is not int or type(end) is not int:
            raise ConfigurationError(
                "Replay range start and end must use the same comparable type"
            )
        if start >= end:
            raise ConfigurationError(
                f"watermark_from ({start}) must be less than watermark_to ({end})"
            )
        return start, end, "integer"

    if isinstance(start, Decimal) or isinstance(end, Decimal):
        if not isinstance(start, Decimal) or not isinstance(end, Decimal):
            raise ConfigurationError(
                "Replay Decimal bounds must use Decimal for both start and end"
            )
        if not start.is_finite() or not end.is_finite():
            raise ConfigurationError("Replay Decimal bounds must be finite")
        if start >= end:
            raise ConfigurationError(
                f"watermark_from ({start}) must be less than watermark_to ({end})"
            )
        return start, end, "decimal"

    if isinstance(start, float) or isinstance(end, float):
        if type(start) is not float or type(end) is not float:
            raise ConfigurationError(
                "Replay float bounds must use float for both start and end"
            )
        if not math.isfinite(start) or not math.isfinite(end):
            raise ConfigurationError("Replay float bounds must be finite")
        if start >= end:
            raise ConfigurationError(
                f"watermark_from ({start}) must be less than watermark_to ({end})"
            )
        return start, end, "float"

    start_value, start_family = _normalize_temporal_bound(start)
    end_value, end_family = _normalize_temporal_bound(end)
    if start_family != end_family:
        raise ConfigurationError(
            "Replay range start and end must use the same date/datetime family"
        )
    if start_family == "datetime":
        start_aware = start_value.tzinfo is not None and start_value.utcoffset() is not None
        end_aware = end_value.tzinfo is not None and end_value.utcoffset() is not None
        if start_aware != end_aware:
            raise ConfigurationError(
                "Replay datetime bounds cannot mix aware and naive values"
            )
        left = _comparison_instant(start_value)
        right = _comparison_instant(end_value)
    else:
        left, right = start_value, end_value
    if left >= right:
        raise ConfigurationError(
            f"watermark_from ({start}) must be earlier than watermark_to ({end})"
        )
    return start_value, end_value, "temporal"


def _normalize_temporal_bound(value: Any) -> Tuple[Any, str]:
    """Normalize one native or ISO temporal bound."""

    if isinstance(value, datetime):
        return value, "datetime"
    if isinstance(value, date):
        return value, "date"
    if not isinstance(value, str):
        raise ConfigurationError(
            "Unsupported watermark type for replay range; use int, Decimal, float, date, "
            "datetime, or ISO string"
        )
    text = value.strip()
    try:
        validate_iso_fraction_precision(text)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    if "T" not in text and " " not in text:
        try:
            return date.fromisoformat(text), "date"
        except ValueError as exc:
            raise ConfigurationError(
                f"Cannot parse '{value}' as date/datetime for replay range"
            ) from exc
    try:
        return datetime.fromisoformat(text.replace(" ", "T")), "datetime"
    except ValueError as exc:
        raise ConfigurationError(
            f"Cannot parse '{value}' as date/datetime for replay range"
        ) from exc


def _comparison_instant(value: datetime) -> datetime:
    """Compare aware datetimes by UTC instant while preserving originals."""

    if value.tzinfo is None or value.utcoffset() is None:
        return value
    return value.astimezone(timezone.utc)


def _generate_integer_chunks(
    start: Any,
    end: Any,
    step: int,
) -> List[Tuple[Any, Any]]:
    """Generate step-aligned chunks for integer watermark columns.

    Boundaries snap to multiples of *step* so that middle chunks always
    cover exactly *step* values.  The first and last chunks may be partial.
    """
    if type(start) is not int or type(end) is not int:
        raise ConfigurationError(
            "step chunking requires integer start and end values; "
            f"got {type(start).__name__} and {type(end).__name__}"
        )
    if type(step) is not int or step <= 0:
        raise ConfigurationError("chunk_interval step must be a positive integer")
    start_val = start
    end_val = end
    if start_val >= end_val:
        raise ConfigurationError(
            f"watermark_from ({start_val}) must be less than watermark_to ({end_val})"
        )

    # Snap to step-aligned boundary
    remainder = start_val % step
    first_boundary = start_val if remainder == 0 else start_val + (step - remainder)

    chunks: List[Tuple[int, int]] = []

    if first_boundary >= end_val:
        # Entire range within one step — single chunk
        chunks.append((start_val, end_val))
    else:
        # Partial first chunk if start is not at a boundary
        if first_boundary > start_val:
            chunks.append((start_val, first_boundary))

        # Full-step chunks
        cursor = first_boundary
        while cursor < end_val:
            upper = min(cursor + step, end_val)
            chunks.append((cursor, upper))
            cursor += step

    return chunks


def _generate_time_chunks(
    start: Any,
    end: Any,
    interval: Dict[str, int],
) -> List[Tuple[Any, Any]]:
    """Generate calendar-aligned chunks for datetime/date watermark columns.

    Boundaries snap to calendar period starts (1st of month, Monday, midnight,
    etc.) so that middle chunks always contain whole calendar periods.
    The first and last chunks may be partial if *start*/*end* do not align.
    """
    start_dt = _to_datetime(start)
    end_dt = _to_datetime(end)

    try:
        invalid_order = start_dt >= end_dt
    except TypeError as exc:
        raise ConfigurationError(
            "Chunk boundaries must use comparable date/datetime values"
        ) from exc
    if invalid_order:
        raise ConfigurationError(
            f"watermark_from ({start_dt}) must be earlier than watermark_to ({end_dt})"
        )

    # Build relativedelta from interval keys. Unknown keys are rejected rather
    # than silently ignored, otherwise a typo changes chunking semantics.
    valid_keys = {"years", "months", "weeks", "days", "hours", "minutes"}
    unknown_keys = set(interval) - valid_keys
    if unknown_keys:
        raise ConfigurationError(
            "chunk_interval must contain at least one time key and no "
            f"unsupported keys; got {sorted(unknown_keys)}"
        )
    delta_kwargs = {k: v for k, v in interval.items() if k in valid_keys}
    if not delta_kwargs:
        raise ConfigurationError(
            f"chunk_interval must contain at least one time key "
            f"({', '.join(sorted(valid_keys))}) or 'step'; got {list(interval.keys())}"
        )

    # Validate all delta values are positive
    for k, v in delta_kwargs.items():
        if type(v) is not int or v <= 0:
            raise ConfigurationError(f"chunk_interval.{k} must be positive, got {v}")

    if isinstance(start, date) and not isinstance(start, datetime):
        if "hours" in delta_kwargs or "minutes" in delta_kwargs:
            raise ConfigurationError(
                "Date-valued chunking cannot use hours or minutes; use a datetime range"
            )
    if isinstance(start, str):
        date_only = "T" not in start and " " not in start.strip()
        if date_only and ("hours" in delta_kwargs or "minutes" in delta_kwargs):
            raise ConfigurationError(
                "Date-only chunking cannot use hours or minutes; use a datetime string"
            )

    delta = relativedelta(**delta_kwargs)

    # Snap to the first calendar boundary >= start
    first_boundary = _ceil_boundary(start_dt, delta_kwargs)

    chunks: List[Tuple[Any, Any]] = []

    if first_boundary >= end_dt:
        # Entire range fits within one period — single chunk
        chunks.append((start_dt, end_dt))
    else:
        # Partial first chunk if start is not at a boundary
        if first_boundary > start_dt:
            chunks.append((start_dt, first_boundary))

        # Full-period chunks from the first boundary onward
        cursor = first_boundary
        while cursor < end_dt:
            next_cursor = cursor + delta
            if next_cursor <= cursor:
                raise ConfigurationError(
                    "chunk_interval produced a non-advancing boundary"
                )
            upper = min(next_cursor, end_dt)
            chunks.append((cursor, upper))
            cursor = next_cursor

    # Return bounds in the same type as the input so the watermark
    # serializer round-trips them with the correct sentinel pattern
    # (__date__ vs __datetime__).
    if isinstance(start, date) and not isinstance(start, datetime):
        # Native date input → return date bounds
        chunks = [(lo.date() if isinstance(lo, datetime) else lo,
                   hi.date() if isinstance(hi, datetime) else hi)
                  for lo, hi in chunks]
    elif isinstance(start, str):
        # String input → return native date (date-only) or datetime
        _date_only = "T" not in start and " " not in start.strip()
        if _date_only:
            chunks = [(lo.date(), hi.date()) for lo, hi in chunks]

    if isinstance(start_dt, datetime) and start_dt.tzinfo is not None:
        _validate_aware_chunks(chunks)

    return chunks


def _validate_aware_chunks(chunks: List[Tuple[Any, Any]]) -> None:
    """Reject unsafe local-time boundaries and verify instant coverage."""

    if not chunks:
        return
    internal_boundaries = [upper for _, upper in chunks[:-1]]
    for boundary in internal_boundaries:
        if not isinstance(boundary, datetime):
            continue
        status = _local_boundary_status(boundary)
        if status != "valid":
            raise ConfigurationError(
                "Automatic chunking cannot represent a "
                f"{status} timezone boundary at {boundary.isoformat()}; "
                "use a fixed-offset range or an explicit one-shot read"
            )

    previous_upper = None
    for lower, upper in chunks:
        if not isinstance(lower, datetime) or not isinstance(upper, datetime):
            continue
        lower_instant = _comparison_instant(lower)
        upper_instant = _comparison_instant(upper)
        if lower_instant >= upper_instant:
            raise ConfigurationError(
                "Automatic aware chunking produced a non-positive UTC interval"
            )
        if previous_upper is not None and lower_instant != previous_upper:
            raise ConfigurationError(
                "Automatic aware chunking produced a gap or overlap in UTC instants"
            )
        previous_upper = upper_instant


def _local_boundary_status(value: datetime) -> str:
    """Classify a named-zone local datetime as valid, ambiguous or missing."""

    if value.tzinfo is None or value.utcoffset() is None:
        return "valid"
    valid_instants = []
    for fold in (0, 1):
        candidate = value.replace(fold=fold)
        instant = candidate.astimezone(timezone.utc)
        roundtrip = instant.astimezone(value.tzinfo)
        same_wall = roundtrip.replace(tzinfo=None) == candidate.replace(tzinfo=None)
        same_offset = roundtrip.utcoffset() == candidate.utcoffset()
        if same_wall and same_offset:
            valid_instants.append(instant)
    if not valid_instants:
        return "nonexistent"
    if len(set(valid_instants)) > 1:
        return "ambiguous"
    return "valid"


def _to_datetime(value: Any) -> datetime:
    """Coerce a value to :class:`datetime` for boundary calculation."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            validate_iso_fraction_precision(value)
            return datetime.fromisoformat(value.replace(" ", "T"))
        except ValueError as exc:
            if "fractional precision beyond six" in str(exc):
                raise ConfigurationError(str(exc)) from exc
            raise ConfigurationError(
                f"Cannot parse '{value}' as datetime for chunking"
            ) from exc
    raise ConfigurationError(
        f"Unsupported watermark type for time-based chunking: {type(value).__name__}"
    )
