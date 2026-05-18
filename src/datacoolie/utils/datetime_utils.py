"""Date/time range utilities for the DataCoolie framework."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

from dateutil.relativedelta import relativedelta

from datacoolie.core.exceptions import ConfigurationError


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

    # -- Integer step mode -------------------------------------------------
    if "step" in interval:
        return _generate_integer_chunks(start, end, interval["step"])

    # -- Time-based mode ---------------------------------------------------
    return _generate_time_chunks(start, end, interval)


def _generate_integer_chunks(
    start: Any,
    end: Any,
    step: int,
) -> List[Tuple[Any, Any]]:
    """Generate step-aligned chunks for integer watermark columns.

    Boundaries snap to multiples of *step* so that middle chunks always
    cover exactly *step* values.  The first and last chunks may be partial.
    """
    start_val = int(start)
    end_val = int(end)

    if step <= 0:
        raise ConfigurationError("chunk_interval step must be a positive integer")
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

    if start_dt >= end_dt:
        raise ConfigurationError(
            f"watermark_from ({start_dt}) must be earlier than watermark_to ({end_dt})"
        )

    # Build relativedelta from interval keys
    valid_keys = {"years", "months", "weeks", "days", "hours", "minutes"}
    delta_kwargs = {k: v for k, v in interval.items() if k in valid_keys}
    if not delta_kwargs:
        raise ConfigurationError(
            f"chunk_interval must contain at least one time key "
            f"({', '.join(sorted(valid_keys))}) or 'step'; got {list(interval.keys())}"
        )

    # Validate all delta values are positive
    for k, v in delta_kwargs.items():
        if v <= 0:
            raise ConfigurationError(f"chunk_interval.{k} must be positive, got {v}")

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

    return chunks


def _to_datetime(value: Any) -> datetime:
    """Coerce a value to :class:`datetime` for boundary calculation."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace(" ", "T"))
        except (ValueError, TypeError) as exc:
            raise ConfigurationError(
                f"Cannot parse '{value}' as datetime for chunking"
            ) from exc
    raise ConfigurationError(
        f"Unsupported watermark type for time-based chunking: {type(value).__name__}"
    )
