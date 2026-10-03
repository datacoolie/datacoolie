"""Watermark serialization and range-window helpers for API sources."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from datacoolie.core.exceptions import SourceError
from datacoolie.logging.runtime.manager import get_logger

logger = get_logger(__name__)

def resolve_timezone(tz_str: Optional[str]) -> Any:
    """Resolve a timezone string to a :class:`~datetime.tzinfo` object.

    Accepts IANA timezone names (e.g. ``"Asia/Ho_Chi_Minh"``, ``"UTC"``)
    when :mod:`zoneinfo` is available (Python 3.9+), and ``±HH:MM`` UTC
    offset strings (e.g. ``"+07:00"``, ``"-05:30"``) as a universal
    fallback.  Returns :data:`~datetime.timezone.utc` when *tz_str* is
    ``None`` or empty.

    Args:
        tz_str: Timezone name or UTC offset string, or ``None``.

    Returns:
        A :class:`~datetime.tzinfo` compatible with :func:`datetime.now`.

    Raises:
        SourceError: When *tz_str* is non-empty but cannot be parsed.
    """
    if not tz_str:
        return timezone.utc

    # Try IANA name via stdlib zoneinfo (Python 3.9+)
    try:
        return ZoneInfo(tz_str)
    except Exception as exc:
        logger.debug("ZoneInfo lookup failed for '%s', trying offset: %s", tz_str, exc)

    # Try UTC offset format: ±HH:MM or ±HHMM
    m = re.match(r"^([+-])(\d{1,2}):?(\d{2})$", tz_str.strip())
    if m:
        sign = 1 if m.group(1) == "+" else -1
        hours = int(m.group(2))
        minutes = int(m.group(3))
        return timezone(timedelta(hours=sign * hours, minutes=sign * minutes))

    raise SourceError(
        f"Cannot resolve timezone {tz_str!r}. "
        "Use an IANA name (e.g. 'Asia/Ho_Chi_Minh') or a UTC offset (e.g. '+07:00').",
    )

def build_watermark_ranges(
    from_dt: datetime,
    to_dt: datetime,
    amount: Union[int, str],
    unit: str,
) -> List[Tuple[datetime, datetime]]:
    """Split ``[from_dt, to_dt)`` into equal-sized intervals.

    Args:
        from_dt: Start of the overall window (inclusive).
        to_dt:   End of the overall window (exclusive).
        amount:  Positive integer number of ``unit`` per interval (e.g. ``3``
                 for 3 hours). Numeric strings are accepted for metadata input.
        unit:    One of ``"hour"``, ``"day"``, ``"month"``, ``"year"``.

    Returns:
        List of ``(range_start, range_end)`` tuples covering the window
        without gaps or overlaps.  The final range's end is clamped to
        ``to_dt``.  Returns an empty list if ``from_dt >= to_dt``.
    """
    if isinstance(amount, bool):
        raise SourceError("watermark_range_interval_amount must be a positive integer.")
    try:
        parsed_amount = int(amount)
    except (TypeError, ValueError) as exc:
        raise SourceError(
            "watermark_range_interval_amount must be a positive integer."
        ) from exc
    if not isinstance(amount, (int, str)) or parsed_amount <= 0:
        raise SourceError("watermark_range_interval_amount must be a positive integer.")
    amount = parsed_amount

    if from_dt >= to_dt:
        return []

    if unit not in ("hour", "day", "month", "year"):
        raise SourceError(
            f"watermark_range_interval_unit must be one of "
            f"'hour', 'day', 'month', 'year' — got {unit!r}",
        )

    ranges: List[Tuple[datetime, datetime]] = []
    current = from_dt

    while current < to_dt:
        if unit in ("month", "year"):
            kwargs = {"months": amount} if unit == "month" else {"years": amount}
            next_dt = current + relativedelta(**kwargs)
        elif unit == "day":
            next_dt = current + timedelta(days=amount)
        else:  # "hour"
            next_dt = current + timedelta(hours=amount)

        end = min(next_dt, to_dt)
        ranges.append((current, end))
        current = next_dt  # full step — next range starts right after, clamping handled above

    return ranges

def resolve_range_from_dt(
    watermark: Optional[Dict[str, Any]],
    wm_mapping: Dict[str, str],
    range_start_str: Optional[str],
    tz: Any = timezone.utc,
) -> Optional[datetime]:
    """Determine the lower bound of a range-split window.

    Iterates *all* keys in *wm_mapping* and returns the first non-``None``
    watermark value as a :class:`~datetime.datetime`, coercing ISO-8601
    strings and ``date`` objects as needed.  Falls back to parsing
    *range_start_str* when no stored watermark value is found.

    Args:
        watermark:        Previously stored watermark dict, or ``None``.
        wm_mapping:       ``{wm_col: api_param}`` push-down mapping.
        range_start_str:  ISO-8601 fallback start for the first run.
        tz:               Timezone attached to naive datetime values parsed
                          from ISO strings or promoted from ``date`` objects.
                          Defaults to ``timezone.utc``.

    Returns:
        A ``datetime`` representing the lower bound, or ``None`` when
        neither the watermark nor *range_start_str* is available.
    """
    if watermark and wm_mapping:
        for col in wm_mapping:
            raw = watermark.get(col)
            if raw is None:
                continue
            if isinstance(raw, str):
                dt = datetime.fromisoformat(raw)
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=tz)
            if type(raw) is date:  # exact — datetime IS-A date
                return datetime(raw.year, raw.month, raw.day, tzinfo=tz)
            return raw  # already a datetime

    if range_start_str:
        dt = datetime.fromisoformat(range_start_str)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=tz)

    return None

__all__ = ["build_watermark_ranges", "resolve_range_from_dt", "resolve_timezone"]
