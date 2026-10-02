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

def inject_stored_watermark(
    target: Dict[str, Any],
    wm_mapping: Dict[str, str],
    watermark: Dict[str, Any],
    wm_format: str,
) -> None:
    """Inject stored watermark values into *target* (params or body dict).

    For each ``{wm_col: api_param}`` pair in *wm_mapping*, looks up
    ``wm_col`` in *watermark* and, when a non-``None`` value is found,
    serialises it with :meth:`_format_watermark_value` and writes it into
    *target* under ``api_param``.  Mutates *target* in place.

    ``date``, ``datetime``, and ISO-8601 string values are accepted for the
    historical temporal formats.  The explicit ``integer`` format preserves
    an integer as an integer on the wire; this is used by the new API range
    binding contract and is intentionally not inferred from ``iso``.

    Raises:
        SourceError: When a pushed watermark value is not a ``date``,
            ``datetime``, or parseable ISO-8601 string.
    """
    for wm_col, api_param in wm_mapping.items():
        wm_val = watermark.get(wm_col)
        if wm_val is None:
            continue
        if wm_format in {"integer", "int", "native_integer", "number"}:
            if isinstance(wm_val, bool) or type(wm_val) is not int:
                raise SourceError(
                    f"Integer watermark push-down requires an int. "
                    f"Column {wm_col!r} has unsupported type {type(wm_val).__name__!r}.",
                    details={"column": wm_col, "type": type(wm_val).__name__, "api_param": api_param},
                )
        elif not isinstance(wm_val, (date, datetime, str)):
            raise SourceError(
                f"Watermark push-down only supports date/datetime values. "
                f"Column {wm_col!r} has unsupported type {type(wm_val).__name__!r}.",
                details={"column": wm_col, "type": type(wm_val).__name__, "api_param": api_param},
            )
        if isinstance(wm_val, str):
            try:
                datetime.fromisoformat(wm_val)
            except ValueError as exc:
                raise SourceError(
                    f"Watermark push-down column {wm_col!r} string value is not "
                    f"a valid ISO-8601 date/datetime: {wm_val!r}.",
                    details={"column": wm_col, "value": wm_val, "api_param": api_param},
                ) from exc
        target[api_param] = format_watermark_value(wm_val, wm_format)
        logger.debug(
            "APIReader watermark push-down: %s=%s (from %s)",
            api_param, target[api_param], wm_col,
        )

def format_watermark_value(
    value: Union[datetime, date, str, int],
    fmt: str,
) -> Any:
    """Serialise a watermark value to the string format the API expects.

    Args:
        value: A ``datetime`` or an ISO-8601 string (as stored by WatermarkManager).
        fmt:   ``"iso"`` | ``"datetime"`` | ``"datetime_ms"`` | ``"date"``
               | ``"timestamp"`` | ``"timestamp_ms"``

    Returns:
        String representation ready to be set as a query param or body field.
    """
    if fmt in {"integer", "int", "native_integer", "number"}:
        if isinstance(value, bool) or type(value) is not int:
            raise SourceError(
                f"Integer watermark format requires an int, got {type(value).__name__}."
            )
        return value  # type: ignore[return-value]

    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            # Unparseable string — pass through unchanged
            return value

    if type(value) is date:  # exact check — datetime IS-A date, must come before datetime block
        if fmt == "date":
            return value.isoformat()
        value = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    if not isinstance(value, datetime):
        return str(value)

    if fmt == "date":
        return value.date().isoformat()
    if fmt == "timestamp":
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return str(value.timestamp())
    if fmt == "timestamp_ms":
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return str(int(value.timestamp() * 1000))
    if fmt == "datetime":
        # ISO datetime without timezone, second precision (e.g. "2024-01-15T12:30:45")
        return value.replace(tzinfo=None, microsecond=0).isoformat()
    if fmt == "datetime_ms":
        # ISO datetime without timezone, millisecond precision (e.g. "2024-01-15T12:30:45.123")
        naive = value.replace(tzinfo=None)
        return naive.strftime("%Y-%m-%dT%H:%M:%S.") + f"{naive.microsecond // 1000:03d}"
    # "iso" (default) — preserve whatever timezone state the datetime has
    return value.isoformat()

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

def adjust_range_to(dt: datetime, mode: Optional[str]) -> datetime:
    """Subtract a small epsilon from *dt* before sending it to the API.

    Prevents duplicate rows at range split boundaries when the API uses
    inclusive ``BETWEEN from AND to`` semantics.  The adjusted value is
    only sent to the API; the internal boundary used to start the next
    sub-range in :meth:`_build_watermark_ranges` is unchanged so that
    adjacent ranges remain contiguous with no gaps.

    Args:
        dt:   Upper-bound datetime for a single sub-range.
        mode: ``"1ms"`` (1 millisecond), ``"1s"`` (1 second),
              ``"1day"`` (1 day), or ``None`` (no adjustment, default).

    Returns:
        Adjusted :class:`~datetime.datetime`, or *dt* unchanged when
        *mode* is ``None`` or unrecognised.
    """
    if mode == "1ms":
        return dt - timedelta(milliseconds=1)
    if mode == "1s":
        return dt - timedelta(seconds=1)
    if mode == "1day":
        return dt - timedelta(days=1)
    return dt

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

__all__ = ["adjust_range_to", "build_watermark_ranges", "format_watermark_value", "inject_stored_watermark", "resolve_range_from_dt", "resolve_timezone"]
