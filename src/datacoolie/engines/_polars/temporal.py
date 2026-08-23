"""Polars temporal boundary and predicate helpers."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple


def to_iso8601(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value).replace(" ", "T")


def build_window_predicate(window: Dict[str, tuple], quote_char: str = "`") -> str:
    return " AND ".join(
        f"{quote_char}{column}{quote_char} > '{lower}' AND "
        f"{quote_char}{column}{quote_char} <= '{upper}'"
        for column, (lower, upper) in window.items()
    )


def align_ms_boundaries(
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    start = None
    end = None
    if start_time is not None:
        start = start_time.replace(microsecond=(start_time.microsecond // 1000) * 1000)
    if end_time is not None:
        ceil_us = ((end_time.microsecond + 999) // 1000) * 1000
        end = (
            end_time.replace(microsecond=0) + timedelta(seconds=1)
            if ceil_us >= 1_000_000
            else end_time.replace(microsecond=ceil_us)
        )
    return start, end
