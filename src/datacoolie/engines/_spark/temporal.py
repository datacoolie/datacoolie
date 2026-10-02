"""Spark temporal boundary and predicate helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

from datacoolie.engines.contracts.windows import WindowSpec, normalize_window


def build_window_predicate(
    window: WindowSpec, quote_char: str = "`"
) -> str:
    spec = normalize_window(window)
    clauses = [
        f"{quote_char}{column}{quote_char} {spec.lower_operator} '{lower}' AND "
        f"{quote_char}{column}{quote_char} {spec.upper_operator} '{upper}'"
        for column, (lower, upper) in spec.items()
    ]
    if not clauses:
        return "1 = 0"
    return f" {spec.combine_operator} ".join(clauses)


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
