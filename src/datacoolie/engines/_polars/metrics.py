"""Polars metric and schema inspection operations."""

from __future__ import annotations

from datetime import timezone
from typing import Any, Dict, List, Tuple

import polars as pl

from datacoolie.engines._polars.type_mapping import polars_type_to_hive


def count_rows(df: pl.LazyFrame) -> int:
    return df.select(pl.len()).collect().item()


def is_empty(df: pl.LazyFrame) -> bool:
    return len(df.head(1).collect()) == 0


def get_columns(df: pl.LazyFrame) -> List[str]:
    return df.collect_schema().names()


def get_schema(df: pl.LazyFrame) -> Dict[str, str]:
    return {name: str(dtype) for name, dtype in df.collect_schema().items()}


def get_hive_schema(df: pl.LazyFrame) -> Dict[str, str]:
    return {
        name: polars_type_to_hive(dtype) for name, dtype in df.collect_schema().items()
    }


def _collect_row_safe(result: pl.LazyFrame) -> Dict[str, Any]:
    """Collect one row without relying on the Windows zoneinfo database."""
    schema = result.collect_schema()
    tz_columns = [
        name
        for name, dtype in schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
    ]
    if tz_columns:
        result = result.with_columns(
            pl.col(column).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            for column in tz_columns
        )
    values = dict(result.collect().row(0, named=True))
    for column in tz_columns:
        if values.get(column) is not None:
            values[column] = values[column].replace(tzinfo=timezone.utc)
    return values


def get_max_values(df: pl.LazyFrame, columns: List[str]) -> Dict[str, Any]:
    expressions = [pl.col(column).max().alias(column) for column in columns]
    return _collect_row_safe(df.select(expressions))


def get_count_and_max_values(
    df: pl.LazyFrame,
    columns: List[str],
) -> Tuple[int, Dict[str, Any]]:
    expressions = [pl.len().alias("__row_count")]
    expressions.extend(pl.col(column).max().alias(column) for column in columns)
    values = _collect_row_safe(df.select(expressions))
    count = values.pop("__row_count", 0)
    return count, values
