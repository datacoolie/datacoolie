"""Stateless native Polars transformations.

Column-name resolution remains in the public engine facade. Functions here
receive resolved names and never retain engine state.
"""

from __future__ import annotations

import importlib
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import polars as pl

from datacoolie.core.constants import DEFAULT_AUTHOR, XXHASH64_SEED, SystemColumn
from datacoolie.core.exceptions import EngineError, TransformError
from datacoolie.core.models import HashColumn, MaskingRule, ValueRule
from datacoolie.engines.base import BaseEngine


def add_column(
    df: pl.LazyFrame, column_name: str, expression: str
) -> pl.LazyFrame:
    return df.with_columns(pl.sql_expr(expression).alias(column_name))


def drop_columns(df: pl.LazyFrame, columns: Sequence[str]) -> pl.LazyFrame:
    return df.drop(columns) if columns else df


def select_columns(df: pl.LazyFrame, columns: Sequence[str]) -> pl.LazyFrame:
    return df.select(columns)


def rename_column(
    df: pl.LazyFrame, old_name: str, new_name: str
) -> pl.LazyFrame:
    return df.rename({old_name: new_name})


def rename_columns(
    df: pl.LazyFrame, mapping: Dict[str, str]
) -> pl.LazyFrame:
    return df.rename(mapping) if mapping else df


def filter_rows(df: pl.LazyFrame, condition: str) -> pl.LazyFrame:
    return df.filter(pl.sql_expr(condition))


def _coerce_literal(
    value: Any,
    dtype: pl.DataType,
    *,
    field_path: str,
    column: str,
) -> Any:
    kwargs: Dict[str, Any] = {"field_path": field_path, "column": column}
    if dtype == pl.String:
        return BaseEngine._coerce_scalar_literal(value, kind="string", **kwargs)
    if dtype == pl.Boolean:
        return BaseEngine._coerce_scalar_literal(value, kind="boolean", **kwargs)
    integer_bounds = {
        pl.Int8: (-(2**7), 2**7 - 1),
        pl.Int16: (-(2**15), 2**15 - 1),
        pl.Int32: (-(2**31), 2**31 - 1),
        pl.Int64: (-(2**63), 2**63 - 1),
        pl.UInt8: (0, 2**8 - 1),
        pl.UInt16: (0, 2**16 - 1),
        pl.UInt32: (0, 2**32 - 1),
        pl.UInt64: (0, 2**64 - 1),
    }
    if dtype in integer_bounds:
        minimum, maximum = integer_bounds[dtype]
        return BaseEngine._coerce_scalar_literal(
            value,
            kind="integer",
            minimum=minimum,
            maximum=maximum,
            **kwargs,
        )
    if dtype in {pl.Float32, pl.Float64}:
        return BaseEngine._coerce_scalar_literal(value, kind="float", **kwargs)
    if isinstance(dtype, pl.Decimal):
        return BaseEngine._coerce_scalar_literal(
            value,
            kind="decimal",
            precision=dtype.precision,
            scale=dtype.scale,
            **kwargs,
        )
    if dtype == pl.Date:
        return BaseEngine._coerce_scalar_literal(value, kind="date", **kwargs)
    if isinstance(dtype, pl.Datetime):
        return BaseEngine._coerce_scalar_literal(
            value,
            kind="timestamp",
            timezone_aware=dtype.time_zone is not None,
            **kwargs,
        )
    return BaseEngine._coerce_scalar_literal(value, kind="unsupported", **kwargs)


def apply_value_rule(
    df: pl.LazyFrame,
    rule: ValueRule,
    schema: pl.Schema,
    columns: Sequence[str],
) -> pl.LazyFrame:
    expressions: List[pl.Expr] = []
    for column in columns:
        dtype = schema[column]
        source = pl.col(column)
        if rule.operation in {
            "trim",
            "case",
            "regex_replace",
            "empty_to_null",
            "map",
        } and dtype != pl.String:
            raise TransformError(
                f"Value rule {rule.operation!r} requires a string column",
                details={"column": column, "data_type": str(dtype)},
            )
        if rule.operation == "trim":
            result = source.str.strip_chars(" ")
        elif rule.operation == "case":
            result = (
                source.str.to_lowercase()
                if rule.mode == "lower"
                else source.str.to_uppercase()
            )
        elif rule.operation == "regex_replace":
            result = source.str.replace_all(
                rule.pattern or "", rule.replacement.replace("$", "$$")
            )
        elif rule.operation == "empty_to_null":
            result = pl.when(source == "").then(None).otherwise(source)
        elif rule.operation == "fill_null":
            literal = _coerce_literal(
                rule.value,
                dtype,
                field_path="value_rules.value",
                column=column,
            )
            result = source.fill_null(pl.lit(literal).cast(dtype))
        else:
            mapped = source.replace_strict(
                rule.mapping, default=None, return_dtype=pl.String
            )
            result = (
                pl.coalesce(mapped, source) if rule.on_unmapped == "keep" else mapped
            )
        expressions.append(result.alias(column))
    return df.with_columns(expressions) if expressions else df


def apply_masking_rule(
    df: pl.LazyFrame,
    rule: MaskingRule,
    schema: pl.Schema,
    columns: Sequence[str],
) -> pl.LazyFrame:
    expressions: List[pl.Expr] = []
    for column in columns:
        dtype = schema[column]
        source = pl.col(column)
        if rule.method == "redact":
            literal = _coerce_literal(
                rule.value,
                dtype,
                field_path="masking_rules.value",
                column=column,
            )
            result = (
                pl.when(source.is_null())
                .then(source)
                .otherwise(pl.lit(literal).cast(dtype))
            )
        elif rule.method == "nullify":
            result = pl.lit(None).cast(dtype)
        elif rule.method == "partial":
            if dtype != pl.String:
                raise TransformError(
                    "partial masking requires a string column",
                    details={"column": column},
                )
            prefix = source.str.slice(0, rule.keep_start) if rule.keep_start else pl.lit("")
            suffix = (
                source.str.slice(-rule.keep_end, rule.keep_end)
                if rule.keep_end
                else pl.lit("")
            )
            masked = pl.concat_str(prefix, pl.lit(rule.mask_char), suffix)
            result = (
                pl.when(source.is_null())
                .then(source)
                .when(source == "")
                .then(source)
                .when(source.str.len_chars() <= rule.keep_start + rule.keep_end)
                .then(pl.lit(rule.mask_char))
                .otherwise(masked)
            )
        elif rule.method == "numeric_bucket":
            if not dtype.is_numeric():
                raise TransformError(
                    "numeric_bucket requires a numeric column",
                    details={"column": column},
                )
            result = ((source / rule.bucket_size).floor() * rule.bucket_size).cast(dtype)
        else:
            if dtype == pl.Date and rule.unit == "hour":
                raise TransformError(
                    "date_truncate hour requires a datetime column",
                    details={"column": column},
                )
            if dtype != pl.Date and not isinstance(dtype, pl.Datetime):
                raise TransformError(
                    "date_truncate requires a date or datetime column",
                    details={"column": column},
                )
            every = {"year": "1y", "month": "1mo", "day": "1d", "hour": "1h"}[
                rule.unit
            ]
            result = source.dt.truncate(every).cast(dtype)
        expressions.append(result.alias(column))
    return df.with_columns(expressions) if expressions else df


def add_hash_column(
    df: pl.LazyFrame,
    definition: HashColumn,
    schema: pl.Schema,
    columns: Sequence[str],
) -> pl.LazyFrame:
    try:
        polars_hash = importlib.import_module("polars_hash")
    except (ImportError, OSError) as exc:
        raise EngineError(
            "hash_columns with the Polars engine requires the optional polars-hash package",
            details={"install": "pip install 'datacoolie[polars-hash]'"},
        ) from exc
    components: List[pl.Expr] = []
    for column in columns:
        dtype = schema[column]
        source = pl.col(column)
        if dtype == pl.String:
            tag, value = "S", source
        elif dtype.is_integer():
            tag, value = "I", source.cast(pl.String)
        elif dtype == pl.Boolean:
            tag = "B"
            value = pl.when(source).then(pl.lit("true")).otherwise(pl.lit("false"))
        elif dtype == pl.Date:
            tag, value = "D", source.dt.strftime("%Y-%m-%d")
        else:
            raise TransformError(
                "hash_columns supports only string, integer, boolean, and date inputs",
                details={"column": column, "data_type": str(dtype)},
            )
        components.append(
            pl.when(source.is_null())
            .then(pl.lit(f"{tag}N;"))
            .otherwise(
                pl.concat_str(
                    pl.lit(tag),
                    value.str.len_bytes().cast(pl.String),
                    pl.lit(":"),
                    value,
                    pl.lit(";"),
                )
            )
        )
    payload = polars_hash.concat_str([pl.lit("DCH1;"), *components])
    if definition.algorithm == "sha256":
        result = payload.chash.sha2_256()
    elif definition.algorithm == "xxhash64":
        result = payload.nchash.xxhash64(seed=XXHASH64_SEED).reinterpret(signed=True)
    else:  # HashColumn validation prevents this branch for public callers.
        raise TransformError(
            "Unsupported hash_columns algorithm",
            details={"algorithm": definition.algorithm},
        )
    return df.with_columns(result.alias(definition.target_column))


def apply_watermark_filter(
    df: pl.LazyFrame,
    columns: Sequence[tuple[str, Any, Any]],
    *,
    start_operator: str,
    end_operator: str,
) -> pl.LazyFrame:
    combined: Optional[pl.Expr] = None
    for column, lower, upper in columns:
        condition: Optional[pl.Expr] = None
        expression = pl.col(column)
        if lower is not None:
            lower = lower.isoformat() if isinstance(lower, (datetime, date)) else lower
            condition = expression >= pl.lit(lower) if start_operator == ">=" else expression > pl.lit(lower)
        if upper is not None:
            upper = upper.isoformat() if isinstance(upper, (datetime, date)) else upper
            upper_condition = expression <= pl.lit(upper) if end_operator == "<=" else expression < pl.lit(upper)
            condition = upper_condition if condition is None else condition & upper_condition
        combined = condition if combined is None else combined | condition
    return df if combined is None else df.filter(combined)


def deduplicate(
    df: pl.LazyFrame,
    partition_columns: List[str],
    order_columns: Optional[List[str]],
    order: str,
) -> pl.LazyFrame:
    ordering = order_columns or partition_columns
    return df.sort(ordering, descending=order != "asc").unique(
        subset=partition_columns, keep="first"
    )


def deduplicate_by_rank(
    df: pl.LazyFrame,
    partition_columns: List[str],
    order_columns: List[str],
    order: str,
) -> pl.LazyFrame:
    descending = order != "asc"
    if len(order_columns) == 1:
        column = order_columns[0]
        best = (
            pl.col(column).max() if descending else pl.col(column).min()
        ).over(partition_columns)
        return df.filter(pl.col(column) == best)
    flags = [descending] * len(order_columns)
    best_exprs = [
        pl.col(column)
        .sort_by(order_columns, descending=flags)
        .first()
        .over(partition_columns)
        .alias(f"__best_{column}")
        for column in order_columns
    ]
    condition = pl.all_horizontal(
        pl.col(column) == pl.col(f"__best_{column}") for column in order_columns
    )
    return df.with_columns(best_exprs).filter(condition).drop(
        [f"__best_{column}" for column in order_columns]
    )


def add_system_columns(
    df: pl.LazyFrame,
    author: Optional[str],
    dataflow_run_id: Optional[str],
) -> pl.LazyFrame:
    now = datetime.now(tz=timezone.utc)
    expressions = [
        pl.lit(now).alias(SystemColumn.CREATED_AT),
        pl.lit(now).alias(SystemColumn.UPDATED_AT),
        pl.lit(author or DEFAULT_AUTHOR).alias(SystemColumn.UPDATED_BY),
    ]
    if dataflow_run_id is not None:
        expressions.append(pl.lit(dataflow_run_id).alias(SystemColumn.DATAFLOW_RUN_ID))
    return df.with_columns(expressions)


def convert_timestamp_ntz_to_timestamp(df: pl.LazyFrame) -> pl.LazyFrame:
    conversions = [
        pl.col(name)
        .cast(pl.Datetime(dtype.time_unit or "us"))
        .dt.replace_time_zone("UTC")
        .alias(name)
        for name, dtype in df.collect_schema().items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is None
    ]
    return df.with_columns(conversions) if conversions else df
