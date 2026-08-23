"""Stateless native Spark transformations."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as sf
from pyspark.sql import types as T

from datacoolie.core.constants import DEFAULT_AUTHOR, SystemColumn
from datacoolie.core.exceptions import TransformError
from datacoolie.core.models import HashColumn, MaskingRule, ValueRule
from datacoolie.engines.base import BaseEngine


def _coerce_literal(
    value: Any, dtype: T.DataType, *, field_path: str, column: str
) -> Any:
    kwargs: Dict[str, Any] = {"field_path": field_path, "column": column}
    if isinstance(dtype, T.StringType):
        return BaseEngine._coerce_scalar_literal(value, kind="string", **kwargs)
    if isinstance(dtype, T.BooleanType):
        return BaseEngine._coerce_scalar_literal(value, kind="boolean", **kwargs)
    integer_bounds = {
        T.ByteType: (-(2**7), 2**7 - 1),
        T.ShortType: (-(2**15), 2**15 - 1),
        T.IntegerType: (-(2**31), 2**31 - 1),
        T.LongType: (-(2**63), 2**63 - 1),
    }
    for dtype_class, (minimum, maximum) in integer_bounds.items():
        if isinstance(dtype, dtype_class):
            return BaseEngine._coerce_scalar_literal(
                value, kind="integer", minimum=minimum, maximum=maximum, **kwargs
            )
    if isinstance(dtype, (T.FloatType, T.DoubleType)):
        return BaseEngine._coerce_scalar_literal(value, kind="float", **kwargs)
    if isinstance(dtype, T.DecimalType):
        return BaseEngine._coerce_scalar_literal(
            value,
            kind="decimal",
            precision=dtype.precision,
            scale=dtype.scale,
            **kwargs,
        )
    if isinstance(dtype, T.DateType):
        return BaseEngine._coerce_scalar_literal(value, kind="date", **kwargs)
    if isinstance(dtype, T.TimestampNTZType):
        return BaseEngine._coerce_scalar_literal(
            value, kind="timestamp", timezone_aware=False, **kwargs
        )
    if isinstance(dtype, T.TimestampType):
        return BaseEngine._coerce_scalar_literal(
            value, kind="timestamp", timezone_aware=True, **kwargs
        )
    return BaseEngine._coerce_scalar_literal(value, kind="unsupported", **kwargs)


def add_column(df: DataFrame, column_name: str, expression: str) -> DataFrame:
    return df.withColumn(column_name, sf.expr(expression))


def drop_columns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    existing = [column for column in columns if column in df.columns]
    return df.drop(*existing) if existing else df


def select_columns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    return df.select(*columns)


def rename_column(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    return df.withColumnRenamed(old_name, new_name)


def rename_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    return df.withColumnsRenamed(mapping) if mapping else df


def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    return df.filter(condition)


def apply_value_rule(
    df: DataFrame, rule: ValueRule, columns: Sequence[str]
) -> DataFrame:
    schema = df.schema
    mapping_expr: Optional[Column] = None
    if rule.operation == "map":
        entries = [
            item
            for pair in rule.mapping.items()
            for item in (sf.lit(pair[0]), sf.lit(pair[1]))
        ]
        mapping_expr = sf.create_map(*entries)
    expressions: Dict[str, Column] = {}
    for column in columns:
        dtype = schema[column].dataType
        source = sf.col(column)
        if rule.operation in {
            "trim",
            "case",
            "regex_replace",
            "empty_to_null",
            "map",
        } and not isinstance(dtype, T.StringType):
            raise TransformError(
                f"Value rule {rule.operation!r} requires a string column",
                details={"column": column, "data_type": str(dtype)},
            )
        if rule.operation == "trim":
            result = sf.trim(source)
        elif rule.operation == "case":
            result = sf.lower(source) if rule.mode == "lower" else sf.upper(source)
        elif rule.operation == "regex_replace":
            replacement = rule.replacement.replace("\\", "\\\\").replace("$", "\\$")
            result = sf.regexp_replace(source, rule.pattern or "", replacement)
        elif rule.operation == "empty_to_null":
            result = sf.when(source == "", sf.lit(None)).otherwise(source)
        elif rule.operation == "fill_null":
            literal = _coerce_literal(
                rule.value, dtype, field_path="value_rules.value", column=column
            )
            result = sf.coalesce(source, sf.lit(literal).cast(dtype))
        else:
            assert mapping_expr is not None
            mapped = sf.element_at(mapping_expr, source)
            result = (
                sf.coalesce(mapped, source) if rule.on_unmapped == "keep" else mapped
            )
        expressions[column] = result
    return df.withColumns(expressions) if expressions else df


def apply_masking_rule(
    df: DataFrame, rule: MaskingRule, columns: Sequence[str]
) -> DataFrame:
    schema = df.schema
    expressions: Dict[str, Column] = {}
    for column in columns:
        dtype = schema[column].dataType
        source = sf.col(column)
        if rule.method == "redact":
            literal = _coerce_literal(
                rule.value, dtype, field_path="masking_rules.value", column=column
            )
            result = sf.when(source.isNull(), source).otherwise(
                sf.lit(literal).cast(dtype)
            )
        elif rule.method == "nullify":
            result = sf.lit(None).cast(dtype)
        elif rule.method == "partial":
            if not isinstance(dtype, T.StringType):
                raise TransformError(
                    "partial masking requires a string column",
                    details={"column": column},
                )
            prefix = (
                sf.substring(source, 1, rule.keep_start)
                if rule.keep_start
                else sf.lit("")
            )
            suffix = (
                sf.substring(source, -rule.keep_end, rule.keep_end)
                if rule.keep_end
                else sf.lit("")
            )
            masked = sf.concat(prefix, sf.lit(rule.mask_char), suffix)
            result = (
                sf.when(source.isNull(), source)
                .when(source == "", source)
                .when(
                    sf.length(source) <= rule.keep_start + rule.keep_end,
                    sf.lit(rule.mask_char),
                )
                .otherwise(masked)
            )
        elif rule.method == "numeric_bucket":
            if not isinstance(dtype, T.NumericType):
                raise TransformError(
                    "numeric_bucket requires a numeric column",
                    details={"column": column},
                )
            result = (
                sf.floor(source / sf.lit(rule.bucket_size)) * sf.lit(rule.bucket_size)
            ).cast(dtype)
        else:
            if isinstance(dtype, T.DateType):
                if rule.unit == "hour":
                    raise TransformError(
                        "date_truncate hour requires a timestamp column",
                        details={"column": column},
                    )
                result = (
                    source
                    if rule.unit == "day"
                    else sf.trunc(source, rule.unit).cast(dtype)
                )
            elif isinstance(dtype, (T.TimestampType, T.TimestampNTZType)):
                result = sf.date_trunc(rule.unit, source).cast(dtype)
            else:
                raise TransformError(
                    "date_truncate requires a date or timestamp column",
                    details={"column": column},
                )
        expressions[column] = result
    return df.withColumns(expressions) if expressions else df


def add_hash_column(
    df: DataFrame, definition: HashColumn, columns: Sequence[str]
) -> DataFrame:
    components: List[Column] = []
    for column in columns:
        dtype = df.schema[column].dataType
        source = sf.col(column)
        if isinstance(dtype, T.StringType):
            tag, text = "S", source
        elif isinstance(dtype, T.IntegralType):
            tag, text = "I", source.cast("string")
        elif isinstance(dtype, T.BooleanType):
            tag, text = "B", sf.when(source, sf.lit("true")).otherwise(sf.lit("false"))
        elif isinstance(dtype, T.DateType):
            tag, text = "D", sf.date_format(source, "yyyy-MM-dd")
        else:
            raise TransformError(
                "hash_columns supports only string, integer, boolean, and date inputs",
                details={"column": column, "data_type": str(dtype)},
            )
        components.append(
            sf.when(source.isNull(), sf.lit(f"{tag}N;")).otherwise(
                sf.concat(
                    sf.lit(tag),
                    sf.octet_length(text).cast("string"),
                    sf.lit(":"),
                    text,
                    sf.lit(";"),
                )
            )
        )
    payload = sf.concat(sf.lit("DCH1;"), *components)
    if definition.algorithm == "sha256":
        result = sf.sha2(payload, 256)
    elif definition.algorithm == "xxhash64":
        result = sf.xxhash64(payload)
    else:  # HashColumn validation prevents this branch for public callers.
        raise TransformError(
            "Unsupported hash_columns algorithm",
            details={"algorithm": definition.algorithm},
        )
    return df.withColumn(definition.target_column, result)


def apply_watermark_filter(
    df: DataFrame,
    watermark_columns: Sequence[str],
    watermark_start: Dict[str, Any],
    *,
    start_operator: str,
    watermark_end: Optional[Dict[str, Any]],
    end_operator: str,
) -> DataFrame:
    combined = None
    for column in watermark_columns:
        lower = watermark_start.get(column)
        upper = (watermark_end or {}).get(column)
        if lower is None and upper is None:
            continue
        expression = sf.col(column)
        condition = None
        if lower is not None:
            if isinstance(lower, (datetime, date)):
                lower = lower.isoformat()
            condition = (
                expression >= sf.lit(lower)
                if start_operator == ">="
                else expression > sf.lit(lower)
            )
        if upper is not None:
            if isinstance(upper, (datetime, date)):
                upper = upper.isoformat()
            upper_condition = (
                expression <= sf.lit(upper)
                if end_operator == "<="
                else expression < sf.lit(upper)
            )
            condition = (
                condition & upper_condition
                if condition is not None
                else upper_condition
            )
        combined = condition if combined is None else combined | condition
    return df if combined is None else df.filter(combined)


def deduplicate(
    df: DataFrame,
    partition_columns: Sequence[str],
    order_columns: Optional[Sequence[str]],
    order: str,
    *,
    rank: bool,
) -> DataFrame:
    ordering = order_columns or partition_columns
    order_exprs = (
        [sf.col(column).asc() for column in ordering]
        if order == "asc"
        else [sf.col(column).desc() for column in ordering]
    )
    window = Window.partitionBy(*partition_columns).orderBy(*order_exprs)
    marker = "__rank" if rank else "__row_number"
    expression = sf.rank() if rank else sf.row_number()
    return (
        df.withColumn(marker, expression.over(window))
        .filter(sf.col(marker) == 1)
        .drop(marker)
    )


def add_system_columns(
    df: DataFrame,
    author: Optional[str],
    dataflow_run_id: Optional[str],
) -> DataFrame:
    result = (
        df.withColumn(
            SystemColumn.CREATED_AT,
            sf.from_utc_timestamp(sf.current_timestamp(), "UTC"),
        )
        .withColumn(
            SystemColumn.UPDATED_AT,
            sf.from_utc_timestamp(sf.current_timestamp(), "UTC"),
        )
        .withColumn(SystemColumn.UPDATED_BY, sf.lit(author or DEFAULT_AUTHOR))
    )
    if dataflow_run_id is not None:
        result = result.withColumn(
            SystemColumn.DATAFLOW_RUN_ID, sf.lit(dataflow_run_id)
        )
    return result


def convert_timestamp_ntz_to_timestamp(df: DataFrame) -> DataFrame:
    for column, dtype in df.dtypes:
        if dtype == "timestamp_ntz":
            df = df.withColumn(column, sf.col(column).cast("timestamp"))
    return df
