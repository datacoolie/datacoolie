"""Polars datatype interpretation, native casts, and format adaptation."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Mapping, Optional

import polars as pl

from datacoolie.core.constants import Format
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.data_types import (
    LogicalKind,
    ResolvedDataType,
    TimestampKind,
    resolve_schema_hint,
)
from datacoolie.engines.data_types.formats import normalize_output_format, output_type_for_format
from datacoolie.logging.runtime.manager import get_logger

logger = get_logger(__name__)

_TYPE_ALIASES: Mapping[str, pl.DataType] = MappingProxyType(
    {
        # Output-format adaptation uses these Spark-compatible aliases. Casts
        # from metadata use the logical resolver below instead.
        "string": pl.Utf8,
        "tinyint": pl.Int8,
        "smallint": pl.Int16,
        "int": pl.Int32,
        "bigint": pl.Int64,
        "float": pl.Float32,
        "double": pl.Float64,
        "boolean": pl.Boolean,
        "date": pl.Date,
        "timestamp": pl.Datetime("us", "UTC"),
        "timestamp_ntz": pl.Datetime("us"),
        "binary": pl.Binary,
    }
)

_DECIMAL_PATTERN = re.compile(r"^(?:decimal|numeric|dec|number)\((\d+),\s*(\d+)\)$")


def to_chrono_format(fmt: str) -> str:
    """Convert a Java DateTimeFormatter pattern to chrono/strftime syntax."""
    if "%" in fmt:
        return fmt
    result = fmt
    # Convert Java fractional-second runs before replacing ``ss`` with the
    # chrono seconds directive; otherwise the fallback would also rewrite the
    # newly-created ``%S`` token.
    result = re.sub(
        r"\.(S+)",
        lambda match: f"%.{len(match.group(1))}f",
        result,
    )
    result = re.sub(
        r"(?<!%)S+",
        lambda match: f"%{len(match.group(0))}f",
        result,
    )
    for java, chrono in (
        ("yyyy", "%Y"),
        ("yy", "%y"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
    ):
        result = result.replace(java, chrono)
    return result


def _resolve_type(
    target_type: str | ResolvedDataType,
    *,
    type_system: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> ResolvedDataType:
    if isinstance(target_type, ResolvedDataType):
        return target_type
    return resolve_schema_hint(
        target_type,
        type_system=type_system,
        precision=precision,
        scale=scale,
    )


def _native_dtype(resolved: ResolvedDataType) -> pl.DataType:
    """Build the native Polars dtype for one logical datatype."""
    if resolved.kind is LogicalKind.BOOLEAN:
        return pl.Boolean
    if resolved.kind is LogicalKind.SIGNED_INTEGER:
        return {8: pl.Int8, 16: pl.Int16, 32: pl.Int32, 64: pl.Int64}[resolved.bit_width]
    if resolved.kind is LogicalKind.UNSIGNED_INTEGER:
        return {
            8: pl.Int16,
            16: pl.Int32,
            32: pl.Int64,
            64: pl.Decimal(resolved.precision or 20, resolved.scale or 0),
        }[resolved.bit_width]
    if resolved.kind is LogicalKind.FLOAT:
        return pl.Float32 if resolved.bit_width == 32 else pl.Float64
    if resolved.kind is LogicalKind.DECIMAL:
        return pl.Decimal(resolved.precision or 1, resolved.scale or 0)
    if resolved.kind is LogicalKind.STRING:
        return pl.String
    if resolved.kind is LogicalKind.BINARY:
        return pl.Binary
    if resolved.kind is LogicalKind.DATE:
        return pl.Date
    if resolved.kind is LogicalKind.TIMESTAMP:
        return pl.Datetime(
            "us",
            "UTC" if resolved.timestamp_kind is TimestampKind.INSTANT else None,
        )
    raise ConfigurationError(
        "No Polars adapter exists for resolved datatype",
        details={"kind": resolved.kind.value, "source_type": resolved.source_type},
    )


def build_cast_expr(
    col_name: str,
    target_type: str | ResolvedDataType,
    src_dtype: pl.DataType,
    fmt: Optional[str],
    *,
    type_system: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> pl.Expr:
    """Build a native cast, resolving a source declaration at most once."""
    try:
        resolved = _resolve_type(
            target_type,
            type_system=type_system,
            precision=precision,
            scale=scale,
        )
    except ConfigurationError:
        logger.debug("PolarsEngine.cast_column: unsupported source type %r", target_type)
        raise

    col = pl.col(col_name)
    if resolved.kind is LogicalKind.DATE:
        if fmt and isinstance(src_dtype, (pl.String, pl.Utf8)):
            chrono_fmt = to_chrono_format(fmt)
            has_time = bool(
                re.search(r"(?:%H|%M|%S|%f|HH|mm|ss|S)", fmt)
            )
            if has_time:
                return col.str.to_datetime(chrono_fmt, time_unit="us").dt.date()
            return col.str.to_date(chrono_fmt)
        return col.cast(pl.Date)

    if resolved.kind is LogicalKind.TIMESTAMP:
        native = _native_dtype(resolved)
        if fmt and isinstance(src_dtype, (pl.String, pl.Utf8)):
            return col.str.to_datetime(
                to_chrono_format(fmt),
                time_unit="us",
                time_zone=native.time_zone,
            )
        if native.time_zone:
            if isinstance(src_dtype, (pl.String, pl.Utf8)):
                return col.str.to_datetime(time_unit="us", time_zone=native.time_zone)
            if isinstance(src_dtype, pl.Datetime) and src_dtype.time_zone:
                return col.dt.convert_time_zone(native.time_zone)
            return col.cast(pl.Datetime("us")).dt.replace_time_zone(native.time_zone)
        return col.cast(pl.Datetime("us"))

    return col.cast(_native_dtype(resolved))


_POLARS_INTEGER_TYPES = (
    (pl.Int8, "tinyint", False),
    (pl.Int16, "smallint", False),
    (pl.Int32, "int", False),
    (pl.Int64, "bigint", False),
    (pl.UInt8, "tinyint unsigned", True),
    (pl.UInt16, "smallint unsigned", True),
    (pl.UInt32, "int unsigned", True),
    (pl.UInt64, "bigint unsigned", True),
)


def _logical_output_type_for_native(dtype: pl.DataType) -> Optional[str]:
    """Map a native integer to a range-preserving format rule.

    This is an engine/output concern.  It must not pretend that an arbitrary
    dataframe originated from MySQL merely because an unsigned native dtype
    needs widening for a persisted format.
    """
    for dtype_class, alias, unsigned in _POLARS_INTEGER_TYPES:
        if isinstance(dtype, dtype_class):
            if not unsigned:
                return alias
            return {
                "tinyint unsigned": "smallint",
                "smallint unsigned": "int",
                "int unsigned": "bigint",
                "bigint unsigned": "decimal(20,0)",
            }[alias]
    return None


def _target_dtype(target_type: str) -> pl.DataType:
    """Return the native Polars dtype for a format-rule alias."""

    decimal_match = _DECIMAL_PATTERN.match(target_type)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        return pl.Decimal(precision, scale)
    try:
        return _TYPE_ALIASES[target_type]
    except KeyError as exc:
        raise ConfigurationError(
            "No Polars adapter exists for output logical type",
            details={"target_type": target_type},
        ) from exc


def normalize_output_frame(
    frame: pl.LazyFrame, output_format: str | Format
) -> pl.LazyFrame:
    """Apply persisted-format integer rules before a Polars write or merge.

    Source-hint casting remains the responsibility of ``SchemaConverter``.
    This function only translates the already-native frame to the selected
    format's portable physical dtype.  Lazy schema inspection avoids reading
    the data before the writer owns materialisation.
    """

    try:
        normalized = normalize_output_format(output_format)
    except ConfigurationError:
        return frame

    schema = frame.collect_schema()
    expressions: list[pl.Expr] = []
    for column in schema.names():
        dtype = schema[column]
        if hasattr(pl, "Int128") and isinstance(dtype, pl.Int128):
            raise ConfigurationError(
                "Polars Int128 has no cross-engine persisted datatype mapping; "
                "provide a schema hint with an explicit supported range",
                details={"column": column, "dtype": str(dtype)},
            )
        logical_type = _logical_output_type_for_native(dtype)
        if logical_type is None:
            continue
        target = output_type_for_format(logical_type, normalized)
        target_dtype = _target_dtype(target)
        if dtype != target_dtype:
            # The format policy has already selected the target from the
            # native dtype. Cast directly instead of resolving the same
            # alias again through the source-hint path.
            expressions.append(pl.col(column).cast(target_dtype).alias(column))

    return frame.with_columns(expressions) if expressions else frame


def polars_type_to_hive(dtype: pl.DataType) -> str:
    """Recursively convert a Polars dtype to a Hive/Athena DDL type."""
    if isinstance(dtype, pl.Int64):
        return "BIGINT"
    if isinstance(dtype, pl.Int32):
        return "INT"
    if isinstance(dtype, pl.Int16):
        return "SMALLINT"
    if isinstance(dtype, pl.Int8):
        return "TINYINT"
    if isinstance(dtype, pl.UInt8):
        return "SMALLINT"
    if isinstance(dtype, pl.UInt16):
        return "INT"
    if isinstance(dtype, (pl.UInt32, pl.UInt64)):
        return "BIGINT"
    if isinstance(dtype, pl.Float32):
        return "FLOAT"
    if isinstance(dtype, pl.Float64):
        return "DOUBLE"
    if isinstance(dtype, pl.Boolean):
        return "BOOLEAN"
    if isinstance(dtype, pl.Binary):
        return "BINARY"
    if isinstance(dtype, pl.Date):
        return "DATE"
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
    if isinstance(dtype, pl.Decimal):
        precision = dtype.precision if dtype.precision is not None else 38
        scale = dtype.scale if dtype.scale is not None else 0
        return f"DECIMAL({precision},{scale})"
    if isinstance(dtype, (pl.Duration, pl.Time, pl.Null)):
        return "STRING"
    if isinstance(dtype, (pl.String, pl.Utf8, pl.Categorical)):
        return "STRING"
    if isinstance(dtype, pl.List):
        return f"ARRAY<{polars_type_to_hive(dtype.inner)}>"
    if isinstance(dtype, pl.Struct):
        fields = [
            f"{field.name}:{polars_type_to_hive(field.dtype)}" for field in dtype.fields
        ]
        return f"STRUCT<{','.join(fields)}>"
    return "STRING"
