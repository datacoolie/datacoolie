"""Polars type aliases and conversion expressions."""

from __future__ import annotations

import re
from types import MappingProxyType
from typing import Mapping, Optional

import polars as pl

from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

_TYPE_ALIASES: Mapping[str, pl.DataType] = MappingProxyType(
    {
        "string": pl.Utf8,
        "str": pl.Utf8,
        "utf8": pl.Utf8,
        "varchar": pl.Utf8,
        "varchar2": pl.Utf8,
        "nvarchar": pl.Utf8,
        "nvarchar2": pl.Utf8,
        "char": pl.Utf8,
        "nchar": pl.Utf8,
        "character": pl.Utf8,
        "character varying": pl.Utf8,
        "text": pl.Utf8,
        "ntext": pl.Utf8,
        "tinytext": pl.Utf8,
        "mediumtext": pl.Utf8,
        "longtext": pl.Utf8,
        "clob": pl.Utf8,
        "nclob": pl.Utf8,
        "enum": pl.Utf8,
        "set": pl.Utf8,
        "uuid": pl.Utf8,
        "uniqueidentifier": pl.Utf8,
        "json": pl.Utf8,
        "jsonb": pl.Utf8,
        "xml": pl.Utf8,
        "citext": pl.Utf8,
        "byte": pl.Int8,
        "short": pl.Int16,
        "int": pl.Int32,
        "integer": pl.Int32,
        "int2": pl.Int16,
        "int4": pl.Int32,
        "int8": pl.Int64,
        "int16": pl.Int16,
        "int32": pl.Int32,
        "int64": pl.Int64,
        "long": pl.Int64,
        "tinyint": pl.Int8,
        "smallint": pl.Int16,
        "mediumint": pl.Int32,
        "bigint": pl.Int64,
        "byteint": pl.Int8,
        "hugeint": pl.Int64,
        "serial": pl.Int32,
        "smallserial": pl.Int16,
        "bigserial": pl.Int64,
        "number": pl.Decimal,
        "uint8": pl.UInt8,
        "uint16": pl.UInt16,
        "uint32": pl.UInt32,
        "uint64": pl.UInt64,
        "unsigned": pl.UInt64,
        "float": pl.Float32,
        "real": pl.Float32,
        "float4": pl.Float32,
        "float8": pl.Float64,
        "float32": pl.Float32,
        "float64": pl.Float64,
        "double": pl.Float64,
        "double precision": pl.Float64,
        "decimal": pl.Decimal,
        "numeric": pl.Decimal,
        "dec": pl.Decimal,
        "money": pl.Decimal,
        "smallmoney": pl.Decimal,
        "boolean": pl.Boolean,
        "bool": pl.Boolean,
        "bit": pl.Boolean,
        "logical": pl.Boolean,
        "date": pl.Date,
        "date32": pl.Date,
        "time": pl.Time,
        "timetz": pl.Time,
        "time with time zone": pl.Time,
        "time without time zone": pl.Time,
        "timestamp": pl.Datetime("us", "UTC"),
        "timestamptz": pl.Datetime("us", "UTC"),
        "timestamp_tz": pl.Datetime("us", "UTC"),
        "timestamp with time zone": pl.Datetime("us", "UTC"),
        "datetimeoffset": pl.Datetime("us", "UTC"),
        "timestamp_ntz": pl.Datetime("us"),
        "datetime": pl.Datetime("us"),
        "datetime2": pl.Datetime("us"),
        "smalldatetime": pl.Datetime("us"),
        "timestamp without time zone": pl.Datetime("us"),
        "interval": pl.Duration,
        "duration": pl.Duration,
        "binary": pl.Binary,
        "varbinary": pl.Binary,
        "bytea": pl.Binary,
        "blob": pl.Binary,
        "tinyblob": pl.Binary,
        "mediumblob": pl.Binary,
        "longblob": pl.Binary,
        "image": pl.Binary,
        "bytes": pl.Binary,
        "raw": pl.Binary,
        "long raw": pl.Binary,
    }
)

_DECIMAL_PATTERN = re.compile(r"^(?:decimal|numeric|dec|number)\((\d+),\s*(\d+)\)$")


def to_chrono_format(fmt: str) -> str:
    """Convert a Java DateTimeFormatter pattern to chrono/strftime syntax."""
    if "%" in fmt:
        return fmt
    result = fmt
    for java, chrono in (
        ("yyyy", "%Y"),
        ("yy", "%y"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
        ("SSS", "%f"),
    ):
        result = result.replace(java, chrono)
    return result


def build_cast_expr(
    col_name: str,
    target_type: str,
    src_dtype: pl.DataType,
    fmt: Optional[str],
) -> Optional[pl.Expr]:
    """Build a cast expression, returning ``None`` for an unknown type."""
    target_lower = target_type.lower()
    col = pl.col(col_name)

    decimal_match = _DECIMAL_PATTERN.match(target_lower)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        return col.cast(pl.Decimal(precision=precision, scale=scale))

    pl_type = _TYPE_ALIASES.get(target_lower)
    if pl_type is None:
        logger.debug(
            "PolarsEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
            target_lower,
        )
        return None

    if pl_type == pl.Date:
        if fmt:
            return col.str.to_date(to_chrono_format(fmt))
        return col.cast(pl.Date)

    if isinstance(pl_type, pl.Datetime):
        time_zone = pl_type.time_zone
        time_unit = pl_type.time_unit or "us"
        if fmt:
            return col.str.to_datetime(
                to_chrono_format(fmt),
                time_unit=time_unit,
                time_zone=time_zone,
            )
        if time_zone:
            if isinstance(src_dtype, pl.String):
                return col.str.to_datetime(time_unit=time_unit, time_zone=time_zone)
            if isinstance(src_dtype, pl.Datetime) and src_dtype.time_zone:
                return col.dt.convert_time_zone(time_zone)
            return col.cast(pl.Datetime(time_unit)).dt.replace_time_zone(time_zone)
        return col.cast(pl.Datetime(time_unit))

    return col.cast(pl_type)


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
