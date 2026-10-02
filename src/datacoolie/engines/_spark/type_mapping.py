"""Spark datatype interpretation, native casting, and Hive conversion."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import types as T

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


_SPARK_INTEGER_TYPES = (
    (T.ByteType, "tinyint"),
    (T.ShortType, "smallint"),
    (T.IntegerType, "int"),
    (T.LongType, "bigint"),
)


def resolve_type(
    target_type: str | ResolvedDataType,
    *,
    type_system: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> ResolvedDataType:
    """Resolve one authored source declaration for the Spark adapter.

    The returned object is a dependency-free logical description.  Native
    Spark types are constructed by :func:`_spark_dtype`; no intermediate
    target string is serialized and parsed again.
    """
    if isinstance(target_type, ResolvedDataType):
        return target_type
    try:
        return resolve_schema_hint(
            target_type,
            type_system=type_system,
            precision=precision,
            scale=scale,
        )
    except ConfigurationError:
        logger.debug(
            "SparkEngine.cast_column: unsupported source type %r",
            target_type,
        )
        raise


def _spark_dtype(resolved: ResolvedDataType) -> T.DataType:
    """Build the native Spark datatype for a resolved logical declaration."""
    if resolved.kind is LogicalKind.BOOLEAN:
        return T.BooleanType()
    if resolved.kind is LogicalKind.SIGNED_INTEGER:
        return {
            8: T.ByteType(),
            16: T.ShortType(),
            32: T.IntegerType(),
            64: T.LongType(),
        }[resolved.bit_width]
    if resolved.kind is LogicalKind.UNSIGNED_INTEGER:
        return {
            8: T.ShortType(),
            16: T.IntegerType(),
            32: T.LongType(),
            64: T.DecimalType(resolved.precision or 20, resolved.scale or 0),
        }[resolved.bit_width]
    if resolved.kind is LogicalKind.FLOAT:
        return T.FloatType() if resolved.bit_width == 32 else T.DoubleType()
    if resolved.kind is LogicalKind.DECIMAL:
        return T.DecimalType(resolved.precision or 1, resolved.scale or 0)
    if resolved.kind is LogicalKind.STRING:
        return T.StringType()
    if resolved.kind is LogicalKind.BINARY:
        return T.BinaryType()
    if resolved.kind is LogicalKind.DATE:
        return T.DateType()
    if resolved.kind is LogicalKind.TIMESTAMP:
        return (
            T.TimestampNTZType()
            if resolved.timestamp_kind is TimestampKind.NAIVE
            else T.TimestampType()
        )
    raise ConfigurationError(
        "No Spark adapter exists for resolved datatype",
        details={"kind": resolved.kind.value, "source_type": resolved.source_type},
    )


def cast_column(
    df: DataFrame,
    column_name: str,
    target_type: str | ResolvedDataType,
    fmt: Optional[str] = None,
    *,
    type_system: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> DataFrame:
    """Cast a column using the source declaration and Spark-native APIs."""
    resolved = resolve_type(
        target_type,
        type_system=type_system,
        precision=precision,
        scale=scale,
    )
    column = sf.col(column_name)
    # MySQL's JDBC driver exposes YEAR as a java.sql.Date (Spark therefore
    # reads it as DateType), while the authored MySQL semantic is a numeric
    # calendar year.  A plain Spark ``date.cast(short)`` yields NULL, which
    # would silently lose the value during the schema-hint cast.  Normalize
    # only this vendor/type pair at the engine boundary; native numeric
    # readers continue through the ordinary cast path.
    if (
        type_system == "mysql"
        and resolved.source_type.strip().lower() == "year"
        and isinstance(df.schema[column_name].dataType, (T.DateType, T.TimestampType, T.TimestampNTZType))
    ):
        return df.withColumn(
            column_name,
            sf.year(column).cast(_spark_dtype(resolved)),
        )
    if resolved.kind is LogicalKind.DATE and fmt:
        return df.withColumn(column_name, sf.to_date(column, fmt))
    if resolved.kind is LogicalKind.TIMESTAMP and fmt:
        if resolved.timestamp_kind is TimestampKind.NAIVE:
            return df.withColumn(
                column_name,
                sf.to_timestamp_ntz(column, sf.lit(fmt)),
            )
        return df.withColumn(column_name, sf.to_timestamp(column, fmt))
    return df.withColumn(column_name, column.cast(_spark_dtype(resolved)))


def normalize_output_frame(df: DataFrame, output_format: str | Format) -> DataFrame:
    """Apply persisted-format integer rules at the native write boundary."""
    try:
        normalized = normalize_output_format(output_format)
    except ConfigurationError:
        return df

    result = df
    for field in df.schema.fields:
        source_alias = next(
            (
                alias
                for dtype, alias in _SPARK_INTEGER_TYPES
                if isinstance(field.dataType, dtype)
            ),
            None,
        )
        if source_alias is None:
            continue
        target = output_type_for_format(source_alias, normalized)
        if target != source_alias:
            result = result.withColumn(
                field.name,
                sf.col(field.name).cast(target),
            )
    return result


def spark_type_to_hive(dt: T.DataType) -> str:
    """Recursively convert a PySpark DataType to a Hive/Athena DDL string."""
    if isinstance(dt, T.LongType):
        return "BIGINT"
    if isinstance(dt, T.IntegerType):
        return "INT"
    if isinstance(dt, T.ShortType):
        return "SMALLINT"
    if isinstance(dt, (T.ByteType, T.NullType)):
        return "TINYINT"
    if isinstance(dt, T.FloatType):
        return "FLOAT"
    if isinstance(dt, T.DoubleType):
        return "DOUBLE"
    if isinstance(dt, T.BooleanType):
        return "BOOLEAN"
    if isinstance(dt, T.BinaryType):
        return "BINARY"
    if isinstance(dt, T.DateType):
        return "DATE"
    if isinstance(dt, (T.TimestampType, T.TimestampNTZType)):
        return "TIMESTAMP"
    if isinstance(dt, T.DecimalType):
        return f"DECIMAL({dt.precision},{dt.scale})"
    if isinstance(dt, T.StringType):
        return "STRING"
    if isinstance(dt, T.ArrayType):
        return f"ARRAY<{spark_type_to_hive(dt.elementType)}>"
    if isinstance(dt, T.MapType):
        return f"MAP<{spark_type_to_hive(dt.keyType)},{spark_type_to_hive(dt.valueType)}>"
    if isinstance(dt, T.StructType):
        fields = [
            f"{field.name}:{spark_type_to_hive(field.dataType)}"
            for field in dt.fields
        ]
        return f"STRUCT<{','.join(fields)}>"
    return "STRING"
