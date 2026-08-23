"""Spark type alias resolution, casting, and Hive type conversion."""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import types as T

from datacoolie.logging.base import get_logger

logger = get_logger(__name__)

SPARK_TYPE_MAP: Dict[str, str] = {
    "string": "string",
    "boolean": "boolean",
    "byte": "byte",
    "tinyint": "tinyint",
    "short": "short",
    "smallint": "smallint",
    "int": "int",
    "integer": "integer",
    "long": "long",
    "bigint": "bigint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp_ntz",
    "interval": "interval",
    "void": "void",
    "binary": "binary",
    "str": "string",
    "varchar": "string",
    "varchar2": "string",
    "nvarchar": "string",
    "nvarchar2": "string",
    "char": "string",
    "nchar": "string",
    "character": "string",
    "character varying": "string",
    "text": "string",
    "ntext": "string",
    "tinytext": "string",
    "mediumtext": "string",
    "longtext": "string",
    "clob": "string",
    "nclob": "string",
    "enum": "string",
    "set": "string",
    "uuid": "string",
    "uniqueidentifier": "string",
    "json": "string",
    "jsonb": "string",
    "xml": "string",
    "citext": "string",
    "time": "string",
    "timetz": "string",
    "time with time zone": "string",
    "time without time zone": "string",
    "bool": "boolean",
    "bit": "boolean",
    "logical": "boolean",
    "byteint": "byte",
    "uint8": "tinyint",
    "int2": "smallint",
    "int16": "smallint",
    "smallserial": "smallint",
    "uint16": "smallint",
    "int4": "int",
    "int32": "int",
    "mediumint": "int",
    "serial": "int",
    "uint32": "int",
    "int8": "bigint",
    "int64": "bigint",
    "hugeint": "bigint",
    "bigserial": "bigint",
    "uint64": "bigint",
    "unsigned": "bigint",
    "real": "float",
    "float4": "float",
    "float32": "float",
    "float8": "double",
    "float64": "double",
    "double precision": "double",
    "numeric": "decimal",
    "dec": "decimal",
    "number": "decimal",
    "money": "decimal",
    "smallmoney": "decimal",
    "timestamptz": "timestamp",
    "timestamp_tz": "timestamp",
    "timestamp with time zone": "timestamp",
    "datetimeoffset": "timestamp",
    "datetime": "timestamp_ntz",
    "datetime2": "timestamp_ntz",
    "smalldatetime": "timestamp_ntz",
    "timestamp without time zone": "timestamp_ntz",
    "varbinary": "binary",
    "bytea": "binary",
    "blob": "binary",
    "tinyblob": "binary",
    "mediumblob": "binary",
    "longblob": "binary",
    "image": "binary",
    "bytes": "binary",
    "raw": "binary",
    "long raw": "binary",
}


def resolve_type(target_type: str) -> Optional[str]:
    """Resolve SQL aliases and preserve parameter suffixes."""
    if "(" in target_type:
        base, params = target_type.split("(", 1)
        resolved_base = SPARK_TYPE_MAP.get(base.lower())
        if resolved_base is None:
            logger.debug(
                "SparkEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
                target_type,
            )
            return None
        return f"{resolved_base}({params}"
    resolved = SPARK_TYPE_MAP.get(target_type.lower())
    if resolved is None:
        logger.debug(
            "SparkEngine.cast_column: unknown type %r — bypassing cast, column kept as-is",
            target_type,
        )
    return resolved


def cast_column(
    df: DataFrame,
    column_name: str,
    target_type: str,
    fmt: Optional[str] = None,
) -> DataFrame:
    """Cast a column with the same alias and format behavior as SparkEngine."""
    resolved = resolve_type(target_type)
    if resolved is None:
        return df
    lower = resolved.lower()
    if lower == "date" and fmt:
        return df.withColumn(column_name, sf.to_date(sf.col(column_name), fmt))
    if lower == "timestamp" and fmt:
        return df.withColumn(column_name, sf.to_timestamp(sf.col(column_name), fmt))
    return df.withColumn(column_name, sf.col(column_name).cast(resolved))


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
        return (
            f"MAP<{spark_type_to_hive(dt.keyType)},{spark_type_to_hive(dt.valueType)}>"
        )
    if isinstance(dt, T.StructType):
        fields = [
            f"{field.name}:{spark_type_to_hive(field.dataType)}" for field in dt.fields
        ]
        return f"STRUCT<{','.join(fields)}>"
    return "STRING"
