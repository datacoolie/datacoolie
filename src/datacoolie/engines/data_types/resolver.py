"""Source dialect parsing and logical datatype resolution."""

from __future__ import annotations

import re

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.engines.data_types.models import (
    LogicalKind,
    ResolvedDataType,
    TimestampKind,
    TypeSystem,
)


_TYPE_RE = re.compile(r"^(?P<base>.+?)(?:\((?P<args>[^()]*)\))?$")
_ALIASES = {
    "spark": TypeSystem.SPARK_SQL.value,
    "sparksql": TypeSystem.SPARK_SQL.value,
    "spark sql": TypeSystem.SPARK_SQL.value,
    "postgres": TypeSystem.POSTGRESQL.value,
    "postgresql": TypeSystem.POSTGRESQL.value,
    "sqlserver": TypeSystem.MSSQL.value,
    "sql server": TypeSystem.MSSQL.value,
    "mssql": TypeSystem.MSSQL.value,
    "mariadb": TypeSystem.MYSQL.value,
}


def normalize_type_system(value: str | TypeSystem | None) -> str:
    """Normalize a documented dialect name or fail explicitly."""

    if value is None:
        return TypeSystem.SPARK_SQL.value
    if isinstance(value, str) and not value.strip():
        raise ConfigurationError(
            "schema_hint_type_system must not be blank",
            details={"value": value},
        )
    if isinstance(value, TypeSystem):
        return value.value
    if not isinstance(value, str):
        raise ConfigurationError(
            "schema_hint_type_system must be a string",
            details={"value_type": type(value).__name__},
        )
    normalized = " ".join(value.strip().lower().replace("_", " ").split())
    normalized = _ALIASES.get(normalized, normalized.replace(" ", "_"))
    supported = {item.value for item in TypeSystem}
    if normalized not in supported:
        raise ConfigurationError(
            f"Unsupported schema hint type system: {value!r}",
            details={"supported": sorted(supported)},
        )
    return normalized


def infer_type_system(
    database_type: str | None = None,
    explicit_type_system: str | TypeSystem | None = None,
) -> str:
    """Choose the authored hint dialect from explicit source metadata.

    The execution engine is intentionally not an input.  A lakehouse/file,
    API, function, or database without a declared vendor has no reliable
    dialect signal, so Spark SQL is the documented neutral convention.
    """

    if explicit_type_system is not None:
        return normalize_type_system(explicit_type_system)
    if database_type:
        return normalize_type_system(database_type)
    return TypeSystem.SPARK_SQL.value


def _parse_type(data_type: str) -> tuple[str, list[int]]:
    if not isinstance(data_type, str) or not data_type.strip():
        raise ConfigurationError("Schema hint data_type must be a non-empty string")
    text = " ".join(data_type.strip().lower().split())
    match = _TYPE_RE.fullmatch(text)
    if not match:
        raise ConfigurationError(
            "Invalid schema hint datatype", details={"data_type": data_type}
        )
    base = " ".join(match.group("base").strip().split())
    args_text = match.group("args")
    if not args_text:
        return base, []
    args: list[int] = []
    for part in args_text.split(","):
        try:
            args.append(int(part.strip()))
        except ValueError as exc:
            raise ConfigurationError(
                "Datatype parameters must be integers",
                details={"data_type": data_type, "parameters": args_text},
            ) from exc
    if len(args) > 2:
        raise ConfigurationError(
            "Datatype accepts at most precision and scale parameters",
            details={"data_type": data_type},
        )
    return base, args


def _parameters(
    type_system: str,
    args: list[int],
    precision: int | None,
    scale: int | None,
    *,
    data_type: str,
    default_scale: int | None = None,
    allow_negative_scale: bool = False,
) -> tuple[int, int]:
    embedded_precision = args[0] if args else None
    embedded_scale = args[1] if len(args) == 2 else None
    if (
        precision is not None
        and embedded_precision is not None
        and precision != embedded_precision
    ):
        raise ConfigurationError(
            "Schema hint precision conflicts with embedded datatype precision",
            details={
                "data_type": data_type,
                "hint_precision": precision,
                "embedded_precision": embedded_precision,
            },
        )
    if scale is not None and embedded_scale is not None and scale != embedded_scale:
        raise ConfigurationError(
            "Schema hint scale conflicts with embedded datatype scale",
            details={
                "data_type": data_type,
                "hint_scale": scale,
                "embedded_scale": embedded_scale,
            },
        )
    resolved_precision = precision if precision is not None else embedded_precision
    resolved_scale = scale if scale is not None else embedded_scale
    if resolved_scale is None and resolved_precision is not None:
        resolved_scale = default_scale
    if resolved_precision is None or resolved_scale is None:
        raise ConfigurationError(
            "Decimal types require explicit precision and scale",
            details={"data_type": data_type, "type_system": type_system},
        )
    if resolved_scale < 0 and not allow_negative_scale:
        raise ConfigurationError(
            "Negative decimal scale is not supported by this type system",
            details={"data_type": data_type, "type_system": type_system},
        )
    return resolved_precision, resolved_scale


def _decimal(
    source_type: str,
    type_system: str,
    args: list[int],
    precision: int | None,
    scale: int | None,
    *,
    default_scale: int | None = None,
    allow_negative_scale: bool = False,
) -> ResolvedDataType:
    resolved_precision, resolved_scale = _parameters(
        type_system,
        args,
        precision,
        scale,
        data_type=source_type,
        default_scale=default_scale,
        allow_negative_scale=allow_negative_scale,
    )
    # Oracle and PostgreSQL permit negative scales.  A negative scale means
    # source values are rounded to positions left of the decimal point.  The
    # portable contract represents the resulting integral value with the
    # additional magnitude digits in its precision instead of leaking a
    # dialect-specific negative scale into Spark/Polars.
    if resolved_scale < 0:
        resolved_precision -= resolved_scale
        resolved_scale = 0
    return ResolvedDataType(
        kind=LogicalKind.DECIMAL,
        source_type=source_type,
        source_type_system=type_system,
        precision=resolved_precision,
        scale=resolved_scale,
    )


def _integer(
    source_type: str,
    type_system: str,
    bits: int,
    *,
    unsigned: bool = False,
) -> ResolvedDataType:
    return ResolvedDataType(
        kind=LogicalKind.UNSIGNED_INTEGER if unsigned else LogicalKind.SIGNED_INTEGER,
        source_type=source_type,
        source_type_system=type_system,
        bit_width=bits,
    )


def _float(source_type: str, type_system: str, bits: int) -> ResolvedDataType:
    return ResolvedDataType(
        kind=LogicalKind.FLOAT,
        source_type=source_type,
        source_type_system=type_system,
        bit_width=bits,
    )


def _float_precision(
    source_type: str,
    type_system: str,
    args: list[int],
    *,
    maximum: int,
    minimum: int = 1,
    single_precision_limit: int,
    default_bits: int,
) -> ResolvedDataType:
    """Resolve vendor float precision into the supported IEEE width.

    Source systems expose precision differently, but the current logical
    contract deliberately carries only 32-bit or 64-bit approximate values.
    The vendor-specific range is validated before choosing the smallest
    width that can represent the declared precision.
    """
    if len(args) > 1:
        raise ConfigurationError(
            "Floating-point datatype accepts at most one precision parameter",
            details={"data_type": source_type, "type_system": type_system},
        )
    if not args:
        return _float(source_type, type_system, default_bits)
    precision = args[0]
    if precision < minimum or precision > maximum:
        raise ConfigurationError(
            "Floating-point precision is outside the supported source range",
            details={
                "data_type": source_type,
                "precision": precision,
                "minimum": minimum,
                "maximum": maximum,
                "type_system": type_system,
            },
        )
    return _float(
        source_type,
        type_system,
        32 if precision <= single_precision_limit else 64,
    )


def _mysql_float(
    source_type: str, args: list[int], *, default_bits: int
) -> ResolvedDataType:
    """Resolve MySQL FLOAT/DOUBLE precision forms, including ``(M,D)``."""
    system = TypeSystem.MYSQL.value
    if len(args) == 2:
        digits, scale = args
        if digits < 1 or scale < 0 or scale > digits:
            raise ConfigurationError(
                "MySQL floating-point digits/scale are invalid",
                details={
                    "data_type": source_type,
                    "digits": digits,
                    "scale": scale,
                },
            )
        return _float(source_type, system, default_bits)
    return _float_precision(
        source_type,
        system,
        args,
        maximum=53,
        minimum=0,
        single_precision_limit=24,
        default_bits=default_bits,
    )


def _timestamp(
    source_type: str, type_system: str, kind: TimestampKind
) -> ResolvedDataType:
    return ResolvedDataType(
        kind=LogicalKind.TIMESTAMP,
        source_type=source_type,
        source_type_system=type_system,
        timestamp_kind=kind,
    )


def _simple(kind: LogicalKind, source_type: str, type_system: str) -> ResolvedDataType:
    return ResolvedDataType(
        kind=kind, source_type=source_type, source_type_system=type_system
    )


def _resolve_spark(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    if base in {"boolean", "bool"}:
        return _simple(LogicalKind.BOOLEAN, raw, TypeSystem.SPARK_SQL.value)
    if base in {"byte", "tinyint"}:
        return _integer(raw, TypeSystem.SPARK_SQL.value, 8)
    if base in {"short", "smallint"}:
        return _integer(raw, TypeSystem.SPARK_SQL.value, 16)
    if base in {"int", "integer"}:
        return _integer(raw, TypeSystem.SPARK_SQL.value, 32)
    if base in {"long", "bigint"}:
        return _integer(raw, TypeSystem.SPARK_SQL.value, 64)
    if base == "float":
        if args:
            raise ConfigurationError(
                "Spark SQL FLOAT does not accept a precision parameter",
                details={"data_type": raw},
            )
        return _float(raw, TypeSystem.SPARK_SQL.value, 32)
    if base == "real":
        if args:
            raise ConfigurationError(
                "Spark SQL REAL does not accept a precision parameter",
                details={"data_type": raw},
            )
        return _float(raw, TypeSystem.SPARK_SQL.value, 32)
    if base in {"double", "double precision"}:
        if args:
            raise ConfigurationError(
                "Spark SQL DOUBLE does not accept a precision parameter",
                details={"data_type": raw},
            )
        return _float(raw, TypeSystem.SPARK_SQL.value, 64)
    if base in {"decimal", "numeric", "dec"}:
        return _decimal(raw, TypeSystem.SPARK_SQL.value, args, precision, scale)
    if base in {"string", "varchar", "char", "text"}:
        return _simple(LogicalKind.STRING, raw, TypeSystem.SPARK_SQL.value)
    if base == "binary":
        return _simple(LogicalKind.BINARY, raw, TypeSystem.SPARK_SQL.value)
    if base == "date":
        return _simple(LogicalKind.DATE, raw, TypeSystem.SPARK_SQL.value)
    if base in {"timestamp", "timestamp_ltz"}:
        return _timestamp(raw, TypeSystem.SPARK_SQL.value, TimestampKind.INSTANT)
    if base == "timestamp_ntz":
        return _timestamp(raw, TypeSystem.SPARK_SQL.value, TimestampKind.NAIVE)
    raise ConfigurationError(
        f"Unsupported Spark SQL schema hint datatype: {raw!r}",
        details={"type_system": TypeSystem.SPARK_SQL.value},
    )


def _resolve_postgresql(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    system = TypeSystem.POSTGRESQL.value
    if base in {"bool", "boolean"}:
        return _simple(LogicalKind.BOOLEAN, raw, system)
    if base in {"int2", "smallint"}:
        return _integer(raw, system, 16)
    if base in {"int4", "integer", "int", "serial", "serial4"}:
        return _integer(raw, system, 32)
    if base in {"int8", "bigint", "bigserial", "serial8"}:
        return _integer(raw, system, 64)
    if base in {"real", "float4"}:
        return _float(raw, system, 32)
    if base in {"double precision", "float8"}:
        return _float(raw, system, 64)
    if base in {"numeric", "decimal"}:
        return _decimal(
            raw,
            system,
            args,
            precision,
            scale,
            default_scale=0,
            allow_negative_scale=True,
        )
    if base in {"text", "varchar", "character varying", "char", "character"}:
        return _simple(LogicalKind.STRING, raw, system)
    if base == "bytea":
        return _simple(LogicalKind.BINARY, raw, system)
    if base == "date":
        return _simple(LogicalKind.DATE, raw, system)
    if base in {"timestamp", "timestamp without time zone"}:
        return _timestamp(raw, system, TimestampKind.NAIVE)
    if base in {"timestamptz", "timestamp with time zone"}:
        return _timestamp(raw, system, TimestampKind.INSTANT)
    raise ConfigurationError(
        f"Unsupported PostgreSQL schema hint datatype: {raw!r}",
        details={"type_system": system},
    )


def _resolve_mysql(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    system = TypeSystem.MYSQL.value
    unsigned = base.endswith(" unsigned")
    base_name = base.removesuffix(" unsigned").strip()
    if base_name in {"tinyint"}:
        return _integer(raw, system, 8, unsigned=unsigned)
    if base_name in {"smallint"}:
        return _integer(raw, system, 16, unsigned=unsigned)
    if base_name in {"mediumint", "int", "integer"}:
        return _integer(raw, system, 32, unsigned=unsigned)
    if base_name == "bigint":
        return _integer(raw, system, 64, unsigned=unsigned)
    if base_name in {"decimal", "numeric", "dec"}:
        return _decimal(
            raw,
            system,
            args,
            precision,
            scale,
            default_scale=0,
        )
    if base_name == "float":
        return _mysql_float(raw, args, default_bits=32)
    if base_name == "real":
        return _mysql_float(raw, args, default_bits=64)
    if base_name in {"double", "double precision"}:
        return _mysql_float(raw, args, default_bits=64)
    if base_name in {"bool", "boolean"}:
        return _simple(LogicalKind.BOOLEAN, raw, system)
    if base_name in {
        "char",
        "varchar",
        "text",
        "tinytext",
        "mediumtext",
        "longtext",
        "enum",
        "set",
    }:
        return _simple(LogicalKind.STRING, raw, system)
    if base_name in {
        "binary",
        "varbinary",
        "blob",
        "tinyblob",
        "mediumblob",
        "longblob",
    }:
        return _simple(LogicalKind.BINARY, raw, system)
    if base_name == "date":
        return _simple(LogicalKind.DATE, raw, system)
    if base_name in {"datetime", "year"}:
        return (
            _timestamp(raw, system, TimestampKind.NAIVE)
            if base_name == "datetime"
            else _integer(raw, system, 16)
        )
    if base_name == "timestamp":
        return _timestamp(raw, system, TimestampKind.INSTANT)
    raise ConfigurationError(
        f"Unsupported MySQL schema hint datatype: {raw!r}",
        details={"type_system": system},
    )


def _resolve_mssql(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    system = TypeSystem.MSSQL.value
    if base == "bit":
        return _simple(LogicalKind.BOOLEAN, raw, system)
    if base == "tinyint":
        return _integer(raw, system, 8, unsigned=True)
    if base == "smallint":
        return _integer(raw, system, 16)
    if base == "int":
        return _integer(raw, system, 32)
    if base == "bigint":
        return _integer(raw, system, 64)
    if base in {"decimal", "numeric"}:
        return _decimal(raw, system, args, precision, scale, default_scale=0)
    if base in {"money"}:
        return _decimal(
            raw, system, args, precision or 19, scale if scale is not None else 4
        )
    if base in {"smallmoney"}:
        return _decimal(
            raw, system, args, precision or 10, scale if scale is not None else 4
        )
    if base == "real":
        if args:
            raise ConfigurationError(
                "SQL Server REAL does not accept a precision parameter",
                details={"data_type": raw},
            )
        return _float(raw, system, 32)
    if base == "float":
        return _float_precision(
            raw,
            system,
            args,
            maximum=53,
            single_precision_limit=24,
            default_bits=64,
        )
    if base in {"char", "varchar", "text", "nchar", "nvarchar", "ntext", "xml"}:
        return _simple(LogicalKind.STRING, raw, system)
    if base in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return _simple(LogicalKind.BINARY, raw, system)
    if base == "date":
        return _simple(LogicalKind.DATE, raw, system)
    if base in {"datetime", "datetime2", "smalldatetime"}:
        return _timestamp(raw, system, TimestampKind.NAIVE)
    if base == "datetimeoffset":
        return _timestamp(raw, system, TimestampKind.INSTANT)
    raise ConfigurationError(
        f"Unsupported SQL Server schema hint datatype: {raw!r}",
        details={"type_system": system},
    )


def _resolve_oracle(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    system = TypeSystem.ORACLE.value
    if base == "number":
        return _decimal(
            raw,
            system,
            args,
            precision,
            scale,
            default_scale=0,
            allow_negative_scale=True,
        )
    if base == "binary_float":
        return _float(raw, system, 32)
    if base == "binary_double":
        return _float(raw, system, 64)
    if base in {"float", "real"}:
        return _float_precision(
            raw,
            system,
            args,
            maximum=126 if base == "float" else 63,
            single_precision_limit=24,
            default_bits=64,
        )
    if base in {"double", "double precision"}:
        if args:
            raise ConfigurationError(
                "Oracle DOUBLE PRECISION does not accept a precision parameter",
                details={"data_type": raw},
            )
        return _float(raw, system, 64)
    if base in {
        "varchar2",
        "nvarchar2",
        "varchar",
        "char",
        "nchar",
        "clob",
        "nclob",
        "long",
    }:
        return _simple(LogicalKind.STRING, raw, system)
    if base in {"raw", "long raw", "blob", "bfile"}:
        return _simple(LogicalKind.BINARY, raw, system)
    if base == "date":
        return _timestamp(raw, system, TimestampKind.NAIVE)
    if base in {"timestamp", "timestamp without time zone", "timestamp(6)"}:
        return _timestamp(raw, system, TimestampKind.NAIVE)
    if base in {"timestamp with time zone", "timestamptz"}:
        return _timestamp(raw, system, TimestampKind.INSTANT)
    if base in {"timestamp with local time zone", "local timestamp"}:
        return _timestamp(raw, system, TimestampKind.INSTANT)
    raise ConfigurationError(
        f"Unsupported Oracle schema hint datatype: {raw!r}",
        details={"type_system": system},
    )


def _resolve_sqlite(
    base: str, args: list[int], raw: str, precision: int | None, scale: int | None
) -> ResolvedDataType:
    system = TypeSystem.SQLITE.value
    if base in {"integer", "int", "bigint", "tinyint", "smallint"}:
        return _integer(raw, system, 64)
    if base in {"real", "double", "float"}:
        return _float(raw, system, 64)
    if base in {"numeric", "decimal"}:
        return _decimal(raw, system, args, precision, scale)
    if base in {"text", "char", "varchar", "clob"}:
        return _simple(LogicalKind.STRING, raw, system)
    if base == "blob":
        return _simple(LogicalKind.BINARY, raw, system)
    if base == "date":
        return _simple(LogicalKind.DATE, raw, system)
    if base in {"datetime", "timestamp"}:
        return _timestamp(raw, system, TimestampKind.NAIVE)
    raise ConfigurationError(
        f"Unsupported SQLite schema hint datatype: {raw!r}",
        details={"type_system": system},
    )


def resolve_schema_hint(
    data_type: str,
    *,
    type_system: str | TypeSystem | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> ResolvedDataType:
    """Resolve one authored datatype string to the shared logical contract."""

    system = normalize_type_system(type_system)
    base, args = _parse_type(data_type)
    if system == TypeSystem.SPARK_SQL.value:
        return _resolve_spark(base, args, data_type, precision, scale)
    if system == TypeSystem.POSTGRESQL.value:
        return _resolve_postgresql(base, args, data_type, precision, scale)
    if system == TypeSystem.MYSQL.value:
        return _resolve_mysql(base, args, data_type, precision, scale)
    if system == TypeSystem.MSSQL.value:
        return _resolve_mssql(base, args, data_type, precision, scale)
    if system == TypeSystem.ORACLE.value:
        return _resolve_oracle(base, args, data_type, precision, scale)
    if system == TypeSystem.SQLITE.value:
        return _resolve_sqlite(base, args, data_type, precision, scale)
    raise ConfigurationError(
        "No resolver registered for datatype system", details={"type_system": system}
    )
