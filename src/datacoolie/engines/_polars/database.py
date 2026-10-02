"""Database connection and read helpers for :class:`PolarsEngine`."""

from __future__ import annotations

import os
import re
import sqlite3
from decimal import Decimal
from typing import Any, Collection, Dict, Optional, Tuple
from urllib.parse import parse_qs, quote, quote_plus, unquote, urlsplit

import polars as pl

from datacoolie.core.constants import DatabaseAuthType, DatabaseType
from datacoolie.core.exceptions import EngineError

_FRAMEWORK_KEYS = frozenset(
    {
        "database_type",
        "host",
        "port",
        "database",
        "user",
        "password",
        "driver",
        "database_read_engine",
        "read_options",
        "use_schema_hint",
        "schema_hint_type_system",
        "tenant_id",
        "token",
    }
)
_SAFE_TABLE_PATTERN = re.compile(r"^[\w]+(?:\.[\w]+)*$")


def _parse_driver_uri(
    connection_uri: str,
    *,
    label: str,
    schemes: Collection[str],
    allowed_query_keys: Collection[str] = (),
) -> Any:
    """Parse a native-driver URI without leaking credentials in errors.

    Native DB-API readers intentionally accept only options they can apply.
    Silently dropping URI query parameters (TLS, charset, driver flags, …)
    would make a connection appear configured while changing its behavior.
    Callers must pass supported options through the explicit driver config,
    except for Oracle's ``service_name`` URL convenience.
    """

    try:
        parsed = urlsplit(connection_uri)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        query_keys = set(parse_qs(parsed.query, keep_blank_values=True))
        unsupported = query_keys - set(allowed_query_keys)
        if scheme not in {value.lower() for value in schemes} or not hostname:
            raise ValueError("missing supported scheme or hostname")
        if parsed.fragment:
            raise ValueError("fragments are not supported")
        if unsupported:
            raise ValueError(
                "unsupported query parameters: " + ", ".join(sorted(unsupported))
            )
        # Accessing .port validates malformed/non-numeric ports now, rather
        # than allowing a later driver call to fail with an opaque message.
        _ = parsed.port
        return parsed
    except (TypeError, ValueError) as exc:
        raise EngineError(
            f"Cannot parse {label} connection URI; provide a supported URI "
            "without unsupported query parameters"
        ) from exc


def _strip_framework_keys(
    options: Dict[str, Any], driver_connection_keys: Collection[str]
) -> None:
    for key in _FRAMEWORK_KEYS | frozenset(driver_connection_keys):
        options.pop(key, None)


def read_database(
    *,
    table: Optional[str],
    query: Optional[str],
    options: Optional[Dict[str, Any]],
    driver_connection_keys: Collection[str],
) -> pl.LazyFrame:
    """Read a database query using the configured authentication path."""
    merged: Dict[str, Any] = dict(options or {})
    db_type = merged.get("database_type", "")
    auth_type = merged.pop("auth_type", DatabaseAuthType.PASSWORD)

    if table and not query and not _SAFE_TABLE_PATTERN.match(table):
        raise EngineError(f"Invalid table name: {table!r}")

    if auth_type in (
        DatabaseAuthType.SERVICE_PRINCIPAL,
        DatabaseAuthType.MANAGED_IDENTITY,
    ):
        if db_type not in (DatabaseType.MSSQL, "mssql"):
            raise EngineError(
                f"PolarsEngine: {auth_type} auth is only supported for MSSQL, "
                f"got database_type={db_type!r}"
            )
        connection_uri, attrs_before = build_mssql_odbc_connection(auth_type, merged)
        sql = query if query else f"SELECT * FROM {table}"
        _strip_framework_keys(merged, driver_connection_keys)
        return read_database_odbc(sql, connection_uri, attrs_before, **merged)

    if auth_type == DatabaseAuthType.ACCESS_TOKEN and db_type in (
        DatabaseType.MSSQL,
        "mssql",
    ):
        connection_uri, attrs_before = build_mssql_odbc_connection(auth_type, merged)
        sql = query if query else f"SELECT * FROM {table}"
        _strip_framework_keys(merged, driver_connection_keys)
        return read_database_odbc(sql, connection_uri, attrs_before, **merged)

    if auth_type == DatabaseAuthType.ACCESS_TOKEN:
        token = merged.pop("token", "")
        if token:
            merged["password"] = token
        merged.pop("tenant_id", None)

    connection_uri = merged.pop("url", None)
    if connection_uri is None:
        connection_uri = build_connection_string(merged)
    default_read_engine = (
        "connectorx"
        if db_type in (DatabaseType.POSTGRESQL, "postgresql")
        else "native"
    )
    read_engine = str(
        merged.pop("database_read_engine", default_read_engine)
    ).strip().lower()
    sql = query if query else f"SELECT * FROM {table}"

    if db_type in (
        DatabaseType.ORACLE,
        DatabaseType.POSTGRESQL,
        "oracle",
        "postgresql",
        DatabaseType.MYSQL,
        DatabaseType.MSSQL,
        DatabaseType.SQLITE,
        "mysql",
        "mssql",
        "sqlite",
    ):
        if read_engine not in {"native", "connectorx"}:
            raise EngineError(
                "Unsupported Polars database read engine",
                details={
                    "database_read_engine": read_engine,
                    "supported": ["native", "connectorx"],
                },
            )
        if read_engine == "native":
            # Keep driver-specific options visible to the native helper so it
            # can either apply or reject them explicitly.  Stripping them
            # before dispatch would silently drop TLS/ODBC settings.
            native_options = dict(merged)
            for key in _FRAMEWORK_KEYS:
                native_options.pop(key, None)
            if db_type in (DatabaseType.ORACLE, "oracle"):
                return read_database_oracle(sql, connection_uri, **native_options)
            if db_type in (DatabaseType.SQLITE, "sqlite"):
                return read_database_sqlite(sql, connection_uri, **native_options)
            return read_database_native(
                sql,
                db_type=(
                    db_type.value if isinstance(db_type, DatabaseType) else str(db_type)
                ),
                connection_uri=connection_uri,
                **native_options,
            )
    _strip_framework_keys(merged, driver_connection_keys)
    return pl.read_database_uri(sql, connection_uri, **merged).lazy()


def read_database_sqlite(
    sql: str, connection_uri: str, **kwargs: Any
) -> pl.LazyFrame:
    """Read SQLite through the standard library DB-API.

    SQLite has no native date/time storage class.  ConnectorX guesses a
    temporal type from text and can reject otherwise valid source values (for
    example a date-time string carrying fractional seconds).  The DB-API
    route deliberately preserves those values as strings; the authored
    schema hint, when present, remains the transform boundary responsible for
    projecting them to a logical date or timestamp.
    """
    parsed = urlsplit(connection_uri)
    if parsed.scheme.lower() != "sqlite":
        raise EngineError(
            "SQLite native reading requires a sqlite:// connection URI"
        )
    if parsed.query or parsed.fragment:
        raise EngineError(
            "SQLite native reading does not support URI query parameters or fragments"
        )
    if parsed.netloc and parsed.netloc not in {"localhost"}:
        database = f"//{parsed.netloc}{unquote(parsed.path)}"
    else:
        database = unquote(parsed.path)
    if database.startswith("/") and len(database) > 2 and database[2] == ":":
        database = database[1:]
    if not database:
        raise EngineError("SQLite connection URI must include a database path")
    batch_size = kwargs.pop("batch_size", kwargs.pop("fetchsize", None))
    infer_schema_length = kwargs.pop("infer_schema_length", None)
    if kwargs:
        raise EngineError(
            "Native SQLite database reading does not support these options",
            details={"options": sorted(kwargs)},
        )
    connection = sqlite3.connect(database)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            column_names = [str(description[0]) for description in cursor.description or ()]
            if batch_size is None:
                rows = cursor.fetchall()
            else:
                size = int(batch_size)
                if size <= 0:
                    raise EngineError("Native SQLite database batch_size must be positive")
                rows = []
                while True:
                    batch = cursor.fetchmany(size)
                    if not batch:
                        break
                    rows.extend(batch)
            return pl.DataFrame(
                rows,
                schema=column_names,
                orient="row",
                infer_schema_length=infer_schema_length,
            ).lazy()
        finally:
            cursor.close()
    finally:
        connection.close()


def read_database_native(
    sql: str,
    *,
    db_type: str,
    connection_uri: str,
    **kwargs: Any,
) -> pl.LazyFrame:
    """Read a PostgreSQL/MySQL/MSSQL result through a DB-API cursor.

    ConnectorX can project vendor decimal and unsigned-integer result columns
    to a widened or floating type for some transports. A DB-API cursor keeps
    Python ``Decimal``/``int`` values intact so Polars can construct an exact
    logical schema.  The connection is closed immediately after the eager
    read; the returned LazyFrame owns the materialized result, not the socket.
    """

    parsed = _parse_driver_uri(
        connection_uri,
        label=db_type,
        schemes={
            "mysql",
            "mysql+pymysql",
            "mssql",
            "mssql+pymssql",
            "postgresql",
            "postgresql+psycopg2",
            "postgres",
        },
    )
    host = parsed.hostname or kwargs.pop("host", "localhost")
    port = parsed.port or kwargs.pop("port", None)
    user = unquote(parsed.username or str(kwargs.pop("user", "")))
    password = unquote(parsed.password or str(kwargs.pop("password", "")))
    database = unquote(parsed.path.lstrip("/")) or str(kwargs.pop("database", ""))
    batch_size = kwargs.pop("batch_size", None)
    if batch_size is None:
        batch_size = kwargs.pop("fetchsize", None)
    infer_schema_length = kwargs.pop("infer_schema_length", None)
    if kwargs:
        raise EngineError(
            "Native Polars database reading does not support these options",
            details={"options": sorted(kwargs)},
        )
    try:
        if db_type in (DatabaseType.MYSQL, "mysql"):
            import pymysql  # noqa: PLC0415

            connection = pymysql.connect(
                host=host,
                port=int(port or 3306),
                user=user,
                password=password,
                database=database,
            )
        elif db_type in (DatabaseType.MSSQL, "mssql"):
            import pymssql  # noqa: PLC0415

            connection = pymssql.connect(
                server=host,
                port=int(port or 1433),
                user=user,
                password=password,
                database=database,
            )
        elif db_type in (DatabaseType.POSTGRESQL, "postgresql"):
            import psycopg2  # noqa: PLC0415

            connection = psycopg2.connect(
                host=host,
                port=int(port or 5432),
                user=user,
                password=password,
                dbname=database,
            )
        else:
            raise EngineError(
                "Native Polars database reading is not supported for this type",
                details={"database_type": db_type},
            )
    except ImportError as exc:
        package = {
            DatabaseType.MYSQL: "pymysql",
            "mysql": "pymysql",
            DatabaseType.MSSQL: "pymssql",
            "mssql": "pymssql",
            DatabaseType.POSTGRESQL: "psycopg2-binary",
            "postgresql": "psycopg2-binary",
        }.get(db_type, "the database driver")
        raise EngineError(
            f"{package} is required for exact {db_type} result typing; install "
            f"{package} or set configure.database_read_engine='connectorx' explicitly if "
            "lossy floating-point projection is acceptable"
        ) from exc

    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            descriptions = tuple(cursor.description or ())
            column_names = [str(description[0]) for description in descriptions]
            if batch_size is None:
                rows = cursor.fetchall()
            else:
                size = int(batch_size)
                if size <= 0:
                    raise EngineError("Native database batch_size must be positive")
                rows = []
                while True:
                    batch = cursor.fetchmany(size)
                    if not batch:
                        break
                    rows.extend(batch)
            # Construct from row values rather than through ConnectorX so
            # Python Decimal/int objects retain exact source precision.
            frame = pl.DataFrame(
                rows,
                schema=column_names,
                orient="row",
                infer_schema_length=infer_schema_length,
            )
            # Python ``Decimal`` inference cannot recover a database column's
            # declared precision/scale (it normally widens to Decimal(38, n)).
            # DB-API descriptions expose those values for PostgreSQL/MySQL/
            # SQL Server; apply them after materialisation so a source
            # NUMERIC(18,2) remains NUMERIC(18,2) without schema hints.
            for description in descriptions:
                precision = getattr(description, "precision", None)
                scale = getattr(description, "scale", None)
                internal_size = getattr(description, "internal_size", None)
                if precision is None and len(description) > 5:
                    precision = description[4]
                    scale = description[5]
                    internal_size = description[3]
                if precision is None or scale is None:
                    continue
                try:
                    precision = int(precision)
                    scale = int(scale)
                    internal_size = (
                        int(internal_size) if internal_size is not None else None
                    )
                except (TypeError, ValueError):
                    continue
                if precision <= 0 or scale < 0 or scale > precision:
                    continue
                # PyMySQL reports DECIMAL display width (precision plus sign
                # and decimal-point characters) in both ``internal_size`` and
                # ``precision``.  Correct that driver-specific presentation
                # detail before using it as the logical Decimal precision.
                # Other drivers either expose the declared precision or leave
                # it unset, so they retain their DB-API values unchanged.
                database_name = (
                    db_type.value
                    if isinstance(db_type, DatabaseType)
                    else str(db_type).lower()
                )
                if (
                    database_name == DatabaseType.MYSQL.value
                    and internal_size == precision
                ):
                    precision -= 1 + int(scale > 0)
                    if precision <= 0 or scale > precision:
                        continue
                column_name = str(description[0])
                # Some DB-API drivers report precision/scale for integer and
                # floating columns too.  Only apply the declared decimal
                # contract to columns that Polars inferred as Decimal; this
                # keeps BIGINT/DOUBLE source semantics intact.
                if (
                    column_name not in frame.columns
                    or not isinstance(frame.schema[column_name], pl.Decimal)
                ):
                    continue
                frame = frame.with_columns(
                    pl.col(column_name).cast(
                        pl.Decimal(precision=precision, scale=scale), strict=True
                    )
                )
            return frame.lazy()
        finally:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
    finally:
        connection.close()


def read_database_oracle(
    sql: str, connection_uri: str, **kwargs: Any
) -> pl.LazyFrame:
    """Read Oracle through a precision-preserving thin-driver cursor."""
    try:
        import oracledb  # noqa: PLC0415
    except ImportError as exc:
        raise EngineError(
            "oracledb package is required for Oracle reads — pip install oracledb"
        ) from exc

    parsed = _parse_driver_uri(
        connection_uri,
        label="Oracle",
        schemes={"oracle", "oracle+oracledb"},
        allowed_query_keys={"service_name"},
    )
    service = unquote(parsed.path.lstrip("/"))
    query_service = parse_qs(parsed.query).get("service_name", [""])[0]
    if service and query_service and unquote(query_service) != service:
        raise EngineError(
            "Oracle connection URI has conflicting path and service_name values"
        )
    if not service:
        service = unquote(query_service)
    if not service:
        raise EngineError(
            "Oracle connection URI must include a service name in its path or "
            "service_name query parameter"
        )
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname
    port = parsed.port or 1521
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service}",
    )
    try:
        # oracledb otherwise exposes NUMBER as float for some precisions.  The
        # output handler keeps the DB-API Decimal object intact so the
        # transform engine can apply an explicit schema hint without losing
        # source precision.
        connection.outputtypehandler = (
            lambda cursor, metadata: cursor.var(
                Decimal, arraysize=cursor.arraysize
            )
            if metadata.type_code is oracledb.DB_TYPE_NUMBER
            else None
        )
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            column_names = [
                str(description[0]) for description in cursor.description or ()
            ]
            batch_size = kwargs.pop("batch_size", None)
            if batch_size is None:
                batch_size = kwargs.pop("fetchsize", None)
            infer_schema_length = kwargs.pop("infer_schema_length", None)
            if kwargs:
                raise EngineError(
                    "Native Oracle database reading does not support these options",
                    details={"options": sorted(kwargs)},
                )
            if batch_size is None:
                rows = cursor.fetchall()
            else:
                size = int(batch_size)
                if size <= 0:
                    raise EngineError("Native database batch_size must be positive")
                rows = []
                while True:
                    batch = cursor.fetchmany(size)
                    if not batch:
                        break
                    rows.extend(batch)
            return pl.DataFrame(
                rows,
                schema=column_names,
                orient="row",
                infer_schema_length=infer_schema_length,
            ).lazy()
        finally:
            close_cursor = getattr(cursor, "close", None)
            if callable(close_cursor):
                close_cursor()
    finally:
        connection.close()


def build_mssql_odbc_connection(
    auth_type: str, options: Dict[str, Any]
) -> Tuple[str, Optional[bytes]]:
    """Build an MSSQL ODBC URI and optional access-token structure."""
    host = options.get("host", "localhost")
    port = options.get("port", 1433)
    database = options.get("database", "")
    driver = options.get("driver", "ODBC Driver 18 for SQL Server")

    if auth_type == DatabaseAuthType.SERVICE_PRINCIPAL:
        connection = (
            f"Driver={{{driver}}};Server={host},{port};Database={database};"
            "Authentication=ActiveDirectoryServicePrincipal;"
            f"UID={options.get('user', '')};PWD={options.get('password', '')}"
        )
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(connection)}", None

    if auth_type == DatabaseAuthType.MANAGED_IDENTITY:
        connection = (
            f"Driver={{{driver}}};Server={host},{port};Database={database};"
            "Authentication=ActiveDirectoryMsi"
        )
        user = options.get("user")
        if user:
            connection += f";UID={user}"
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(connection)}", None

    if auth_type == DatabaseAuthType.ACCESS_TOKEN:
        import struct  # noqa: PLC0415

        token_bytes = options.get("token", "").encode("UTF-16-LE")
        token_struct = struct.pack(
            f"<I{len(token_bytes)}s", len(token_bytes), token_bytes
        )
        connection = f"Driver={{{driver}}};Server={host},{port};Database={database}"
        return (
            f"mssql+pyodbc:///?odbc_connect={quote_plus(connection)}",
            token_struct,
        )

    raise EngineError(f"PolarsEngine: unsupported MSSQL auth_type {auth_type!r}")


def read_database_odbc(
    sql: str,
    connection_uri: str,
    attrs_before: Optional[bytes] = None,
    **kwargs: Any,
) -> pl.LazyFrame:
    """Read through SQLAlchemy and pyodbc for non-password MSSQL auth."""
    try:
        from sqlalchemy import create_engine  # noqa: PLC0415
    except ImportError as exc:
        raise EngineError(
            "sqlalchemy is required for non-password MSSQL auth — pip install sqlalchemy"
        ) from exc

    connect_args: Dict[str, Any] = {}
    if attrs_before is not None:
        connect_args["attrs_before"] = {1256: attrs_before}
    engine = create_engine(connection_uri, connect_args=connect_args)
    try:
        return pl.read_database(sql, connection=engine, **kwargs).lazy()
    finally:
        engine.dispose()


def build_connection_string(options: Dict[str, Any]) -> str:
    """Build a connectorx/SQLAlchemy URI from normalized database options."""
    db_type = options.get("database_type")
    if not db_type:
        raise EngineError(
            "PolarsEngine.read_database requires 'url' or 'database_type' in options"
        )

    user = options.get("user", "")
    password = options.get("password", "")
    host = options.get("host", "localhost")
    port = options.get("port")
    database = options.get("database", "")

    if db_type == DatabaseType.MYSQL:
        port, scheme = port or 3306, "mysql"
    elif db_type == DatabaseType.MSSQL:
        port, scheme = port or 1433, "mssql"
    elif db_type == DatabaseType.POSTGRESQL:
        port, scheme = port or 5432, "postgresql"
    elif db_type == DatabaseType.ORACLE:
        port, scheme = port or 1521, "oracle"
    elif db_type == DatabaseType.SQLITE:
        if not os.path.isabs(database):
            database = os.path.abspath(database)
        return f"sqlite:///{database}"
    else:
        raise EngineError(f"PolarsEngine: unsupported database_type {db_type!r}")

    auth = f"{quote(user, safe='')}:{quote(password, safe='')}@" if user else ""
    return f"{scheme}://{auth}{host}:{port}/{database}"
