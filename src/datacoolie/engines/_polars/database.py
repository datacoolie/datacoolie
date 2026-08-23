"""Database connection and read helpers for :class:`PolarsEngine`."""

from __future__ import annotations

import os
import re
from typing import Any, Collection, Dict, Optional, Tuple
from urllib.parse import quote, quote_plus, unquote

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
        "tenant_id",
        "token",
    }
)
_SAFE_TABLE_PATTERN = re.compile(r"^[\w]+(?:\.[\w]+)*$")


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
    _strip_framework_keys(merged, driver_connection_keys)
    sql = query if query else f"SELECT * FROM {table}"

    if db_type in (DatabaseType.ORACLE, "oracle"):
        return read_database_oracle(sql, connection_uri, **merged)
    return pl.read_database_uri(sql, connection_uri, **merged).lazy()


def read_database_oracle(
    sql: str, connection_uri: str, **kwargs: Any
) -> pl.LazyFrame:
    """Read Oracle through the pure-Python thin driver."""
    try:
        import oracledb  # noqa: PLC0415
    except ImportError as exc:
        raise EngineError(
            "oracledb package is required for Oracle reads — pip install oracledb"
        ) from exc

    match = re.match(
        r"oracle://(?:([^:@]+)(?::([^@]*))?@)?([^:/]+):(\d+)/(.+)",
        connection_uri,
    )
    if not match:
        raise EngineError(f"Cannot parse Oracle connection URI: {connection_uri!r}")

    user, password, host, port, service = match.groups()
    connection = oracledb.connect(
        user=unquote(user) if user else user,
        password=unquote(password) if password else password,
        dsn=f"{host}:{port}/{service}",
    )
    try:
        return pl.read_database(sql, connection=connection, **kwargs).lazy()
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
