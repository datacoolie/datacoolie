"""Stateless Spark JDBC option construction and database reads."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession

from datacoolie.core.constants import DatabaseAuthType, DatabaseType
from datacoolie.core.exceptions import EngineError

JDBC_DRIVERS: Dict[str, str] = {
    DatabaseType.MYSQL: "com.mysql.cj.jdbc.Driver",
    DatabaseType.MSSQL: "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    DatabaseType.POSTGRESQL: "org.postgresql.Driver",
    DatabaseType.ORACLE: "oracle.jdbc.OracleDriver",
    DatabaseType.SQLITE: "org.sqlite.JDBC",
}


def build_jdbc_auth_properties(opts: Dict[str, Any]) -> Dict[str, Any]:
    """Consume framework auth keys and return JDBC authentication properties."""
    auth_type = opts.pop("auth_type", DatabaseAuthType.PASSWORD)
    db_type = opts.get("database_type", "")
    props: Dict[str, Any] = {}

    if auth_type == DatabaseAuthType.SERVICE_PRINCIPAL:
        props["authentication"] = "ActiveDirectoryServicePrincipal"
        props["AADSecurePrincipalId"] = opts.pop("user", "")
        props["AADSecurePrincipalSecret"] = opts.pop("password", "")
        opts.pop("tenant_id", None)
    elif auth_type == DatabaseAuthType.MANAGED_IDENTITY:
        props["authentication"] = "ActiveDirectoryMSI"
        msi_client = opts.pop("user", None)
        if msi_client:
            props["msiClientId"] = msi_client
        opts.pop("password", None)
        opts.pop("tenant_id", None)
    elif auth_type == DatabaseAuthType.ACCESS_TOKEN:
        token = opts.pop("token", "")
        if db_type in (DatabaseType.MSSQL, "mssql"):
            props["accessToken"] = token
        else:
            opts["password"] = token
        opts.pop("tenant_id", None)

    opts.pop("token", None)
    opts.pop("tenant_id", None)
    return props


def build_jdbc_url(
    opts: Dict[str, Any],
    driver_connection_keys: Iterable[str],
) -> str:
    """Build a JDBC URL and apply the default driver to *opts*."""
    db_type = opts.get("database_type")
    if not db_type:
        raise EngineError(
            "SparkEngine.read_database requires 'url' or 'database_type' in options"
        )
    host = opts.get("host", "localhost")
    port = opts.get("port")
    database = opts.get("database", "")

    if db_type == DatabaseType.MYSQL:
        url = f"jdbc:mysql://{host}:{port or 3306}/{database}"
    elif db_type == DatabaseType.MSSQL:
        url = f"jdbc:sqlserver://{host}:{port or 1433};databaseName={database}"
        for prop in driver_connection_keys:
            if prop in opts:
                url += f";{prop}={opts.pop(prop)}"
    elif db_type == DatabaseType.POSTGRESQL:
        url = f"jdbc:postgresql://{host}:{port or 5432}/{database}"
    elif db_type == DatabaseType.ORACLE:
        url = f"jdbc:oracle:thin:@{host}:{port or 1521}/{database}"
    elif db_type == DatabaseType.SQLITE:
        url = f"jdbc:sqlite:{database}"
    else:
        raise EngineError(f"SparkEngine: unsupported database_type {db_type!r}")

    if "driver" not in opts:
        driver = JDBC_DRIVERS.get(db_type)
        if driver:
            opts["driver"] = driver
    return url


def read_database(
    spark: SparkSession,
    *,
    table: Optional[str],
    query: Optional[str],
    options: Optional[Dict[str, Any]],
    driver_connection_keys: Iterable[str],
) -> DataFrame:
    """Read a table or query through Spark JDBC without mutating caller options."""
    merged: Dict[str, Any] = dict(options or {})
    merged["dbtable"] = f"({query.strip()}) q" if query else table
    auth_props = build_jdbc_auth_properties(merged)
    if "url" not in merged:
        merged["url"] = build_jdbc_url(merged, driver_connection_keys)
    for key in ("database_type", "host", "port", "database"):
        merged.pop(key, None)
    merged.update(auth_props)
    reader = spark.read.format("jdbc")
    for key, value in merged.items():
        reader = reader.option(key, value)
    return reader.load()
