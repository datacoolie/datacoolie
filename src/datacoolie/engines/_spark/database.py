"""Stateless Spark JDBC option construction and database reads."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl

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


def _normalise_jdbc_string_types(
    frame: DataFrame, *, database_type: str | DatabaseType | None = None
) -> DataFrame:
    """Normalize JDBC character columns to Spark's unbounded string type.

    Some JDBC drivers (notably the SQLite driver) report an unbounded text
    expression as ``VARCHAR(0)``. Spark preserves that metadata, but Delta
    interprets it as a real length constraint and rejects every non-empty
    value. SQLite character results are recast to the portable unbounded
    string semantic; valid bounded character declarations from other
    databases remain untouched.
    """

    from pyspark.sql.functions import col
    from pyspark.sql.types import CharType, StringType, VarcharType

    is_sqlite = database_type in (DatabaseType.SQLITE, "sqlite")
    for field in frame.schema.fields:
        data_type = field.dataType
        if isinstance(data_type, StringType):
            # SQLite may expose an unbounded text column as ``StringType`` in
            # Python while retaining ``VARCHAR(0)`` in the JVM logical plan.
            should_cast = is_sqlite
        elif isinstance(data_type, (CharType, VarcharType)):
            should_cast = is_sqlite or int(getattr(data_type, "length", 0)) <= 0
        else:
            should_cast = False
        if not should_cast:
            continue
        frame = frame.withColumn(field.name, col(field.name).cast(StringType()))
    return frame


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
    elif "driver" not in merged:
        # An explicit JDBC URL still needs the matching driver option.  URL
        # construction is intentionally bypassed in this branch, but driver
        # discovery remains the engine's responsibility; otherwise Spark's
        # DriverManager reports the opaque "No suitable driver" error.
        driver = JDBC_DRIVERS.get(merged.get("database_type"))
        if driver:
            merged["driver"] = driver
    database_type = merged.get("database_type")
    if str(merged["url"]).startswith("jdbc:mysql:"):
        # Connector/J's date representation loses YEAR 0 (it becomes 2000).
        # Preserve integers by default, respecting explicit driver settings.
        url_options = dict(parse_qsl(str(merged["url"]).partition("?")[2]))
        option_names = {key.lower() for key in (*merged, *url_options)}
        if "yearisdatetype" not in option_names:
            merged["yearIsDateType"] = "false"
    for key in (
        "database_type",
        "host",
        "port",
        "database",
        "database_read_engine",
        "read_options",
        "use_schema_hint",
        "schema_hint_type_system",
    ):
        merged.pop(key, None)
    merged.update(auth_props)
    reader = spark.read.format("jdbc")
    for key, value in merged.items():
        reader = reader.option(key, value)
    return _normalise_jdbc_string_types(
        reader.load(), database_type=database_type
    )
