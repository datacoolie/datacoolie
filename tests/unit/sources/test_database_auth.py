"""Tests for database auth_type support.

Covers auth_type forwarding in DatabaseReader._build_options,
Spark JDBC auth property mapping, Polars connection string building,
and Connection model validation.
"""

from __future__ import annotations

from urllib.parse import unquote_plus

import pytest

from datacoolie.core.constants import DatabaseAuthType, DatabaseType
from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import Connection, Source
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.sources.database_reader import DatabaseReader


# ============================================================================
# Helper: build a database Source with given configure
# ============================================================================


def _db_source(**configure_overrides) -> Source:
    """Create a database Source with configurable connection settings."""
    configure = {
        "database_type": "mssql",
        "host": "server.database.windows.net",
        "port": 1433,
        "username": "myuser",
        "password": "mypass",
    }
    configure.update(configure_overrides)
    conn = Connection(
        name="test_db",
        connection_type="database",
        format="sql",
        database="testdb",
        configure=configure,
    )
    return Source(connection=conn, table="dbo.orders")


# ============================================================================
# DatabaseReader._build_options — auth_type forwarding
# ============================================================================


class TestBuildOptionsAuthType:
    """Verify _build_options forwards auth-related fields to engines."""

    def test_default_auth_type_is_password(self) -> None:
        """No auth_type in configure → defaults to 'password'."""
        src = _db_source()
        opts = DatabaseReader._build_options(src)
        assert opts["auth_type"] == DatabaseAuthType.PASSWORD

    def test_explicit_password_auth(self) -> None:
        src = _db_source(auth_type="password")
        opts = DatabaseReader._build_options(src)
        assert opts["auth_type"] == "password"
        assert opts["user"] == "myuser"
        assert opts["password"] == "mypass"

    def test_service_principal_fields_forwarded(self) -> None:
        src = _db_source(
            auth_type="service_principal",
            tenant_id="tenant-123",
        )
        opts = DatabaseReader._build_options(src)
        assert opts["auth_type"] == "service_principal"
        assert opts["user"] == "myuser"  # client_id via username
        assert opts["password"] == "mypass"  # client_secret via password
        assert opts["tenant_id"] == "tenant-123"

    def test_managed_identity_fields_forwarded(self) -> None:
        src = _db_source(auth_type="managed_identity")
        opts = DatabaseReader._build_options(src)
        assert opts["auth_type"] == "managed_identity"

    def test_access_token_fields_forwarded(self) -> None:
        src = _db_source(auth_type="access_token", token="jwt-token-abc")
        opts = DatabaseReader._build_options(src)
        assert opts["auth_type"] == "access_token"
        assert opts["token"] == "jwt-token-abc"

    def test_backward_compat_no_auth_type(self) -> None:
        """Existing configs without auth_type produce identical options."""
        src = _db_source()
        opts = DatabaseReader._build_options(src)
        assert opts["user"] == "myuser"
        assert opts["password"] == "mypass"
        assert opts["host"] == "server.database.windows.net"
        assert opts["database_type"] == "mssql"


# ============================================================================
# SparkEngine._build_jdbc_auth_properties
# ============================================================================


class TestSparkJdbcAuthProperties:
    """Verify Spark JDBC auth property mapping."""

    def test_password_returns_empty(self) -> None:
        opts = {"auth_type": DatabaseAuthType.PASSWORD, "user": "u", "password": "p"}
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert props == {}
        assert "auth_type" not in opts  # consumed
        assert opts["user"] == "u"  # preserved for JDBC

    def test_service_principal_mssql(self) -> None:
        opts = {
            "auth_type": DatabaseAuthType.SERVICE_PRINCIPAL,
            "database_type": "mssql",
            "user": "client-id",
            "password": "client-secret",
            "tenant_id": "tenant-xyz",
        }
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert props["authentication"] == "ActiveDirectoryServicePrincipal"
        assert props["AADSecurePrincipalId"] == "client-id"
        assert props["AADSecurePrincipalSecret"] == "client-secret"
        # user/password/tenant_id consumed from opts
        assert "user" not in opts
        assert "password" not in opts
        assert "tenant_id" not in opts

    def test_managed_identity(self) -> None:
        opts = {
            "auth_type": DatabaseAuthType.MANAGED_IDENTITY,
            "database_type": "mssql",
        }
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert props["authentication"] == "ActiveDirectoryMSI"
        assert "msiClientId" not in props  # no user → system-assigned

    def test_managed_identity_user_assigned(self) -> None:
        opts = {
            "auth_type": DatabaseAuthType.MANAGED_IDENTITY,
            "database_type": "mssql",
            "user": "msi-client-id",
        }
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert props["authentication"] == "ActiveDirectoryMSI"
        assert props["msiClientId"] == "msi-client-id"

    def test_access_token_mssql(self) -> None:
        opts = {
            "auth_type": DatabaseAuthType.ACCESS_TOKEN,
            "database_type": "mssql",
            "token": "eyJhbGciOi...",
        }
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert props["accessToken"] == "eyJhbGciOi..."
        assert "token" not in opts

    def test_access_token_postgresql(self) -> None:
        opts = {
            "auth_type": DatabaseAuthType.ACCESS_TOKEN,
            "database_type": "postgresql",
            "token": "iam-token-123",
            "user": "iam_user",
        }
        props = SparkEngine._build_jdbc_auth_properties(opts)
        assert "accessToken" not in props
        assert opts["password"] == "iam-token-123"  # token injected as password


# ============================================================================
# PolarsEngine._build_mssql_odbc_connection
# ============================================================================


class TestPolarsMssqlOdbcConnection:
    """Verify Polars ODBC connection string building for MSSQL non-password auth."""

    def test_service_principal(self) -> None:
        opts = {
            "host": "myserver.database.windows.net",
            "port": 1433,
            "database": "mydb",
            "user": "client-id",
            "password": "client-secret",
        }
        uri, attrs = PolarsEngine._build_mssql_odbc_connection(
            DatabaseAuthType.SERVICE_PRINCIPAL, opts
        )
        decoded = unquote_plus(uri)
        assert "ActiveDirectoryServicePrincipal" in decoded
        assert "UID=client-id" in decoded
        assert "PWD=client-secret" in decoded
        assert attrs is None

    def test_managed_identity(self) -> None:
        opts = {
            "host": "myserver.database.windows.net",
            "port": 1433,
            "database": "mydb",
        }
        uri, attrs = PolarsEngine._build_mssql_odbc_connection(
            DatabaseAuthType.MANAGED_IDENTITY, opts
        )
        assert "ActiveDirectoryMsi" in uri
        assert attrs is None

    def test_managed_identity_user_assigned(self) -> None:
        opts = {
            "host": "myserver.database.windows.net",
            "port": 1433,
            "database": "mydb",
            "user": "msi-client-id",
        }
        uri, attrs = PolarsEngine._build_mssql_odbc_connection(
            DatabaseAuthType.MANAGED_IDENTITY, opts
        )
        decoded = unquote_plus(uri)
        assert "ActiveDirectoryMsi" in decoded
        assert "UID=msi-client-id" in decoded

    def test_access_token(self) -> None:
        opts = {
            "host": "myserver.database.windows.net",
            "port": 1433,
            "database": "mydb",
            "token": "eyJhbGciOi...",
        }
        uri, attrs = PolarsEngine._build_mssql_odbc_connection(
            DatabaseAuthType.ACCESS_TOKEN, opts
        )
        assert "mssql+pyodbc" in uri
        assert attrs is not None  # token struct bytes
        assert isinstance(attrs, bytes)


# ============================================================================
# Connection model validation
# ============================================================================


class TestConnectionAuthValidation:
    """Verify Connection model validates auth_type requirements."""

    def test_password_no_validation_error(self) -> None:
        """Password auth requires no extra validation."""
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={"auth_type": "password", "username": "u", "password": "p"},
        )
        assert conn.auth_type == "password"

    def test_no_auth_type_backward_compat(self) -> None:
        """No auth_type at all → no validation error."""
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={"username": "u", "password": "p"},
        )
        assert conn.auth_type is None

    def test_service_principal_valid(self) -> None:
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={
                "auth_type": "service_principal",
                "username": "client-id",
                "password": "client-secret",
                "tenant_id": "tenant-123",
            },
        )
        assert conn.auth_type == "service_principal"
        assert conn.tenant_id == "tenant-123"

    def test_service_principal_missing_tenant_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="tenant_id"):
            Connection(
                name="test", connection_type="database", format="sql",
                configure={
                    "auth_type": "service_principal",
                    "username": "client-id",
                    "password": "client-secret",
                },
            )

    def test_service_principal_missing_credentials_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="username.*password"):
            Connection(
                name="test", connection_type="database", format="sql",
                configure={
                    "auth_type": "service_principal",
                    "tenant_id": "tenant-123",
                },
            )

    def test_access_token_valid(self) -> None:
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={"auth_type": "access_token", "token": "jwt-token"},
        )
        assert conn.token == "jwt-token"

    def test_access_token_missing_token_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="token"):
            Connection(
                name="test", connection_type="database", format="sql",
                configure={"auth_type": "access_token"},
            )

    def test_managed_identity_no_extra_fields(self) -> None:
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={"auth_type": "managed_identity"},
        )
        assert conn.auth_type == "managed_identity"

    def test_fabric_host_rejects_password(self) -> None:
        with pytest.raises(ConfigurationError, match="Fabric SQL endpoint"):
            Connection(
                name="test", connection_type="database", format="sql",
                configure={
                    "auth_type": "password",
                    "host": "xyz.datawarehouse.fabric.microsoft.com",
                    "username": "u",
                    "password": "p",
                },
            )

    def test_fabric_host_allows_service_principal(self) -> None:
        conn = Connection(
            name="test", connection_type="database", format="sql",
            configure={
                "auth_type": "service_principal",
                "host": "xyz.datawarehouse.fabric.microsoft.com",
                "username": "client-id",
                "password": "client-secret",
                "tenant_id": "tenant-123",
            },
        )
        assert conn.auth_type == "service_principal"

    def test_non_database_connection_skips_auth_validation(self) -> None:
        """auth_type validation only applies to database connections."""
        conn = Connection(
            name="test", connection_type="file", format="parquet",
            configure={"base_path": "/data"},
        )
        assert conn.auth_type is None


# ============================================================================
# PolarsEngine.read_database — auth guard & table validation
# ============================================================================


class TestPolarsReadDatabaseGuards:
    """Verify PolarsEngine.read_database rejects unsupported auth+db combos
    and invalid table names."""

    def test_service_principal_non_mssql_raises(self) -> None:
        """SPN auth on PostgreSQL should raise EngineError."""
        from datacoolie.core.exceptions import EngineError

        engine = PolarsEngine()
        with pytest.raises(EngineError, match="only supported for MSSQL"):
            engine.read_database(
                table="orders",
                options={
                    "database_type": "postgresql",
                    "auth_type": DatabaseAuthType.SERVICE_PRINCIPAL,
                    "host": "localhost",
                    "user": "client-id",
                    "password": "client-secret",
                    "tenant_id": "tenant-123",
                },
            )

    def test_managed_identity_non_mssql_raises(self) -> None:
        """MI auth on MySQL should raise EngineError."""
        from datacoolie.core.exceptions import EngineError

        engine = PolarsEngine()
        with pytest.raises(EngineError, match="only supported for MSSQL"):
            engine.read_database(
                table="orders",
                options={
                    "database_type": "mysql",
                    "auth_type": DatabaseAuthType.MANAGED_IDENTITY,
                    "host": "localhost",
                },
            )

    def test_invalid_table_name_raises(self) -> None:
        """Table name with SQL injection payload should raise EngineError."""
        from datacoolie.core.exceptions import EngineError

        engine = PolarsEngine()
        with pytest.raises(EngineError, match="Invalid table name"):
            engine.read_database(
                table="orders; DROP TABLE users--",
                options={
                    "database_type": "postgresql",
                    "host": "localhost",
                    "user": "u",
                    "password": "p",
                },
            )

    def test_valid_table_name_accepted(self) -> None:
        """schema.table pattern should pass validation (will fail at connectorx, not at guard)."""
        from datacoolie.core.exceptions import EngineError

        engine = PolarsEngine()
        # Should NOT raise EngineError for table validation — it will fail
        # later at connectorx connection, which is expected.
        with pytest.raises(Exception, match="(?!Invalid table name)"):
            engine.read_database(
                table="dbo.orders",
                options={
                    "database_type": "postgresql",
                    "host": "localhost",
                    "user": "u",
                    "password": "p",
                },
            )

    def test_query_bypasses_table_validation(self) -> None:
        """When query is provided, table validation should not trigger."""
        from datacoolie.core.exceptions import EngineError

        engine = PolarsEngine()
        # Should NOT raise EngineError for table — will fail at connectorx instead
        with pytest.raises(Exception, match="(?!Invalid table name)"):
            engine.read_database(
                query="SELECT * FROM orders",
                options={
                    "database_type": "postgresql",
                    "host": "localhost",
                    "user": "u",
                    "password": "p",
                },
            )
