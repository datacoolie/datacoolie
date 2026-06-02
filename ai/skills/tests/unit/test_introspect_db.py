"""test_introspect_db.py — Unit tests for database introspection script.

Uses mocked SQLAlchemy Inspector to avoid needing a real database.
"""
import csv
import io
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.types import (
    BigInteger, Boolean, Date, DateTime, Float, Integer, LargeBinary,
    Numeric, SmallInteger, String, Text, Time,
)

import introspect_db


# ---------------------------------------------------------------------------
# map_type tests
# ---------------------------------------------------------------------------

class TestMapType:
    """Test canonical type mapping from SQLAlchemy types."""

    def test_integer(self):
        assert introspect_db.map_type(Integer()) == ("integer", "", "", "")

    def test_big_integer(self):
        assert introspect_db.map_type(BigInteger()) == ("long", "", "", "")

    def test_small_integer(self):
        assert introspect_db.map_type(SmallInteger()) == ("short", "", "", "")

    def test_float(self):
        assert introspect_db.map_type(Float()) == ("double", "", "", "")

    def test_boolean(self):
        assert introspect_db.map_type(Boolean()) == ("boolean", "", "", "")

    def test_string_no_length(self):
        result = introspect_db.map_type(String())
        assert result[0] == "string"

    def test_string_with_length(self):
        result = introspect_db.map_type(String(255))
        assert result == ("string", "", "255", "")

    def test_text(self):
        result = introspect_db.map_type(Text())
        assert result[0] == "string"

    def test_numeric(self):
        result = introspect_db.map_type(Numeric(precision=18, scale=2))
        assert result == ("decimal(18,2)", "", "18", "2")

    def test_numeric_no_precision(self):
        result = introspect_db.map_type(Numeric())
        assert result[0] in ("decimal", "double")

    def test_date(self):
        assert introspect_db.map_type(Date()) == ("date", "", "", "")

    def test_datetime(self):
        result = introspect_db.map_type(DateTime())
        assert result[0] in ("timestamp", "timestamp_tz")

    def test_time(self):
        assert introspect_db.map_type(Time()) == ("time", "", "", "")

    def test_large_binary(self):
        result = introspect_db.map_type(LargeBinary())
        assert result[0] == "binary"

    def test_unknown_type(self):
        result = introspect_db.map_type(MagicMock(__class__=type("WeirdType", (), {})))
        assert isinstance(result[0], str)


# ---------------------------------------------------------------------------
# _build_fk_map tests
# ---------------------------------------------------------------------------

class TestBuildFkMap:
    def test_simple_fk(self):
        fk_list = [{
            "constrained_columns": ["customer_id"],
            "referred_schema": "public",
            "referred_table": "customers",
            "referred_columns": ["id"],
        }]
        result = introspect_db._build_fk_map(fk_list)
        assert result["customer_id"] == "→ public.customers.id"

    def test_no_schema(self):
        fk_list = [{
            "constrained_columns": ["order_id"],
            "referred_schema": None,
            "referred_table": "orders",
            "referred_columns": ["id"],
        }]
        result = introspect_db._build_fk_map(fk_list)
        assert result["order_id"] == "→ orders.id"

    def test_empty(self):
        assert introspect_db._build_fk_map([]) == {}


# ---------------------------------------------------------------------------
# _mask_url tests
# ---------------------------------------------------------------------------

class TestMaskUrl:
    def test_masks_password(self):
        url = "postgresql://user:secret@host:5432/db"
        result = introspect_db._mask_url(url)
        assert "secret" not in result
        assert "***" in result

    def test_no_password(self):
        url = "sqlite:///local.db"
        result = introspect_db._mask_url(url)
        assert "local.db" in result


# ---------------------------------------------------------------------------
# CSV header contract
# ---------------------------------------------------------------------------

class TestCsvContract:
    def test_header_has_14_columns(self):
        assert len(introspect_db.CSV_HEADER) == 14

    def test_header_columns(self):
        expected = [
            "source", "schema", "table", "column", "type", "format",
            "precision", "scale", "nullable", "pk", "fk",
            "ordinal_position", "row_estimate", "notes",
        ]
        assert introspect_db.CSV_HEADER == expected


# ---------------------------------------------------------------------------
# _resolve_dialect tests
# ---------------------------------------------------------------------------

class TestResolveDialect:
    def test_postgresql(self):
        engine = MagicMock()
        engine.dialect.name = "postgresql"
        assert introspect_db._resolve_dialect(engine) == "postgresql"

    def test_mssql(self):
        engine = MagicMock()
        engine.dialect.name = "mssql"
        assert introspect_db._resolve_dialect(engine) == "mssql"


# ---------------------------------------------------------------------------
# _get_system_schemas tests
# ---------------------------------------------------------------------------

class TestGetSystemSchemas:
    def test_postgresql(self):
        schemas = introspect_db._get_system_schemas("postgresql")
        assert "pg_catalog" in schemas
        assert "information_schema" in schemas

    def test_unknown_dialect(self):
        schemas = introspect_db._get_system_schemas("exotic_db")
        assert isinstance(schemas, set)


# ---------------------------------------------------------------------------
# Full introspect (mocked)
# ---------------------------------------------------------------------------

class TestIntrospect:
    """Test the full introspect() function with mocked SQLAlchemy."""

    def _make_mock_inspector(self):
        """Build a mock Inspector that returns a simple schema."""
        inspector = MagicMock()
        inspector.get_schema_names.return_value = ["public"]
        inspector.get_table_names.return_value = ["users", "orders"]
        inspector.get_columns.side_effect = lambda table, schema: {
            "users": [
                {"name": "id", "type": Integer(), "nullable": False},
                {"name": "name", "type": String(100), "nullable": False},
                {"name": "email", "type": String(255), "nullable": True},
            ],
            "orders": [
                {"name": "id", "type": Integer(), "nullable": False},
                {"name": "user_id", "type": Integer(), "nullable": False},
                {"name": "amount", "type": Numeric(10, 2), "nullable": True},
                {"name": "created_at", "type": DateTime(), "nullable": False},
            ],
        }.get(table, [])

        inspector.get_pk_constraint.side_effect = lambda table, schema: {
            "users": {"constrained_columns": ["id"]},
            "orders": {"constrained_columns": ["id"]},
        }.get(table, {"constrained_columns": []})

        inspector.get_foreign_keys.side_effect = lambda table, schema: {
            "orders": [{
                "constrained_columns": ["user_id"],
                "referred_schema": "public",
                "referred_table": "users",
                "referred_columns": ["id"],
            }],
        }.get(table, [])

        return inspector

    @patch("introspect_db.create_engine")
    @patch("introspect_db.inspect")
    def test_introspect_writes_csv(self, mock_inspect, mock_create_engine, tmp_path):
        mock_engine = MagicMock()
        mock_engine.dialect.name = "postgresql"
        mock_engine.connect.return_value.__enter__ = MagicMock()
        mock_engine.connect.return_value.__exit__ = MagicMock()
        mock_create_engine.return_value = mock_engine

        inspector = self._make_mock_inspector()
        mock_inspect.return_value = inspector

        out = tmp_path / "out.csv"
        introspect_db.introspect(
            url="postgresql://user:pass@localhost/test",
            source="test",
            schema_filter=None,
            table_filter=None,
            output_path=str(out),
        )

        reader = csv.reader(io.StringIO(out.read_text()))
        rows = list(reader)

        # Header + data rows
        assert rows[0] == introspect_db.CSV_HEADER
        assert len(rows) > 1

        # Check we got users and orders columns
        tables_found = {row[2] for row in rows[1:]}
        assert "users" in tables_found
        assert "orders" in tables_found

        # Check FK reference
        user_id_rows = [r for r in rows[1:] if r[3] == "user_id"]
        assert len(user_id_rows) == 1
        assert "→ public.users.id" in user_id_rows[0][10]

        # Check PK
        id_rows = [r for r in rows[1:] if r[3] == "id"]
        assert all(r[9] == "true" for r in id_rows)

    @patch("introspect_db.create_engine")
    @patch("introspect_db.inspect")
    def test_system_schemas_filtered(self, mock_inspect, mock_create_engine, tmp_path):
        mock_engine = MagicMock()
        mock_engine.dialect.name = "postgresql"
        mock_engine.connect.return_value.__enter__ = MagicMock()
        mock_engine.connect.return_value.__exit__ = MagicMock()
        mock_create_engine.return_value = mock_engine

        inspector = MagicMock()
        inspector.get_schema_names.return_value = [
            "public", "pg_catalog", "information_schema",
        ]
        inspector.get_table_names.return_value = ["sys_table"]
        inspector.get_columns.return_value = [
            {"name": "id", "type": Integer(), "nullable": False},
        ]
        inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
        inspector.get_foreign_keys.return_value = []
        mock_inspect.return_value = inspector

        introspect_db.introspect(
            url="postgresql://user:pass@localhost/test",
            source="test",
            schema_filter=None,
            table_filter=None,
            output_path=tmp_path / "out.csv",
        )

        buf = io.StringIO((tmp_path / "out.csv").read_text())
        reader = csv.reader(buf)
        rows = list(reader)
        schemas_found = {row[1] for row in rows[1:]}
        assert "pg_catalog" not in schemas_found
        assert "information_schema" not in schemas_found


# ---------------------------------------------------------------------------
# CLI arg parsing
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_minimal(self):
        args = introspect_db.parse_args(["--url", "sqlite:///test.db", "--source", "test"])
        assert args.url == "sqlite:///test.db"
        assert args.source == "test"

    def test_with_schemas(self):
        args = introspect_db.parse_args([
            "--url", "pg://x", "--source", "s", "--schemas", "a,b",
        ])
        assert args.schemas == "a,b"
