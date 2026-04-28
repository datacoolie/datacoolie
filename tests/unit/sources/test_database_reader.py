"""Tests for DatabaseReader.

This module verifies database reading functionality including table/query modes,
watermark filtering, connection configuration, and SQL injection prevention.
"""

from __future__ import annotations

from datetime import datetime, date

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.database_reader import DatabaseReader

from tests.unit.sources.support import MockEngine, db_source, engine, query_source


# ============================================================================
# Basic Read Operations
# ============================================================================


class TestDatabaseReader:
    """Verify basic DatabaseReader table and query reading functionality."""

    def test_read_from_table(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_max_values({"updated_at": "2024-06-01"})
        reader = DatabaseReader(engine)
        result = reader.read(db_source)
        assert result is not None
        info = reader.get_runtime_info()
        assert info.rows_read == 3

    def test_read_from_query(self, engine: MockEngine, query_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DatabaseReader(engine)
        result = reader.read(query_source)
        assert result is not None

    def test_read_with_watermark_table_mode(self, engine: MockEngine, db_source: Source) -> None:
        """Table mode: watermark pushed to SQL as WHERE clause."""
        engine.set_max_values({"updated_at": "2024-06-15"})
        reader = DatabaseReader(engine)
        wm = {"updated_at": "2024-06-01"}
        result = reader.read(db_source, watermark=wm)
        assert result is not None
        # Table mode uses a query with WHERE clause instead of post-filter
        assert engine._last_query is not None
        assert "updated_at > '2024-06-01'" in engine._last_query
        assert engine._last_table is None

    def test_read_zero_rows(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_data({}, 0)
        reader = DatabaseReader(engine)
        result = reader.read(db_source)
        assert result is None

    def test_read_no_table_no_query_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="bad", connection_type="database", format="sql", configure={})
        src = Source(connection=conn)
        reader = DatabaseReader(engine)
        with pytest.raises(SourceError, match="requires source.query or source.table"):
            reader.read(src)

    def test_build_connection_config(self, db_source: Source) -> None:
        config = DatabaseReader._build_options(db_source)
        assert config["host"] == "db.example.com"
        assert config["port"] == 5432
        assert config["database"] == "mydb"
        assert config["user"] == "reader"
        assert config["password"] == "secret"

    def test_read_table_passes_full_table_name(self, engine: MockEngine, db_source: Source) -> None:
        reader = DatabaseReader(engine)
        reader.read(db_source)
        # Engine receives the unquoted schema.table for JDBC
        assert engine._last_table == "public.users"
        assert engine._last_query is None

    def test_source_action_recorded(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_max_values({"updated_at": "2024-06-01"})
        reader = DatabaseReader(engine)
        reader.read(db_source)
        info = reader.get_runtime_info()
        assert info.source_action["reader"] == "DatabaseReader"
        assert info.source_action["table"] == "public.users"

    def test_build_options_includes_connection_extras_and_runtime_override(self) -> None:
        conn = Connection(
            name="db_opts",
            connection_type="database",
            format="sql",
            database="d1",
            configure={
                "host": "h1",
                "port": 1433,
                "username": "u1",
                "password": "p1",
                "driver": "com.example.Driver",
                "url": "jdbc:example://h1/d1",
                "read_options": {"fetchsize": 100},
            },
        )
        src = Source(connection=conn, table="t1")

        opts = DatabaseReader._build_options(src, configure={"fetchsize": 200, "custom": "x"})
        assert opts["driver"] == "com.example.Driver"
        assert opts["url"] == "jdbc:example://h1/d1"
        assert opts["fetchsize"] == 200
        assert opts["custom"] == "x"


class TestEscapeValue:
    def test_string_value(self) -> None:
        assert DatabaseReader._escape_value("hello") == "'hello'"

    def test_datetime_value(self) -> None:
        dt = datetime(2024, 6, 1, 12, 0, 0)
        assert DatabaseReader._escape_value(dt) == "'2024-06-01T12:00:00'"

    def test_date_value(self) -> None:
        d = date(2024, 6, 1)
        assert DatabaseReader._escape_value(d) == "'2024-06-01'"

    def test_int_value(self) -> None:
        assert DatabaseReader._escape_value(42) == "42"

    def test_float_value(self) -> None:
        assert DatabaseReader._escape_value(3.14) == "3.14"


class TestBuildWhereClause:
    def test_single_column(self) -> None:
        clause = DatabaseReader._build_where_clause({"updated_at": "2024-06-01"})
        assert clause == "updated_at > '2024-06-01'"

    def test_multi_column_or(self) -> None:
        clause = DatabaseReader._build_where_clause(
            {"modified_at": "2024-01-01", "seq_id": 100}
        )
        assert "modified_at > '2024-01-01'" in clause
        assert "seq_id > 100" in clause
        assert " OR " in clause

    def test_datetime_column(self) -> None:
        dt = datetime(2024, 6, 15, 10, 30, 0)
        clause = DatabaseReader._build_where_clause({"ts": dt})
        assert clause == "ts > '2024-06-15T10:30:00'"

    def test_empty_watermark(self) -> None:
        clause = DatabaseReader._build_where_clause({})
        assert clause == ""


class TestTableModeWatermark:
    def test_table_watermark_generates_query(self, engine: MockEngine) -> None:
        conn = Connection(
            name="wm_db",
            connection_type="database",
            format="sql",
            database="testdb",
            configure={"host": "localhost", "port": 5432, "username": "u", "password": "p"},
        )
        src = Source(
            connection=conn, schema_name="public", table="events",
            watermark_columns=["event_time", "seq_id"],
        )
        engine.set_max_values({"event_time": "2024-07-01", "seq_id": 200})
        reader = DatabaseReader(engine)
        result = reader.read(src, watermark={"event_time": "2024-06-01", "seq_id": 100})
        assert result is not None
        assert engine._last_query is not None
        assert "event_time > '2024-06-01'" in engine._last_query
        assert "seq_id > 100" in engine._last_query
        assert " OR " in engine._last_query

    def test_table_no_watermark_uses_table(self, engine: MockEngine, db_source: Source) -> None:
        engine.set_max_values({"updated_at": "2024-06-01"})
        reader = DatabaseReader(engine)
        reader.read(db_source)
        assert engine._last_table == "public.users"
        assert engine._last_query is None

    def test_table_first_run_no_watermark_uses_table(self, engine: MockEngine, db_source: Source) -> None:
        """First run (watermark=None): reads table directly, no WHERE clause."""
        engine.set_max_values({"updated_at": "2024-06-01"})
        reader = DatabaseReader(engine)
        reader.read(db_source, watermark=None)
        assert engine._last_table == "public.users"
        assert engine._last_query is None


class TestQueryModeWatermark:
    def test_query_first_run_no_watermark(self, engine: MockEngine) -> None:
        """First run (watermark=None): query runs as-is, no wrapping."""
        conn = Connection(
            name="q_db", connection_type="database", format="sql",
            configure={"host": "h", "port": 1, "username": "u", "password": "p"},
        )
        original_query = "SELECT * FROM orders WHERE active = 1"
        src = Source(
            connection=conn,
            query=original_query,
            watermark_columns=["modified_at"],
        )
        engine.set_max_values({"modified_at": "2024-07-01"})
        reader = DatabaseReader(engine)
        reader.read(src, watermark=None)
        assert engine._last_query == original_query

    def test_query_with_watermark_wraps_subquery(self, engine: MockEngine) -> None:
        """Incremental run: query wrapped as subquery with WHERE clause."""
        conn = Connection(
            name="q_db", connection_type="database", format="sql",
            configure={"host": "h", "port": 1, "username": "u", "password": "p"},
        )
        original_query = "SELECT * FROM orders WHERE active = 1"
        src = Source(
            connection=conn,
            query=original_query,
            watermark_columns=["modified_at"],
        )
        engine.set_max_values({"modified_at": "2024-07-01"})
        reader = DatabaseReader(engine)
        reader.read(src, watermark={"modified_at": "2024-06-01"})
        expected = f"SELECT * FROM ({original_query}) AS t1 WHERE modified_at > '2024-06-01'"
        assert engine._last_query == expected

    def test_query_multi_watermark_columns(self, engine: MockEngine) -> None:
        conn = Connection(
            name="q_db", connection_type="database", format="sql",
            configure={"host": "h", "port": 1, "username": "u", "password": "p"},
        )
        src = Source(
            connection=conn,
            query="SELECT id, ts, seq FROM events",
            watermark_columns=["ts", "seq"],
        )
        engine.set_max_values({"ts": "2024-07-01", "seq": 200})
        reader = DatabaseReader(engine)
        reader.read(src, watermark={"ts": "2024-06-01", "seq": 100})
        assert engine._last_query is not None
        assert "AS t1 WHERE" in engine._last_query
        assert "ts > '2024-06-01'" in engine._last_query
        assert "seq > 100" in engine._last_query
        assert " OR " in engine._last_query

    def test_query_with_none_watermark_value_runs_as_is(self, engine: MockEngine) -> None:
        """Watermark dict exists but column value is None → no wrapping."""
        conn = Connection(
            name="q_db", connection_type="database", format="sql",
            configure={"host": "h", "port": 1, "username": "u", "password": "p"},
        )
        original_query = "SELECT * FROM orders"
        src = Source(
            connection=conn,
            query=original_query,
            watermark_columns=["modified_at"],
        )
        engine.set_max_values({"modified_at": "2024-07-01"})
        reader = DatabaseReader(engine)
        reader.read(src, watermark={"modified_at": None})
        assert engine._last_query == original_query


# ============================================================================
# Phase A — SQL injection mitigation
# ============================================================================


class TestColumnNameValidation:
    """Verify _build_where_clause rejects invalid column names."""

    def test_column_with_space_raises(self) -> None:
        with pytest.raises(SourceError, match="Invalid watermark column name"):
            DatabaseReader._build_where_clause({"updated at": 1})

    def test_column_with_semicolon_raises(self) -> None:
        with pytest.raises(SourceError, match="Invalid watermark column name"):
            DatabaseReader._build_where_clause({"col; DROP TABLE x": 1})

    def test_column_with_single_quote_raises(self) -> None:
        with pytest.raises(SourceError, match="Invalid watermark column name"):
            DatabaseReader._build_where_clause({"col'name": 1})

    def test_column_with_parentheses_raises(self) -> None:
        with pytest.raises(SourceError, match="Invalid watermark column name"):
            DatabaseReader._build_where_clause({"col()": 1})

    def test_column_with_dash_raises(self) -> None:
        with pytest.raises(SourceError, match="Invalid watermark column name"):
            DatabaseReader._build_where_clause({"col-name": 1})

    def test_valid_simple_column(self) -> None:
        clause = DatabaseReader._build_where_clause({"updated_at": 1})
        assert "updated_at > 1" == clause

    def test_valid_dotted_column(self) -> None:
        clause = DatabaseReader._build_where_clause({"schema.col": 1})
        assert "schema.col > 1" == clause

    def test_valid_underscore_prefix(self) -> None:
        clause = DatabaseReader._build_where_clause({"_col": 1})
        assert "_col > 1" == clause


class TestEscapeValueSingleQuotes:
    """Verify _escape_value prevents SQL injection via string values."""

    def test_string_with_single_quote(self) -> None:
        assert DatabaseReader._escape_value("O'Brien") == "'O''Brien'"

    def test_string_with_multiple_quotes(self) -> None:
        assert DatabaseReader._escape_value("it's a 'test'") == "'it''s a ''test'''"

    def test_injection_attempt_escaped(self) -> None:
        malicious = "x' OR '1'='1"
        escaped = DatabaseReader._escape_value(malicious)
        assert escaped == "'x'' OR ''1''=''1'"
