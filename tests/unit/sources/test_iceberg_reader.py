"""Tests for IcebergReader."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources.iceberg_reader import IcebergReader

from tests.unit.sources.support import MockEngine, engine, read_request


class TestIcebergReader:
    def test_read_with_catalog(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        conn = Connection(
            name="ice",
            connection_type="lakehouse",
            format="iceberg",
            configure={"catalog": "my_catalog", "database": "db"},
        )
        src = Source(
            connection=conn,
            schema_name="sch",
            table="tbl",
            watermark_columns=["modified_at"],
        )
        reader = IcebergReader(engine)
        result = reader.read(src, request=read_request())
        assert result is not None
        info = result.runtime
        assert info.rows_read == 3

    def test_read_with_query(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        conn = Connection(name="ice", connection_type="lakehouse", format="iceberg", configure={})
        src = Source(
            connection=conn,
            query="SELECT * FROM ice_table WHERE active = 1",
            watermark_columns=["modified_at"],
        )
        reader = IcebergReader(engine)
        with patch.object(engine, "execute_sql", wraps=engine.execute_sql) as execute_sql, \
             patch.object(engine, "read", wraps=engine.read) as read:
            result = reader.read(src, request=read_request())

        execute_sql.assert_called_once_with(src.query)
        read.assert_not_called()
        assert result.frame == engine._data
        assert result.rows_read == 3
        assert result.runtime is not None
        assert result.runtime.source_action == {
            "reader": "IcebergReader",
            "query": src.query,
        }

    def test_read_with_path_fallback(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        conn = Connection(
            name="ice",
            connection_type="lakehouse",
            format="iceberg",
            configure={"base_path": "/data/iceberg"},
        )
        src = Source(
            connection=conn,
            schema_name="sch",
            table="tbl",
            watermark_columns=["modified_at"],
        )
        reader = IcebergReader(engine)
        with patch.object(engine, "read", wraps=engine.read) as read, \
             patch.object(engine, "execute_sql", wraps=engine.execute_sql) as execute_sql:
            result = reader.read(src, request=read_request())

        read.assert_called_once_with(
            fmt="iceberg",
            table_name="`sch`.`tbl`",
            path="/data/iceberg/sch/tbl",
            options=None,
        )
        execute_sql.assert_not_called()
        assert result.frame == engine._data
        assert result.rows_read == 3
        assert result.runtime is not None
        assert result.runtime.source_action == {
            "reader": "IcebergReader",
            "table": "`sch`.`tbl`",
            "path": "/data/iceberg/sch/tbl",
        }

    def test_read_zero_rows_returns_none(self, engine: MockEngine) -> None:
        engine.set_data({}, 0)
        conn = Connection(
            name="ice",
            connection_type="lakehouse",
            format="iceberg",
            configure={"catalog": "cat", "database": "db"},
        )
        src = Source(connection=conn, schema_name="sch", table="tbl")
        reader = IcebergReader(engine)
        result = reader.read(src, request=read_request())
        assert result.frame is None

    def test_read_no_path_no_query_no_catalog_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="ice", connection_type="lakehouse", format="iceberg", configure={})
        src = Source(connection=conn)
        reader = IcebergReader(engine)
        with pytest.raises(SourceError, match="requires table_name or path"):
            reader.read(src, request=read_request())

    def test_read_applies_watermark_filter_when_present(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        conn = Connection(name="ice", connection_type="lakehouse", format="iceberg", configure={"base_path": "/data/ice"})
        src = Source(connection=conn, table="t1", watermark_columns=["modified_at"])
        reader = IcebergReader(engine)

        reader.read(
            src,
            request=read_request(watermark_start={"modified_at": "2024-06-01"}),
        )
        assert engine._filtered is True
