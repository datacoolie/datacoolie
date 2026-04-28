"""Tests for IcebergReader."""

from __future__ import annotations

import pytest

from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.iceberg_reader import IcebergReader

from tests.unit.sources.support import MockEngine, engine


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
        result = reader.read(src)
        assert result is not None
        info = reader.get_runtime_info()
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
        result = reader.read(src)
        assert result is not None

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
        result = reader.read(src)
        assert result is not None

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
        result = reader.read(src)
        assert result is None

    def test_read_no_path_no_query_no_catalog_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="ice", connection_type="lakehouse", format="iceberg", configure={})
        src = Source(connection=conn)
        reader = IcebergReader(engine)
        with pytest.raises(SourceError, match="requires table_name or path"):
            reader.read(src)

    def test_read_applies_watermark_filter_when_present(self, engine: MockEngine) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        conn = Connection(name="ice", connection_type="lakehouse", format="iceberg", configure={"base_path": "/data/ice"})
        src = Source(connection=conn, table="t1", watermark_columns=["modified_at"])
        reader = IcebergReader(engine)

        reader.read(src, watermark={"modified_at": "2024-06-01"})
        assert engine._filtered is True
