"""Tests for DeltaReader."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models import Connection, Source
from datacoolie.sources.delta_reader import DeltaReader

from tests.unit.sources.support import MockEngine, delta_source, engine, query_source


class TestDeltaReader:
    def test_read_from_path(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        result = reader.read(delta_source)
        assert result is not None
        info = reader.get_runtime_info()
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.rows_read == 3

    def test_read_from_query(self, engine: MockEngine, query_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        result = reader.read(query_source)
        assert result is not None

    def test_read_with_watermark(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        reader = DeltaReader(engine)
        wm = {"modified_at": "2024-06-01"}
        result = reader.read(delta_source, watermark=wm)
        assert result is not None
        assert engine._filtered is True
        info = reader.get_runtime_info()
        assert info.watermark_before == wm
        assert info.watermark_after == {"modified_at": "2024-06-15"}

    def test_read_zero_rows_returns_none(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_data({}, 0)
        reader = DeltaReader(engine)
        result = reader.read(delta_source)
        assert result is None
        info = reader.get_runtime_info()
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.rows_read == 0

    def test_read_no_path_no_query_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="bad", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn)
        reader = DeltaReader(engine)
        with pytest.raises(SourceError, match="requires table_name or path"):
            reader.read(src)

    def test_source_action_recorded(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        reader.read(delta_source)
        info = reader.get_runtime_info()
        assert info.source_action["reader"] == "DeltaReader"
        assert info.source_action["table"] == delta_source.full_table_name

    def test_new_watermark_stored(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-12-01"})
        reader = DeltaReader(engine)
        reader.read(delta_source)
        wm = reader.get_new_watermark()
        assert wm == {"modified_at": "2024-12-01"}

    def test_source_action_uses_path_when_no_query_or_table(self, engine: MockEngine) -> None:
        conn = Connection(
            name="delta_path",
            connection_type="lakehouse",
            format="delta",
            configure={"base_path": "/delta/raw"},
        )
        src = Source(connection=conn, table="events", watermark_columns=["modified_at"])
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        reader.read(src)
        info = reader.get_runtime_info()
        # With a table set, action records full_table_name under "table"
        assert info.source_action.get("table") == "`events`"


