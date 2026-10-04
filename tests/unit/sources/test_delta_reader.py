"""Tests for DeltaReader."""

from __future__ import annotations

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import SourceError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.source import Source
from datacoolie.sources.delta_reader import DeltaReader

from tests.unit.sources.support import (
    MockEngine,
    delta_source,
    engine,
    query_source,
    read_request,
)


class TestDeltaReader:
    def test_read_from_path(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        result = reader.read(delta_source, request=read_request())
        assert result is not None
        info = result.runtime
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.rows_read == 3

    def test_read_from_query(self, engine: MockEngine, query_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        result = reader.read(query_source, request=read_request())
        assert result is not None

    def test_read_with_watermark(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-15"})
        reader = DeltaReader(engine)
        wm = {"modified_at": "2024-06-01"}
        result = reader.read(
            delta_source,
            request=read_request(watermark_start=wm),
        )
        assert result is not None
        assert engine._filtered is True
        info = result.runtime
        assert result.selection.lower == wm
        assert result.candidate is not None
        assert result.candidate.values == {"modified_at": "2024-06-15"}

    def test_read_zero_rows_returns_none(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_data({}, 0)
        reader = DeltaReader(engine)
        result = reader.read(delta_source, request=read_request())
        assert result.frame is None
        info = result.runtime
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert info.rows_read == 0

    def test_read_no_path_no_query_raises(self, engine: MockEngine) -> None:
        conn = Connection(name="bad", connection_type="lakehouse", format="delta", configure={})
        src = Source(connection=conn)
        reader = DeltaReader(engine)
        with pytest.raises(SourceError, match="requires table_name or path"):
            reader.read(src, request=read_request())

    def test_source_action_recorded(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-06-01"})
        reader = DeltaReader(engine)
        result = reader.read(delta_source, request=read_request())
        info = result.runtime
        assert info.source_action["reader"] == "DeltaReader"
        assert info.source_action["table"] == delta_source.full_table_name

    def test_new_watermark_stored(self, engine: MockEngine, delta_source: Source) -> None:
        engine.set_max_values({"modified_at": "2024-12-01"})
        reader = DeltaReader(engine)
        result = reader.read(delta_source, request=read_request())
        assert result.candidate is not None
        assert result.candidate.values == {"modified_at": "2024-12-01"}

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
        result = reader.read(src, request=read_request())
        info = result.runtime
        # With a table set, action records full_table_name under "table"
        assert info.source_action.get("table") == "`events`"

