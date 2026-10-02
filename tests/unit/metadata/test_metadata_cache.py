"""Tests for MetadataCache — in-memory metadata store."""

from __future__ import annotations

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.transform import SchemaHint
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import Transform
from datacoolie.metadata.base import MetadataCache


# ---------------------------------------------------------------------------
# Helpers — minimal model builders
# ---------------------------------------------------------------------------


def _conn(name: str = "test_conn", connection_id: str = "c-1") -> Connection:
    return Connection(connection_id=connection_id, name=name, connection_type="file")


def _dataflow(
    dataflow_id: str = "df-1",
    name: str = "test_df",
    conn: Connection | None = None,
) -> DataFlow:
    conn = conn or _conn()
    return DataFlow(
        dataflow_id=dataflow_id,
        name=name,
        source=Source(connection=conn, table="src_table"),
        destination=Destination(connection=conn, table="dest_table"),
        transform=Transform(),
    )


# ===========================================================================
# Connection caching
# ===========================================================================


class TestMetadataCacheSnapshots:
    """Metadata is read from complete snapshots published by the provider."""

    def test_empty_cache_returns_none(self) -> None:
        cache = MetadataCache()
        assert cache.get_connection("missing") is None
        assert cache.get_connection_by_name("missing") is None
        assert cache.get_dataflow("missing") is None
        assert cache.get_schema_hints("c-1", None, "orders") is None

    def test_publish_snapshot_populates_all_read_indexes(self) -> None:
        cache = MetadataCache()
        connection = _conn(name="my_conn", connection_id="c-42")
        dataflow = _dataflow(dataflow_id="df-42", conn=connection)
        hints = [SchemaHint(column_name="id", data_type="INT")]
        grouped_hints = {("c-42", "dbo", "orders"): hints}

        cache.publish_snapshot([connection], [dataflow], grouped_hints)

        assert cache.get_connection("c-42") is connection
        assert cache.get_connection_by_name("my_conn") is connection
        assert cache.get_all_connections() == [connection]
        assert cache.get_dataflow("df-42") is dataflow
        assert cache.get_all_dataflows() == [dataflow]
        assert cache.get_schema_hints("c-42", "dbo", "orders") == hints
        assert cache.get_all_schema_hints() == grouped_hints

    def test_publish_snapshot_replaces_all_previous_entries(self) -> None:
        cache = MetadataCache()
        old_connection = _conn(name="old", connection_id="c-old")
        old_dataflow = _dataflow(dataflow_id="df-old", conn=old_connection)
        old_hints = [SchemaHint(column_name="old", data_type="INT")]
        cache.publish_snapshot(
            [old_connection],
            [old_dataflow],
            {("c-old", None, "orders"): old_hints},
        )

        new_connection = _conn(name="new", connection_id="c-new")
        new_dataflow = _dataflow(dataflow_id="df-new", conn=new_connection)
        new_hints = [SchemaHint(column_name="new", data_type="STRING")]
        cache.publish_snapshot(
            [new_connection],
            [new_dataflow],
            {("c-new", None, "orders"): new_hints},
        )

        assert cache.get_connection("c-old") is None
        assert cache.get_connection_by_name("old") is None
        assert cache.get_dataflow("df-old") is None
        assert cache.get_schema_hints("c-old", None, "orders") is None
        assert cache.get_connection("c-new") is new_connection
        assert cache.get_dataflow("df-new") is new_dataflow
        assert cache.get_schema_hints("c-new", None, "orders") == new_hints

    def test_clear_removes_all_snapshot_entries(self) -> None:
        cache = MetadataCache()
        connection = _conn()
        cache.publish_snapshot(
            [connection],
            [_dataflow(conn=connection)],
            {("c-1", None, "orders"): [
                SchemaHint(column_name="id", data_type="INT")
            ]},
        )

        cache.clear()

        assert cache.get_connection("c-1") is None
        assert cache.get_connection_by_name("test_conn") is None
        assert cache.get_dataflow("df-1") is None
        assert cache.get_schema_hints("c-1", None, "orders") is None
