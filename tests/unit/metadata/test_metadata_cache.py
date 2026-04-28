"""Tests for MetadataCache — in-memory metadata store."""

from __future__ import annotations

import pytest

from datacoolie.core.models import Connection, DataFlow, Destination, SchemaHint, Source, Transform
from datacoolie.metadata.base import MetadataCache


# ---------------------------------------------------------------------------
# Helpers — minimal model builders
# ---------------------------------------------------------------------------


def _conn(name: str = "test_conn", connection_id: str = "c-1") -> Connection:
    return Connection(connection_id=connection_id, name=name, connection_type="file")


def _dataflow(dataflow_id: str = "df-1", name: str = "test_df") -> DataFlow:
    conn = _conn()
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


class TestMetadataCacheConnections:
    """Connection get/set/clear behaviour."""

    def test_empty_cache_returns_none(self) -> None:
        cache = MetadataCache()
        assert cache.get_connection("missing") is None
        assert cache.get_connection_by_name("missing") is None

    def test_set_and_get_connection_by_id(self) -> None:
        cache = MetadataCache()
        conn = _conn(name="my_conn", connection_id="c-42")
        cache.set_connection(conn)
        assert cache.get_connection("c-42") is conn

    def test_set_and_get_connection_by_name(self) -> None:
        cache = MetadataCache()
        conn = _conn(name="my_conn", connection_id="c-42")
        cache.set_connection(conn)
        assert cache.get_connection_by_name("my_conn") is conn

    def test_set_connections_bulk(self) -> None:
        cache = MetadataCache()
        c1 = _conn(name="a", connection_id="c-1")
        c2 = _conn(name="b", connection_id="c-2")
        cache.set_connections([c1, c2])
        assert cache.get_connection("c-1") is c1
        assert cache.get_connection("c-2") is c2
        assert cache.get_connection_by_name("a") is c1

    def test_set_connections_replaces_previous(self) -> None:
        cache = MetadataCache()
        old = _conn(name="old", connection_id="c-old")
        cache.set_connection(old)
        new = _conn(name="new", connection_id="c-new")
        cache.set_connections([new])  # replaces everything
        assert cache.get_connection("c-old") is None
        assert cache.get_connection("c-new") is new


# ===========================================================================
# Dataflow caching
# ===========================================================================


class TestMetadataCacheDataflows:
    """Dataflow get/set/clear behaviour."""

    def test_empty_cache_returns_none(self) -> None:
        cache = MetadataCache()
        assert cache.get_dataflow("missing") is None

    def test_set_and_get_dataflow(self) -> None:
        cache = MetadataCache()
        df = _dataflow(dataflow_id="df-99")
        cache.set_dataflow(df)
        assert cache.get_dataflow("df-99") is df

    def test_set_dataflows_bulk(self) -> None:
        cache = MetadataCache()
        df1 = _dataflow(dataflow_id="df-1")
        df2 = _dataflow(dataflow_id="df-2")
        cache.set_dataflows([df1, df2])
        assert cache.get_dataflow("df-1") is df1
        assert cache.get_dataflow("df-2") is df2

    def test_set_dataflows_replaces_previous(self) -> None:
        cache = MetadataCache()
        old = _dataflow(dataflow_id="old")
        cache.set_dataflow(old)
        new = _dataflow(dataflow_id="new")
        cache.set_dataflows([new])
        assert cache.get_dataflow("old") is None
        assert cache.get_dataflow("new") is new


# ===========================================================================
# Schema hint caching
# ===========================================================================


class TestMetadataCacheSchemaHints:
    """Schema hint get/set behaviour."""

    def test_empty_cache_returns_none(self) -> None:
        cache = MetadataCache()
        assert cache.get_schema_hints("c1", None, "tbl") is None

    def test_set_and_get_schema_hints(self) -> None:
        cache = MetadataCache()
        hints = [SchemaHint(column_name="col1", data_type="INT")]
        cache.set_schema_hints("c1", "dbo", "orders", hints)
        assert cache.get_schema_hints("c1", "dbo", "orders") == hints

    def test_schema_hints_composite_key(self) -> None:
        cache = MetadataCache()
        h1 = [SchemaHint(column_name="a", data_type="INT")]
        h2 = [SchemaHint(column_name="b", data_type="STRING")]
        cache.set_schema_hints("c1", "dbo", "t1", h1)
        cache.set_schema_hints("c1", "dbo", "t2", h2)
        assert cache.get_schema_hints("c1", "dbo", "t1") == h1
        assert cache.get_schema_hints("c1", "dbo", "t2") == h2

    def test_none_schema_name_key(self) -> None:
        cache = MetadataCache()
        hints = [SchemaHint(column_name="x", data_type="DATE")]
        cache.set_schema_hints("c1", None, "tbl", hints)
        assert cache.get_schema_hints("c1", None, "tbl") == hints
        assert cache.get_schema_hints("c1", "dbo", "tbl") is None


# ===========================================================================
# Clear
# ===========================================================================


class TestMetadataCacheClear:
    """Cache clear empties all stores."""

    def test_clear_removes_all(self) -> None:
        cache = MetadataCache()
        cache.set_connection(_conn())
        cache.set_dataflow(_dataflow())
        cache.set_schema_hints("c1", None, "t", [SchemaHint(column_name="x", data_type="INT")])
        cache.clear()
        assert cache.get_connection("c-1") is None
        assert cache.get_dataflow("df-1") is None
        assert cache.get_schema_hints("c1", None, "t") is None


# ===========================================================================
# Thread safety
# ===========================================================================


class TestMetadataCacheThreadSafety:
    """Concurrent access must not corrupt state or raise."""

    NUM_THREADS = 8
    ITERATIONS = 200

    def test_concurrent_set_dataflows_no_corruption(self) -> None:
        """Parallel set_dataflows calls must not leave garbled state."""
        import concurrent.futures

        cache = MetadataCache()
        batches = [
            [_dataflow(dataflow_id=f"df-{t}-{i}") for i in range(5)]
            for t in range(self.NUM_THREADS)
        ]

        def _writer(batch: list) -> None:
            for _ in range(self.ITERATIONS):
                cache.set_dataflows(batch)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.NUM_THREADS) as pool:
            list(pool.map(_writer, batches))

        # After all threads finish, cache should hold exactly one
        # batch's worth of dataflows (the last writer wins).
        # The critical property: no KeyError, no partial clear, no crash.
        stored = [cache.get_dataflow(f"df-0-{i}") for i in range(5)]
        # Either all present (this batch won) or all None (another batch won).
        assert all(v is not None for v in stored) or all(v is None for v in stored)

    def test_concurrent_get_and_set_no_exception(self) -> None:
        """Interleaved reads and writes must not raise."""
        import concurrent.futures

        cache = MetadataCache()
        cache.set_dataflow(_dataflow(dataflow_id="seed"))

        errors: list = []

        def _reader() -> None:
            for _ in range(self.ITERATIONS):
                try:
                    cache.get_dataflow("seed")
                    cache.get_connection("c-1")
                except Exception as exc:
                    errors.append(exc)

        def _writer() -> None:
            for _ in range(self.ITERATIONS):
                try:
                    cache.set_dataflows([_dataflow(dataflow_id="seed")])
                    cache.set_connections([_conn()])
                except Exception as exc:
                    errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.NUM_THREADS) as pool:
            futs = []
            for i in range(self.NUM_THREADS):
                futs.append(pool.submit(_reader if i % 2 == 0 else _writer))
            for f in futs:
                f.result()

        assert errors == [], f"Concurrent access raised: {errors}"
