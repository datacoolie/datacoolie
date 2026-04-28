"""Tests for BaseMetadataProvider — ABC contract, caching, schema hint attachment."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from unittest.mock import MagicMock

import pytest

from datacoolie.core.models import Connection, DataFlow, Destination, SchemaHint, Source, Transform
from datacoolie.metadata.base import BaseMetadataProvider, MetadataCache


# ---------------------------------------------------------------------------
# Concrete stub provider for testing the ABC
# ---------------------------------------------------------------------------


class StubProvider(BaseMetadataProvider):
    """Minimal concrete implementation for testing the base class."""

    def __init__(self, *, enable_cache: bool = True) -> None:
        super().__init__(enable_cache=enable_cache)
        self.connections: List[Connection] = []
        self.dataflows: List[DataFlow] = []
        self.hints: Dict[str, List[SchemaHint]] = {}  # keyed by table_name
        self.watermarks: Dict[str, Dict[str, Any]] = {}
        # Track fetch calls for testing
        self.fetch_counts: Dict[str, int] = {}

    def _inc(self, key: str) -> None:
        self.fetch_counts[key] = self.fetch_counts.get(key, 0) + 1

    # -- connections -------------------------------------------------------

    def _fetch_connections(self, *, active_only: bool = True) -> List[Connection]:
        self._inc("connections")
        if active_only:
            return [c for c in self.connections if c.is_active]
        return list(self.connections)

    def _fetch_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        self._inc("connection_by_id")
        for c in self.connections:
            if c.connection_id == connection_id:
                return c
        return None

    def _fetch_connection_by_name(self, name: str) -> Optional[Connection]:
        self._inc("connection_by_name")
        for c in self.connections:
            if c.name == name:
                return c
        return None

    # -- dataflows ---------------------------------------------------------

    def _fetch_dataflows(
        self, *, stages: Optional[List[str]] = None, active_only: bool = True
    ) -> List[DataFlow]:
        self._inc("dataflows")
        result = self.dataflows
        if active_only:
            result = [d for d in result if d.is_active]
        if stages is not None:
            result = [d for d in result if d.stage in stages]
        return result

    def _fetch_dataflow_by_id(self, dataflow_id: str) -> Optional[DataFlow]:
        self._inc("dataflow_by_id")
        for d in self.dataflows:
            if d.dataflow_id == dataflow_id:
                return d
        return None

    # -- schema hints ------------------------------------------------------

    def _fetch_schema_hints(
        self,
        connection_id: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> List[SchemaHint]:
        self._inc("schema_hints")
        return self.hints.get(table_name, [])

    # -- watermark ---------------------------------------------------------

    def get_watermark(self, dataflow_id: str) -> Optional[Dict[str, Any]]:
        return self.watermarks.get(dataflow_id)

    def update_watermark(
        self,
        dataflow_id: str,
        watermark_value: str,
        *,
        job_id: Optional[str] = None,
        dataflow_run_id: Optional[str] = None,
    ) -> None:
        import json
        self.watermarks[dataflow_id] = json.loads(watermark_value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn(name: str = "conn_a", connection_id: str = "c-1", **kw: Any) -> Connection:
    return Connection(connection_id=connection_id, name=name, **kw)


def _dataflow(
    dataflow_id: str = "df-1",
    name: str = "my_df",
    dest_conn: Optional[Connection] = None,
    transform: Optional[Transform] = None,
) -> DataFlow:
    conn = dest_conn or _conn()
    return DataFlow(
        dataflow_id=dataflow_id,
        name=name,
        source=Source(connection=conn, table="src"),
        destination=Destination(connection=conn, table="dest"),
        transform=transform or Transform(),
    )


# ===========================================================================
# ABC contract
# ===========================================================================


class TestBaseMetadataProviderABC:
    """Cannot instantiate the ABC directly."""

    def test_cannot_instantiate(self) -> None:
        with pytest.raises(TypeError):
            BaseMetadataProvider()  # type: ignore[abstract]


# ===========================================================================
# Connection methods
# ===========================================================================


class TestBaseMetadataProviderConnections:
    """get_connections, get_connection_by_id, get_connection_by_name."""

    def test_get_connections_returns_list(self) -> None:
        p = StubProvider()
        c = _conn()
        p.connections = [c]
        result = p.get_connections()
        assert result == [c]

    def test_get_connections_populates_cache(self) -> None:
        p = StubProvider()
        c = _conn()
        p.connections = [c]
        p.get_connections()
        # Second call should still fetch (get_connections always fetches)
        p.get_connections()
        assert p.fetch_counts["connections"] == 2

    def test_get_connection_by_id_cache_hit(self) -> None:
        p = StubProvider()
        c = _conn(connection_id="c-99")
        p.connections = [c]
        # Pre-populate cache via get_connections
        p.get_connections()
        result = p.get_connection_by_id("c-99")
        assert result is not None
        assert result.connection_id == "c-99"
        # _fetch_connection_by_id should NOT have been called
        assert p.fetch_counts.get("connection_by_id", 0) == 0

    def test_get_connection_by_id_cache_miss(self) -> None:
        p = StubProvider()
        c = _conn(connection_id="c-99")
        p.connections = [c]
        result = p.get_connection_by_id("c-99")
        assert result is not None
        assert p.fetch_counts["connection_by_id"] == 1

    def test_get_connection_by_id_not_found(self) -> None:
        p = StubProvider()
        assert p.get_connection_by_id("nope") is None

    def test_get_connection_by_name_cache_hit(self) -> None:
        p = StubProvider()
        c = _conn(name="my_conn")
        p.connections = [c]
        p.get_connections()
        result = p.get_connection_by_name("my_conn")
        assert result is not None
        assert result.name == "my_conn"
        assert p.fetch_counts.get("connection_by_name", 0) == 0

    def test_get_connection_by_name_cache_miss(self) -> None:
        p = StubProvider()
        c = _conn(name="my_conn")
        p.connections = [c]
        result = p.get_connection_by_name("my_conn")
        assert result is not None
        assert p.fetch_counts["connection_by_name"] == 1

    def test_get_connection_by_name_not_found(self) -> None:
        p = StubProvider()
        assert p.get_connection_by_name("nope") is None

    def test_no_cache_mode(self) -> None:
        p = StubProvider(enable_cache=False)
        c = _conn(connection_id="c-1")
        p.connections = [c]
        # First call via get_connections
        p.get_connections()
        # get_connection_by_id must fetch even though we just loaded all
        result = p.get_connection_by_id("c-1")
        assert result is not None
        assert p.fetch_counts["connection_by_id"] == 1


# ===========================================================================
# Dataflow methods
# ===========================================================================


class TestBaseMetadataProviderDataflows:
    """get_dataflows, get_dataflow_by_id with cache + schema hint attachment."""

    def test_get_dataflows_returns_list(self) -> None:
        p = StubProvider()
        df = _dataflow()
        p.dataflows = [df]
        result = p.get_dataflows(attach_schema_hints=False)
        assert result == [df]

    def test_get_dataflows_caches(self) -> None:
        p = StubProvider()
        df = _dataflow()
        p.dataflows = [df]
        p.get_dataflows(attach_schema_hints=False)
        # get_dataflow_by_id should hit cache
        result = p.get_dataflow_by_id(df.dataflow_id, attach_schema_hints=False)
        assert result is not None
        assert p.fetch_counts.get("dataflow_by_id", 0) == 0

    def test_get_dataflow_by_id_miss(self) -> None:
        p = StubProvider()
        df = _dataflow(dataflow_id="df-42")
        p.dataflows = [df]
        result = p.get_dataflow_by_id("df-42", attach_schema_hints=False)
        assert result is not None
        assert p.fetch_counts["dataflow_by_id"] == 1

    def test_get_dataflow_by_id_not_found(self) -> None:
        p = StubProvider()
        assert p.get_dataflow_by_id("nope") is None

    def test_get_dataflows_filters_by_stage(self) -> None:
        p = StubProvider()
        df1 = _dataflow(dataflow_id="df-1", name="a")
        df1.stage = "bronze2silver"
        df2 = _dataflow(dataflow_id="df-2", name="b")
        df2.stage = "silver2gold"
        p.dataflows = [df1, df2]
        result = p.get_dataflows(stage="bronze2silver", attach_schema_hints=False)
        assert len(result) == 1
        assert result[0].dataflow_id == "df-1"


class TestGetMaintenanceDataflows:
    """get_maintenance_dataflows — lakehouse-only + connection filter."""

    def test_lakehouse_only(self) -> None:
        p = StubProvider()
        delta_conn = _conn(name="lh", connection_id="c-lh", format="delta")
        csv_conn = _conn(name="csv", connection_id="c-csv", format="csv")
        df1 = _dataflow(dataflow_id="df-1", name="a", dest_conn=delta_conn)
        df2 = _dataflow(dataflow_id="df-2", name="b", dest_conn=csv_conn)
        p.dataflows = [df1, df2]
        result = p.get_maintenance_dataflows()
        assert len(result) == 1
        assert result[0].dataflow_id == "df-1"

    def test_filters_by_connection_name(self) -> None:
        p = StubProvider()
        conn_a = _conn(name="lakehouse_a", connection_id="c-a", format="delta")
        conn_b = _conn(name="lakehouse_b", connection_id="c-b", format="delta")
        df1 = _dataflow(dataflow_id="df-1", name="a", dest_conn=conn_a)
        df2 = _dataflow(dataflow_id="df-2", name="b", dest_conn=conn_b)
        p.dataflows = [df1, df2]
        result = p.get_maintenance_dataflows(connection="lakehouse_a")
        assert len(result) == 1
        assert result[0].dataflow_id == "df-1"

    def test_filters_by_connection_id(self) -> None:
        p = StubProvider()
        conn_a = _conn(name="a", connection_id="conn-id-1", format="delta")
        conn_b = _conn(name="b", connection_id="conn-id-2", format="delta")
        df1 = _dataflow(dataflow_id="df-1", name="x", dest_conn=conn_a)
        df2 = _dataflow(dataflow_id="df-2", name="y", dest_conn=conn_b)
        p.dataflows = [df1, df2]
        result = p.get_maintenance_dataflows(connection="conn-id-1")
        assert len(result) == 1
        assert result[0].dataflow_id == "df-1"

    def test_connection_comma_separated(self) -> None:
        p = StubProvider()
        conn_a = _conn(name="lh_a", connection_id="c-a", format="delta")
        conn_b = _conn(name="lh_b", connection_id="c-b", format="iceberg")
        conn_c = _conn(name="lh_c", connection_id="c-c", format="delta")
        df1 = _dataflow(dataflow_id="df-1", name="a", dest_conn=conn_a)
        df2 = _dataflow(dataflow_id="df-2", name="b", dest_conn=conn_b)
        df3 = _dataflow(dataflow_id="df-3", name="c", dest_conn=conn_c)
        p.dataflows = [df1, df2, df3]
        result = p.get_maintenance_dataflows(connection="lh_a,lh_b")
        assert len(result) == 2
        ids = {r.dataflow_id for r in result}
        assert ids == {"df-1", "df-2"}

    def test_non_lakehouse_excluded_even_with_connection_match(self) -> None:
        p = StubProvider()
        delta_conn = _conn(name="lh", connection_id="c-lh", format="delta")
        csv_conn = _conn(name="flat", connection_id="c-flat", format="csv")
        df1 = _dataflow(dataflow_id="df-1", name="a", dest_conn=delta_conn)
        df2 = _dataflow(dataflow_id="df-2", name="b", dest_conn=csv_conn)
        p.dataflows = [df1, df2]
        result = p.get_maintenance_dataflows(connection="lh,flat")
        assert len(result) == 1
        assert result[0].dataflow_id == "df-1"

    def test_maintenance_attach_schema_hints(self) -> None:
        p = StubProvider()
        conn = _conn(name="lh", connection_id="c-lh", format="delta", configure={"use_schema_hint": True})
        df = _dataflow(dataflow_id="df-1", name="a", dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        result = p.get_maintenance_dataflows(attach_schema_hints=True)
        assert len(result) == 1
        assert result[0].transform.schema_hints[0].column_name == "col"

    def test_maintenance_with_cache_disabled(self) -> None:
        p = StubProvider(enable_cache=False)
        conn = _conn(name="lh", connection_id="c-lh", format="delta")
        p.dataflows = [_dataflow(dataflow_id="df-1", name="a", dest_conn=conn)]
        result = p.get_maintenance_dataflows(attach_schema_hints=False)
        assert len(result) == 1


# ===========================================================================
# Schema hint attachment
# ===========================================================================


class TestSchemaHintAttachment:
    """_attach_schema_hints logic."""

    def test_attach_hints_when_use_schema_hint_true(self) -> None:
        p = StubProvider()
        # Create connection with use_schema_hint=True
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        result = p.get_dataflows(attach_schema_hints=True)
        assert len(result[0].transform.schema_hints) == 1
        assert result[0].transform.schema_hints[0].column_name == "col"

    def test_skip_hints_when_use_schema_hint_false(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": False})
        df = _dataflow(dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        result = p.get_dataflows(attach_schema_hints=True)
        assert len(result[0].transform.schema_hints) == 0

    def test_skip_hints_when_already_attached(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        existing = [SchemaHint(column_name="existing", data_type="STRING")]
        df = _dataflow(dest_conn=conn, transform=Transform(schema_hints=existing))
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="new", data_type="INT")]

        result = p.get_dataflows(attach_schema_hints=True)
        # Should keep existing, not replace
        assert result[0].transform.schema_hints[0].column_name == "existing"

    def test_attach_schema_hints_disabled_no_fetch(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        result = p.get_dataflows(attach_schema_hints=False)
        assert result[0].transform.schema_hints == []
        assert p.fetch_counts.get("schema_hints", 0) == 0


# ===========================================================================
# Lifecycle — context manager + clear_cache
# ===========================================================================


class TestBaseMetadataProviderLifecycle:
    """Context manager and close/clear_cache."""

    def test_context_manager(self) -> None:
        p = StubProvider()
        c = _conn()
        p.connections = [c]
        p.get_connections()  # populate cache
        with p:
            pass
        # After close, cache should be cleared
        assert p._cache is not None
        assert p._cache.get_connection("c-1") is None

    def test_clear_cache(self) -> None:
        p = StubProvider()
        c = _conn()
        p.connections = [c]
        p.get_connections()
        p.clear_cache()
        assert p._cache.get_connection("c-1") is None

    def test_clear_cache_noop_when_disabled(self) -> None:
        p = StubProvider(enable_cache=False)
        p.clear_cache()  # should not raise


class TestGetDataflowByIdAttachToggle:
    def test_cached_dataflow_no_attach_when_disabled(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dataflow_id="df-1", dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        p.get_dataflows(attach_schema_hints=False)
        out = p.get_dataflow_by_id("df-1", attach_schema_hints=False)
        assert out is not None
        assert out.transform.schema_hints == []

    def test_cached_dataflow_attach_when_enabled(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dataflow_id="df-1", dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        p.get_dataflows(attach_schema_hints=False)
        out = p.get_dataflow_by_id("df-1", attach_schema_hints=True)
        assert out is not None
        assert out.transform.schema_hints[0].column_name == "col"

    def test_fetched_dataflow_attach_when_enabled(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dataflow_id="df-1", dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = [SchemaHint(column_name="col", data_type="INT")]

        out = p.get_dataflow_by_id("df-1", attach_schema_hints=True)
        assert out is not None
        assert out.transform.schema_hints[0].column_name == "col"

    def test_attach_schema_hints_no_hints_found_keeps_empty(self) -> None:
        p = StubProvider()
        conn = _conn(configure={"use_schema_hint": True})
        df = _dataflow(dataflow_id="df-1", dest_conn=conn, transform=Transform())
        p.dataflows = [df]
        p.hints["src"] = []

        out = p.get_dataflow_by_id("df-1", attach_schema_hints=True)
        assert out is not None
        assert out.transform.schema_hints == []


# ===========================================================================
# prefetch_all — bulk-load semantics, idempotence, thread safety
# ===========================================================================


class TestPrefetchAll:
    """Covers the cache-authoritative bulk-load contract."""

    def test_prefetch_all_marks_cache_loaded(self) -> None:
        p = StubProvider()
        p.connections = [_conn()]
        p.dataflows = []
        p.prefetch_all()
        assert p._cache is not None
        assert p._cache.is_loaded() is True

    def test_prefetch_all_is_idempotent(self) -> None:
        p = StubProvider()
        p.connections = [_conn()]
        p.dataflows = []
        p.prefetch_all()
        p.prefetch_all()  # second call must not re-run _bulk_load
        # Default _bulk_load delegates to _fetch_connections / _fetch_dataflows
        assert p.fetch_counts.get("connections", 0) == 1
        assert p.fetch_counts.get("dataflows", 0) == 1

    def test_prefetch_all_noop_when_cache_disabled(self) -> None:
        p = StubProvider(enable_cache=False)
        p.connections = [_conn()]
        p.prefetch_all()  # must not raise
        assert p.fetch_counts.get("connections", 0) == 0

    def test_get_schema_hints_returns_empty_after_bulk_load_when_no_hints(self) -> None:
        """Negative cache: after prefetch_all, missing key means no hints —
        do NOT fall through to _fetch_schema_hints."""
        p = StubProvider()
        p.connections = [_conn()]
        p.dataflows = []
        p.hints = {}  # no hints anywhere
        p.prefetch_all()
        baseline = p.fetch_counts.get("schema_hints", 0)

        result = p.get_schema_hints(connection_id="c-1", table_name="absent_table")

        assert result == []
        # No remote fetch should have been triggered.
        assert p.fetch_counts.get("schema_hints", 0) == baseline

    def test_prefetch_all_thread_safe(self) -> None:
        """Concurrent callers must result in a single bulk-load."""
        import threading

        p = StubProvider()
        p.connections = [_conn()]
        p.dataflows = []

        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            p.prefetch_all()

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one bulk-load occurred despite 8 racing callers.
        assert p.fetch_counts.get("connections", 0) == 1
        assert p.fetch_counts.get("dataflows", 0) == 1


class TestEagerPrefetch:
    """`eager_prefetch=True` triggers bulk-load at construction time."""

    def test_eager_prefetch_loads_at_init(self) -> None:
        # Subclass that calls _maybe_eager_prefetch at end of __init__,
        # matching the pattern used by FileProvider / DatabaseProvider / APIClient.
        class EagerStub(StubProvider):
            def __init__(self, *, eager_prefetch: bool = False) -> None:
                super().__init__()
                self._eager_prefetch = eager_prefetch
                self.connections = [_conn()]
                self.dataflows = []
                self._maybe_eager_prefetch()

        p = EagerStub(eager_prefetch=True)
        assert p._cache is not None
        assert p._cache.is_loaded() is True
        assert p.fetch_counts.get("connections", 0) == 1

    def test_eager_prefetch_disabled_by_default(self) -> None:
        p = StubProvider()
        assert p._cache is not None
        assert p._cache.is_loaded() is False
        assert p.fetch_counts.get("connections", 0) == 0
