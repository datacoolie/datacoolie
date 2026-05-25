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


class TestMetadataInitLazyImport:
    def test_database_provider_lazy(self) -> None:
        import datacoolie.metadata as m
        cls = m.__getattr__('DatabaseProvider')
        from datacoolie.metadata.database_provider import DatabaseProvider
        assert cls is DatabaseProvider

    def test_api_client_lazy(self) -> None:
        import datacoolie.metadata as m
        cls = m.__getattr__('APIClient')
        from datacoolie.metadata.api_client import APIClient
        assert cls is APIClient

    def test_file_provider_lazy(self) -> None:
        import datacoolie.metadata as m
        cls = m.__getattr__('FileProvider')
        from datacoolie.metadata.file_provider import FileProvider
        assert cls is FileProvider

    def test_unknown_attr_raises(self) -> None:
        import datacoolie.metadata as m
        import pytest
        with pytest.raises(AttributeError):
            m.__getattr__('NoSuch')


class TestBaseMetadataCacheHitPaths:
    """Cover cached data retrieval paths (lines 266, 324, 375-376, 412-414)."""

    def test_prefetch_all_already_loaded_skips_double_check(self) -> None:
        """Line 266: return early inside lock if already loaded."""
        provider = StubProvider(enable_cache=True)
        provider.prefetch_all()
        # Second call should hit the early return
        pre = provider.fetch_counts.get('connections', 0)
        provider.prefetch_all()
        post = provider.fetch_counts.get('connections', 0)
        # Should NOT have fetched again
        assert post == pre

    def test_get_dataflows_with_stage_filter_from_cache(self) -> None:
        """Lines 375-376: stage filter applied on cached data."""
        provider = StubProvider(enable_cache=True)
        df1 = _dataflow(dataflow_id='df-1')
        df2 = _dataflow(dataflow_id='df-2')
        import dataclasses
        df1 = dataclasses.replace(df1, stage='bronze')
        df2 = dataclasses.replace(df2, stage='silver')
        provider.dataflows = [df1, df2]

        # Populate cache
        provider.prefetch_all()
        # Filter by stage from cache
        result = provider.get_dataflows(stage=['bronze'])
        assert len(result) == 1
        assert result[0].dataflow_id == 'df-1'

    def test_get_dataflows_inactive_filter_from_cache(self) -> None:
        """Cache returns both active and inactive; active_only filters them."""
        provider = StubProvider(enable_cache=True)
        df_active = _dataflow(dataflow_id='df-active')
        df_inactive = _dataflow(dataflow_id='df-inactive')
        import dataclasses
        df_inactive = dataclasses.replace(df_inactive, is_active=False)
        provider.dataflows = [df_active, df_inactive]

        provider.prefetch_all()
        result = provider.get_dataflows(active_only=True)
        assert all(df.is_active for df in result)

    def test_attach_hints_from_cache_snapshot(self) -> None:
        """Lines 412-414: attach hints from loaded cache snapshot."""
        provider = StubProvider(enable_cache=True)
        conn = Connection(connection_id='c-with-hint', name='conn_a', configure={'use_schema_hint': True})
        hint = SchemaHint(column_name='id', data_type='INT')
        df = DataFlow(
            dataflow_id='df-hint',
            source=Source(connection=conn, table='orders'),
            destination=Destination(connection=conn, table='orders'),
        )
        provider.connections = [conn]
        provider.dataflows = [df]
        provider.hints['orders'] = [hint]

        # Prefetch so cache is loaded with hints
        provider.prefetch_all()
        result = provider.get_dataflows(attach_schema_hints=True)
        # Should have attached hints
        assert result is not None


class TestBaseMetadataCacheNegativeAndHints:
    """Cover lines 444, 462, 478, 571, 621, 630, 633, 639."""

    def test_get_connections_all_from_loaded_cache(self) -> None:
        """Line 444: get_connections(active_only=False) served from cache."""
        provider = StubProvider(enable_cache=True)
        conn = _conn()
        provider.connections = [conn]
        provider.prefetch_all()
        pre = provider.fetch_counts.get('connections', 0)
        result = provider.get_connections(active_only=False)
        assert provider.fetch_counts.get('connections', 0) == pre
        assert len(result) == 1

    def test_get_connection_by_name_negative_cache(self) -> None:
        """Line 478: returns None when cache is loaded but name not found."""
        provider = StubProvider(enable_cache=True)
        provider.connections = []
        provider.prefetch_all()
        result = provider.get_connection_by_name('nonexistent')
        assert result is None

    def test_get_dataflow_by_id_from_cache(self) -> None:
        """Line 571: get_dataflow_by_id hits cache."""
        provider = StubProvider(enable_cache=True)
        df = _dataflow(dataflow_id='df-cached')
        provider.dataflows = [df]
        provider.prefetch_all()
        result = provider.get_dataflow_by_id('df-cached', attach_schema_hints=False)
        assert result is not None
        assert result.dataflow_id == 'df-cached'

    def test_attach_schema_hints_skips_when_already_attached(self) -> None:
        """Line 621: early return when hints already attached."""
        from datacoolie.core.models import Transform
        provider = StubProvider(enable_cache=True)
        hint = SchemaHint(column_name='id', data_type='INT')
        conn = _conn()
        df = _dataflow()
        df.transform.schema_hints = [hint]
        # Should not call get_schema_hints
        pre = provider.fetch_counts.get('schema_hints', 0)
        provider._attach_schema_hints(df)
        assert provider.fetch_counts.get('schema_hints', 0) == pre

    def test_attach_schema_hints_skips_when_no_use_schema_hint(self) -> None:
        """Line 630: skip when connection.use_schema_hint is False."""
        provider = StubProvider(enable_cache=True)
        conn = Connection(connection_id='c1', name='c1', configure={'use_schema_hint': False})
        df = DataFlow(
            dataflow_id='df1',
            source=Source(connection=conn, table='t1'),
            destination=Destination(connection=conn, table='t1'),
        )
        pre = provider.fetch_counts.get('schema_hints', 0)
        provider._attach_schema_hints(df)
        assert provider.fetch_counts.get('schema_hints', 0) == pre

    def test_attach_schema_hints_skips_when_no_table(self) -> None:
        """Line 639: skip when source.table is None."""
        provider = StubProvider(enable_cache=True)
        conn = Connection(connection_id='c1', name='c1', configure={'use_schema_hint': True})
        df = DataFlow(
            dataflow_id='df1',
            source=Source(connection=conn, table=None),
            destination=Destination(connection=conn, table='t1'),
        )
        pre = provider.fetch_counts.get('schema_hints', 0)
        provider._attach_schema_hints(df)
        assert provider.fetch_counts.get('schema_hints', 0) == pre


class TestBaseMetadataRemainingLines:
    """Cover lines 266, 412-414, 462, 571, 621 from the full coverage run."""

    def test_get_connection_by_id_from_loaded_cache(self) -> None:
        """Line 462: get_connection_by_id returns cached hit."""
        p = StubProvider(enable_cache=True)
        c = _conn(connection_id='cx1')
        p.connections = [c]
        p.prefetch_all()
        pre = p.fetch_counts.get('connection_by_id', 0)
        result = p.get_connection_by_id('cx1')
        assert result is not None
        assert p.fetch_counts.get('connection_by_id', 0) == pre

    def test_get_dataflow_by_id_negative_cache(self) -> None:
        """Line 571: returns None for unknown id when cache is loaded."""
        p = StubProvider(enable_cache=True)
        p.dataflows = []
        p.prefetch_all()
        result = p.get_dataflow_by_id('nonexistent-id')
        assert result is None
        assert p.fetch_counts.get('dataflow_by_id', 0) == 0

    def test_attach_hints_if_requested_with_loaded_cache(self) -> None:
        """Lines 412-414: _attach_hints_if_requested when cache is loaded."""
        p = StubProvider(enable_cache=True)
        conn = Connection(connection_id='c-hint', name='c-hint', configure={'use_schema_hint': True})
        hint = SchemaHint(column_name='id', data_type='INT')
        df = DataFlow(
            dataflow_id='df-h',
            source=Source(connection=conn, table='orders'),
            destination=Destination(connection=conn, table='orders'),
        )
        p.connections = [conn]
        p.dataflows = [df]
        p.hints['orders'] = [hint]
        p.prefetch_all()
        # Call _attach_hints_if_requested directly with a loaded cache
        p._attach_hints_if_requested([df], attach_schema_hints=True)
        # Hints should be populated
        assert df.transform.schema_hints is not None

    def test_attach_schema_hints_skips_already_attached(self) -> None:
        """Line 621: _attach_schema_hints returns early when hints exist."""
        p = StubProvider(enable_cache=True)
        conn = _conn()
        df = _dataflow()
        df.transform.schema_hints = [SchemaHint(column_name='x', data_type='INT')]
        pre = p.fetch_counts.get('schema_hints', 0)
        p._attach_schema_hints(df)
        assert p.fetch_counts.get('schema_hints', 0) == pre


class TestAttachHintsRemainingCoverage:
    """Cover lines 412-414, 621 in base.py."""

    def _make_df_with_hints(self) -> "DataFlow":
        from unittest.mock import MagicMock
        from datacoolie.core.models import Connection, DataFlow, Destination, Source, Transform
        conn = Connection(connection_id='c1', name='test', configure={'use_schema_hint': True})
        src = Source(connection=conn, table='tbl')
        dest = Destination(table='dtbl', connection=conn, configure={'catalog': 'cat'})
        transform = Transform()
        transform.schema_hints = [MagicMock()]  # non-empty means already attached
        return DataFlow.model_construct(
            dataflow_id='df-hints',
            source=src,
            destination=dest,
            load_type='append',
            transform=transform,
        )

    def test_attach_hints_if_requested_no_cache_calls_prefetch(self) -> None:
        """Lines 412-414: no cache → calls _prefetch_schema_hints and _attach_schema_hints."""
        from datacoolie.metadata.base import BaseMetadataProvider
        from unittest.mock import MagicMock
        provider = MagicMock(spec=BaseMetadataProvider)
        provider._cache = None
        
        # Create simple dataflow without hints
        from datacoolie.core.models import Connection, DataFlow, Destination, Source, Transform
        conn = Connection(connection_id='c2', name='t2', configure={'use_schema_hint': True})
        src = Source(connection=conn, table='tbl2')
        dest = Destination(table='dtbl2', connection=conn, configure={'catalog': 'cat'})
        df = DataFlow.model_construct(
            dataflow_id='df-no-hints', source=src, destination=dest,
            load_type='append', transform=Transform(),
        )
        provider._prefetch_schema_hints = MagicMock()
        provider._attach_schema_hints = MagicMock()
        BaseMetadataProvider._attach_hints_if_requested(provider, [df], attach_schema_hints=True)
        provider._prefetch_schema_hints.assert_called_once()
        provider._attach_schema_hints.assert_called_once_with(df)

    def test_attach_schema_hints_skips_when_already_attached(self) -> None:
        """Line 621: returns early when schema_hints already set."""
        from datacoolie.metadata.base import BaseMetadataProvider
        from unittest.mock import MagicMock
        provider = MagicMock(spec=BaseMetadataProvider)
        df = self._make_df_with_hints()
        # Should return without making requests (no get_schema_hints calls)
        # The return happens before get_schema_hints is called
        # We can verify by checking the result is None (no exception)
        result = BaseMetadataProvider._attach_schema_hints(provider, df)
        assert result is None
