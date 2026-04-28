"""Concurrent tests for MetadataCache aligned with current cache APIs.

These tests focus on thread-safety and consistency for the concrete
connection/dataflow/schema-hint cache operations implemented in
``datacoolie.metadata.base.MetadataCache``.
"""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Optional

from datacoolie.core.models import Connection, DataFlow, Destination, SchemaHint, Source, Transform
from datacoolie.metadata.base import MetadataCache


def _conn(i: int) -> Connection:
    return Connection(
        connection_id=f"c-{i}",
        name=f"conn_{i}",
        connection_type="file",
        format="parquet",
    )


def _df(i: int, conn: Optional[Connection] = None) -> DataFlow:
    c = conn or _conn(i)
    return DataFlow(
        dataflow_id=f"df-{i}",
        name=f"df_{i}",
        source=Source(connection=c, table=f"src_{i}"),
        destination=Destination(connection=c, table=f"dest_{i}"),
        transform=Transform(),
    )


class TestMetadataCacheConcurrentConnections:
    def test_concurrent_set_and_get_connections(self) -> None:
        cache = MetadataCache()

        def write_then_read(i: int) -> bool:
            conn = _conn(i)
            cache.set_connection(conn)
            by_id = cache.get_connection(conn.connection_id)
            by_name = cache.get_connection_by_name(conn.name)
            return by_id is not None and by_name is not None

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            ok_flags = list(executor.map(write_then_read, range(60)))

        assert all(ok_flags)
        # Spot-check several entries are still accessible after concurrent writes.
        assert cache.get_connection("c-0") is not None
        assert cache.get_connection_by_name("conn_59") is not None

    def test_concurrent_bulk_replacement_and_reads(self) -> None:
        cache = MetadataCache()
        errors: list[Exception] = []
        lock = threading.Lock()

        def bulk_replace(offset: int) -> None:
            try:
                cache.set_connections([_conn(offset + i) for i in range(20)])
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        def read_loop() -> None:
            try:
                for i in range(80):
                    _ = cache.get_connection(f"c-{i}")
                    _ = cache.get_connection_by_name(f"conn_{i}")
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(bulk_replace, b * 20) for b in range(3)]
            futures.extend(executor.submit(read_loop) for _ in range(5))
            for f in futures:
                f.result()

        assert errors == []


class TestMetadataCacheConcurrentDataflows:
    def test_concurrent_set_and_get_dataflows(self) -> None:
        cache = MetadataCache()

        def write_then_read(i: int) -> bool:
            df = _df(i)
            cache.set_dataflow(df)
            out = cache.get_dataflow(df.dataflow_id)
            return out is not None and out.dataflow_id == df.dataflow_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            flags = list(executor.map(write_then_read, range(50)))

        assert all(flags)

    def test_set_dataflows_replaces_under_contention(self) -> None:
        cache = MetadataCache()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(cache.set_dataflows, [_df(i), _df(i + 100)])
                for i in range(10)
            ]
            for f in futures:
                f.result()

        # After replacement cycles, only one of the latest snapshots should remain.
        present = [cache.get_dataflow(f"df-{i}") for i in range(120)]
        assert sum(1 for p in present if p is not None) <= 2


class TestMetadataCacheConcurrentSchemaHints:
    def test_concurrent_schema_hint_updates_per_composite_key(self) -> None:
        cache = MetadataCache()

        def set_key(i: int) -> None:
            hints = [SchemaHint(column_name=f"col_{i}", data_type="STRING")]
            cache.set_schema_hints(f"c-{i}", "dbo", "orders", hints)

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(set_key, range(40)))

        # Composite keys must remain isolated across connection ids.
        for i in (0, 7, 21, 39):
            hints = cache.get_schema_hints(f"c-{i}", "dbo", "orders")
            assert hints is not None
            assert hints[0].column_name == f"col_{i}"

    def test_clear_during_mixed_access_is_safe(self) -> None:
        cache = MetadataCache()
        stop = threading.Event()
        errors: list[Exception] = []
        lock = threading.Lock()

        def writer() -> None:
            try:
                i = 0
                while not stop.is_set():
                    conn = _conn(i)
                    cache.set_connection(conn)
                    cache.set_dataflow(_df(i, conn))
                    cache.set_schema_hints(conn.connection_id, None, "orders", [
                        SchemaHint(column_name="id", data_type="INT")
                    ])
                    i += 1
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        def clearer() -> None:
            try:
                for _ in range(60):
                    cache.clear()
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)
            finally:
                stop.set()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(writer) for _ in range(2)]
            futures.append(executor.submit(clearer))
            for f in futures:
                f.result()

        assert errors == []
        # Final state after repeated clear operations should be empty or valid.
        assert cache.get_connection("c-0") is None or isinstance(cache.get_connection("c-0"), Connection)
