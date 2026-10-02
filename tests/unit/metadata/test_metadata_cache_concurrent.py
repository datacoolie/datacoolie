"""Concurrent tests for atomic MetadataCache snapshot publication."""

from __future__ import annotations

import concurrent.futures
import threading
from typing import Optional

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.core.models.transform import SchemaHint, Transform
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


def _publish(cache: MetadataCache, i: int) -> None:
    connection = _conn(i)
    cache.publish_snapshot(
        [connection],
        [_df(i, connection)],
        {
            (connection.connection_id, "dbo", "orders"):
                [SchemaHint(column_name=f"col_{i}", data_type="STRING")]
        },
    )


def _assert_coherent_snapshot(cache: MetadataCache) -> None:
    connections = cache.get_all_connections()
    dataflows = cache.get_all_dataflows()
    hints = cache.get_all_schema_hints()
    assert len(connections) == len(dataflows) == len(hints) == 1
    connection = connections[0]
    dataflow = dataflows[0]
    (connection_id, schema_name, table_name), schema_hints = next(iter(hints.items()))
    assert connection.connection_id == connection_id
    assert schema_name == "dbo" and table_name == "orders"
    assert dataflow.dataflow_id == f"df-{connection_id.removeprefix('c-')}"
    assert schema_hints[0].column_name == f"col_{connection_id.removeprefix('c-')}"


class TestMetadataCacheConcurrentSnapshots:
    def test_concurrent_replacements_and_reads_remain_coherent(self) -> None:
        cache = MetadataCache()
        errors: list[Exception] = []
        lock = threading.Lock()

        def publish_many() -> None:
            try:
                for i in range(60):
                    _publish(cache, i)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        def read_many() -> None:
            try:
                for i in range(200):
                    cache.get_connection(f"c-{i}")
                    cache.get_dataflow(f"df-{i}")
                    cache.get_schema_hints(f"c-{i}", "dbo", "orders")
                    cache.get_all_connections()
                    cache.get_all_dataflows()
                    cache.get_all_schema_hints()
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(publish_many) for _ in range(3)]
            futures.extend(executor.submit(read_many) for _ in range(5))
            for future in futures:
                future.result()

        assert errors == []
        _assert_coherent_snapshot(cache)

    def test_clear_during_concurrent_snapshot_publication_is_safe(self) -> None:
        cache = MetadataCache()
        stop = threading.Event()
        errors: list[Exception] = []
        lock = threading.Lock()

        def writer(offset: int) -> None:
            try:
                i = offset
                while not stop.is_set():
                    _publish(cache, i)
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
            futures = [executor.submit(writer, offset) for offset in (0, 1000)]
            futures.append(executor.submit(clearer))
            for future in futures:
                future.result()

        assert errors == []
        connections = cache.get_all_connections()
        if connections:
            _assert_coherent_snapshot(cache)
        else:
            assert cache.get_all_dataflows() == []
            assert cache.get_all_schema_hints() == {}
