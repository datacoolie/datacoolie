"""Focused tests for Iceberg catalog operations."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datacoolie.engines._polars.iceberg import operations


class _FixedDateTime(datetime):
    current = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        value = cls.current
        return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)


def test_table_exists_normalizes_catalog_prefix() -> None:
    catalog = MagicMock()
    catalog.table_exists.return_value = True
    assert operations.table_exists("catalog.namespace.table", catalog=catalog) is True
    catalog.table_exists.assert_called_once_with("namespace.table")


def test_table_exists_propagates_catalog_error() -> None:
    catalog = MagicMock()
    catalog.table_exists.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        operations.table_exists("namespace.table", catalog=catalog)


def test_write_table_creates_a_new_table_when_catalog_reports_absent() -> None:
    catalog = MagicMock()
    catalog.table_exists.return_value = False
    created = MagicMock()
    created.properties = {"schema.name-mapping.default": "{}"}
    catalog.create_table.return_value = created

    frame = pl.DataFrame({"id": [1]}).lazy()
    with patch.object(
        operations.iceberg_schema,
        "align_arrow_to_table",
        side_effect=lambda arrow, _table: arrow,
    ):
        operations.write_table(
            frame,
            "catalog.default.new_table",
            "overwrite",
            None,
            catalog=catalog,
        )

    catalog.create_namespace_if_not_exists.assert_called_once_with("default")
    catalog.create_table.assert_called_once()
    created.overwrite.assert_called_once()


def test_expire_snapshots_preserves_current_and_cutoff_boundary() -> None:
    fixed = _FixedDateTime.current
    cutoff_ms = int((fixed.timestamp() - 2 * 3600) * 1000)
    table = MagicMock()
    table.metadata.current_snapshot_id = 4
    table.snapshots.return_value = [
        MagicMock(snapshot_id=1, timestamp_ms=cutoff_ms - 1),
        MagicMock(snapshot_id=2, timestamp_ms=cutoff_ms),
        MagicMock(snapshot_id=3, timestamp_ms=cutoff_ms + 1),
        MagicMock(snapshot_id=4, timestamp_ms=cutoff_ms - 10_000),
    ]

    with patch.object(operations, "datetime", _FixedDateTime):
        operations.expire_snapshots(table, retention_hours=2)

    expire = table.maintenance.expire_snapshots
    expire.assert_called_once_with()
    expire.return_value.older_than.assert_called_once()
    expire.return_value.older_than.return_value.commit.assert_called_once()


def test_expire_snapshots_skips_commit_when_no_non_current_snapshot_is_old() -> None:
    fixed = _FixedDateTime.current
    cutoff_ms = int((fixed.timestamp() - 2 * 3600) * 1000)
    table = MagicMock()
    table.metadata.current_snapshot_id = None
    table.snapshots.return_value = [
        MagicMock(snapshot_id=1, timestamp_ms=cutoff_ms),
    ]

    with patch.object(operations, "datetime", _FixedDateTime):
        operations.expire_snapshots(table, retention_hours=2)

    table.maintenance.expire_snapshots.assert_not_called()


def test_expiration_failure_propagates_and_protected_snapshot_avoids_empty_commit():
    table = MagicMock()
    table.metadata.current_snapshot_id = 2
    table.snapshots.return_value = [MagicMock(snapshot_id=1, timestamp_ms=0)]
    table.refs.return_value = {"tag": MagicMock(snapshot_id=1)}
    operations.expire_snapshots(table, 168)
    table.maintenance.expire_snapshots.assert_not_called()
    table.refs.return_value = {}
    table.maintenance.expire_snapshots.return_value.older_than.return_value.commit.side_effect = RuntimeError("native commit failed")
    with pytest.raises(RuntimeError, match="native commit failed"):
        operations.expire_snapshots(table, 168)


@pytest.mark.runtime_qualification
def test_expire_snapshots_sql_catalog_retains_current_snapshot(tmp_path) -> None:
    pyiceberg = pytest.importorskip("pyiceberg")
    from packaging.version import Version

    if Version(pyiceberg.__version__) < Version("0.12.0"):
        pytest.skip("PyIceberg 0.12.0+ is required for the declared local qualification")
    pa = pytest.importorskip("pyarrow")
    from pyiceberg.catalog import load_catalog

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog_path = tmp_path / "catalog.sqlite"
    catalog = load_catalog(
        "retention-qualification",
        type="sql",
        uri=f"sqlite:///{catalog_path.as_posix()}",
        # PyArrow's Windows URI normalization turns ``file:///C:/...`` into
        # ``/C:/...``.  SqlCatalog appends table paths to this value, so pass
        # the native path form for local-file qualification.
        warehouse=str(warehouse),
    )
    try:
        catalog.create_namespace_if_not_exists("retention")
        table = catalog.create_table(
            "retention.events",
            schema=pa.schema([pa.field("id", pa.int64())]),
        )
        table.append(pa.table({"id": [1]}))
        table.append(pa.table({"id": [2]}))
        current_id = table.metadata.current_snapshot_id
        snapshot_ids = {snapshot.snapshot_id for snapshot in table.snapshots()}
        assert len(snapshot_ids) >= 2

        operations.expire_snapshots(table, retention_hours=0)

        reloaded = catalog.load_table("retention.events")
        remaining_ids = {snapshot.snapshot_id for snapshot in reloaded.snapshots()}
        assert current_id in remaining_ids
        assert remaining_ids == {current_id}
        rows = reloaded.scan().to_arrow().to_pylist()
        assert sorted(rows, key=lambda row: row["id"]) == [{"id": 1}, {"id": 2}]
    finally:
        catalog.engine.dispose()
