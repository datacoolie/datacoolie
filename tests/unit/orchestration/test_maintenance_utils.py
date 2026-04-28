"""Tests for datacoolie.orchestration.maintenance_utils."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from datacoolie.core.constants import DataFlowStatus, Format, LoadType
from datacoolie.core.models import (
    Connection,
    DataCoolieRunConfig,
    DataFlow,
    Destination,
    DestinationRuntimeInfo,
    Source,
)
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.orchestration.maintenance_utils import dedupe_by_destination


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _maint_df(
    dataflow_id: str,
    *,
    catalog: str | None = "c",
    database: str | None = "d",
    schema_name: str | None = "s",
    table: str = "t",
    base_path: str | None = None,
) -> DataFlow:
    """Build a maintenance-eligible DataFlow aimed at a given destination."""
    cfg: Dict[str, Any] = {}
    if base_path is not None:
        cfg["base_path"] = base_path
    if catalog is not None:
        cfg["catalog"] = catalog
    if database is not None:
        cfg["database"] = database
    conn = Connection(
        name=f"conn-{dataflow_id}",
        connection_type="lakehouse",
        format=Format.DELTA.value,
        configure=cfg,
    )
    dest_kwargs: Dict[str, Any] = {
        "connection": conn,
        "table": table,
        "load_type": LoadType.APPEND.value,
    }
    if schema_name is not None:
        dest_kwargs["schema_name"] = schema_name
    return DataFlow(
        dataflow_id=dataflow_id,
        source=Source(connection=conn, table="src"),
        destination=Destination(**dest_kwargs),
    )


def _mock_driver_deps() -> tuple:
    engine = MagicMock()
    engine.platform = None
    engine.set_platform.side_effect = lambda p: setattr(engine, "platform", p)
    engine.count_rows.return_value = 5
    engine.table_exists.return_value = True
    engine.get_maintenance_metrics.return_value = {}
    platform = MagicMock()
    metadata = MagicMock()
    metadata.get_dataflows.return_value = []
    metadata.get_watermark.return_value = {}
    watermark = MagicMock()
    watermark.get_watermark.return_value = {}
    return engine, platform, metadata, watermark


def _make_driver(dataflows: List[DataFlow] | None = None) -> tuple:
    engine, platform, metadata, watermark = _mock_driver_deps()
    metadata.get_dataflows.return_value = dataflows or []
    driver = DataCoolieDriver(
        engine=engine,
        platform=platform,
        metadata_provider=metadata,
        watermark_manager=watermark,
        config=DataCoolieRunConfig(),
    )
    return driver, engine, metadata, watermark


# ---------------------------------------------------------------------------
# dedupe_by_destination
# ---------------------------------------------------------------------------


class TestMaintenanceDedup:
    """Dedupe fan-in maintenance by destination identity."""

    def test_same_qualified_table_deduped(self):
        a = _maint_df("df-a", table="orders")
        b = _maint_df("df-b", table="orders")
        unique = dedupe_by_destination([a, b])
        assert len(unique) == 1

    def test_distinct_destinations_not_deduped(self):
        a = _maint_df("df-a", table="orders")
        b = _maint_df("df-b", table="customers")
        unique = dedupe_by_destination([a, b])
        assert {df.dataflow_id for df in unique} == {"df-a", "df-b"}

    def test_path_fallback_when_no_catalog_or_database(self):
        a = _maint_df(
            "df-a", catalog=None, database=None,
            base_path="/lake/", schema_name=None, table="orders",
        )
        b = _maint_df(
            "df-b", catalog=None, database=None,
            base_path="/lake/", schema_name=None, table="orders",
        )
        unique = dedupe_by_destination([a, b])
        assert len(unique) == 1

    def test_path_trailing_slash_normalized(self):
        a = _maint_df(
            "df-a", catalog=None, database=None,
            base_path="/lake", schema_name=None, table="orders",
        )
        b = _maint_df(
            "df-b", catalog=None, database=None,
            base_path="/lake/", schema_name=None, table="orders",
        )
        unique = dedupe_by_destination([a, b])
        assert len(unique) == 1

    def test_case_insensitive_equivalence(self):
        # Destination normalizes table to lowercase on input; force case
        # variance through the connection catalog/database instead.
        a = _maint_df("df-a", catalog="CAT", database="DB", table="orders")
        b = _maint_df("df-b", catalog="cat", database="db", table="orders")
        unique = dedupe_by_destination([a, b])
        assert len(unique) == 1

    def test_deterministic_first_wins_across_orderings(self):
        dfs = [_maint_df(f"df-{c}") for c in ("c", "a", "b")]
        forward = dedupe_by_destination(list(dfs))
        reverse = dedupe_by_destination(list(reversed(dfs)))
        # Sort-by-dataflow_id means "df-a" wins regardless of input order.
        assert forward[0].dataflow_id == "df-a"
        assert reverse[0].dataflow_id == "df-a"

    def test_skips_destination_with_no_identity(self):
        """Dataflow whose destination raises on key lookup is skipped, not fatal."""
        good = _maint_df("df-good")
        bad = _maint_df("df-bad")
        # Swap destination for a stub whose maintenance_key raises
        # ConfigurationError, so the dedupe must warn-and-skip.
        from datacoolie.core.exceptions import ConfigurationError

        class _NoIdentity:
            @property
            def maintenance_key(self):
                raise ConfigurationError("no identity")

        bad.destination = _NoIdentity()  # type: ignore[assignment]
        unique = dedupe_by_destination([good, bad])
        assert [df.dataflow_id for df in unique] == ["df-good"]

    def test_run_maintenance_invokes_writer_once_per_destination(self):
        """End-to-end: 3 DFs → 2 destinations → writer called 2 times."""
        d, _, md, _ = _make_driver()
        md.get_maintenance_dataflows.return_value = [
            _maint_df("df-1", table="orders"),
            _maint_df("df-2", table="orders"),
            _maint_df("df-3", table="customers"),
        ]

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d.run_maintenance()

        assert result.total == 2
        assert mock_writer.run_maintenance.call_count == 2

    def test_run_maintenance_dedupes_caller_supplied_dataflows(self):
        d, _, md, _ = _make_driver()
        dfs = [
            _maint_df("df-1", table="orders"),
            _maint_df("df-2", table="orders"),
        ]

        mock_writer = MagicMock()
        mock_writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(d, "_create_destination_writer", return_value=mock_writer):
            result = d.run_maintenance(dataflows=dfs)

        assert result.total == 1
        mock_writer.run_maintenance.assert_called_once()
        md.get_maintenance_dataflows.assert_not_called()
