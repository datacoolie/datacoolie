"""Tests for datacoolie.orchestration.maintenance."""

from __future__ import annotations

import pytest

from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from datacoolie.core.constants import DataFlowStatus, Format, LoadType
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.run_config import DataCoolieRunConfig
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.destinations.runtime import DestinationRuntimeInfo
from datacoolie.core.models.source import Source
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.orchestration.maintenance import dedupe_by_destination


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
    engine.preferred_target_addressing.return_value = "table"
    engine.target_name_identity.side_effect = lambda *, fmt, table_name: ("test-backend", table_name)
    from datacoolie.engines.contracts.maintenance import PreparedMaintenanceTarget
    def prepare_target(*, fmt, table_name=None, path=None, table_address=None):
        native = engine.target_name_identity(fmt=fmt, table_name=table_name)
        if native is None:
            from datacoolie.core.exceptions import ConfigurationError
            raise ConfigurationError("Unqualified maintenance target")
        return PreparedMaintenanceTarget(fmt, table_name, path, f"{native[0]}:{native[1]}")
    engine.prepare_maintenance_target.side_effect = prepare_target
    engine.maintenance_preparation.side_effect = lambda: engine.prepare_maintenance_target
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

    def test_inactive_first_id_cannot_hide_active_same_destination(self):
        inactive = _maint_df("a-inactive", table="orders")
        inactive.source.connection.is_active = False
        active = _maint_df("z-active", table="orders")
        selected = dedupe_by_destination([active, inactive])
        assert [item.dataflow_id for item in selected] == ["a-inactive", "z-active"]

    def test_maintenance_runs_active_shared_destination_and_skips_inactive(self):
        driver, _, metadata, _ = _make_driver()
        inactive = _maint_df("a-inactive", table="orders")
        inactive.destination.connection.is_active = False
        active = _maint_df("z-active", table="orders")
        metadata.get_maintenance_dataflows.return_value = [active, inactive]
        writer = MagicMock()
        writer.run_maintenance.return_value = DestinationRuntimeInfo(
            status=DataFlowStatus.SUCCEEDED.value,
            operation_type="maintenance",
        )

        with patch.object(driver, "_create_destination_writer", return_value=writer):
            result = driver.run_maintenance()

        assert (result.total, result.succeeded, result.skipped, result.failed) == (2, 1, 1, 0)
        writer.run_maintenance.assert_called_once()

    def test_all_inactive_maintenance_never_creates_writer(self):
        driver, _, _, _ = _make_driver()
        inactive = _maint_df("inactive", table="orders")
        inactive.is_active = False
        with patch.object(driver, "_create_destination_writer") as writer:
            result = driver.run_maintenance(dataflows=[inactive])
        assert (result.total, result.skipped, result.failed) == (1, 1, 0)
        writer.assert_not_called()

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
        # Only a qualified backend may declare this equivalence.
        a = _maint_df("df-a", catalog="CAT", database="DB", table="orders")
        b = _maint_df("df-b", catalog="cat", database="db", table="orders")
        engine = MagicMock()
        engine.preferred_target_addressing.return_value = "table"
        engine.target_name_identity.side_effect = lambda *, fmt, table_name: ("case-insensitive-backend", table_name.lower())
        unique = dedupe_by_destination([a, b], engine=engine)
        assert len(unique) == 1

    def test_case_sensitive_backend_keeps_distinct_names(self):
        engine, _, _, _ = _mock_driver_deps()
        a = _maint_df("a", table="Orders")
        b = _maint_df("b", table="orders")
        assert len(dedupe_by_destination([a, b], engine=engine)) == 2

    def test_unqualified_engine_fails_both_driver_entrypoints_before_writer(self):
        import pytest
        from datacoolie.core.exceptions import ConfigurationError
        driver, engine, metadata, _ = _make_driver()
        engine.target_name_identity.return_value = None
        engine.target_name_identity.side_effect = None
        flow = _maint_df("unknown")
        metadata.get_maintenance_dataflows.return_value = [flow]
        for supplied in (None, [flow]):
            with patch.object(driver, "_create_destination_writer") as writer:
                with pytest.raises(ConfigurationError, match="Unqualified"):
                    driver.run_maintenance(dataflows=supplied)
                writer.assert_not_called()

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
        # Swap destination for a stub whose target contract raises
        # ConfigurationError, so the dedupe must warn-and-skip.
        from datacoolie.core.exceptions import ConfigurationError

        class _NoIdentity:
            @property
            def connection(self):
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

class BrokenMaintenanceDuration(DestinationRuntimeInfo):
    @property
    def duration_seconds(self):
        raise RuntimeError("diagnostic duration failed")


@pytest.mark.parametrize("status, calls, totals", [
    ("succeeded", 2, (2, 2, 0, 0)),
    ("skipped", 2, (2, 0, 2, 0)),
    ("failed", 3, (2, 0, 0, 1)),
])
def test_broken_maintenance_diagnostics_preserve_retries_admission_and_summary(status, calls, totals):
    engine, platform, metadata, watermark = _mock_driver_deps()
    from datacoolie.logging import ExecutionLogger, LogConfig
    execution_logger = ExecutionLogger(LogConfig(), platform)
    execution_logger.close = MagicMock()
    driver = DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata,
        watermark_manager=watermark, execution_logger=execution_logger,
        config=DataCoolieRunConfig(retry_count=2, retry_delay=0, stop_on_error=True, max_workers=1))
    writer = MagicMock()
    writer.run_maintenance.return_value = BrokenMaintenanceDuration(status=status, message="work evidence")
    seen = []
    original_log = driver._log_dataflow_result
    def observe(df, runtime):
        seen.append(runtime)
        original_log(df, runtime)
    with patch.object(driver, "_create_destination_writer", return_value=writer), \
         patch.object(driver, "_log_dataflow_result", side_effect=observe):
        result = driver.run_maintenance(dataflows=[_maint_df("a", table="a"), _maint_df("b", table="b")])
    assert writer.run_maintenance.call_count == calls
    assert (result.total, result.succeeded, result.skipped, result.failed) == totals
    assert len(seen) == (1 if status == "failed" else 2)
    assert all(runtime.status == status and "work evidence" in runtime.message for runtime in seen)
    assert all(runtime.final_attempt.destination.end_time is not None for runtime in seen)
    watermark.save_watermark.assert_not_called()
    summary = execution_logger.job_runtime
    assert (summary.total_succeeded, summary.total_skipped, summary.total_failed) == totals[1:]
    if status == 'failed':
        assert 'a' in summary.message

class ReadonlyMaintenanceRuntime(DestinationRuntimeInfo):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, '_readonly', True)

    def __setattr__(self, name, value):
        if self.__dict__.get('_readonly', False):
            raise AttributeError('readonly diagnostic snapshot')
        super().__setattr__(name, value)


@pytest.mark.parametrize('status, expected_calls', [('succeeded', 1), ('skipped', 1), ('failed', 3)])
def test_readonly_maintenance_snapshot_preserves_driver_outcome_and_call_count(status, expected_calls):
    from copy import deepcopy
    from datacoolie.logging import ExecutionLogger, LogConfig
    engine, platform, metadata, watermark = _mock_driver_deps()
    execution_logger = ExecutionLogger(LogConfig(), platform)
    execution_logger.close = MagicMock()
    driver = DataCoolieDriver(engine=engine, platform=platform, metadata_provider=metadata,
        watermark_manager=watermark, execution_logger=execution_logger,
        config=DataCoolieRunConfig(retry_count=2, retry_delay=0, stop_on_error=True, max_workers=1))
    returned = ReadonlyMaintenanceRuntime(status=status, message='operational evidence', rows_written=7)
    snapshot = deepcopy(returned)
    assert snapshot is not returned
    with pytest.raises(AttributeError, match='readonly'):
        snapshot.status = 'failed'
    writer = MagicMock()
    writer.run_maintenance.return_value = returned
    writer.get_runtime_info.return_value = returned
    seen = []
    original_log = driver._log_dataflow_result
    def observe(df, runtime):
        seen.append(runtime)
        original_log(df, runtime)
    with patch.object(driver, '_create_destination_writer', return_value=writer), \
         patch.object(driver, '_log_dataflow_result', side_effect=observe):
        result = driver.run_maintenance(dataflows=[_maint_df('readonly')])
    assert writer.run_maintenance.call_count == expected_calls
    assert (result.succeeded, result.skipped, result.failed) == (
        int(status == 'succeeded'), int(status == 'skipped'), int(status == 'failed'))
    assert len(seen) == 1
    runtime = seen[0]
    assert runtime.status == status
    assert runtime.message == 'operational evidence'
    assert len(runtime.attempts) == expected_calls
    for attempt in runtime.attempts:
        assert attempt.status == status
        assert type(attempt.destination) is DestinationRuntimeInfo
        assert attempt.destination.status == status
        assert attempt.destination.message == 'operational evidence'
        assert attempt.destination.rows_written == 7
        assert attempt.destination.duration_seconds >= 0
    assert returned.status == status and returned.end_time is None
    watermark.save_watermark.assert_not_called()
    summary = execution_logger.job_runtime
    assert (summary.total_succeeded, summary.total_skipped, summary.total_failed) == (
        result.succeeded, result.skipped, result.failed)
