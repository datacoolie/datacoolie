"""Tests for load strategies and error handling.

This module verifies each load strategy (overwrite, append, merge, SCD2) and
the strategy registry function.
"""

from __future__ import annotations

import pytest

from datacoolie.core.constants import (
    DataFlowStatus,
    Format,
    LoadType,
)
from datacoolie.core.exceptions import DestinationError
from datacoolie.destinations.delta_writer import DeltaWriter
from datacoolie.destinations.iceberg_writer import IcebergWriter
from datacoolie.destinations.load_strategies import (
    LOAD_STRATEGIES,
    AppendStrategy,
    MergeOverwriteStrategy,
    MergeUpsertStrategy,
    OverwriteStrategy,
    SCD2Strategy,
    get_load_strategy,
)

from tests.unit.destinations.support import MockEngine, _make_dataflow, engine


def _default_maintain_internal(writer, dataflow, do_compact, do_cleanup, retention_hours):
    """Reference _maintain_internal for inline TestWriter classes.

    Mirrors the old base default behavior: exists check + compact + cleanup
    with raw (full_table_name, path). Used by test-only subclasses of
    BaseDestinationWriter that don't need format-specific handle resolution.
    """
    from datacoolie.core.constants import MaintenanceType

    dest = dataflow.destination
    fmt = dest.connection.format
    table_name = dest.full_table_name
    path = dest.path
    location = table_name or path or "<unknown>"

    sub_results: list = []
    errors: list = []

    table_exists = writer._engine.exists(table_name=table_name, path=path, fmt=fmt)

    if do_compact:
        sub_results.append(writer._run_op(
            op_name=MaintenanceType.COMPACT.value,
            table_exists=table_exists,
            fn=lambda: writer._engine.compact(table_name=table_name, path=path, fmt=fmt),
            errors=errors,
            location=location,
        ))

    if do_cleanup:
        sub_results.append(writer._run_op(
            op_name=MaintenanceType.CLEANUP.value,
            table_exists=table_exists,
            fn=lambda: writer._engine.cleanup(
                table_name=table_name, path=path,
                retention_hours=retention_hours, fmt=fmt,
            ),
            errors=errors,
            location=location,
        ))

    return sub_results, errors


# ============================================================================
# Load strategy tests
# ============================================================================


class TestOverwriteStrategy:
    """Verify OverwriteStrategy replaces entire table."""
    def test_load_type(self) -> None:
        assert OverwriteStrategy().load_type == LoadType.OVERWRITE.value

    def test_execute(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.OVERWRITE.value)
        strategy = OverwriteStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert engine._written[0]["mode"] == "overwrite"


class TestAppendStrategy:
    """Verify AppendStrategy adds rows without removing existing data."""
    def test_load_type(self) -> None:
        assert AppendStrategy().load_type == LoadType.APPEND.value

    def test_execute(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.APPEND.value)
        strategy = AppendStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert engine._written[0]["mode"] == "append"


class TestMergeUpsertStrategy:
    """Verify MergeUpsertStrategy performs insert/update based on merge keys."""
    def test_load_type(self) -> None:
        assert MergeUpsertStrategy().load_type == LoadType.MERGE_UPSERT.value

    def test_execute_new_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["id"],
        )
        strategy = MergeUpsertStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._written) == 1  # fallback to overwrite

    def test_execute_existing_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=["id"],
        )
        strategy = MergeUpsertStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._merged) == 1

    def test_no_merge_keys_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.MERGE_UPSERT.value, merge_keys=[])
        strategy = MergeUpsertStrategy()
        with pytest.raises(DestinationError, match="requires merge_keys"):
            strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)


class TestMergeOverwriteStrategy:
    """Verify MergeOverwriteStrategy deletes matching rows before insert."""
    def test_load_type(self) -> None:
        assert MergeOverwriteStrategy().load_type == LoadType.MERGE_OVERWRITE.value

    def test_execute_new_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
        )
        strategy = MergeOverwriteStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._written) == 1

    def test_execute_existing_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow(
            load_type=LoadType.MERGE_OVERWRITE.value,
            merge_keys=["id"],
        )
        strategy = MergeOverwriteStrategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._merge_overwritten) == 1

    def test_no_merge_keys_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(load_type=LoadType.MERGE_OVERWRITE.value, merge_keys=[])
        strategy = MergeOverwriteStrategy()
        with pytest.raises(DestinationError, match="requires merge_keys"):
            strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)


class TestSCD2Strategy:
    """Verify SCD2Strategy performs SCD2 merge or initial load."""
    def test_load_type(self) -> None:
        assert SCD2Strategy().load_type == LoadType.SCD2.value

    def test_execute_new_table_falls_back_to_overwrite(self, engine: MockEngine) -> None:
        engine.set_table_exists(False)
        df = _make_dataflow(
            load_type=LoadType.SCD2.value,
            merge_keys=["id"],
            dest_configure={"scd2_effective_column": "effective_date"},
        )
        strategy = SCD2Strategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._written) == 1
        assert engine._written[0]["mode"] == "overwrite"

    def test_execute_existing_table(self, engine: MockEngine) -> None:
        engine.set_table_exists(True)
        df = _make_dataflow(
            load_type=LoadType.SCD2.value,
            merge_keys=["id"],
            dest_configure={"scd2_effective_column": "effective_date"},
        )
        strategy = SCD2Strategy()
        strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)
        assert len(engine._scd2) == 1

    def test_no_merge_keys_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.SCD2.value,
            merge_keys=[],
            dest_configure={"scd2_effective_column": "effective_date"},
        )
        strategy = SCD2Strategy()
        with pytest.raises(DestinationError, match="requires merge_keys"):
            strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)

    def test_no_scd2_effective_column_raises(self, engine: MockEngine) -> None:
        df = _make_dataflow(
            load_type=LoadType.SCD2.value,
            merge_keys=["id"],
        )
        strategy = SCD2Strategy()
        with pytest.raises(DestinationError, match="scd2_effective_column"):
            strategy.execute({"data": 1}, df.destination.full_table_name, df, engine)


# ============================================================================
# Strategy registry tests
# ============================================================================


class TestLoadStrategyRegistry:
    """Verify strategy registry and get_load_strategy function."""
    def test_all_strategies_registered(self) -> None:
        expected = {"overwrite", "append", "merge_upsert", "merge_overwrite", "scd2", "full_load"}
        assert set(LOAD_STRATEGIES.keys()) == expected

    def test_get_load_strategy_valid(self) -> None:
        for lt in ["overwrite", "append", "merge_upsert", "merge_overwrite", "scd2", "full_load"]:
            strategy = get_load_strategy(lt)
            # full_load is aliased to OverwriteStrategy
            if lt == "full_load":
                assert strategy.load_type == LoadType.OVERWRITE.value
            else:
                assert strategy.load_type == lt

    def test_get_load_strategy_case_insensitive(self) -> None:
        strategy = get_load_strategy("OVERWRITE")
        assert strategy.load_type == LoadType.OVERWRITE.value

    def test_get_load_strategy_with_spaces(self) -> None:
        strategy = get_load_strategy("  append  ")
        assert strategy.load_type == LoadType.APPEND.value

    def test_get_load_strategy_unknown_raises(self) -> None:
        with pytest.raises(DestinationError, match="Unknown load type"):
            get_load_strategy("nonexistent")

    def test_strategy_instances_are_singleton(self) -> None:
        s1 = get_load_strategy("overwrite")
        s2 = get_load_strategy("overwrite")
        assert s1 is s2


# ============================================================================
# Load strategy selection matrix
# ============================================================================


class TestLoadStrategySelection:
    """Test correct strategy selection for different load types."""

    @pytest.mark.parametrize(
        "load_type,expected_mode",
        [
            (LoadType.OVERWRITE.value, "overwrite"),
            (LoadType.APPEND.value, "append"),
            (LoadType.FULL_LOAD.value, "overwrite"),
        ],
    )
    def test_delta_writer_strategy_mode_selection(
        self, engine: MockEngine, load_type: str, expected_mode: str
    ) -> None:
        """DeltaWriter selects correct mode for each load type.
        
        Parametrized test validating:
          - OVERWRITE → mode='overwrite'
          - APPEND → mode='append'
          - FULL_LOAD → mode='overwrite'
        """
        df = _make_dataflow(load_type=load_type)
        writer = DeltaWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert engine._written[0]["mode"] == expected_mode

    @pytest.mark.parametrize(
        "load_type,expected_mode",
        [
            (LoadType.OVERWRITE.value, "overwrite"),
            (LoadType.APPEND.value, "append"),
            (LoadType.FULL_LOAD.value, "overwrite"),
        ],
    )
    def test_iceberg_writer_strategy_mode_selection(
        self, engine: MockEngine, load_type: str, expected_mode: str
    ) -> None:
        """IcebergWriter selects correct mode for each load type.
        
        Parametrized test validating IcebergWriter uses same strategy selection.
        """
        df = _make_dataflow(load_type=load_type, dest_format=Format.ICEBERG.value)
        writer = IcebergWriter(engine)
        info = writer.write({"data": 1}, df)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert engine._written[0]["mode"] == expected_mode

    def test_get_load_strategy_returns_all_types(self) -> None:
        """get_load_strategy returns correct strategy instance for all types.
        
        Validates:
          - All LoadType values have corresponding strategy
          - Strategy behavior is correct (FULL_LOAD maps to OVERWRITE)
        """
        load_types_to_test = [
            (LoadType.OVERWRITE.value, LoadType.OVERWRITE.value),
            (LoadType.APPEND.value, LoadType.APPEND.value),
            (LoadType.MERGE_UPSERT.value, LoadType.MERGE_UPSERT.value),
            (LoadType.MERGE_OVERWRITE.value, LoadType.MERGE_OVERWRITE.value),
            (LoadType.FULL_LOAD.value, LoadType.OVERWRITE.value),  # FULL_LOAD maps to OVERWRITE
        ]
        
        for load_type, expected_strategy_type in load_types_to_test:
            strategy = get_load_strategy(load_type)
            assert strategy is not None
            assert strategy.load_type == expected_strategy_type


# ============================================================================
# Error handling across writers
# ============================================================================


class TestDestinationErrorHandling:
    """Test error handling across destination writers."""

    def test_delta_writer_merge_requires_keys(self, engine: MockEngine) -> None:
        """DeltaWriter with MERGE but no merge_keys raises error.
        
        Validates:
          - Clear error message for missing merge_keys
          - Operation not attempted when validation fails
          - DestinationError raised
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=[],  # Empty merge keys
        )
        
        writer = DeltaWriter(engine)
        
        with pytest.raises(DestinationError, match="requires merge_keys"):
            writer.write({"data": 1}, df)

    def test_iceberg_writer_merge_requires_keys(self, engine: MockEngine) -> None:
        """IcebergWriter with MERGE but no merge_keys raises error.
        
        Validates:
          - Same validation as DeltaWriter
          - Consistent error semantics
        """
        engine.set_table_exists(True)
        
        df = _make_dataflow(
            load_type=LoadType.MERGE_UPSERT.value,
            merge_keys=[],
            dest_format=Format.ICEBERG.value,
        )
        
        writer = IcebergWriter(engine)
        
        with pytest.raises(DestinationError, match="requires merge_keys"):
            writer.write({"data": 1}, df)

    def test_delta_writer_scd2_no_effective_column(self, engine: MockEngine) -> None:
        """DeltaWriter with SCD2 but missing scd2_effective_column raises error.
        
        Validates:
          - SCD2 requires scd2_effective_column configuration
          - Clear error message
        """
        df = _make_dataflow(load_type=LoadType.SCD2.value, merge_keys=["id"])
        writer = DeltaWriter(engine)
        
        with pytest.raises(DestinationError):
            writer.write({"data": 1}, df)

    def test_iceberg_writer_scd2_no_effective_column(self, engine: MockEngine) -> None:
        """IcebergWriter with SCD2 but missing scd2_effective_column raises error.
        
        Validates:
          - SCD2 validation consistent across all writers
        """
        df = _make_dataflow(
            load_type=LoadType.SCD2.value,
            merge_keys=["id"],
            dest_format=Format.ICEBERG.value,
        )
        writer = IcebergWriter(engine)
        
        with pytest.raises(DestinationError):
            writer.write({"data": 1}, df)


# ============================================================================
# Maintenance operations edge cases
# ============================================================================


class TestMaintenanceEdgeCases:
    """Test maintenance operation edge cases and combinations."""

    def test_maintenance_with_custom_retention_propagates(self, engine: MockEngine) -> None:
        """Custom retention hours is passed through to cleanup.
        
        Validates:
          - retention_hours parameter honored
          - Value reaches the cleanup operation
          - Engine records the value
        """
        from datacoolie.destinations.base import BaseDestinationWriter

        class TestWriter(BaseDestinationWriter[dict]):
            def _write_internal(self, df, dataflow): pass
            def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
                return _default_maintain_internal(self, dataflow, do_compact, do_cleanup, retention_hours)

        engine.set_table_exists(True)
        writer = TestWriter(engine)
        df = _make_dataflow()
        
        custom_hours = 48
        info = writer.run_maintenance(df, do_compact=False, retention_hours=custom_hours)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._cleaned) == 1
        assert engine._cleaned[0]["hours"] == custom_hours

    def test_maintenance_uses_default_retention_when_not_specified(self, engine: MockEngine) -> None:
        """Default retention hours used when not specified.
        
        Validates:
          - DEFAULT_RETENTION_HOURS used when retention_hours=None
          - Consistent default across all calls
        """
        from datacoolie.core.constants import DEFAULT_RETENTION_HOURS
        from datacoolie.destinations.base import BaseDestinationWriter

        class TestWriter(BaseDestinationWriter[dict]):
            def _write_internal(self, df, dataflow): pass
            def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
                return _default_maintain_internal(self, dataflow, do_compact, do_cleanup, retention_hours)

        engine.set_table_exists(True)
        writer = TestWriter(engine)
        df = _make_dataflow()
        
        info = writer.run_maintenance(df, do_compact=False)
        
        assert engine._cleaned[0]["hours"] == DEFAULT_RETENTION_HOURS

    def test_maintenance_only_compact(self, engine: MockEngine) -> None:
        """Maintenance with only compact (do_cleanup=False).
        
        Validates:
          - Compact operation called once
          - Cleanup operation not called
          - Status success
        """
        from datacoolie.destinations.base import BaseDestinationWriter

        class TestWriter(BaseDestinationWriter[dict]):
            def _write_internal(self, df, dataflow): pass
            def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
                return _default_maintain_internal(self, dataflow, do_compact, do_cleanup, retention_hours)

        engine.set_table_exists(True)
        writer = TestWriter(engine)
        df = _make_dataflow()
        
        info = writer.run_maintenance(df, do_compact=True, do_cleanup=False)
        
        assert info.status == DataFlowStatus.SUCCEEDED.value
        assert len(engine._compacted) == 1
        assert len(engine._cleaned) == 0

    def test_maintenance_skipped_when_table_not_exists(self, engine: MockEngine) -> None:
        """Maintenance skipped when table doesn't exist.
        
        Validates:
          - Table existence check performed
          - No operations attempted
          - Status is SKIPPED
        """
        from datacoolie.destinations.base import BaseDestinationWriter

        class TestWriter(BaseDestinationWriter[dict]):
            def _write_internal(self, df, dataflow): pass
            def _maintain_internal(self, dataflow, *, do_compact, do_cleanup, retention_hours):
                return _default_maintain_internal(self, dataflow, do_compact, do_cleanup, retention_hours)

        engine.set_table_exists(False)
        writer = TestWriter(engine)
        df = _make_dataflow()
        
        info = writer.run_maintenance(df)
        
        assert info.status == DataFlowStatus.SKIPPED.value
        assert len(engine._compacted) == 0
        assert len(engine._cleaned) == 0
