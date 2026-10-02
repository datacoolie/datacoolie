"""Activation blocks execution while preserving declarative metadata."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.run_config import ReplayConfig
from datacoolie.core.models.source import Source
from datacoolie.orchestration.execution.activation import inactive_reason
from datacoolie.orchestration.execution.lifecycle import (
    run_dataflow_execution,
    run_dry_run_execution,
)
from datacoolie.orchestration.execution.replay import process_replay
from datacoolie.utils.retry import RetryHandler


def _dataflow(*, flow_active=True, source_active=True, destination_active=True):
    source = Connection(name="source", format="delta", is_active=source_active)
    destination = Connection(name="destination", format="delta", is_active=destination_active)
    return DataFlow(
        dataflow_id="activation-case",
        is_active=flow_active,
        source=Source(connection=source, table="input"),
        destination=Destination(connection=destination, table="output"),
    )


@pytest.mark.parametrize("flow_active", [False, True])
@pytest.mark.parametrize("source_active", [False, True])
@pytest.mark.parametrize("destination_active", [False, True])
def test_activation_decision_covers_all_flags(flow_active, source_active, destination_active):
    flow = _dataflow(
        flow_active=flow_active,
        source_active=source_active,
        destination_active=destination_active,
    )
    reason = inactive_reason(flow)
    assert (reason is None) is (flow_active and source_active and destination_active)
    if not flow_active:
        assert "dataflow is inactive" in reason
    if not source_active:
        assert "source connection 'source' is inactive" in reason
    if not destination_active:
        assert "destination connection 'destination' is inactive" in reason
    assert (flow.is_active, flow.source.connection.is_active, flow.destination.connection.is_active) == (
        flow_active, source_active, destination_active,
    )


@pytest.mark.parametrize("blocked_role", ["flow", "source", "destination"])
def test_inactive_etl_skips_before_preparation_and_retry(blocked_role):
    flow = _dataflow(
        flow_active=blocked_role != "flow",
        source_active=blocked_role != "source",
        destination_active=blocked_role != "destination",
    )
    prepare = Mock(side_effect=AssertionError("preparation must not run"))
    attempt = Mock(side_effect=AssertionError("pipeline must not run"))
    logged = Mock()
    runtime = run_dataflow_execution(
        flow,
        operation_type="etl",
        prepare_execution_dataflow=prepare,
        retry_handler=RetryHandler(retry_count=2, retry_delay=0),
        preflight=Mock(side_effect=AssertionError("preflight must not run")),
        attempt_runner=attempt,
        log_result=logged,
    )
    assert runtime.status == DataFlowStatus.SKIPPED.value
    assert runtime.message
    assert runtime.retry_attempts == 0
    assert runtime.source.status == DataFlowStatus.PENDING.value
    assert runtime.destination.status == DataFlowStatus.PENDING.value
    prepare.assert_not_called()
    attempt.assert_not_called()
    logged.assert_called_once()
    assert logged.call_args.args[0] is not flow


def test_inactive_dry_run_skips_before_validation():
    flow = _dataflow(destination_active=False)
    validate = Mock(side_effect=AssertionError("validation must not run"))
    runtime = run_dry_run_execution(
        flow,
        operation_type="etl",
        validate=validate,
        log_result=Mock(),
    )
    assert runtime.status == DataFlowStatus.SKIPPED.value
    assert "destination connection" in runtime.message
    validate.assert_not_called()


def test_inactive_replay_skips_without_chunks_or_watermark_io():
    flow = _dataflow(source_active=False)
    prepare = Mock(side_effect=AssertionError("preparation must not run"))
    watermark = Mock()
    chunks = Mock()
    logged = Mock()
    runtime = process_replay(
        flow,
        ReplayConfig(start=1, end=3),
        column_name_mode="lower",
        prepare_execution_dataflow=prepare,
        validate_watermark_storage=Mock(),
        watermark_manager=watermark,
        run_single_pipeline=Mock(),
        log_result=logged,
        on_chunk_complete=chunks,
    )
    assert runtime.status == DataFlowStatus.SKIPPED.value
    assert "source connection" in runtime.message
    prepare.assert_not_called()
    watermark.get_watermark.assert_not_called()
    chunks.assert_not_called()
    logged.assert_called_once()
