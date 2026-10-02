"""Focused tests for the Driver-independent execution lifecycle."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from datacoolie.core.constants import DataFlowStatus
from datacoolie.core.exceptions import PipelineError
from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.runtime import DataFlowRuntimeInfo, PipelineAttemptResult
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.orchestration.execution.lifecycle import (
    run_dataflow_execution,
    run_dry_run_execution,
    run_prepared_execution,
)
from datacoolie.orchestration.preparation import PreparedDataFlow
from datacoolie.logging.runtime.context import (
    dataflow_context,
    get_dataflow_id,
    get_dataflow_run_id,
)
from datacoolie.utils.time import utc_now
from datacoolie.utils.retry import RetryHandler


def _prepared() -> PreparedDataFlow:
    connection = Connection(name="c", format="delta", configure={"base_path": "/data"})
    dataflow = DataFlow(
        dataflow_id="df-1",
        source=Source(connection=connection, query="sql/orders.sql"),
        destination=Destination(connection=connection, table="orders"),
    )
    return PreparedDataFlow(metadata=dataflow.model_copy(deep=True), execution=dataflow)


def test_exhausted_retry_count_and_metadata_snapshot_are_isolated() -> None:
    prepared = _prepared()
    attempts: list[DataFlow] = []
    logged: list[tuple[DataFlow, DataFlowRuntimeInfo]] = []

    def attempt(dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        attempts.append(dataflow)
        dataflow.source.query = "SELECT 1"
        raise PipelineError(
            "write failed",
            partial_result=PipelineAttemptResult(status=DataFlowStatus.FAILED.value),
        )

    runtime = run_prepared_execution(
        prepared,
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=2, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=attempt,
        log_result=lambda metadata, result: logged.append((metadata, result)),
    )

    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.retry_attempts == 2
    assert len(attempts) == 3
    assert prepared.execution.source.query == "sql/orders.sql"
    assert prepared.metadata.source.query == "sql/orders.sql"
    assert logged[0][0].source.query == "sql/orders.sql"


def test_preflight_failure_does_not_invoke_attempt_or_retry() -> None:
    prepared = _prepared()
    invoked = False
    logged: list[DataFlowRuntimeInfo] = []

    def attempt(_dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        nonlocal invoked
        invoked = True
        return PipelineAttemptResult(status=DataFlowStatus.SUCCEEDED.value)

    def fail_preflight(_dataflow: DataFlow) -> None:
        raise ValueError("invalid watermark")

    runtime = run_prepared_execution(
        prepared,
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=3, retry_delay=0),
        preflight=fail_preflight,
        attempt_runner=attempt,
        log_result=lambda _metadata, result: logged.append(result),
    )

    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.retry_attempts == 0
    assert invoked is False
    assert logged and logged[0].message == "invalid watermark"


def test_returned_failure_message_and_empty_exception_are_terminal_explanations() -> None:
    prepared = _prepared()

    runtime = run_prepared_execution(
        prepared,
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=lambda _dataflow, **_kwargs: PipelineAttemptResult(
            status=DataFlowStatus.FAILED.value,
            message="attempt returned a failure",
        ),
        log_result=lambda _metadata, _runtime: None,
    )
    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.message == "attempt returned a failure"

    def empty_preflight(_dataflow: DataFlow) -> None:
        raise ValueError()

    failed = run_prepared_execution(
        prepared,
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=empty_preflight,
        attempt_runner=lambda _dataflow, **_kwargs: PipelineAttemptResult(
            status=DataFlowStatus.SUCCEEDED.value
        ),
        log_result=lambda _metadata, _runtime: None,
    )
    assert failed.status == DataFlowStatus.FAILED.value
    assert failed.message == "ValueError"


def test_result_log_failure_does_not_change_terminal_status() -> None:
    prepared = _prepared()

    def attempt(_dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        return PipelineAttemptResult(status=DataFlowStatus.SUCCEEDED.value)

    def broken_log(_metadata: DataFlow, _runtime: DataFlowRuntimeInfo) -> None:
        raise OSError("log sink unavailable")

    runtime = run_prepared_execution(
        prepared,
        operation_type="etl",
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=attempt,
        log_result=broken_log,
    )

    assert runtime.status == DataFlowStatus.SUCCEEDED.value


def test_dataflow_runtime_includes_preparation_and_observer_isolation() -> None:
    prepared = _prepared()
    preparation_started = []
    observed: list[tuple[DataFlow, DataFlowRuntimeInfo]] = []

    def prepare(dataflow: DataFlow, **_kwargs: object) -> PreparedDataFlow:
        preparation_started.append(utc_now())
        return PreparedDataFlow(
            metadata=dataflow.model_copy(deep=True),
            execution=prepared.execution.model_copy(deep=True),
        )

    def attempt(_dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        return PipelineAttemptResult(status=DataFlowStatus.SUCCEEDED.value)

    def mutate_observer(metadata: DataFlow, runtime: DataFlowRuntimeInfo) -> None:
        observed.append((metadata, runtime))
        metadata.source.query = "observer mutation"
        runtime.status = DataFlowStatus.FAILED.value

    runtime = run_dataflow_execution(
        prepared.metadata,
        operation_type="etl",
        prepare_execution_dataflow=prepare,
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=attempt,
        log_result=mutate_observer,
    )

    assert preparation_started
    assert runtime.start_time <= preparation_started[0]
    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert runtime.message is None
    assert observed[0][0].source.query == "observer mutation"
    assert prepared.metadata.source.query == "sql/orders.sql"


def test_dataflow_runtime_covers_preparation_failure_without_attempt() -> None:
    dataflow = _prepared().metadata
    preparation_started = []
    attempts = 0
    logged: list[DataFlowRuntimeInfo] = []

    def fail_prepare(_dataflow: DataFlow, **_kwargs: object) -> PreparedDataFlow:
        preparation_started.append(utc_now())
        raise ValueError("query file missing")

    def attempt(_dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        nonlocal attempts
        attempts += 1
        return PipelineAttemptResult(status=DataFlowStatus.SUCCEEDED.value)

    runtime = run_dataflow_execution(
        dataflow,
        operation_type="etl",
        prepare_execution_dataflow=fail_prepare,
        retry_handler=RetryHandler(retry_count=2, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=attempt,
        log_result=lambda _metadata, result: logged.append(result),
    )

    assert preparation_started
    assert runtime.start_time <= preparation_started[0]
    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.message == "query file missing"
    assert runtime.retry_attempts == 0
    assert attempts == 0
    assert logged and logged[0].status == DataFlowStatus.FAILED.value


def test_execution_emits_one_terminal_pair_and_restores_parent_context(caplog) -> None:
    """Lifecycle anchors cover preparation/result logging without leaking scope."""
    prepared = _prepared()
    caplog.set_level(logging.INFO, logger="datacoolie")

    def attempt(_dataflow: DataFlow, **_kwargs: object) -> PipelineAttemptResult:
        return PipelineAttemptResult(status=DataFlowStatus.SUCCEEDED.value)

    with dataflow_context("parent", "parent-run"):
        runtime = run_prepared_execution(
            prepared,
            operation_type="etl",
            retry_handler=RetryHandler(retry_count=0, retry_delay=0),
            preflight=lambda _dataflow: None,
            attempt_runner=attempt,
            log_result=lambda _metadata, _runtime: None,
        )
        assert get_dataflow_id() == "parent"
        assert get_dataflow_run_id() == "parent-run"

    events = [
        record.event_name
        for record in caplog.records
        if getattr(record, "event_name", None) in {
            "dataflow.started",
            "dataflow.finished",
        }
    ]
    assert events == ["dataflow.started", "dataflow.finished"]
    assert runtime.status == DataFlowStatus.SUCCEEDED.value
    assert get_dataflow_id() == ""
    assert get_dataflow_run_id() == ""


def test_preparation_failure_emits_only_one_terminal_traceback(caplog) -> None:
    """Preparation errors are represented at the lifecycle boundary once."""
    dataflow = _prepared().metadata
    caplog.set_level(logging.DEBUG, logger="datacoolie")

    def fail_prepare(_dataflow: DataFlow, **_kwargs: object) -> PreparedDataFlow:
        raise ValueError("query file missing")

    runtime = run_dataflow_execution(
        dataflow,
        operation_type="etl",
        prepare_execution_dataflow=fail_prepare,
        retry_handler=RetryHandler(retry_count=0, retry_delay=0),
        preflight=lambda _dataflow: None,
        attempt_runner=lambda _dataflow, **_kwargs: PipelineAttemptResult(
            status=DataFlowStatus.SUCCEEDED.value
        ),
        log_result=lambda _metadata, _runtime: None,
    )

    finished = [
        record
        for record in caplog.records
        if getattr(record, "event_name", None) == "dataflow.finished"
    ]
    assert runtime.status == DataFlowStatus.FAILED.value
    assert len(finished) == 1
    assert finished[0].levelno == logging.ERROR
    assert finished[0].exc_info is not None


def test_dry_run_execution_owns_context_and_single_observation() -> None:
    dataflow = _prepared().metadata
    validated: list[DataFlow] = []
    logged: list[tuple[DataFlow, DataFlowRuntimeInfo]] = []

    with dataflow_context("parent", "parent-run"):
        runtime = run_dry_run_execution(
            dataflow,
            operation_type="etl",
            validate=lambda execution: validated.append(execution),
            log_result=lambda metadata, result: logged.append((metadata, result)),
        )
        assert get_dataflow_id() == "parent"
        assert get_dataflow_run_id() == "parent-run"

    assert runtime.status == DataFlowStatus.SKIPPED.value
    assert runtime.message == "Dry-run validation passed; pipeline execution was not performed"
    assert runtime.end_time is not None
    assert len(validated) == 1
    assert validated[0] is not dataflow
    assert len(logged) == 1
    assert logged[0][0].source.query == "sql/orders.sql"
    assert logged[0][1].status == DataFlowStatus.SKIPPED.value
    assert logged[0][1].message == runtime.message
    assert get_dataflow_id() == ""
    assert get_dataflow_run_id() == ""


def test_dry_run_execution_converts_validation_error_to_failed_observation() -> None:
    dataflow = _prepared().metadata
    logged: list[DataFlowRuntimeInfo] = []

    def fail(_execution: DataFlow) -> None:
        raise ValueError("invalid SQL root")

    runtime = run_dry_run_execution(
        dataflow,
        operation_type="replay",
        validate=fail,
        log_result=lambda _metadata, result: logged.append(result),
    )

    assert runtime.status == DataFlowStatus.FAILED.value
    assert runtime.message == "invalid SQL root"
    assert logged and logged[0].status == DataFlowStatus.FAILED.value


def test_concurrent_execution_contexts_do_not_cross_contaminate() -> None:
    observed: list[tuple[str, str, str]] = []

    def execute(dataflow_id: str) -> DataFlowRuntimeInfo:
        prepared = _prepared()
        prepared.metadata.dataflow_id = dataflow_id
        prepared.execution.dataflow_id = dataflow_id

        def observe(_metadata: DataFlow, runtime: DataFlowRuntimeInfo) -> None:
            observed.append(
                (get_dataflow_id(), get_dataflow_run_id(), runtime.dataflow_id)
            )

        return run_prepared_execution(
            prepared,
            operation_type="etl",
            retry_handler=RetryHandler(retry_count=0, retry_delay=0),
            preflight=lambda _dataflow: None,
            attempt_runner=lambda _dataflow, **_kwargs: PipelineAttemptResult(
                status=DataFlowStatus.SUCCEEDED.value
            ),
            log_result=observe,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        runtimes = list(pool.map(execute, ("df-a", "df-b")))

    assert {item[0] for item in observed} == {"df-a", "df-b"}
    assert all(dataflow_id == runtime_id for dataflow_id, _run_id, runtime_id in observed)
    assert len({run_id for _dataflow_id, run_id, _runtime_id in observed}) == 2
    assert all(runtime.status == DataFlowStatus.SUCCEEDED.value for runtime in runtimes)
