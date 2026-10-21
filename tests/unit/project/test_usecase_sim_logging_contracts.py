"""Current simulator logging validators reject stale versions and fake zeroes."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from datacoolie.core.models.connection import Connection
from datacoolie.core.models.dataflow import DataFlow
from datacoolie.core.models.destination import Destination
from datacoolie.core.models.source import Source
from datacoolie.logging.configuration.constants import LOG_SCHEMA_VERSION
from datacoolie.logging.persistence.projection import (
    build_dataflow_entry,
    build_system_entry,
    flatten_job_runtime,
)
from datacoolie.logging.runtime.capture import LogRecord
from datacoolie.orchestration.execution.runtime import (
    AttemptRuntimeInfo,
    DataFlowRuntimeInfo,
)
from datacoolie.orchestration.runtime import JobRuntimeInfo
from datacoolie.sources.contracts import (
    SourceReadMode,
    SourceReadSelection,
    SourceReadUpperOrigin,
)
from datacoolie.sources.runtime import SourceRuntimeInfo


PRODUCT_ROOT = Path(__file__).resolve().parents[3]


def _load_validator(relative: str, name: str):
    path = PRODUCT_ROOT / relative
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def execution_validator():
    return _load_validator(
        "usecase-sim/scripts/features/logging/validate_execution_logs.py",
        "execution_log_validator_test",
    )


def _current_emitter_records() -> tuple[dict, dict, dict]:
    """Build records through the current production projectors."""

    observed_at = datetime(2026, 10, 11, tzinfo=timezone.utc)
    connection = Connection(
        name="orders_source",
        format="delta",
        configure={"base_path": "/data/orders"},
    )
    dataflow = DataFlow(
        dataflow_id="orders",
        name="orders_flow",
        source=Source(connection=connection, query="SELECT * FROM orders"),
        destination=Destination(
            connection=connection,
            table="orders_out",
            load_type="append",
        ),
    )
    selection = SourceReadSelection(
        mode=SourceReadMode.RANGE,
        lower={"id": 1},
        upper={"id": 3},
        lower_operator=">=",
        upper_operator="<",
        upper_origin=SourceReadUpperOrigin.EXPLICIT,
    )
    source = SourceRuntimeInfo(
        start_time=observed_at,
        end_time=observed_at,
        status="succeeded",
        rows_read=2,
        source_action={"query": "SELECT * FROM orders"},
        selection=selection,
    )
    runtime = DataFlowRuntimeInfo(
        dataflow_id="orders",
        dataflow_run_id="orders-run",
        status="succeeded",
        start_time=observed_at,
        end_time=observed_at,
        attempts=[
            AttemptRuntimeInfo(
                attempt_number=1,
                start_time=observed_at,
                end_time=observed_at,
                status="succeeded",
                source=source,
                # The destination was not entered, so all destination metrics
                # must remain null in the observed-value contract.
                destination=None,
            )
        ],
    )
    run_attributes = {"pipeline_run_id": "run-1"}
    job = JobRuntimeInfo(
        job_id="orders-job",
        job_num=1,
        job_index=0,
        status="succeeded",
        total_dataflows=1,
        total_succeeded=1,
        total_rows_read=2,
        total_rows_written=None,
        run_attributes=json.dumps(run_attributes),
    )
    dataflow_record = build_dataflow_entry(
        dataflow,
        runtime,
        job_info=job,
        log_session_id="orders-session",
    )
    job_record = flatten_job_runtime(job, log_session_id="orders-session")
    system_record = build_system_entry(
        LogRecord(
            timestamp=observed_at,
            level="INFO",
            logger_name="test.current",
            message="finished",
        ),
        job_id="orders-job",
        job_num=1,
        job_index=0,
        log_session_id="orders-session",
    )
    return dataflow_record, job_record, system_record


def _attach_records(monkeypatch, validator, records):
    dataflow, job, system = records

    def current_records(_root, job_id, category, session_id):
        assert job_id == "orders-job"
        assert session_id == "orders-session"
        if category == "system_logs":
            return [(Path("system_snapshot.json"), system)]
        return [
            (Path("dataflow_snapshot.json"), dataflow),
            (Path("job_snapshot.json"), job),
        ]

    monkeypatch.setattr(validator, "records", current_records)


def test_execution_validator_accepts_current_emitter_record_and_observed_nulls(
    execution_validator, monkeypatch
):
    records = _current_emitter_records()
    _attach_records(monkeypatch, execution_validator, records)

    execution_validator.validate(
        "orders-job",
        dataflow_name="orders_flow",
        source_query="SELECT * FROM orders",
        mode="snapshot",
        log_session_id="orders-session",
        run_attributes={"pipeline_run_id": "run-1"},
    )


@pytest.mark.parametrize("mutation", ["version", "unknown_zero", "bad_metric", "source_rows"])
def test_execution_validator_rejects_contract_mutations(
    execution_validator, monkeypatch, mutation
):
    dataflow, job, system = _current_emitter_records()
    dataflow = deepcopy(dataflow)
    job = deepcopy(job)
    system = deepcopy(system)
    if mutation == "version":
        dataflow["log_schema_version"] = LOG_SCHEMA_VERSION - 1
    elif mutation == "unknown_zero":
        dataflow["metrics"]["rows_written"] = 0
    elif mutation == "bad_metric":
        dataflow["metrics"]["rows_read"] = -1
    else:
        dataflow["attempts"][-1]["source"]["rows_read"] = 7
    _attach_records(monkeypatch, execution_validator, (dataflow, job, system))

    with pytest.raises(AssertionError):
        execution_validator.validate(
            "orders-job",
            dataflow_name="orders_flow",
            source_query="SELECT * FROM orders",
            mode="snapshot",
            log_session_id="orders-session",
            run_attributes={"pipeline_run_id": "run-1"},
        )
