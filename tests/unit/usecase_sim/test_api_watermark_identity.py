"""Repeatability and identity contracts for the API watermark fixture."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest


SIM = Path(__file__).resolve().parents[3] / "usecase-sim"


def _load_validator():
    path = SIM / "scripts/features/workflows/validate_api_watermark_lifecycle.py"
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _case():
    return SimpleNamespace(
        document={
            "connections": [
                {
                    "name": "source",
                    "connection_type": "file",
                    "format": "parquet",
                    "configure": {},
                },
                {
                    "name": "destination",
                    "connection_type": "file",
                    "format": "parquet",
                    "configure": {},
                },
            ],
            "dataflows": [
                {
                    "name": "api-watermark-lifecycle",
                    "stage": "api_watermark_lifecycle",
                    "source": {
                        "connection_name": "source",
                        "table": "rows",
                        "watermark_columns": ["modified_at"],
                    },
                    "destination": {
                        "connection_name": "destination",
                        "table": "orders",
                        "load_type": "append",
                    },
                }
            ],
        }
    )


class _Cursor:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, params=None):
        self.calls.append((statement, params))

    def fetchone(self):
        return ("dc_framework_watermarks", "dc_framework_schema_hints")


class _Database:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return _Cursor(self.calls)


def test_seed_uses_isolated_workspace_ids_and_api_mapper_preserves_explicit_id(
    monkeypatch,
):
    validator = _load_validator()
    calls = []
    monkeypatch.setattr(validator, "_database", lambda: _Database(calls))
    case = _case()
    authored_name = "api-watermark-lifecycle"
    workspaces = [
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    ]

    persisted_ids = [
        validator._seed(case, workspace, authored_name)
        for workspace in workspaces
    ]
    expected_ids = [
        validator.name_to_uuid(f"{workspace}:{authored_name}")
        for workspace in workspaces
    ]
    assert persisted_ids == expected_ids
    assert persisted_ids[0] != persisted_ids[1]
    assert all(str(uuid.UUID(value)) == value for value in persisted_ids)

    flow_inserts = [
        params
        for statement, params in calls
        if "INSERT INTO dc_framework_dataflows" in statement
    ]
    assert [params[0] for params in flow_inserts] == persisted_ids
    assert [params[1] for params in flow_inserts] == workspaces
    assert [params[2] for params in flow_inserts] == [authored_name, authored_name]

    from datacoolie.metadata.api_provider import APIProvider

    workspace = workspaces[0]
    source_connection = {
        "connection_id": "source-id",
        "workspace_id": workspace,
        "name": "source",
        "connection_type": "file",
        "format": "parquet",
        "configure": {},
    }
    destination_connection = {
        **source_connection,
        "connection_id": "destination-id",
        "name": "destination",
    }
    mapped = APIProvider._dict_to_dataflow(
        {
            "dataflow_id": persisted_ids[0],
            "workspace_id": workspace,
            "name": authored_name,
            "stage": "api_watermark_lifecycle",
            "source": {
                "connection": source_connection,
                "table": "rows",
                "watermark_columns": ["modified_at"],
            },
            "destination": {
                "connection": destination_connection,
                "table": "orders",
                "load_type": "append",
            },
        }
    )
    assert mapped.dataflow_id == persisted_ids[0]
    assert mapped.workspace_id == workspace
    assert mapped.name == authored_name


@pytest.mark.parametrize("mutation", ["wrong_id", "duplicate", "skipped", "missing_session"])
def test_shared_success_helper_requires_exact_persisted_row_id(mutation):
    validator = _load_validator()
    persisted_id = validator._runtime_flow_id(
        "11111111-1111-4111-8111-111111111111",
        "api-watermark-lifecycle",
    )
    record = {
        "record_type": "dataflow_run_log",
        "dataflow_id": persisted_id,
        "status": "succeeded",
    }
    evidence = SimpleNamespace(
        result={"succeeded": 1, "failed": 0, "skipped": 0},
        session_id="session",
        records=(record,),
    )
    if mutation == "wrong_id":
        expected_id = validator.name_to_uuid("api-watermark-lifecycle")
    elif mutation == "duplicate":
        expected_id = persisted_id
        evidence.records = (record, dict(record))
    elif mutation == "skipped":
        expected_id = persisted_id
        evidence.result = {"succeeded": 0, "failed": 0, "skipped": 1}
    else:
        expected_id = persisted_id
        evidence.session_id = ""
    if mutation == "wrong_id":
        with pytest.raises(AssertionError, match="expected-flow evidence"):
            validator.assert_successful_flow(
                evidence,
                "api-watermark-lifecycle",
                expected_dataflow_id=expected_id,
            )
    else:
        with pytest.raises(AssertionError):
            validator.assert_successful_flow(
                evidence,
                "api-watermark-lifecycle",
                expected_dataflow_id=expected_id,
            )


def test_shared_success_helper_accepts_explicit_persisted_id():
    validator = _load_validator()
    persisted_id = validator._runtime_flow_id(
        "11111111-1111-4111-8111-111111111111",
        "api-watermark-lifecycle",
    )
    evidence = SimpleNamespace(
        result={"succeeded": 1, "failed": 0, "skipped": 0},
        session_id="session",
        records=(
            {
                "record_type": "dataflow_run_log",
                "dataflow_id": persisted_id,
                "status": "succeeded",
            },
        ),
    )
    validator.assert_successful_flow(
        evidence,
        "api-watermark-lifecycle",
        expected_dataflow_id=persisted_id,
    )
