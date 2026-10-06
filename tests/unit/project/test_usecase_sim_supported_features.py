"""Shared execution evidence and supported-feature mapping regressions."""

from __future__ import annotations

import copy
import dataclasses
import sys
from pathlib import Path

import pytest

FEATURES = Path(__file__).resolve().parents[3] / "usecase-sim/scripts/features"
if str(FEATURES) not in sys.path:
    sys.path.insert(0, str(FEATURES))

from _stateful_support import DriverEvidence, assert_successful_flow  # noqa: E402
from datacoolie.core.models.connection import Connection  # noqa: E402
from datacoolie.core.models.destination import Destination  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


def test_destination_options_override_same_key_without_mutating_inputs() -> None:
    connection_config = {"write_options": {"header": True, "sep": ";"}}
    destination_config = {"write_options": {"header": False}}
    original = copy.deepcopy((connection_config, destination_config))
    connection = Connection(
        name="csv", connection_type="file", format="csv", configure=connection_config
    )
    destination = Destination(
        connection=connection, table="records", configure=destination_config
    )
    assert destination.write_options == {"header": False, "sep": ";"}
    assert (connection_config, destination_config) == original
    destination.write_options["sep"] = ","
    assert connection.write_options == {"header": True, "sep": ";"}


def successful_evidence() -> DriverEvidence:
    return DriverEvidence(
        job_id="job", stage="append", session_id="current",
        result={"succeeded": 1, "failed": 0, "skipped": 0},
        records=({"record_type": "dataflow_run_log", "dataflow_id": name_to_uuid("append"), "status": "succeeded"},),
    )


def test_successful_expected_flow_evidence_is_accepted() -> None:
    assert_successful_flow(successful_evidence(), "append")


@pytest.mark.parametrize("mutation", ["skipped", "failed", "missing_record", "wrong_flow", "wrong_status", "duplicate_record", "missing_session"])
def test_successful_flow_rejects_invalid_invocation_evidence(mutation: str) -> None:
    evidence = successful_evidence()
    if mutation in {"skipped", "failed"}:
        evidence = dataclasses.replace(evidence, result={"succeeded": 0, "failed": int(mutation == "failed"), "skipped": int(mutation == "skipped")})
    elif mutation == "missing_record":
        evidence = dataclasses.replace(evidence, records=())
    elif mutation == "wrong_flow":
        evidence = dataclasses.replace(evidence, records=({**evidence.records[0], "dataflow_id": name_to_uuid("other")},))
    elif mutation == "wrong_status":
        evidence = dataclasses.replace(evidence, records=({**evidence.records[0], "status": "skipped"},))
    elif mutation == "duplicate_record":
        evidence = dataclasses.replace(evidence, records=evidence.records * 2)
    else:
        evidence = dataclasses.replace(evidence, session_id="")
    with pytest.raises(AssertionError):
        assert_successful_flow(evidence, "append")
