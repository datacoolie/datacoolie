"""Focused oracle and metadata checks for explicit timestamp-NTZ conversion."""

from __future__ import annotations

import copy
import dataclasses
import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
FEATURES_ROOT = SIM_ROOT / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from _stateful_support import DriverEvidence, assert_successful_flow  # noqa: E402
from schema_types.validate_ntz_timezone import (  # noqa: E402
    EXPECTED_ROWS,
    EXPECTED_UTC_INSTANT,
    INPUT_SCHEMA,
    PERSISTED_SCHEMA,
    assert_input_ntz_schema,
    assert_ntz_output,
)
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


METADATA_PATH = SIM_ROOT / "metadata" / "file" / "schema_types" / "ntz_timezone.json"


def test_authored_ntz_fixture_activates_explicit_timezone_conversion() -> None:
    metadata = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    connections = {item["name"]: item for item in metadata["connections"]}
    flows = metadata["dataflows"]

    assert len(flows) == 1
    flow = flows[0]
    assert flow["name"] == "ntz-timezone-conversion"
    assert flow["stage"] == "ntz_timezone"
    assert flow["source"] == {
        "connection_name": "ntz_timezone_source",
        "table": "rows",
    }
    assert flow["transform"]["configure"] == {
        "convert_timestamp_ntz": True,
        "timestamp_timezone": "Asia/Ho_Chi_Minh",
    }
    assert flow["destination"] == {
        "connection_name": "ntz_timezone_destination",
        "table": "timestamps",
        "load_type": "overwrite",
    }
    assert connections["ntz_timezone_source"]["format"] == "parquet"
    assert connections["ntz_timezone_destination"]["format"] == "delta"


def test_input_oracle_requires_naive_microseconds_and_aware_utc_control() -> None:
    assert_input_ntz_schema(INPUT_SCHEMA)

    naive_mutation = dict(INPUT_SCHEMA, wall_clock="Datetime(time_unit='us', time_zone='UTC')")
    with pytest.raises(AssertionError, match="ntz input schema"):
        assert_input_ntz_schema(naive_mutation)

    precision_mutation = dict(INPUT_SCHEMA, wall_clock="Datetime(time_unit='ms', time_zone=None)")
    with pytest.raises(AssertionError, match="ntz input schema"):
        assert_input_ntz_schema(precision_mutation)


@pytest.mark.parametrize(
    "mutation",
    ["wrong_offset", "naive_dtype", "precision", "null", "control", "missing_id", "schema"],
)
def test_ntz_oracle_rejects_business_and_schema_mutations(mutation: str) -> None:
    rows = copy.deepcopy(EXPECTED_ROWS)
    schema = dict(PERSISTED_SCHEMA)
    if mutation == "wrong_offset":
        rows[0]["wall_clock"] = EXPECTED_UTC_INSTANT.replace(hour=4)
    elif mutation == "naive_dtype":
        schema["wall_clock"] = "Datetime(time_unit='us', time_zone=None)"
    elif mutation == "precision":
        rows[0]["wall_clock"] = EXPECTED_UTC_INSTANT.replace(microsecond=123000)
    elif mutation == "null":
        rows[1]["wall_clock"] = EXPECTED_UTC_INSTANT
    elif mutation == "control":
        rows[0]["aware_control"] = EXPECTED_UTC_INSTANT
    elif mutation == "missing_id":
        rows.pop(0)
    else:
        del rows[1]["value"]

    with pytest.raises(AssertionError, match=f"ntz-{mutation}"):
        assert_ntz_output(rows, schema, label=f"ntz-{mutation}")


def _successful_evidence() -> DriverEvidence:
    return DriverEvidence(
        job_id="focused-schema-types-ntz",
        stage="ntz_timezone",
        session_id="session-ntz",
        result={"succeeded": 1, "failed": 0, "skipped": 0},
        records=(
            {
                "record_type": "dataflow_run_log",
                "dataflow_id": name_to_uuid("ntz-timezone-conversion"),
                "status": "succeeded",
            },
        ),
    )


@pytest.mark.parametrize("mutation", ["skipped_rerun", "missing_flow_evidence"])
def test_ntz_rerun_evidence_cannot_be_replaced_by_stable_rows(mutation: str) -> None:
    evidence = _successful_evidence()
    if mutation == "skipped_rerun":
        evidence = dataclasses.replace(
            evidence,
            result={"succeeded": 0, "failed": 0, "skipped": 1},
        )
    else:
        evidence = dataclasses.replace(evidence, records=())

    with pytest.raises(AssertionError, match="ntz-timezone-conversion"):
        assert_successful_flow(evidence, "ntz-timezone-conversion")
