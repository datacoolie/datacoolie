"""Focused oracle and metadata checks for flat-file append cases."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
FEATURES_ROOT = SIM_ROOT / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from _stateful_support import (  # noqa: E402
    CasePaths,
    DriverEvidence,
    assert_successful_flow,
    receipt_payload,
)
from write_basic.validate_flat_append import (  # noqa: E402
    EXPECTED_ROWS,
    EXPECTED_SCHEMA,
    FORMATS,
    assert_append_file_history,
    assert_flat_append_output,
    assert_flat_append_profile,
)
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


@pytest.mark.parametrize("fmt", FORMATS)
def test_authored_flat_append_fixture_is_one_unpartitioned_string_flow(fmt: str) -> None:
    from runner.metadata_sources import recipe_document

    metadata = recipe_document("write_basic", f"append_{fmt}")
    connections = {item["name"]: item for item in metadata["connections"]}
    flows = metadata["dataflows"]

    assert len(flows) == 1
    flow = flows[0]
    assert flow["name"] == f"flat-append-{fmt}"
    assert flow["stage"] == f"flat_append_{fmt}"
    assert flow["source"]["connection_name"] == "flat_append_source"
    assert flow["source"]["table"] == "source"
    assert "watermark_columns" not in flow["source"]
    assert "transform" not in flow
    assert flow["destination"]["load_type"] == "append"
    assert "partition_columns" not in flow["destination"]
    assert connections["flat_append_source"]["format"] == "parquet"
    assert connections[f"flat_append_{fmt}_destination"]["format"] == fmt


def test_flat_append_oracle_accepts_exact_union_and_string_schema() -> None:
    assert_flat_append_output(
        EXPECTED_ROWS,
        EXPECTED_SCHEMA,
        EXPECTED_ROWS,
        label="flat-append-union",
    )


@pytest.mark.parametrize(
    "mutation",
    ["missing_a", "missing_b", "duplicate_id", "wrong_value", "wrong_column", "wrong_type"],
)
def test_flat_append_oracle_rejects_business_mutations(mutation: str) -> None:
    mutated = [dict(row) for row in EXPECTED_ROWS]
    schema = dict(EXPECTED_SCHEMA)
    if mutation == "missing_a":
        mutated.pop(0)
    elif mutation == "missing_b":
        mutated.pop(-1)
    elif mutation == "duplicate_id":
        mutated.append(dict(EXPECTED_ROWS[0]))
    elif mutation == "wrong_value":
        mutated[2]["value"] = "wrong-value"
    elif mutation == "wrong_column":
        del mutated[1]["details"]
    else:
        mutated[0]["id"] = 1
        schema["id"] = "Int64"

    with pytest.raises(AssertionError, match=f"flat-append-{mutation}"):
        assert_flat_append_output(
            mutated,
            schema,
            EXPECTED_ROWS,
            label=f"flat-append-{mutation}",
        )


@pytest.mark.parametrize("mutation", ["missing_a", "changed_a", "no_new_b", "wrong_extension"])
def test_flat_append_file_oracle_rejects_file_mutations(mutation: str) -> None:
    after_a = {"target_1.csv": "hash-a"}
    after_b = {"target_1.csv": "hash-a", "target_2.csv": "hash-b"}
    if mutation == "missing_a":
        after_b = {"target_2.csv": "hash-b"}
    elif mutation == "changed_a":
        after_b["target_1.csv"] = "changed-a"
    elif mutation == "no_new_b":
        after_b = dict(after_a)
    elif mutation == "wrong_extension":
        after_b = {"target_1.csv": "hash-a", "target_2.json": "hash-b"}

    with pytest.raises(AssertionError, match=f"flat-append-files-{mutation}"):
        assert_append_file_history(
            after_a,
            after_b,
            fmt="csv",
            label=f"flat-append-files-{mutation}",
        )


@pytest.mark.parametrize("fmt", FORMATS)
def test_flat_append_receipt_profile_rejects_wrong_format_for_each_profile(fmt: str) -> None:
    wrong_format = next(item for item in FORMATS if item != fmt)
    payload = {"profile": {"destination_format": wrong_format}}

    with pytest.raises(AssertionError, match=f"flat-append-profile-{fmt}"):
        assert_flat_append_profile(payload, fmt, label=f"flat-append-profile-{fmt}")


@pytest.mark.parametrize("fmt", FORMATS)
def test_flat_append_profile_rejects_wrong_authored_format_for_each_profile(fmt: str) -> None:
    payload = {"profile": {"destination_format": fmt}}
    wrong_format = next(item for item in FORMATS if item != fmt)

    with pytest.raises(AssertionError, match=f"flat-append-authored-profile-{fmt}"):
        assert_flat_append_profile(
            payload,
            fmt,
            authored_destination_format=wrong_format,
            label=f"flat-append-authored-profile-{fmt}",
        )


def test_shared_receipt_payload_retains_delta_default() -> None:
    case_root = Path(".scratch") / "flat-append-receipt-default"
    case = CasePaths(
        group="write_basic",
        root=case_root,
        input=case_root / "input",
        output=case_root / "output",
        state=case_root / "state",
        watermarks=case_root / "watermarks",
        receipt_root=case_root / "receipts",
    )
    payload = receipt_payload(case, runs=(), details={}, script=Path(__file__))

    assert payload["profile"]["destination_format"] == "delta"


def _evidence(flow_name: str, *, result: dict[str, int], records: tuple[dict, ...]) -> DriverEvidence:
    return DriverEvidence(
        job_id="focused-write-basic-flat-append",
        stage="flat_append_csv",
        session_id="session-flat-append",
        result=result,
        records=records,
    )


def _successful_record(flow_name: str) -> dict[str, str]:
    return {
        "record_type": "dataflow_run_log",
        "dataflow_id": name_to_uuid(flow_name),
        "status": "succeeded",
    }


def test_flat_append_successful_output_does_not_hide_skipped_invocation() -> None:
    assert_flat_append_output(EXPECTED_ROWS, EXPECTED_SCHEMA, EXPECTED_ROWS)
    evidence = _evidence(
        "flat-append-csv",
        result={"succeeded": 0, "failed": 0, "skipped": 1},
        records=(_successful_record("flat-append-csv"),),
    )

    with pytest.raises(AssertionError, match="did not succeed"):
        assert_successful_flow(evidence, "flat-append-csv")


def test_flat_append_successful_output_does_not_hide_missing_flow_evidence() -> None:
    assert_flat_append_output(EXPECTED_ROWS, EXPECTED_SCHEMA, EXPECTED_ROWS)
    evidence = _evidence(
        "flat-append-csv",
        result={"succeeded": 1, "failed": 0, "skipped": 0},
        records=(),
    )

    with pytest.raises(AssertionError, match="no unique successful expected-flow evidence"):
        assert_successful_flow(evidence, "flat-append-csv")
