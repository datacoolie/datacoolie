"""Mutation tests for the focused composite-key and predicate merge cases."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
FEATURES_ROOT = PRODUCT_ROOT / "usecase-sim" / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from _stateful_support import (  # noqa: E402
    DriverEvidence,
    assert_successful_flow,
)
from datacoolie.utils.identity import name_to_uuid  # noqa: E402
from merge.validate_composite_keys import (  # noqa: E402
    COMPOSITE_SCHEMA,
    FINAL_ROWS as COMPOSITE_FINAL_ROWS,
    assert_composite_output,
)
from merge.validate_custom_predicate import (  # noqa: E402
    CUSTOM_PREDICATE_SCHEMA,
    FINAL_ROWS as PREDICATE_FINAL_ROWS,
    MERGE_OPTIONS,
    assert_custom_predicate_output,
)


def _flow_evidence(
    flow_name: str,
    *,
    result: dict[str, int] | None = None,
    records: tuple[dict[str, str], ...] | None = None,
) -> DriverEvidence:
    return DriverEvidence(
        job_id="focused-merge-test",
        stage="merge_test",
        session_id="session-1",
        result=result
        or {"succeeded": 1, "failed": 0, "skipped": 0},
        records=records
        if records is not None
        else (
            {
                "record_type": "dataflow_run_log",
                "dataflow_id": name_to_uuid(flow_name),
                "status": "succeeded",
            },
        ),
    )


def test_composite_fixture_declares_two_business_keys() -> None:
    path = PRODUCT_ROOT / "usecase-sim/metadata/file/merge/composite_keys.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    flow = payload["dataflows"][0]
    assert len(payload["dataflows"]) == 1
    assert flow["name"] == "merge-composite-keys"
    assert flow["stage"] == "merge_composite_keys"
    assert flow["destination"]["merge_keys"] == ["id", "tenant"]


def test_custom_predicate_fixture_declares_aliases_and_scope_match() -> None:
    path = PRODUCT_ROOT / "usecase-sim/metadata/file/merge/custom_predicate.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    flow = payload["dataflows"][0]
    options = flow["destination"]["configure"]["merge_options"]
    assert len(payload["dataflows"]) == 1
    assert flow["name"] == "merge-custom-predicate"
    assert flow["stage"] == "merge_custom_predicate"
    assert flow["destination"]["merge_keys"] == ["id"]
    assert options == MERGE_OPTIONS


@pytest.mark.parametrize(
    "mutation",
    ["id_only_match", "cross_tenant", "duplicate", "lost_tail", "wrong_schema"],
)
def test_composite_oracle_rejects_mutations(mutation: str) -> None:
    rows = [dict(row) for row in COMPOSITE_FINAL_ROWS]
    schema = dict(COMPOSITE_SCHEMA)
    if mutation == "id_only_match":
        rows[1]["value"] = "new-1A"
        rows[1]["segment"] = "updated"
    elif mutation == "cross_tenant":
        rows[0]["tenant"] = "B"
    elif mutation == "duplicate":
        rows.append(dict(rows[0]))
    elif mutation == "lost_tail":
        rows = rows[:-1]
    else:
        schema["value"] = "Int32"

    with pytest.raises(AssertionError):
        assert_composite_output(rows, schema, COMPOSITE_FINAL_ROWS, label=mutation)


@pytest.mark.parametrize("mutation", ["default_predicate", "skipped_update_insert", "duplicate", "lost_tail", "wrong_schema"])
def test_custom_predicate_oracle_rejects_mutations(mutation: str) -> None:
    rows = [dict(row) for row in PREDICATE_FINAL_ROWS]
    schema = dict(CUSTOM_PREDICATE_SCHEMA)
    if mutation == "default_predicate":
        rows[1]["value"] = "new-1A"
        rows[1]["segment"] = "updated"
    elif mutation == "skipped_update_insert":
        rows = [
            {"id": 1, "scope": "A", "value": "old-1A", "segment": "updated"},
            {"id": 1, "scope": "B", "value": "keep-1B", "segment": "scope-b"},
            {"id": 9, "scope": "tail", "value": "keep-tail", "segment": "tail"},
        ]
    elif mutation == "duplicate":
        rows.append(dict(rows[0]))
    elif mutation == "lost_tail":
        rows = rows[:-1]
    else:
        schema["scope"] = "Int32"

    with pytest.raises(AssertionError):
        assert_custom_predicate_output(rows, schema, PREDICATE_FINAL_ROWS, label=mutation)


@pytest.mark.parametrize("mutation", ["skipped_rerun", "missing_evidence"])
def test_successful_flow_oracle_rejects_rerun_evidence_mutations(mutation: str) -> None:
    # Persisted rows can still be correct while the required rerun proof is
    # absent; the session/evidence oracle must reject that claim.
    assert_composite_output(
        COMPOSITE_FINAL_ROWS,
        COMPOSITE_SCHEMA,
        COMPOSITE_FINAL_ROWS,
        label="rows-right",
    )
    if mutation == "skipped_rerun":
        evidence = _flow_evidence(
            "merge-composite-keys",
            result={"succeeded": 0, "failed": 0, "skipped": 1},
        )
    else:
        evidence = _flow_evidence("merge-composite-keys", records=())

    with pytest.raises(AssertionError):
        assert_successful_flow(evidence, "merge-composite-keys")


def test_successful_flow_oracle_accepts_current_expected_flow() -> None:
    assert_successful_flow(_flow_evidence("merge-custom-predicate"), "merge-custom-predicate")
