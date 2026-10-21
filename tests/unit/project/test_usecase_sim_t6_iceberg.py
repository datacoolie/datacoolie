"""Mutation guards for the focused native Iceberg A9 validators."""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
FEATURES_ROOT = SIM_ROOT / "scripts" / "features"
if str(FEATURES_ROOT) not in sys.path:
    sys.path.insert(0, str(FEATURES_ROOT))

from merge.validate_iceberg_baseline import (  # noqa: E402
    EXPECTED_FINAL as I1_ROWS,
    _assert_admitted as assert_i1_admitted,
    assert_i1_branch_precondition,
    assert_i1_output,
)
from merge.validate_iceberg_partition_change import (  # noqa: E402
    EXPECTED_ROWS as I3_ROWS,
    EXPECTED_SCHEMA as I3_SCHEMA,
    EXPECTED_SPEC as I3_SPEC,
    assert_i3_branch_evidence,
    assert_i3_output,
    assert_i3_schema,
    assert_i3_spec,
)
from merge.validate_iceberg_schema_merge import (  # noqa: E402
    EXPECTED_ROWS as I2_ROWS,
    EXPECTED_SCHEMA as I2_SCHEMA,
    assert_i2_output,
    assert_i2_schema,
    assert_i2_transactional_evidence,
)
from read.validate_iceberg_existing import (  # noqa: E402
    EXPECTED_DESTINATION_SCHEMA as O2_SCHEMA,
    EXPECTED_ROWS as O2_ROWS,
    assert_o2_generated_columns,
    assert_o2_rows,
    assert_o2_schema,
)
from scd2.validate_iceberg_scd2 import (  # noqa: E402
    EXPECTED_AFTER_CHANGE as I4_ROWS,
    EXPECTED_AFTER_REPLAY as I4_REPLAY_ROWS,
    EXPECTED_SCHEMA as I4_SCHEMA,
    assert_i4_admitted_update,
    assert_i4_history,
    assert_i4_schema,
    assert_i4_spec,
    assert_i4_history_types,
    assert_i4_watermark_rerun,
)


def test_i1_native_baseline_oracle_rejects_rows_schema_spec_and_skip_mutants() -> None:
    rows = copy.deepcopy(I1_ROWS)
    rows[0]["amount"] = "11.99"
    with pytest.raises(AssertionError):
        assert_i1_output(rows)

    schema = [{"field_id": 99, "name": "amount", "type": "double", "required": False}]
    with pytest.raises(AssertionError, match="schema"):
        assert_i1_branch_precondition([], schema, [], [])

    with pytest.raises(AssertionError, match="partition spec"):
        assert_i1_branch_precondition([], [], [], [{"field_id": 1000}])

    with pytest.raises(AssertionError, match="admitted"):
        assert_i1_admitted(
            type("Evidence", (), {"result": {"succeeded": 0, "failed": 0, "skipped": 1}})(),
            "i1",
        )


def test_i2_transactional_merge_oracle_requires_changed_existing_and_new_rows() -> None:
    assert_i2_output(I2_ROWS)
    changed = copy.deepcopy(I2_ROWS)
    changed[0]["value"] = "old-one"
    with pytest.raises(AssertionError):
        assert_i2_output(changed)

    missing_field = copy.deepcopy(I2_SCHEMA)
    missing_field.pop(2)
    with pytest.raises(AssertionError, match="schema"):
        assert_i2_schema(missing_field)

    wrong_id = copy.deepcopy(I2_SCHEMA)
    wrong_id[2]["field_id"] = 11
    with pytest.raises(AssertionError, match="schema"):
        assert_i2_schema(wrong_id)

    with pytest.raises(AssertionError, match="schema id"):
        assert_i2_transactional_evidence(
            I2_SCHEMA[:2], I2_SCHEMA, [1], [2], 0, 0
        )


def test_i3_partition_oracle_rejects_row_only_and_business_column_mutants() -> None:
    assert_i3_output(I3_ROWS)
    changed = copy.deepcopy(I3_ROWS)
    changed[-1]["region"] = "wrong"
    with pytest.raises(AssertionError):
        assert_i3_output(changed)

    assert_i3_schema(I3_SCHEMA)
    wrong_spec = copy.deepcopy(I3_SPEC)
    wrong_spec[0]["transform"] = "void"
    with pytest.raises(AssertionError, match="partition spec"):
        assert_i3_spec(wrong_spec)

    with pytest.raises(AssertionError, match="business column"):
        assert_i3_branch_evidence(
            [{"name": "id"}],
            [{"name": "id"}, {"name": "unexpected_business"}],
            [],
            I3_SPEC,
        )


def test_i4_native_history_and_admitted_replay_reject_mutants() -> None:
    assert_i4_history(I4_ROWS)
    assert_i4_schema(I4_SCHEMA)
    assert_i4_spec([])
    replay = copy.deepcopy(I4_REPLAY_ROWS)
    replay[-1]["__is_current"] = False
    with pytest.raises(AssertionError):
        assert_i4_history(replay, I4_REPLAY_ROWS)
    wrong_type = copy.deepcopy(I4_ROWS)
    wrong_type[0]["order_id"] = "1"
    with pytest.raises(AssertionError, match="non-int"):
        assert_i4_history_types(wrong_type)
    with pytest.raises(AssertionError, match="schema"):
        assert_i4_schema(I4_SCHEMA[:-1])
    with pytest.raises(AssertionError, match="partition spec"):
        assert_i4_spec([{"field_id": 1000}])

    assert_i4_admitted_update({"succeeded": 1, "failed": 0, "skipped": 0})
    with pytest.raises(AssertionError, match="admitted"):
        assert_i4_admitted_update({"succeeded": 0, "failed": 0, "skipped": 1})
    assert_i4_watermark_rerun({"succeeded": 1, "failed": 0, "skipped": 0})
    with pytest.raises(AssertionError, match="admitted"):
        assert_i4_watermark_rerun({"succeeded": 0, "failed": 0, "skipped": 1})


def test_o2_existing_readback_oracle_requires_exact_business_and_technical_schema() -> None:
    rows = [
        {
            **row,
            "__created_at": "2026-10-05T00:00:00+00:00",
            "__updated_at": "2026-10-05T00:00:00+00:00",
            "__updated_by": "DataCoolie",
            "__dataflow_run_id": "run",
        }
        for row in O2_ROWS
    ]
    assert_o2_rows(rows)
    assert_o2_generated_columns(rows)
    assert_o2_schema(O2_SCHEMA)

    wrong_rows = copy.deepcopy(rows)
    wrong_rows[1]["value"] = "mutated"
    with pytest.raises(AssertionError):
        assert_o2_rows(wrong_rows)

    wrong_schema = dict(O2_SCHEMA)
    wrong_schema.pop("__dataflow_run_id")
    with pytest.raises(AssertionError, match="schema"):
        assert_o2_schema(wrong_schema)


def test_i3_fixture_declares_existing_identity_partition_without_expression() -> None:
    from runner.metadata_sources import recipe_document
    fixture = recipe_document("merge", "a9_iceberg_partition_change")
    partition = fixture["dataflows"][0]["destination"]["partition_columns"][0]
    assert partition == {"column": "region"}
