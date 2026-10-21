"""Mutation guards for the A9 explicit Delta schema-merge case."""

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

from schema_types.validate_a9_merge_schema import (  # noqa: E402
    EXPECTED_ROWS,
    EXPECTED_SCHEMA,
    NEW_COLUMN,
    PHASE_CONTRACT,
    SEED_SCHEMA,
    assert_a9_merge_schema_metadata,
    assert_merge_schema_output,
    assert_seed_schema,
)


METADATA_PATH = SIM_ROOT / "metadata" / "file" / "schema_types.json"


def test_a9_merge_schema_fixture_uses_explicit_append_option_without_hints() -> None:
    from runner.metadata_sources import recipe_document
    metadata = recipe_document("schema_types", "a9_merge_schema")
    assert_a9_merge_schema_metadata(metadata)
    assert metadata["dataflows"][0].get("transform", {}) not in (
        {"schema_hints": [{"column_name": NEW_COLUMN, "data_type": "INT"}]},
    )


def test_seed_oracle_rejects_pre_materialized_new_column() -> None:
    mutated = dict(SEED_SCHEMA)
    mutated[NEW_COLUMN] = "Int64"
    with pytest.raises(AssertionError, match="pre-materialized"):
        assert_seed_schema(mutated)


@pytest.mark.parametrize("mutation", ["missing_old", "missing_new", "wrong_new_value", "wrong_schema"])
def test_merge_schema_oracle_rejects_rows_and_schema_mutants(mutation: str) -> None:
    rows = copy.deepcopy(EXPECTED_ROWS)
    schema = dict(EXPECTED_SCHEMA)
    if mutation == "missing_old":
        rows.pop(0)
    elif mutation == "missing_new":
        rows.pop()
    elif mutation == "wrong_new_value":
        rows[-1][NEW_COLUMN] = None
    else:
        schema[NEW_COLUMN] = "String"

    with pytest.raises(AssertionError, match="a9-schema-merge-schema"):
        assert_merge_schema_output(rows, schema)


def test_phase_contract_records_native_seed_then_normal_driver_write() -> None:
    assert PHASE_CONTRACT == {
        "seed": {
            "execution": "native_setup",
            "target": "events",
            "new_field_present": False,
        },
        "feature_write": {
            "execution": "normal_driver",
            "stage": "a9_schema_merge_schema_append",
            "flow": "a9-schema-merge-schema-append",
            "expected_result": {"succeeded": 1, "failed": 0, "skipped": 0},
        },
    }
