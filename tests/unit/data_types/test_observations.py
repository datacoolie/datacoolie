"""Tests for independent persisted datatype observations."""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.support.data_types import (
    FrameObservation,
    ObservationMismatch,
    assert_observation_matches,
    compare_observations,
    observe_parquet_dataset,
)


def test_parquet_observation_preserves_decimal_and_timestamp_semantics(tmp_path) -> None:
    output = tmp_path / "dataset"
    output.mkdir()
    pq.write_table(
        pa.table(
            {
                "id": pa.array([2, 1], type=pa.int64()),
                "amount": pa.array([Decimal("2.30"), Decimal("1.00")], type=pa.decimal128(10, 2)),
            }
        ),
        output / "part-000.parquet",
    )

    observation = observe_parquet_dataset(
        tmp_path,
        table_name="dataset",
        case_id="decimal",
        engine="polars",
    )

    assert observation.fields == (
        {"name": "id", "kind": "int64", "nullable": True},
        {
            "name": "amount",
            "kind": "decimal",
            "precision": 10,
            "scale": 2,
            "nullable": True,
        },
    )
    assert observation.rows[0]["id"] == 1
    assert observation.rows[0]["amount"] == {"decimal": "1.00"}


def test_comparison_is_strict_and_detects_schema_or_value_drift() -> None:
    base = FrameObservation(
        case_id="case",
        output_format="parquet",
        engine="spark",
        path="/tmp/a",
        fields=({"name": "id", "kind": "int64", "nullable": True},),
        row_count=1,
        rows=({"id": 1},),
    )
    same = FrameObservation(**{**base.to_dict(), "engine": "polars", "path": "/tmp/b"})
    compare_observations(base, same)

    drifted = FrameObservation(**{**same.to_dict(), "rows": ({"id": 2},)})
    with pytest.raises(ObservationMismatch, match="rows mismatch"):
        compare_observations(base, drifted)

    assert_observation_matches(base, {"fields": base.fields, "rows": base.rows})
    assert_observation_matches(
        base,
        {
            "business_fields": base.fields,
            "generated_fields": (),
            "rows": base.rows,
        },
    )
    generated = FrameObservation(
        **{
            **base.to_dict(),
            "fields": (
                *base.fields,
                {"name": "__created_at", "kind": "string", "nullable": True},
            ),
            "rows": ({"id": 1, "__created_at": "now"},),
        }
    )
    assert_observation_matches(
        generated,
        {
            "business_fields": base.fields,
            "generated_fields": (
                {"name": "__created_at", "kind": "string", "nullable": True},
            ),
            "rows": base.rows,
        },
    )
    with pytest.raises(ObservationMismatch, match="generated_fields mismatch"):
        assert_observation_matches(
            generated,
            {
                "business_fields": base.fields,
                "generated_fields": (
                    {"name": "__created_at", "kind": "timestamp", "nullable": True},
                ),
                "rows": base.rows,
            },
        )
    with pytest.raises(ObservationMismatch, match="fields mismatch"):
        assert_observation_matches(
            base,
            {"fields": ({"name": "id", "kind": "int32", "nullable": True},)},
        )


def test_engine_comparison_ignores_only_system_field_nullability() -> None:
    base = FrameObservation(
        case_id="case",
        output_format="parquet",
        engine="polars",
        path="/tmp/a",
        fields=(
            {"name": "id", "kind": "int64", "nullable": True},
        ),
        row_count=1,
        rows=({"id": 1},),
    )
    left = FrameObservation(
        **{
            **base.to_dict(),
            "fields": (
                *base.fields,
                {"name": "__created_at", "kind": "timestamp", "nullable": True},
            ),
            "rows": ({"id": 1, "__created_at": "a"},),
        }
    )
    right = FrameObservation(
        **{
            **left.to_dict(),
            "fields": (
                *base.fields,
                {"name": "__created_at", "kind": "timestamp", "nullable": False},
            ),
            "engine": "spark",
        }
    )
    compare_observations(left, right)
    with pytest.raises(ObservationMismatch, match="generated_fields mismatch"):
        compare_observations(
            left,
            FrameObservation(
                **{
                    **right.to_dict(),
                    "fields": (
                        *base.fields,
                        {"name": "__created_at", "kind": "string", "nullable": False},
                    ),
                }
            ),
        )


@pytest.mark.parametrize("values", [[1, 1], [None, 2]])
def test_observation_rejects_duplicate_or_missing_stable_keys(values) -> None:
    from tests.support.data_types.observations import observe_arrow_table

    with pytest.raises(AssertionError, match="stable key"):
        observe_arrow_table(
            pa.table({"id": values, "value": ["a", "b"]}),
            case_id="keys",
            output_format="parquet",
            engine="polars",
            path="memory://keys",
        )
