from __future__ import annotations

import pytest

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models import MaskingRule, Transform, ValueRule
from datacoolie.transformers.column_projector import ColumnProjector
from datacoolie.transformers.column_value_transformer import ColumnValueTransformer
from datacoolie.transformers.data_masker import DataMasker
from datacoolie.transformers.hash_column_adder import HashColumnAdder
from tests.unit.transformers.support import MockEngine, make_dataflow


def test_transform_rejects_select_and_drop() -> None:
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        Transform(select_columns=["id"], drop_columns=["name"])


def test_transform_rejects_rename_chain() -> None:
    with pytest.raises(ConfigurationError, match="chains"):
        Transform(rename_columns={"a": "b", "b": "c"})


@pytest.mark.parametrize(
    "pattern",
    [r"^[A-Z][a-z]+$", r"(?:foo|bar)-[0-9]+", r"a.*?b", r"[\t\n -~]"],
)
def test_value_rule_accepts_portable_regex_subset(pattern: str) -> None:
    rule = ValueRule(
        operation="regex_replace",
        columns=["text"],
        pattern=pattern,
    )
    assert rule.pattern == pattern


@pytest.mark.parametrize(
    "pattern",
    [
        r"(?<=prefix)value",
        r"(a)\1",
        r"(?P<name>a)",
        r"(?i)value",
        r"\d+",
        r"\w+",
        r"\s+",
        r"\bword\b",
        r"(a+)+",
        r"(a|aa)+",
        r"(a(b)+)+",
        r"a++",
        r"[a&&b]",
    ],
)
def test_value_rule_rejects_nonportable_or_risky_regex(pattern: str) -> None:
    with pytest.raises(ConfigurationError, match="portable regex"):
        ValueRule(
            operation="regex_replace",
            columns=["text"],
            pattern=pattern,
        )


def test_value_rule_rejects_regex_over_length_limit() -> None:
    with pytest.raises(ConfigurationError, match="4096"):
        ValueRule(
            operation="regex_replace",
            columns=["text"],
            pattern="a" * 4097,
        )


def test_transform_rejects_scalar_rule_collection_with_field_path() -> None:
    with pytest.raises(ConfigurationError) as captured:
        Transform(value_rules="trim")
    assert captured.value.details["field"] == "transform.value_rules"


def test_transform_rejects_invalid_rule_item_with_indexed_field_path() -> None:
    with pytest.raises(ConfigurationError) as captured:
        Transform(value_rules=[{"operation": "trim", "columns": ["name"]}, 2])
    assert captured.value.details["field"] == "transform.value_rules[1]"


def test_transform_rejects_unknown_rule_field_with_indexed_path() -> None:
    with pytest.raises(ConfigurationError) as captured:
        Transform(
            value_rules=[
                {"operation": "trim", "columns": ["name"], "oder": 10},
            ]
        )
    assert captured.value.details["fields"] == ["oder"]
    assert captured.value.details["field"] == "transform.value_rules[0]"


def test_single_rule_dictionary_constructor_shorthand_remains_supported() -> None:
    transform = Transform(value_rules={"operation": "trim", "columns": ["name"]})
    assert transform.value_rules == [ValueRule(operation="trim", columns=["name"])]


def test_value_rules_use_stable_order_before_schema_cast() -> None:
    engine = MockEngine()
    flow = make_dataflow()
    flow.transform = Transform(
        value_rules=[
            {"operation": "trim", "columns": ["name"], "order": 20},
            {"operation": "case", "columns": ["name"], "mode": "lower", "order": 10},
            {"operation": "empty_to_null", "columns": ["name"], "order": 20},
        ]
    )
    transformer = ColumnValueTransformer(engine)
    transformer.transform({"name": [" A "]}, flow)
    assert transformer.order == 5
    assert [rule.operation for rule in engine._value_rules] == [
        "case",
        "trim",
        "empty_to_null",
    ]


def test_value_rules_track_only_rules_with_existing_columns() -> None:
    engine = MockEngine()
    engine.set_columns(["name"])
    flow = make_dataflow()
    flow.transform = Transform(
        value_rules=[
            {"operation": "trim", "columns": ["name"]},
            {"operation": "trim", "columns": ["missing"]},
        ],
        configure={"missing_column_policy": "ignore"},
    )

    transformer = ColumnValueTransformer(engine)
    transformer.transform({"name": [" A "]}, flow)

    assert transformer.applied_label == "ColumnValueTransformer(1 rules)"


def test_value_rules_track_all_missing_rules_as_skipped() -> None:
    engine = MockEngine()
    engine.set_columns(["name"])
    flow = make_dataflow()
    flow.transform = Transform(
        value_rules=[{"operation": "trim", "columns": ["missing"]}],
        configure={"missing_column_policy": "ignore"},
    )

    transformer = ColumnValueTransformer(engine)
    transformer.transform({"name": [" A "]}, flow)

    assert transformer.applied_label is None


def test_masker_rejects_merge_key() -> None:
    engine = MockEngine()
    flow = make_dataflow(merge_keys=["id"])
    flow.transform = Transform(
        masking_rules=[
            MaskingRule(method="nullify", columns=["id"]),
        ]
    )
    with pytest.raises(ConfigurationError, match="cannot target"):
        DataMasker(engine).transform({"id": [1]}, flow)


def test_masker_tracks_only_rules_with_existing_columns() -> None:
    engine = MockEngine()
    engine.set_columns(["email"])
    flow = make_dataflow()
    flow.transform = Transform(
        masking_rules=[
            {"method": "nullify", "columns": ["email"]},
            {"method": "nullify", "columns": ["missing"]},
        ],
        configure={"missing_column_policy": "ignore"},
    )

    transformer = DataMasker(engine)
    transformer.transform({"email": ["a@example.com"]}, flow)

    assert transformer.applied_label == "DataMasker(1 rules)"


def test_masker_tracks_all_missing_rules_as_skipped() -> None:
    engine = MockEngine()
    engine.set_columns(["email"])
    flow = make_dataflow()
    flow.transform = Transform(
        masking_rules=[{"method": "nullify", "columns": ["missing"]}],
        configure={"missing_column_policy": "ignore"},
    )

    transformer = DataMasker(engine)
    transformer.transform({"email": ["a@example.com"]}, flow)

    assert transformer.applied_label is None


def test_projector_selects_then_renames() -> None:
    engine = MockEngine()
    engine.set_columns(["id", "name", "amount"])
    flow = make_dataflow(merge_keys=["id"])
    flow.transform = Transform(
        select_columns=["id", "name"], rename_columns={"name": "full_name"}
    )
    result = ColumnProjector(engine).transform(
        {"id": [1], "name": ["A"], "amount": [10]}, flow
    )
    assert result == {"id": [1], "full_name": ["A"]}
    assert engine._renamed == [("name", "full_name")]
    assert engine._rename_batches == [{"name": "full_name"}]


def test_projector_rejects_dropping_framework_column() -> None:
    engine = MockEngine()
    engine.set_columns(["id", "__updated_at"])
    flow = make_dataflow()
    flow.transform = Transform(drop_columns=["__updated_at"])
    with pytest.raises(ConfigurationError, match="framework-reserved"):
        ColumnProjector(engine).transform(
            {"id": [1], "__updated_at": ["now"]}, flow
        )


def test_projector_all_missing_ignored_drop_and_rename_is_skipped() -> None:
    engine = MockEngine()
    engine.set_columns(["id", "name"])
    flow = make_dataflow()
    flow.transform = Transform(
        drop_columns=["missing_drop"],
        rename_columns={"missing_rename": "renamed"},
        configure={"missing_column_policy": "ignore"},
    )
    transformer = ColumnProjector(engine)

    result = transformer.transform({"id": [1], "name": ["A"]}, flow)

    assert result == {"id": [1], "name": ["A"]}
    assert transformer.applied_label is None
    assert engine._rename_batches == []


def test_one_masking_rule_per_column() -> None:
    with pytest.raises(ConfigurationError, match="only one masking rule"):
        Transform(
            masking_rules=[
                {"method": "redact", "columns": ["email"], "value": "hidden"},
                {"method": "nullify", "columns": ["EMAIL"]},
            ]
        )


def test_hash_column_adder_uses_explicit_target() -> None:
    engine = MockEngine()
    engine.set_columns(["country_code", "customer_id"])
    flow = make_dataflow()
    flow.transform = Transform(
        hash_columns=[
            {
                "target_column": "customer_hash",
                "columns": ["country_code", "customer_id"],
            }
        ]
    )
    result = HashColumnAdder(engine).transform(
        {"country_code": ["VN"], "customer_id": [123]}, flow
    )
    assert result["customer_hash"] == "sha256"
    assert engine._hash_columns[0].algorithm == "sha256"
    assert HashColumnAdder(engine).order == 18


def test_hash_column_rejects_duplicate_targets() -> None:
    with pytest.raises(ConfigurationError, match="duplicate target"):
        Transform(
            hash_columns=[
                {"target_column": "key_hash", "columns": ["a"]},
                {"target_column": "KEY_HASH", "columns": ["b"]},
            ]
        )


@pytest.mark.parametrize("policy", ["error", "ignore"])
def test_missing_column_policy(policy: str) -> None:
    assert (
        Transform(configure={"missing_column_policy": policy}).missing_column_policy
        == policy
    )


def test_invalid_missing_column_policy() -> None:
    with pytest.raises(ConfigurationError, match="missing_column_policy"):
        _ = Transform(configure={"missing_column_policy": "warn"}).missing_column_policy
