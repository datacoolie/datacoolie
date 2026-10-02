"""Shared schema-hint normalization and selection contract tests."""

from __future__ import annotations

import pytest

from datacoolie.core.exceptions import MetadataError
from datacoolie.core.models.transform import SchemaHint
from datacoolie.metadata.resolution.schema_hints import (
    normalize_grouped_hints,
    normalize_schema_name,
    normalized_key,
    select_schema_hints,
)


def _hint(column: str, ordinal: int = 0) -> SchemaHint:
    return SchemaHint(column_name=column, data_type="STRING", ordinal_position=ordinal)


def test_blank_schema_normalizes_to_none() -> None:
    assert normalize_schema_name("  ") is None
    assert normalized_key("c-1", " dbo ", " Orders ") == ("c-1", "dbo", "Orders")


def test_none_schema_selects_the_only_qualified_group() -> None:
    grouped = {
        ("c-1", "Sales", "Orders"): [_hint("z", 2), _hint("a", 1)],
    }

    selected = select_schema_hints(grouped, "c-1", None, "orders")

    assert selected is not None
    assert [hint.column_name for hint in selected] == ["a", "z"]


def test_none_schema_rejects_ambiguous_groups() -> None:
    grouped = {
        ("c-1", "sales", "orders"): [_hint("id")],
        ("c-1", "archive", "orders"): [_hint("legacy_id")],
    }

    with pytest.raises(MetadataError, match="ambiguous"):
        select_schema_hints(grouped, "c-1", None, "orders")


def test_named_schema_matches_case_insensitively_without_fallback() -> None:
    grouped = {
        ("c-1", "sales", "orders"): [_hint("id")],
        ("c-1", "archive", "orders"): [_hint("legacy_id")],
    }

    assert [hint.column_name for hint in select_schema_hints(
        grouped,
        "c-1",
        "SALES",
        "ORDERS",
    ) or []] == ["id"]
    assert select_schema_hints(grouped, "c-1", "missing", "orders") is None


def test_duplicate_columns_after_case_normalization_fail() -> None:
    grouped = {
        ("c-1", None, "orders"): [_hint("id")],
        ("c-1", "", "ORDERS"): [_hint("ID")],
    }

    with pytest.raises(MetadataError, match="Duplicate schema hint column"):
        normalize_grouped_hints(grouped)
