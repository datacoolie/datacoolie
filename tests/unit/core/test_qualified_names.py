from __future__ import annotations

import pytest

from datacoolie.core.qualified_names import (
    QualifiedTableName,
    QualifiedTableNameError,
    parse_name_prefix,
)


def test_parse_dotted_name_and_suffixes() -> None:
    name = QualifiedTableName.parse("Catalog.Database.Schema.Table")

    assert name.parts == ("Catalog", "Database", "Schema", "Table")
    assert name.normalized == ("catalog", "database", "schema", "table")
    assert name.suffix(2) == ("Schema", "Table")
    assert str(name) == "Catalog.Database.Schema.Table"


@pytest.mark.parametrize("value", ["", "db..table", (), ("db", "", "table")])
def test_invalid_empty_components(value: object) -> None:
    with pytest.raises(QualifiedTableNameError):
        QualifiedTableName.parse(value)  # type: ignore[arg-type]


def test_rejects_more_than_four_parts_with_mapping_hint() -> None:
    with pytest.raises(QualifiedTableNameError, match="logical_prefix"):
        QualifiedTableName.parse(("a", "b", "c", "d", "e"))


def test_empty_prefix_is_valid_but_prefix_has_three_part_limit() -> None:
    assert parse_name_prefix(()) == ()
    assert parse_name_prefix("catalog.database") == ("catalog", "database")
    with pytest.raises(QualifiedTableNameError, match="at most 3"):
        parse_name_prefix(("a", "b", "c", "d"))
