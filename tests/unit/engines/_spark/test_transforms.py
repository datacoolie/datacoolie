from unittest.mock import MagicMock

from datacoolie.engines._spark import transforms


def test_rename_columns_uses_single_projection() -> None:
    df = MagicMock()
    renamed = df.withColumnsRenamed.return_value

    mapping = {"old_a": "new_a", "old_b": "new_b"}
    assert transforms.rename_columns(df, mapping) is renamed
    df.withColumnsRenamed.assert_called_once_with(mapping)


def test_select_columns_forwards_sequence_as_projection() -> None:
    df = MagicMock()
    transforms.select_columns(df, ["a", "b"])
    df.select.assert_called_once_with("a", "b")


def test_filter_rows_forwards_sql_condition() -> None:
    df = MagicMock()
    filtered = df.filter.return_value

    assert transforms.filter_rows(df, "id > 10") is filtered
    df.filter.assert_called_once_with("id > 10")
