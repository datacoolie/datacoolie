from unittest.mock import MagicMock, patch

from datacoolie.engines._spark import table_operations


def test_update_columns_excludes_keys_and_created_at_case_insensitively() -> None:
    assert table_operations.update_columns(
        ["ID", "name", "__created_at", "updated_at"], ["id"]
    ) == ["name", "updated_at"]


def test_merge_dispatch_covers_spark4_and_sql_fallback() -> None:
    spark = MagicMock()
    df = MagicMock()
    with (
        patch.object(table_operations, "maybe_evolve_iceberg"),
        patch.object(table_operations, "merge_into_upsert") as native,
        patch.object(table_operations, "merge_sql_upsert") as sql,
        patch.object(
            table_operations.runtime, "supports_merge_into", return_value=True
        ),
    ):
        table_operations.merge_to_table(
            spark, df, "catalog.db.table", ["id"], "delta", None
        )
        native.assert_called_once_with(df, "catalog.db.table", ["id"])
        sql.assert_not_called()


def test_merge_overwrite_unpersists_when_operation_fails() -> None:
    spark = MagicMock()
    df = MagicMock()
    with (
        patch.object(table_operations, "maybe_evolve_iceberg", return_value=False),
        patch.object(table_operations.runtime, "safe_cache", return_value=df),
        patch.object(
            table_operations.runtime, "supports_merge_into", return_value=True
        ),
        patch.object(
            table_operations, "merge_into_overwrite", side_effect=RuntimeError("boom")
        ),
        patch.object(table_operations.runtime, "safe_unpersist") as unpersist,
    ):
        try:
            table_operations.merge_overwrite_to_table(
                spark, df, "catalog.db.table", ["id"], "delta", None, None
            )
        except RuntimeError:
            pass
    unpersist.assert_called_once_with(df)
