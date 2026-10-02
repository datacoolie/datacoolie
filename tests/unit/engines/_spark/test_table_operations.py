from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import EngineError

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


def test_merge_overwrite_validates_write_options_before_evolution() -> None:
    spark = MagicMock()
    df = MagicMock()
    with (
        patch.object(table_operations, "maybe_evolve_iceberg") as evolve,
        patch.object(table_operations.runtime, "safe_cache") as cache,
        pytest.raises(EngineError, match="mergeSchema|overwriteSchema"),
    ):
        table_operations.merge_overwrite_to_table(
            spark,
            df,
            "catalog.db.table",
            ["id"],
            "delta",
            None,
            None,
            write_options={"overwriteSchema": "true"},
        )
    evolve.assert_not_called()
    cache.assert_not_called()


def test_merge_to_table_routes_alias_options_through_sql_merge() -> None:
    spark = MagicMock()
    df = MagicMock()
    options = {"source_alias": "incoming", "target_alias": "current"}
    with (
        patch.object(table_operations, "merge_sql_upsert") as sql_merge,
        patch.object(table_operations, "merge_into_upsert") as native_merge,
        patch.object(table_operations.runtime, "supports_merge_into", return_value=True),
    ):
        table_operations.merge_to_table(
            spark, df, "catalog.db.table", ["id"], "delta", None, options
        )

    sql_merge.assert_called_once_with(
        spark, df, "catalog.db.table", ["id"], options
    )
    native_merge.assert_not_called()


def test_merge_sql_overwrite_uses_alias_options() -> None:
    spark = MagicMock()
    df = MagicMock()
    df.select.return_value.dropDuplicates.return_value = MagicMock()
    options = {"source_alias": "incoming", "target_alias": "current"}
    with patch.object(table_operations, "write_to_table"):
        table_operations.merge_sql_overwrite(
            spark,
            df,
            "catalog.db.table",
            ["id"],
            "delta",
            None,
            options,
            skip_iceberg_evolution=False,
        )

    statement = spark.sql.call_args.args[0]
    assert "AS current" in statement
    assert "AS incoming" in statement
    assert "current.`id` = incoming.`id`" in statement


def test_scd2_merge_parts_use_alias_options() -> None:
    condition, updates, late_guard = table_operations.scd2_merge_parts(
        ["id"], {"source_alias": "incoming", "target_alias": "current"}
    )

    assert "current.`id` = incoming.`id`" in condition
    assert "current.`__is_current`" in condition
    assert updates["`__valid_to`"] == "incoming.`__valid_from`"
    assert "incoming.`__valid_from` > current.`__valid_from`" == late_guard


def test_scd2_merge_parts_groups_custom_predicate() -> None:
    condition, _, _ = table_operations.scd2_merge_parts(
        ["id"],
        {
            "source_alias": "incoming",
            "target_alias": "current",
            "predicate": "a OR b",
        },
    )
    assert condition == "(a OR b) AND current.`__is_current` = true"


def test_merge_alias_types_fail_before_native_mutation() -> None:
    with pytest.raises(EngineError, match="aliases must be strings"):
        table_operations._merge_parts(["id"], {"source_alias": 1})


@pytest.mark.parametrize("options", [{"source_alias": ""}, {"predicate": "   "}])
def test_merge_empty_parts_fail_before_native_mutation(options) -> None:
    with pytest.raises(EngineError):
        table_operations._merge_parts(["id"], options)
