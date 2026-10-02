from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark import delta


def test_merge_to_path_rejects_non_delta_format() -> None:
    with pytest.raises(EngineError, match="only supports delta"):
        delta.merge_to_path(MagicMock(), MagicMock(), "/table", ["id"], "parquet")


def test_table_exists_by_path_propagates_platform_probe_errors() -> None:
    platform = MagicMock()
    platform.folder_exists.side_effect = TimeoutError("timed out")

    with pytest.raises(TimeoutError, match="timed out"):
        delta.table_exists_by_path(MagicMock(), platform, "/table")


def test_table_exists_by_path_allows_absent_and_empty_targets() -> None:
    spark = MagicMock()
    platform = MagicMock()
    platform.folder_exists.side_effect = lambda path: path != "/table/_delta_log"
    platform.file_exists.return_value = False
    platform.list_files.return_value = []
    platform.list_folders.return_value = []

    assert delta.table_exists_by_path(spark, platform, "/table") is False
    spark.sql.assert_not_called()


def test_table_exists_by_path_rejects_occupied_non_delta_target() -> None:
    spark = MagicMock()
    platform = MagicMock()
    platform.folder_exists.side_effect = lambda path: path == "/table"
    platform.file_exists.return_value = False
    platform.list_files.return_value = [MagicMock()]
    platform.list_folders.return_value = []

    with pytest.raises(EngineError, match="no _delta_log"):
        delta.table_exists_by_path(spark, platform, "/table")
    spark.sql.assert_not_called()


def test_table_exists_by_path_classifies_spark_non_delta_error() -> None:
    spark = MagicMock()
    platform = MagicMock()
    platform.folder_exists.side_effect = lambda path: path in {
        "/table",
        "/table/_delta_log",
    }
    platform.file_exists.return_value = False
    platform.list_files.return_value = []
    platform.list_folders.return_value = ["/table/_delta_log"]
    error = RuntimeError("not a Delta table")
    error.getErrorClass = lambda: "DELTA_MISSING_DELTA_TABLE"
    spark.sql.side_effect = error

    with pytest.raises(EngineError, match="not a Delta table"):
        delta.table_exists_by_path(spark, platform, "/table")


def test_delta_compound_write_uses_shared_option_validation() -> None:
    df = MagicMock()
    options = {"mergeSchema": True}
    with (
        patch.object(delta.file_io, "require_portable_timestamp_output"),
        patch.object(delta.file_io, "write_to_path"),
        patch.object(
            delta.file_io,
            "validate_write_options",
            wraps=delta.file_io.validate_write_options,
        ) as validate,
        patch.object(delta, "delta_table"),
    ):
        delta.merge_overwrite_to_path(
            MagicMock(),
            df,
            "/table",
            ["id"],
            "delta",
            None,
            None,
            write_options=options,
        )

    validate.assert_called_once_with(options, mode="append")
    assert options == {"mergeSchema": True}


def test_merge_overwrite_always_unpersists() -> None:
    df = MagicMock()
    with (
        patch.object(delta.runtime, "safe_cache", return_value=df),
        patch.object(delta, "delta_table", side_effect=RuntimeError("boom")),
        patch.object(delta.runtime, "safe_unpersist") as unpersist,
        pytest.raises(RuntimeError, match="boom"),
    ):
        delta.merge_overwrite_to_path(
            MagicMock(), df, "/table", ["id"], "delta", None, None
        )
    unpersist.assert_called_once_with(df)


def test_merge_overwrite_rejects_invalid_write_options_before_delete() -> None:
    df = MagicMock()
    with (
        patch.object(delta.file_io, "require_portable_timestamp_output"),
        patch.object(delta, "delta_table") as table,
        pytest.raises(EngineError, match="mergeSchema|overwriteSchema"),
    ):
        delta.merge_overwrite_to_path(
            MagicMock(),
            df,
            "/table",
            ["id"],
            "delta",
            None,
            None,
            write_options={"overwriteSchema": "true"},
        )
    table.assert_not_called()


def test_scd2_groups_custom_predicate_with_current_guard() -> None:
    df = MagicMock()
    merge = MagicMock()
    merge.whenMatchedUpdate.return_value = merge
    table = MagicMock()
    table.forPath.return_value.alias.return_value.merge.return_value = merge
    with (
        patch.object(delta, "delta_table", return_value=table),
        patch.object(delta.file_io, "require_portable_timestamp_output"),
        patch.object(delta.file_io, "write_to_path"),
    ):
        delta.scd2_to_path(
            MagicMock(),
            df,
            "/table",
            ["id"],
            "delta",
            None,
            {"predicate": "a OR b"},
        )
    condition = table.forPath.return_value.alias.return_value.merge.call_args.args[1]
    assert condition == "(a OR b) AND target.`__is_current` = true"


def test_merge_to_path_uses_merge_alias_options() -> None:
    df = MagicMock()
    df.columns = ["id", "value"]
    merge = MagicMock()
    merge.whenMatchedUpdate.return_value = merge
    merge.whenNotMatchedInsert.return_value = merge
    table = MagicMock()
    table.forPath.return_value.alias.return_value.merge.return_value = merge

    with (
        patch.object(delta, "delta_table", return_value=table),
        patch.object(delta.file_io, "require_portable_timestamp_output"),
    ):
        delta.merge_to_path(
            MagicMock(),
            df,
            "/table",
            ["id"],
            "delta",
            {"source_alias": "incoming", "target_alias": "current"},
        )

    table.forPath.return_value.alias.assert_called_once_with("current")
    df.alias.assert_called_once_with("incoming")
    merge_call = table.forPath.return_value.alias.return_value.merge.call_args
    assert merge_call.args[1] == "current.`id` = incoming.`id`"
