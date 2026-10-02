from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import EngineError
from datacoolie.engines._spark.iceberg import operations


def test_compaction_honors_individual_procedure_toggles() -> None:
    spark = MagicMock()
    operations.compact_by_name(
        spark,
        "catalog.namespace.table",
        {
            "rewrite_data_files": True,
            "rewrite_position_delete_files": False,
            "rewrite_manifests": True,
        },
    )
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert len(statements) == 2
    assert "rewrite_data_files" in statements[0]
    assert "rewrite_manifests" in statements[1]


def test_path_existence_uses_platform_metadata_directory() -> None:
    platform = MagicMock()
    platform.folder_exists.return_value = True
    assert operations.table_exists_by_path(MagicMock(), platform, "s3://bucket/table/")
    platform.folder_exists.assert_called_once_with("s3://bucket/table/metadata")


def test_path_existence_propagates_platform_probe_errors() -> None:
    platform = MagicMock()
    platform.folder_exists.side_effect = TimeoutError("offline")

    with pytest.raises(TimeoutError, match="offline"):
        operations.table_exists_by_path(MagicMock(), platform, "/table")


def test_path_existence_rejects_occupied_non_iceberg_target() -> None:
    spark = MagicMock()
    platform = MagicMock()
    platform.folder_exists.side_effect = lambda path: path == "/table"
    platform.file_exists.return_value = False
    platform.list_files.return_value = [MagicMock()]
    platform.list_folders.return_value = []

    with pytest.raises(EngineError, match="no metadata directory"):
        operations.table_exists_by_path(spark, platform, "/table")
    spark.read.format.assert_not_called()


def test_extract_catalog_uses_first_identifier_level() -> None:
    assert operations.extract_catalog("catalog.namespace.table") == "catalog"
