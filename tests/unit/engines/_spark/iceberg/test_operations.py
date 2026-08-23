from unittest.mock import MagicMock

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


def test_extract_catalog_uses_first_identifier_level() -> None:
    assert operations.extract_catalog("catalog.namespace.table") == "catalog"
