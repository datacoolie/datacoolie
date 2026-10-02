"""Contract tests for inline and file-backed ``Source.query`` values."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacoolie.core.exceptions import MetadataError, PlatformError
from datacoolie.orchestration.preparation.query import classify_query, resolve_query
from datacoolie.platforms.base import BasePlatform
from datacoolie.platforms.local_platform import LocalPlatform


class TestClassifyQuery:
    def test_inline_sql_is_preserved_without_io(self) -> None:
        declared = "  SELECT * FROM orders.sql WHERE id > 1;  "

        reference = classify_query(declared)

        assert reference.kind == "inline"
        assert reference.declared == declared

    @pytest.mark.parametrize(
        "declared",
        ["orders.sql", "orders/incremental.sql", "sql/orders/incremental.sql"],
    )
    def test_relative_sql_shorthand_is_a_file(self, declared: str) -> None:
        reference = classify_query(declared)

        assert reference.kind == "file"
        assert reference.scheme == "shorthand"
        assert reference.relative_path == declared

    def test_explicit_artifact_reference_allows_nonstandard_filename(self) -> None:
        declared = " artifact:/sql/orders latest.txt "

        reference = classify_query(declared)

        assert reference.kind == "file"
        assert reference.scheme == "artifact"
        assert reference.declared == declared
        assert reference.relative_path == "sql/orders latest.txt"

    @pytest.mark.parametrize(
        "declared",
        [
            "artifact://bucket/orders.sql",
            "artifact:/../orders.sql",
            "artifact:/%2e%2e/orders.sql",
            "/release/orders.sql",
            "C:/release/orders.sql",
            "C:/release/query files/orders.sql",
            "s3://bucket/orders.sql",
            "dbfs:/release/orders.sql",
            "orders.sql?version=2",
        ],
    )
    def test_unsafe_or_ambiguous_file_reference_fails(self, declared: str) -> None:
        with pytest.raises(MetadataError):
            classify_query(declared)

    def test_sql_comments_are_not_misclassified(self) -> None:
        reference = classify_query("-- explain orders.sql\nSELECT 1")

        assert reference.kind == "inline"


class TestResolveQuery:
    def test_inline_query_does_not_read_platform(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        declared = "SELECT 1"

        assert resolve_query(declared, platform, artifact_base_path="artifact") == declared
        platform.read_file_under_base.assert_not_called()

    def test_artifact_relative_query_never_probes_manifest(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        platform.file_exists.side_effect = AssertionError("runtime must not inspect manifests")
        platform.read_file_under_base.return_value = "SELECT 1;"

        assert (
            resolve_query(
                "nested/orders.sql",
                platform,
                artifact_base_path="artifact",
            )
            == "SELECT 1;"
        )
        platform.file_exists.assert_not_called()
        platform.read_file.assert_not_called()
        platform.read_file_under_base.assert_called_once_with(
            "artifact", "nested/orders.sql"
        )

    def test_sql_root_takes_precedence_for_shorthand(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        platform.read_file_under_base.return_value = "\nSELECT * FROM orders\n"

        content = resolve_query(
            "orders.sql",
            platform,
            sql_base_path="release/sql",
            artifact_base_path="release",
        )

        assert content == "\nSELECT * FROM orders\n"
        platform.read_file_under_base.assert_called_once_with(
            "release/sql", "orders.sql"
        )

    def test_explicit_artifact_reference_does_not_fallback_to_sql_root(self) -> None:
        platform = MagicMock(spec=BasePlatform)
        platform.read_file_under_base.side_effect = PlatformError("missing")

        with pytest.raises(MetadataError, match="Cannot read SQL file"):
            resolve_query(
                "artifact:/sql/orders.sql",
                platform,
                sql_base_path="release/sql",
                artifact_base_path="release",
            )

        platform.read_file_under_base.assert_called_once_with("release", "sql/orders.sql")

    def test_missing_root_is_a_preparation_error(self) -> None:
        platform = MagicMock(spec=BasePlatform)

        with pytest.raises(MetadataError, match="requires artifact_base_path"):
            resolve_query("orders.sql", platform)

        platform.read_file_under_base.assert_not_called()

    def test_blank_sql_root_does_not_fallback_to_artifact_root(self) -> None:
        platform = MagicMock(spec=BasePlatform)

        with pytest.raises(MetadataError, match="requires sql_base_path"):
            resolve_query(
                "orders.sql",
                platform,
                sql_base_path="",
                artifact_base_path="release",
            )

        platform.read_file_under_base.assert_not_called()

    def test_local_platform_reads_selected_root(self, tmp_path: Path) -> None:
        sql_root = tmp_path / "sql"
        sql_root.mkdir()
        (sql_root / "orders.sql").write_text("SELECT 42;", encoding="utf-8")

        assert resolve_query(
            "orders.sql",
            LocalPlatform(),
            sql_base_path=str(sql_root),
        ) == "SELECT 42;"

    def test_empty_sql_file_fails(self, tmp_path: Path) -> None:
        sql_root = tmp_path / "sql"
        sql_root.mkdir()
        (sql_root / "empty.sql").write_text("  \n", encoding="utf-8")

        with pytest.raises(MetadataError, match="is empty"):
            resolve_query("empty.sql", LocalPlatform(), sql_base_path=str(sql_root))
