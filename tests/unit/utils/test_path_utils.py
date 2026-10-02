"""Tests for datacoolie.utils.path_utils."""

from __future__ import annotations

import pytest

from datacoolie.utils.path_utils import (
    build_path,
    ensure_relative_path,
    is_path_within,
    join_path,
    normalize_path,
    parent_path,
)


class TestNormalizePath:
    def test_trailing_slash(self) -> None:
        assert normalize_path("/data/bronze/") == "/data/bronze"

    def test_backslashes(self) -> None:
        assert normalize_path("C:\\data\\bronze") == "C:/data/bronze"

    def test_double_slashes(self) -> None:
        assert normalize_path("/data//bronze") == "/data/bronze"

    def test_abfss_prefix_preserved(self) -> None:
        path = "abfss://container@account.dfs.core.windows.net/dir/"
        result = normalize_path(path)
        assert result.startswith("abfss://")
        assert result.endswith("/dir")

    def test_empty_returns_empty(self) -> None:
        assert normalize_path("") == ""

    def test_none_returns_empty(self) -> None:
        assert normalize_path(None) == ""

    def test_no_op_for_clean_path(self) -> None:
        assert normalize_path("/data/bronze") == "/data/bronze"

    def test_s3_prefix(self) -> None:
        path = "s3://bucket/prefix//double/"
        result = normalize_path(path)
        assert result.startswith("s3://")
        assert "//" not in result.split("://")[1]

    def test_file_protocol(self) -> None:
        path = "file://C://path//to//file/"
        result = normalize_path(path)
        assert result.startswith("file://")
        assert not result.endswith("/")


class TestBuildPath:
    def test_simple(self) -> None:
        result = build_path("data", "bronze", "orders")
        assert result == "data/bronze/orders"

    def test_with_trailing_slashes(self) -> None:
        result = build_path("data/", "bronze/", "orders")
        assert result == "data/bronze/orders"

    def test_abfss_prefix(self) -> None:
        result = build_path("abfss://container@account.dfs.core.windows.net/", "dir", "sub")
        assert result.startswith("abfss://")
        assert "dir/sub" in result

    def test_single_part(self) -> None:
        assert build_path("data") == "data"

    def test_empty_parts_skipped(self) -> None:
        result = build_path("data", "", "bronze")
        assert result == "data/bronze"

    def test_none_parts_skipped(self) -> None:
        result = build_path("data", None, "bronze")
        assert result == "data/bronze"

    def test_all_none_parts_empty(self) -> None:
        result =build_path(None, None)
        assert result == ""

    def test_all_empty_parts_empty(self) -> None:
        result = build_path("", "", "")
        assert result == ""

    def test_whitespace_only_parts_skipped(self) -> None:
        result = build_path("data", "  ", "bronze")
        assert result == "data/bronze"

    def test_s3_prefix(self) -> None:
        result = build_path("s3://bucket/", "prefix", "file")
        assert result.startswith("s3://")
        assert "prefix/file" in result

    def test_file_protocol(self) -> None:
        result = build_path("file://C:/home", "user", "data.csv")
        assert result.startswith("file://")
        assert "user/data.csv" in result

    def test_protocol_with_no_rest(self) -> None:
        """Protocol prefix with nothing after :// part."""
        result = build_path("s3://bucket", "data", "file.csv")
        assert "data/file.csv" in result

    def test_protocol_handling_with_slashes(self) -> None:
        """Protocol prefix followed by slashes in path."""
        result = build_path("s3://bucket///", "data")
        assert result.count("://") == 1  # Only one protocol marker
        assert "data" in result

    def test_protocol_rest_matches_segments(self) -> None:
        """Protocol rest part matches first extracted segment."""
        # s3://prefix/path where 'prefix/path' matches segment 'prefix'
        result = build_path("s3://prefix", "suffix")
        assert "s3://" in result
        assert "suffix" in result

    def test_protocol_rest_differs_from_segment(self) -> None:
        """Protocol rest part differs from first extracted segment."""
        # s3://original/path, then add 'different/path'
        # rest_of_first will be different from segments[0]
        result = build_path("s3://bucket/rest", "data", "file")
        assert "s3://" in result
        assert "file" in result


class TestScopedPathHelpers:
    @pytest.mark.parametrize(
        ("base", "relative", "expected"),
        [
            ("/release", "sql/orders.sql", "/release/sql/orders.sql"),
            ("C:/release", "sql/orders.sql", "C:/release/sql/orders.sql"),
            ("s3://bucket/release", "sql/orders.sql", "s3://bucket/release/sql/orders.sql"),
            ("dbfs:/release", "sql/orders.sql", "dbfs:/release/sql/orders.sql"),
        ],
    )
    def test_join_preserves_storage_root(self, base: str, relative: str, expected: str) -> None:
        assert join_path(base, relative) == expected

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/logs", "/"),
            ("C:/logs", "C:/"),
            ("s3://bucket/logs", "s3://bucket"),
            ("runtime/logs", "runtime"),
        ],
    )
    def test_parent_preserves_storage_root(self, path: str, expected: str) -> None:
        assert parent_path(path) == expected

    @pytest.mark.parametrize("relative", ["/absolute.sql", "../escape.sql", "s3://bucket/x.sql"])
    def test_scoped_join_rejects_escape(self, relative: str) -> None:
        with pytest.raises(ValueError):
            join_path("release", relative)

    def test_containment_compares_uri_authority_and_segments(self) -> None:
        assert is_path_within("s3://bucket/release", "s3://bucket/release/sql/orders.sql")
        assert not is_path_within("s3://bucket/release", "s3://other/release/sql/orders.sql")
        assert not is_path_within("release", "release-other/orders.sql")

    def test_relative_path_canonicalizes_repeated_separators(self) -> None:
        assert ensure_relative_path("sql//orders.sql") == "sql/orders.sql"
        assert ensure_relative_path("./sql/./orders.sql") == "sql/orders.sql"

    def test_empty_uri_authority_keeps_three_slash_root(self) -> None:
        assert parent_path("file:///tmp/logs") == "file:///tmp"
        assert join_path("file:///tmp", "query.sql") == "file:///tmp/query.sql"
        assert parent_path("file:///") == "file:///"

    def test_current_directory_base_is_contained(self) -> None:
        assert join_path(".", "query.sql") == "query.sql"
        assert is_path_within(".", "query.sql")
