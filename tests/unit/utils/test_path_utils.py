"""Tests for datacoolie.utils.path_utils."""

from __future__ import annotations

from datacoolie.utils.path_utils import build_path, normalize_path


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
