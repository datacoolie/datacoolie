"""Unit tests for _file_introspect — max_files limit and _resolve_storage_options."""
import os
import sys
from pathlib import Path

import pytest

from _file_introspect import _introspect_local, _resolve_storage_options, introspect_files
from _types import SourceResult


# ---------------------------------------------------------------------------
# _resolve_storage_options
# ---------------------------------------------------------------------------

class TestResolveStorageOptions:
    def test_none_returns_empty(self):
        assert _resolve_storage_options(None) == {}

    def test_plain_values_passthrough(self):
        opts = {"key": "AKIATEST", "secret": "sec", "endpoint_url": "http://minio:9000"}
        assert _resolve_storage_options(opts) == opts

    def test_env_var_resolved(self, monkeypatch):
        monkeypatch.setenv("S3_KEY", "resolved-key")
        opts = {"key": "$S3_KEY", "secret": "plain"}
        resolved = _resolve_storage_options(opts)
        assert resolved["key"] == "resolved-key"
        assert resolved["secret"] == "plain"

    def test_missing_env_var_kept_as_is_with_warning(self, monkeypatch, capsys):
        monkeypatch.delenv("MISSING_KEY", raising=False)
        opts = {"key": "$MISSING_KEY"}
        resolved = _resolve_storage_options(opts)
        # Value is kept as-is (not resolved)
        assert resolved["key"] == "$MISSING_KEY"
        captured = capsys.readouterr()
        assert "MISSING_KEY" in captured.err

    def test_nested_dict_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "tok123")
        opts = {"client_kwargs": {"token": "$MY_TOKEN"}}
        resolved = _resolve_storage_options(opts)
        assert resolved["client_kwargs"]["token"] == "tok123"


# ---------------------------------------------------------------------------
# max_files limit via _introspect_local
# ---------------------------------------------------------------------------

class TestMaxFilesLimit:
    def _make_csv_files(self, tmp_path: Path, count: int) -> None:
        for i in range(count):
            (tmp_path / f"table_{i}.csv").write_text(f"id,value\n{i},row\n")

    def test_no_limit_returns_all(self, tmp_path):
        self._make_csv_files(tmp_path, 5)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=1000)
        assert len(result.tables) == 5

    def test_max_files_1_returns_one_table(self, tmp_path):
        self._make_csv_files(tmp_path, 5)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=1)
        assert len(result.tables) <= 1

    def test_max_files_exact_boundary(self, tmp_path):
        self._make_csv_files(tmp_path, 3)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=3)
        assert len(result.tables) == 3

    def test_max_files_exceeded_emits_warning(self, tmp_path, capsys):
        self._make_csv_files(tmp_path, 5)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=2)
        captured = capsys.readouterr()
        assert "WARN" in captured.err
        assert "--max-files" in captured.err

    def test_max_files_not_exceeded_no_warning(self, tmp_path, capsys):
        self._make_csv_files(tmp_path, 3)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=10)
        captured = capsys.readouterr()
        assert "WARN" not in captured.err

    def test_max_files_0_returns_empty(self, tmp_path, capsys):
        self._make_csv_files(tmp_path, 3)
        result = SourceResult(source_type="file")
        _introspect_local(str(tmp_path), result, max_files=0)
        assert len(result.tables) == 0


# ---------------------------------------------------------------------------
# introspect_files — integration smoke with tmp fixtures (no Docker)
# ---------------------------------------------------------------------------

class TestIntrospectFilesLocal:
    def test_csv_and_json_detected(self, tmp_path):
        (tmp_path / "orders.csv").write_text("order_id,amount,status\n1,99.9,paid\n")
        (tmp_path / "users.json").write_text('[{"user_id": 1, "name": "Alice"}]')
        result = introspect_files(str(tmp_path))
        table_names = {t.table_name for t in result.tables}
        assert "orders" in table_names
        assert "users" in table_names

    def test_unsupported_ext_ignored(self, tmp_path):
        (tmp_path / "readme.txt").write_text("ignore me")
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")
        result = introspect_files(str(tmp_path))
        table_names = {t.table_name for t in result.tables}
        assert "readme" not in table_names
        assert "data" in table_names

    def test_empty_dir_returns_no_tables(self, tmp_path):
        result = introspect_files(str(tmp_path))
        assert result.tables == []

    def test_nonexistent_path_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            introspect_files(str(tmp_path / "does_not_exist"))
