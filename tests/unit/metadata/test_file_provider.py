"""Tests for FileProvider — YAML/JSON config loading, connection resolution,
dataflow building, watermark I/O, and schema hints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import WATERMARK_FILE_NAME
from datacoolie.core.exceptions import MetadataError, WatermarkError
from datacoolie.core.models import DataFlow, SchemaHint
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


MINIMAL_CONFIG: Dict[str, Any] = {
    "connections": [
        {
            "connection_id": "c-1",
            "name": "bronze_adls",
            "connection_type": "lakehouse",
            "format": "delta",
            "configure": {"base_path": "abfss://bronze@storage/"},
        },
        {
            "connection_id": "c-2",
            "name": "silver_lakehouse",
            "connection_type": "lakehouse",
            "format": "delta",
            "configure": {"base_path": "abfss://silver@storage/", "use_schema_hint": True},
        },
    ],
    "dataflows": [
        {
            "dataflow_id": "df-1",
            "name": "orders_flow",
            "stage": "bronze2silver",
            "source": {
                "connection_name": "bronze_adls",
                "table": "orders",
                "watermark_columns": ["modified_at"],
            },
            "destination": {
                "connection_name": "silver_lakehouse",
                "table": "dim_orders",
                "load_type": "merge_upsert",
                "merge_keys": ["order_id"],
            },
            "transform": {
                "schema_hints": [
                    {"column_name": "amount", "data_type": "DECIMAL", "precision": 18, "scale": 2},
                ],
            },
        },
    ],
    "schema_hints": [
        {
            "connection_name": "silver_lakehouse",
            "table_name": "dim_orders",
            "hints": [
                {"column_name": "order_date", "data_type": "DATE", "format": "yyyy-MM-dd"},
            ],
        },
    ],
}


def _write_json_config(tmp_path: Path, data: Dict[str, Any], name: str = "config.json") -> str:
    """Write a JSON config file and return its path string."""
    config_path = tmp_path / name
    config_path.write_text(json.dumps(data), encoding="utf-8")
    return str(config_path)


def _write_yaml_config(tmp_path: Path, data: Dict[str, Any], name: str = "config.yaml") -> str:
    """Write a YAML config file and return its path string."""
    import yaml

    config_path = tmp_path / name
    config_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
    return str(config_path)


# ===========================================================================
# Constructor / config loading
# ===========================================================================


class TestFileProviderInit:
    """Configuration loading and parsing."""

    def test_load_json_config(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conns = provider.get_connections(active_only=False)
        assert len(conns) == 2

    def test_load_yaml_config(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        path = _write_yaml_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conns = provider.get_connections(active_only=False)
        assert len(conns) == 2

    def test_missing_config_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(MetadataError, match="Cannot read"):
            FileProvider(config_path=str(tmp_path / "nope.json"), platform=LocalPlatform())

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("not json!!!", encoding="utf-8")
        with pytest.raises(MetadataError, match="Cannot parse"):
            FileProvider(config_path=str(bad), platform=LocalPlatform())

    def test_invalid_yaml_raises(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        bad = tmp_path / "bad.yaml"
        # A YAML list at root level is invalid for our purposes
        bad.write_text("- item1\n- item2", encoding="utf-8")
        with pytest.raises(MetadataError, match="must contain a mapping|Cannot parse"):
            FileProvider(config_path=str(bad), platform=LocalPlatform())

    def test_empty_yaml_gives_empty_data(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        empty = tmp_path / "empty.yaml"
        empty.write_text("", encoding="utf-8")
        provider = FileProvider(config_path=str(empty), platform=LocalPlatform())
        assert provider.get_connections() == []

    def test_custom_watermark_base_path(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path="/custom/wm",
        )
        assert provider._watermark_base_path == "/custom/wm"

    def test_separate_overlay_files_override_sections(self, tmp_path: Path) -> None:
        base = {
            "connections": [{"connection_id": "c-1", "name": "base_conn", "connection_type": "file", "format": "parquet"}],
            "dataflows": [],
            "schema_hints": [{"connection_name": "x", "table_name": "t", "hints": [{"column_name": "a", "data_type": "STRING"}]}],
        }
        base_path = _write_json_config(tmp_path, base, name="base.json")
        conn_overlay = _write_json_config(
            tmp_path,
            {"connections": [{"connection_id": "c-2", "name": "overlay_conn", "connection_type": "file", "format": "parquet"}]},
            name="connections.json",
        )
        hints_overlay = _write_json_config(
            tmp_path,
            {"schema_hints": [{"connection_name": "overlay_conn", "table_name": "orders", "hints": [{"column_name": "id", "data_type": "INT"}]}]},
            name="hints.json",
        )

        provider = FileProvider(
            config_path=base_path,
            connections_path=conn_overlay,
            schema_hints_path=hints_overlay,
            platform=LocalPlatform(),
        )
        assert provider.get_connection_by_name("overlay_conn") is not None
        hints = provider.get_schema_hints("c-2", "orders")
        assert len(hints) == 1


class TestFileProviderParserHelpers:
    def test_cast_and_bool_helpers(self) -> None:
        assert FileProvider._cast("  x  ") == "x"
        assert FileProvider._cast("   ") is None
        assert FileProvider._safe_bool("true") is True
        assert FileProvider._safe_bool("definitely-not-bool") is False

    def test_json_cell_helpers(self) -> None:
        assert FileProvider._json_cell(None) is None
        assert FileProvider._json_cell('{"a": 1}') == {"a": 1}
        with pytest.raises(MetadataError, match="Invalid JSON cell value"):
            FileProvider._json_cell("not-json{")

    def test_excel_sheet_rows_missing_sheet(self) -> None:
        wb = MagicMock()
        wb.sheetnames = ["connections"]
        assert FileProvider._excel_sheet_rows(wb, "dataflows") == []

    def test_parse_excel_import_error(self) -> None:
        with patch("builtins.__import__") as imp:
            real_import = __import__

            def _side_effect(name, *args, **kwargs):
                if name == "openpyxl":
                    raise ImportError("missing")
                return real_import(name, *args, **kwargs)

            imp.side_effect = _side_effect
            with pytest.raises(MetadataError, match="openpyxl"):
                FileProvider._parse_excel("/tmp/no.xlsx")

    def test_parse_excel_load_workbook_error(self) -> None:
        with patch("openpyxl.load_workbook", side_effect=RuntimeError("bad file")):
            with pytest.raises(MetadataError, match="Cannot read Excel metadata file"):
                FileProvider._parse_excel("/tmp/no.xlsx")

    def test_load_file_excel_wraps_unexpected_exception(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with patch.object(FileProvider, "_parse_excel", side_effect=RuntimeError("bad")):
            with pytest.raises(MetadataError, match="Cannot parse metadata config"):
                provider._load_file("dummy.xlsx")

    def test_load_file_excel_reraises_metadata_error(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with patch.object(FileProvider, "_parse_excel", side_effect=MetadataError("bad excel")):
            with pytest.raises(MetadataError, match="bad excel"):
                provider._load_file("dummy.xlsx")

    def test_parse_yaml_missing_dependency(self) -> None:
        import builtins

        real_import = builtins.__import__

        def _block_yaml(name, *args, **kwargs):
            if name == "yaml":
                raise ImportError("No module named yaml")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_block_yaml):
            with pytest.raises(MetadataError, match="PyYAML"):
                FileProvider._parse_yaml("a: 1")

    def test_excel_sheet_rows_parses_values(self) -> None:
        wb = MagicMock()
        ws = MagicMock()
        wb.sheetnames = ["connections"]
        wb.__getitem__.return_value = ws

        class _Cell:
            def __init__(self, value):
                self.value = value

        ws.iter_rows.side_effect = [
            iter([(_Cell("name"), _Cell("format"))]),
            [("c1", "parquet"), (None, None), ("c2", "delta")],
        ]

        rows = FileProvider._excel_sheet_rows(wb, "connections")
        assert rows == [
            {"name": "c1", "format": "parquet"},
            {"name": "c2", "format": "delta"},
        ]

    def test_parse_excel_success_with_mocked_workbook(self) -> None:
        wb = MagicMock()
        with patch("openpyxl.load_workbook", return_value=wb):
            with patch.object(FileProvider, "_parse_excel_connections", return_value=[{"name": "c"}]):
                with patch.object(FileProvider, "_parse_excel_dataflows", return_value=[{"name": "d"}]):
                    with patch.object(FileProvider, "_parse_excel_schema_hints", return_value=[{"h": 1}]):
                        out = FileProvider._parse_excel("/tmp/file.xlsx")
        assert out["connections"][0]["name"] == "c"
        assert out["dataflows"][0]["name"] == "d"
        assert out["schema_hints"][0]["h"] == 1
        wb.close.assert_called_once()

    def test_parse_excel_connections_mixed_fields(self) -> None:
        rows = [
            {
                "name": "conn_a",
                "connection_id": "c-1",
                "configure": '{"host": "h"}',
                "configure_port": "1433",
                "secrets_ref": '{"scope": ["pwd"]}',
                "is_active": "true",
            }
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_connections(MagicMock())
        assert out[0]["configure"]["host"] == "h"
        assert out[0]["configure"]["port"] == "1433"
        assert out[0]["secrets_ref"] == {"scope": ["pwd"]}
        assert out[0]["is_active"] is True

    def test_parse_excel_dataflows_transform_merge_and_lists(self) -> None:
        rows = [
            {
                "name": "df",
                "source_connection_name": "src",
                "source_table": "orders",
                "destination_connection_name": "dst",
                "destination_table": "dim_orders",
                "destination_merge_keys": "id,order_id",
                "transform": '{"schema_hints": [{"column_name": "a", "data_type": "STRING"}]}',
                "transform_configure": '{"x": 1}',
                "is_active": "false",
            }
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_dataflows(MagicMock())
        assert out[0]["is_active"] is False
        assert out[0]["destination"]["merge_keys"] == ["id", "order_id"]
        assert out[0]["transform"]["schema_hints"][0]["column_name"] == "a"
        assert out[0]["transform"]["configure"]["x"] == 1

    def test_parse_excel_schema_hints_grouping(self) -> None:
        rows = [
            {
                "connection_name": "conn_a",
                "table_name": "orders",
                "schema_name": "dbo",
                "column_name": "id",
                "data_type": "INT",
                "precision": "10",
                "scale": "0",
            },
            {
                "connection_name": "conn_a",
                "table_name": "orders",
                "schema_name": "dbo",
                "column_name": "amount",
                "data_type": "DECIMAL",
                "precision": "18",
                "scale": "2",
            },
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_schema_hints(MagicMock())
        assert len(out) == 1
        assert len(out[0]["hints"]) == 2
        assert out[0]["hints"][1]["column_name"] == "amount"

    def test_resolve_connection_inline_dict(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider._resolve_connection(
            {
                "connection_id": "inline-1",
                "name": "inline_conn",
                "connection_type": "file",
                "format": "parquet",
            }
        )
        assert conn.name == "inline_conn"

    def test_build_dataflows_filters_inactive(self, tmp_path: Path) -> None:
        data = {
            "connections": MINIMAL_CONFIG["connections"],
            "dataflows": [
                {
                    "dataflow_id": "df-active",
                    "name": "a",
                    "is_active": True,
                    "source": {"connection_name": "bronze_adls", "table": "t"},
                    "destination": {"connection_name": "silver_lakehouse", "table": "t"},
                },
                {
                    "dataflow_id": "df-inactive",
                    "name": "b",
                    "is_active": False,
                    "source": {"connection_name": "bronze_adls", "table": "t"},
                    "destination": {"connection_name": "silver_lakehouse", "table": "t"},
                },
            ],
            "schema_hints": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        out = provider.get_dataflows(attach_schema_hints=False)
        assert len(out) == 1
        assert out[0].dataflow_id == "df-active"

    def test_parse_excel_connections_branch_matrix(self) -> None:
        rows = [
            {
                "configure": "{}",                 # dict -> cfg update (empty)
                "configure_skip": "",              # None after _cast -> skipped
                "secrets_ref": "",                # _json_cell returns None -> skipped
                "is_active": "true",              # bool branch
                "name": "conn_a",                 # val not None branch
            },
            {
                "configure": "",                  # _json_cell None -> branch false
                "name": "",                       # val None branch
            },
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_connections(MagicMock())
        assert len(out) == 1
        assert out[0]["name"] == "conn_a"
        assert out[0]["is_active"] is True

    def test_parse_excel_dataflows_sparse_rows_cover_branches(self) -> None:
        rows = [
            {
                "transform": "",                           # transform parsed as None
                "source_watermark_columns": "",            # ensure_list empty branch
                "source_table": "",                        # cast none branch
                "destination_table": "orders",             # non-empty normal field
                "transform_configure": "",                 # json none branch
            },
            {
                # Empty row dict leads to no src/dest/transform and not appended
            },
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_dataflows(MagicMock())
        assert len(out) == 1
        assert out[0]["destination"]["table"] == "orders"
        assert "source" not in out[0]

    def test_parse_excel_dataflows_non_prefixed_none_value_skipped(self) -> None:
        rows = [
            {
                "name": "",  # non-prefixed scalar column -> _cast(None) path
            }
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_dataflows(MagicMock())
        assert out == []

    def test_parse_excel_schema_hints_missing_fields_and_non_int_precision(self) -> None:
        rows = [
            {
                "connection_name": None,            # triggers missing key continue
                "table_name": "orders",
                "column_name": "id",
                "data_type": "INT",
            },
            {
                "connection_name": "conn_a",
                "table_name": "orders",
                "schema_name": None,               # schema_name is None branch
                "column_name": "id",
                "data_type": "INT",
                "precision": "",                 # converted is None branch
                "scale": "0",
            },
            {
                "connection_name": "conn_a",
                "table_name": "orders",
                "schema_name": None,
                # no column_name/data_type/format -> hint remains empty branch
            },
        ]
        with patch.object(FileProvider, "_excel_sheet_rows", return_value=rows):
            out = FileProvider._parse_excel_schema_hints(MagicMock())
        assert len(out) == 1
        assert len(out[0]["hints"]) == 1
        assert out[0]["hints"][0]["column_name"] == "id"


# ===========================================================================
# Connection methods
# ===========================================================================


class TestFileProviderConnections:
    """Connection loading and filtering."""

    def test_get_connections_all(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert len(provider.get_connections(active_only=False)) == 2

    def test_get_connection_by_id(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_id("c-1")
        assert conn is not None
        assert conn.name == "bronze_adls"

    def test_get_connection_by_id_not_found(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert provider.get_connection_by_id("missing") is None

    def test_get_connection_by_name(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("silver_lakehouse")
        assert conn is not None
        assert conn.connection_id == "c-2"

    def test_get_connection_by_name_not_found(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert provider.get_connection_by_name("nope") is None

    def test_inactive_connection_filtered(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["connections"] = [
            {
                "connection_id": "c-inactive",
                "name": "old",
                "connection_type": "file",
                "is_active": False,
            },
        ]
        data["dataflows"] = []
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert len(provider.get_connections(active_only=True)) == 0
        assert len(provider.get_connections(active_only=False)) == 1

    def test_invalid_connection_raises(self, tmp_path: Path) -> None:
        data = {"connections": [{"name": ""}]}  # empty name → validation error
        path = _write_json_config(tmp_path, data)
        with pytest.raises(MetadataError, match="Invalid connection"):
            FileProvider(config_path=path, platform=LocalPlatform()).get_connections()

    def test_database_as_direct_field(self, tmp_path: Path) -> None:
        data = {
            "connections": [
                {
                    "connection_id": "c-db",
                    "name": "source_erp",
                    "connection_type": "database",
                    "format": "sql",
                    "database": "ERP",
                    "configure": {"host": "erp.example.com", "port": 1433},
                },
            ],
            "dataflows": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("source_erp")
        assert conn is not None
        assert conn.database == "ERP"

    def test_database_fallback_from_config(self, tmp_path: Path) -> None:
        """database in config dict is still promoted to the field for back-compat."""
        data = {
            "connections": [
                {
                    "connection_id": "c-db",
                    "name": "source_erp",
                    "connection_type": "database",
                    "format": "sql",
                    "configure": {"host": "erp.example.com", "database": "LEGACY"},
                },
            ],
            "dataflows": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("source_erp")
        assert conn is not None
        assert conn.database == "LEGACY"

    def test_secrets_ref_dict_in_config(self, tmp_path: Path) -> None:
        """secrets_ref provided as a dict is preserved on the Connection model."""
        data = {
            "connections": [
                {
                    "connection_id": "c-s",
                    "name": "secure_conn",
                    "connection_type": "database",
                    "format": "sql",
                    "secrets_ref": {"my-scope": ["password", "api_key"]},
                    "configure": {"host": "db.example.com", "password": "vault/db-pass", "api_key": "vault/api-key"},
                },
            ],
            "dataflows": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("secure_conn")
        assert conn is not None
        assert conn.secrets_ref == {"my-scope": ["password", "api_key"]}

    def test_secrets_ref_json_string_in_config(self, tmp_path: Path) -> None:
        """secrets_ref provided as a JSON string is coerced to dict by the model."""
        data = {
            "connections": [
                {
                    "connection_id": "c-s2",
                    "name": "secure_conn2",
                    "connection_type": "database",
                    "format": "sql",
                    "secrets_ref": '{"my-scope": ["password"]}',
                    "configure": {"host": "db.example.com", "password": "vault/db-pass"},
                },
            ],
            "dataflows": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("secure_conn2")
        assert conn is not None
        assert conn.secrets_ref == {"my-scope": ["password"]}

    def test_secrets_ref_none_when_absent(self, tmp_path: Path) -> None:
        """secrets_ref defaults to None when not specified."""
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        conn = provider.get_connection_by_name("bronze_adls")
        assert conn is not None
        assert conn.secrets_ref is None


# ===========================================================================
# Dataflow methods
# ===========================================================================


class TestFileProviderDataflows:
    """Dataflow building and connection resolution."""

    def test_get_dataflows(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        dfs = provider.get_dataflows(attach_schema_hints=False)
        assert len(dfs) == 1
        assert dfs[0].name == "orders_flow"
        assert dfs[0].source.table == "orders"
        assert dfs[0].destination.table == "dim_orders"

    def test_connection_name_resolution(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        dfs = provider.get_dataflows(attach_schema_hints=False)
        # Source connection resolved from "bronze_adls" name
        assert dfs[0].source.connection.name == "bronze_adls"
        assert dfs[0].destination.connection.name == "silver_lakehouse"

    def test_missing_source_connection_raises(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["dataflows"] = [
            {
                "name": "bad",
                "source": {"connection_name": "nonexistent", "table": "t"},
                "destination": {"connection_name": "silver_lakehouse", "table": "t"},
            },
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="Connection not found"):
            provider.get_dataflows()

    def test_missing_destination_connection_raises(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["dataflows"] = [
            {
                "name": "bad",
                "source": {"connection_name": "bronze_adls", "table": "t"},
                "destination": {"connection_name": "nonexistent", "table": "t"},
            },
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="Connection not found"):
            provider.get_dataflows()

    def test_source_without_connection_key_raises(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["dataflows"] = [
            {
                "name": "no_src_conn",
                "source": {"table": "t"},
                "destination": {"connection_name": "silver_lakehouse", "table": "t"},
            },
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="source must have"):
            provider.get_dataflows()

    def test_destination_without_connection_key_raises(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["dataflows"] = [
            {
                "name": "no_dest_conn",
                "source": {"connection_name": "bronze_adls", "table": "t"},
                "destination": {"table": "t"},
            },
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="destination must have"):
            provider.get_dataflows()

    def test_get_dataflow_by_id(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        df = provider.get_dataflow_by_id("df-1", attach_schema_hints=False)
        assert df is not None
        assert df.name == "orders_flow"

    def test_get_dataflow_by_id_not_found(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert provider.get_dataflow_by_id("nope") is None

    def test_get_dataflows_filters_by_stage(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert len(provider.get_dataflows(stage="bronze2silver", attach_schema_hints=False)) == 1
        assert len(provider.get_dataflows(stage="gold", attach_schema_hints=False)) == 0

    def test_inline_transform(self, tmp_path: Path) -> None:
        """Transform section on the dataflow is parsed."""
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        dfs = provider.get_dataflows(attach_schema_hints=False)
        assert dfs[0].transform.schema_hints[0].column_name == "amount"

    def test_build_dataflows_invalid_shape_wrapped(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["dataflows"] = [
            {
                "name": "bad_df",
                "source": {"connection_name": "bronze_adls", "table": "t"},
                "destination": {"connection_name": "silver_lakehouse", "table": "t"},
                # transform must be mapping; this forces generic error wrapping branch
                "transform": "not-a-dict",
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="Invalid dataflow definition"):
            provider.get_dataflows(attach_schema_hints=False)


# ===========================================================================
# Schema hints
# ===========================================================================


class TestFileProviderSchemaHints:
    """Schema hint loading from the schema_hints section."""

    def test_get_schema_hints_by_connection_name(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        hints = provider.get_schema_hints(
            connection_id="c-2",
            table_name="dim_orders",
        )
        assert len(hints) == 1
        assert hints[0].column_name == "order_date"

    def test_get_schema_hints_no_match(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        hints = provider.get_schema_hints(connection_id="c-1", table_name="nonexistent")
        assert hints == []

    def test_schema_hints_caching(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        h1 = provider.get_schema_hints(connection_id="c-2", table_name="dim_orders")
        h2 = provider.get_schema_hints(connection_id="c-2", table_name="dim_orders")
        assert h1 == h2  # same result from cache

    def test_invalid_schema_hint_raises(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["schema_hints"] = [
            {
                "connection_name": "silver_lakehouse",
                "table_name": "dim_orders",
                "hints": [{"column_name": "", "data_type": "INT"}],  # empty name
            },
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="Invalid schema hint"):
            provider.get_schema_hints(connection_id="c-2", table_name="dim_orders")

    def test_schema_hints_schema_name_filter_case_insensitive(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["schema_hints"] = [
            {
                "connection_name": "silver_lakehouse",
                "table_name": "dim_orders",
                "schema_name": "dbo",
                "hints": [{"column_name": "id", "data_type": "INT"}],
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        hints = provider.get_schema_hints(connection_id="c-2", table_name="DIM_ORDERS", schema_name="DBO")
        assert len(hints) == 1

    def test_schema_hints_schema_name_mismatch_returns_empty(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data["schema_hints"] = [
            {
                "connection_name": "silver_lakehouse",
                "table_name": "dim_orders",
                "schema_name": "sales",
                "hints": [{"column_name": "id", "data_type": "INT"}],
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert provider.get_schema_hints(connection_id="c-2", table_name="dim_orders", schema_name="dbo") == []

    def test_schema_hints_table_name_mismatch_returns_empty(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert provider.get_schema_hints(connection_id="c-2", table_name="another_table") == []


# ===========================================================================
# Watermark I/O
# ===========================================================================


class TestFileProviderWatermark:
    """Watermark read/write via the local filesystem."""

    def test_get_watermark_file_not_exists(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=str(tmp_path / "watermarks"),
        )
        assert provider.get_watermark("df-1") is None

    def test_update_and_get_watermark(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        wm_base = str(tmp_path / "watermarks")
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=wm_base,
        )
        wm_json = json.dumps({"modified_at": "2025-01-01T00:00:00"})
        provider.update_watermark("df-1", wm_json)

        raw = provider.get_watermark("df-1")
        assert raw is not None
        result = json.loads(raw)
        assert result["modified_at"] == "2025-01-01T00:00:00"

    def test_watermark_path_construction(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path="/wm",
        )
        assert provider._watermark_path("df-1") == f"/wm/bronze2silver_orders_flow_df-1/{WATERMARK_FILE_NAME}"

    def test_watermark_overwrite(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        wm_base = str(tmp_path / "watermarks")
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=wm_base,
        )
        provider.update_watermark("df-1", json.dumps({"v": 1}))
        provider.update_watermark("df-1", json.dumps({"v": 2}))
        raw = provider.get_watermark("df-1")
        assert json.loads(raw)["v"] == 2

    def test_get_watermark_empty_file(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        wm_base = str(tmp_path / "watermarks")
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=wm_base,
        )
        # Create an empty watermark file
        wm_file = Path(wm_base) / "bronze2silver_orders_flow_df-1" / WATERMARK_FILE_NAME
        wm_file.parent.mkdir(parents=True, exist_ok=True)
        wm_file.write_text("", encoding="utf-8")
        assert provider.get_watermark("df-1") is None

    def test_get_watermark_invalid_json_returns_raw(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        wm_base = str(tmp_path / "watermarks")
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=wm_base,
        )
        wm_file = Path(wm_base) / "bronze2silver_orders_flow_df-1" / WATERMARK_FILE_NAME
        wm_file.parent.mkdir(parents=True, exist_ok=True)
        wm_file.write_text("not valid json!", encoding="utf-8")
        # Provider returns the raw string; validation is WatermarkManager's responsibility
        raw = provider.get_watermark("df-1")
        assert raw == "not valid json!"

    def test_get_watermark_platform_error_wrapped(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        platform = MagicMock()
        platform.read_file.return_value = json.dumps(MINIMAL_CONFIG)
        platform.file_exists.side_effect = RuntimeError("boom")
        provider = FileProvider(config_path=path, platform=platform)
        with pytest.raises(WatermarkError, match="Cannot read watermark"):
            provider.get_watermark("df-1")

    def test_update_watermark_platform_error_wrapped(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        platform = MagicMock()
        platform.read_file.return_value = json.dumps(MINIMAL_CONFIG)
        platform.write_file.side_effect = RuntimeError("boom")
        provider = FileProvider(config_path=path, platform=platform)
        with pytest.raises(WatermarkError, match="Cannot write watermark"):
            provider.update_watermark("df-1", '{"v": 1}')


# ===========================================================================
# Lifecycle
# ===========================================================================


class TestFileProviderLifecycle:
    """Context manager / close clears internal state."""

    def test_close_clears_data(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert len(provider._data) > 0
        provider.close()
        assert len(provider._data) == 0

    def test_context_manager(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        with FileProvider(config_path=path, platform=LocalPlatform()) as provider:
            assert len(provider.get_connections()) > 0
        # After exit, internal data cleared
        assert len(provider._data) == 0
