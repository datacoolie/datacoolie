"""Tests for FileProvider — YAML/JSON config loading, connection resolution,
dataflow building, watermark I/O, and schema hints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datacoolie.core.constants import WATERMARK_FILE_NAME
from datacoolie.core.exceptions import ConfigurationError, MetadataError, WatermarkError
from datacoolie.metadata.documents.parsers import parse_yaml_document
from datacoolie.metadata.documents.excel import (
    cast,
    excel_sheet_rows,
    json_cell,
    parse_excel,
    parse_excel_connections,
    parse_excel_dataflows,
    parse_excel_schema_hints,
    safe_bool,
)
from datacoolie.metadata.documents.mapping import resolve_connection
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.metadata.contracts.context import MetadataProviderStartupContext
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
            "configure": {
                "base_path": "abfss://bronze@storage/",
                "schema_hint_type_system": "postgres",
            },
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
            FileProvider(config_path=str(tmp_path / "nope.json"), platform=LocalPlatform()).initialize()

    def test_invalid_json_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("not json!!!", encoding="utf-8")
        with pytest.raises(MetadataError, match="Cannot parse"):
            FileProvider(config_path=str(bad), platform=LocalPlatform()).initialize()

    def test_dataflow_retains_source_hint_type_system(self, tmp_path: Path) -> None:
        data = json.loads(json.dumps(MINIMAL_CONFIG))
        path = _write_json_config(tmp_path, data)

        provider = FileProvider(config_path=path, platform=LocalPlatform())
        dataflow = provider.get_dataflows(attach_schema_hints=True)[0]

        assert dataflow.source.connection.schema_hint_type_system == "postgres"
        assert dataflow.transform.schema_hints[0].data_type == "DECIMAL"

    @pytest.mark.parametrize("configure", [{}, {"next_link_bound_mode": "opaque"},
                                           {"next_link_bound_mode": "repeat_query_bounds"}])
    def test_source_pagination_config_preserved(self, tmp_path: Path, configure: dict) -> None:
        data = json.loads(json.dumps(MINIMAL_CONFIG))
        data["dataflows"][0]["source"]["configure"] = configure
        provider = FileProvider(config_path=_write_json_config(tmp_path, data), platform=LocalPlatform())

        dataflow = provider.get_dataflows(attach_schema_hints=False)[0]

        assert dataflow.source.configure == configure

    def test_invalid_yaml_raises(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        bad = tmp_path / "bad.yaml"
        # A YAML list at root level is invalid for our purposes
        bad.write_text("- item1\n- item2", encoding="utf-8")
        with pytest.raises(MetadataError, match="must contain a mapping|Cannot parse"):
            FileProvider(config_path=str(bad), platform=LocalPlatform()).initialize()

    def test_empty_yaml_is_rejected(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        empty = tmp_path / "empty.yaml"
        empty.write_text("", encoding="utf-8")
        provider = FileProvider(config_path=str(empty), platform=LocalPlatform())
        with pytest.raises(MetadataError, match="mapping at root level|must contain"):
            provider.get_connections()

    def test_unwrapped_document_is_rejected(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, {"name": "not-a-section"})
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="must contain a connections"):
            provider.initialize()

    def test_schema_markers_are_ignored_by_runtime_provider(self, tmp_path: Path) -> None:
        metadata = tmp_path / "metadata"
        metadata.mkdir()
        (metadata / "connections.json").write_text(
            json.dumps({
                "$schema": "https://datacoolie.github.io/datacoolie/schema/0.2.0/metadata.schema.json",
                "connections": [],
            }),
            encoding="utf-8",
        )
        (metadata / "dataflows.json").write_text(
            json.dumps({
                "$schema": "https://datacoolie.github.io/datacoolie/schema/0.1.0/metadata.schema.json",
                "dataflows": [],
            }),
            encoding="utf-8",
        )

        provider = FileProvider(metadata_base_path=str(metadata), platform=LocalPlatform())
        provider.initialize()
        assert provider.get_connections(active_only=False) == []
        assert provider.get_dataflows(active_only=False) == []

    def test_empty_explicit_overlay_is_rejected(self, tmp_path: Path) -> None:
        primary = _write_json_config(tmp_path, MINIMAL_CONFIG)
        overlay = _write_json_config(tmp_path, {}, name="connections-overlay.json")
        provider = FileProvider(
            config_path=primary,
            connections_path=overlay,
            platform=LocalPlatform(),
        )
        with pytest.raises(MetadataError, match="Explicit connections_path"):
            provider.initialize()

    def test_explicit_empty_section_is_valid(self, tmp_path: Path) -> None:
        primary_data = {**MINIMAL_CONFIG, "dataflows": [], "schema_hints": []}
        primary = _write_json_config(tmp_path, primary_data)
        overlay = _write_json_config(
            tmp_path,
            {"connections": []},
            name="connections-overlay.json",
        )
        provider = FileProvider(
            config_path=primary,
            connections_path=overlay,
            platform=LocalPlatform(),
        )
        provider.initialize()
        assert provider.get_connections() == []

    def test_custom_watermark_base_path(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path="/custom/wm",
        )
        assert provider._watermark_base_path == "/custom/wm"

    @pytest.mark.parametrize("option_name", ["connections_path", "schema_hints_path"])
    def test_blank_overlay_path_is_rejected(self, option_name: str) -> None:
        with pytest.raises(ConfigurationError, match=option_name):
            FileProvider(**{option_name: "   "})

    def test_runtime_binding_prefers_state_root(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())

        provider.configure_context(
            MetadataProviderStartupContext(
                state_base_path="runtime/state",
                log_base_path="runtime/logs",
            )
        )
        bound = provider.watermark_base_path

        assert bound == "runtime/state/watermarks"
        assert provider.watermark_base_path == bound

    def test_runtime_binding_uses_parent_of_log_root(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())

        provider.configure_context(
            MetadataProviderStartupContext(log_base_path="runtime/custom-logs")
        )
        assert provider.watermark_base_path == "runtime/watermarks"

    def test_runtime_binding_rejects_implicit_rebind(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        provider.configure_context(
            MetadataProviderStartupContext(state_base_path="runtime/a")
        )

        with pytest.raises(ConfigurationError, match="already bound"):
            provider.configure_context(
                MetadataProviderStartupContext(state_base_path="runtime/b")
            )

    def test_runtime_binding_rejects_blank_context_root(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())

        with pytest.raises(ConfigurationError, match="state_base_path"):
            provider.configure_context(
                MetadataProviderStartupContext(state_base_path=" ")
            )

    def test_explicit_watermark_root_wins_over_runtime_context(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path="runtime/explicit-watermarks",
        )

        provider.configure_context(
            MetadataProviderStartupContext(state_base_path="runtime/state")
        )
        assert provider.watermark_base_path == "runtime/explicit-watermarks"

    def test_unbound_provider_is_valid_for_metadata_but_not_watermarks(
        self, tmp_path: Path
    ) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())

        assert provider.get_connections()
        with pytest.raises(WatermarkError, match="watermark_base_path"):
            provider.get_watermark("df-1")

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
        assert cast("  x  ") == "x"
        assert cast("   ") is None
        assert safe_bool("true") is True
        assert safe_bool("definitely-not-bool") is False

    def test_json_cell_helpers(self) -> None:
        assert json_cell(None) is None
        assert json_cell('{"a": 1}') == {"a": 1}
        with pytest.raises(MetadataError, match="Invalid JSON cell value"):
            json_cell("not-json{")

    def test_excel_sheet_rows_missing_sheet(self) -> None:
        wb = MagicMock()
        wb.sheetnames = ["connections"]
        assert excel_sheet_rows(wb, "dataflows") == []

    def test_parse_excel_import_error(self) -> None:
        with patch("builtins.__import__") as imp:
            real_import = __import__

            def _side_effect(name, *args, **kwargs):
                if name == "openpyxl":
                    raise ImportError("missing")
                return real_import(name, *args, **kwargs)

            imp.side_effect = _side_effect
            with pytest.raises(MetadataError, match="openpyxl"):
                parse_excel("/tmp/no.xlsx")

    def test_parse_excel_load_workbook_error(self) -> None:
        with patch("openpyxl.load_workbook", side_effect=RuntimeError("bad file")):
            with pytest.raises(MetadataError, match="Cannot read Excel metadata file"):
                parse_excel("/tmp/no.xlsx")

    def test_parse_excel_rejects_workbook_without_metadata_sheets(self) -> None:
        wb = MagicMock()
        wb.sheetnames = ["notes"]
        with patch("openpyxl.load_workbook", return_value=wb):
            with pytest.raises(MetadataError, match="none of the supported sheets"):
                parse_excel("/tmp/notes.xlsx")
        wb.close.assert_called_once()

    def test_load_file_excel_wraps_unexpected_exception(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        provider._read_metadata_bytes = MagicMock(return_value=b"ignored")
        with patch("datacoolie.metadata.file_provider.parse_excel", side_effect=RuntimeError("bad")):
            with pytest.raises(MetadataError, match="Cannot parse metadata config"):
                provider._load_file("dummy.xlsx")

    def test_load_file_excel_reraises_metadata_error(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        provider._read_metadata_bytes = MagicMock(return_value=b"ignored")
        with patch("datacoolie.metadata.file_provider.parse_excel", side_effect=MetadataError("bad excel")):
            with pytest.raises(MetadataError, match="bad excel"):
                provider._load_file("dummy.xlsx")

    def test_load_file_rejects_legacy_xls(self, tmp_path: Path) -> None:
        provider = FileProvider(
            config_path=str(tmp_path / "metadata.xls"),
            platform=LocalPlatform(),
        )
        with pytest.raises(MetadataError, match=r"\.xls is not supported"):
            provider.initialize()

    def test_parse_yaml_missing_dependency(self) -> None:
        import builtins

        real_import = builtins.__import__

        def _block_yaml(name, *args, **kwargs):
            if name == "yaml":
                raise ImportError("No module named yaml")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_block_yaml):
            with pytest.raises(MetadataError, match="PyYAML"):
                parse_yaml_document("a: 1", "metadata.yaml")

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

        rows = excel_sheet_rows(wb, "connections")
        assert rows == [
            (2, {"name": "c1", "format": "parquet"}),
            (4, {"name": "c2", "format": "delta"}),
        ]

    def test_parse_excel_success_with_mocked_workbook(self) -> None:
        wb = MagicMock()
        wb.sheetnames = ["connections"]
        with patch("openpyxl.load_workbook", return_value=wb):
            with patch("datacoolie.metadata.documents.excel.parse_excel_connections", return_value=[{"name": "c"}]):
                with patch("datacoolie.metadata.documents.excel.parse_excel_dataflows", return_value=[{"name": "d"}]):
                    with patch("datacoolie.metadata.documents.excel.parse_excel_schema_hints", return_value=[{"h": 1}]):
                        out = parse_excel("/tmp/file.xlsx")
        assert out["connections"][0]["name"] == "c"
        assert "dataflows" not in out
        assert "schema_hints" not in out
        wb.close.assert_called_once()

    def test_parse_excel_connections_mixed_fields(self) -> None:
        rows = [
            {
                "name": "conn_a",
                "connection_id": "c-1",
                "connection_type": "file",
                "configure": '{"host": "h"}',
                "configure_port": "1433",
                "secrets_ref": '{"scope": ["pwd"]}',
                "is_active": "true",
            }
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            out = parse_excel_connections(MagicMock())
        assert out[0]["configure"]["host"] == "h"
        assert out[0]["configure"]["port"] == "1433"
        assert out[0]["secrets_ref"] == {"scope": ["pwd"]}
        assert out[0]["is_active"] is True

    def test_parse_excel_connections_rejects_nonblank_incomplete_row(self) -> None:
        rows = [{"name": "missing-type"}]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            with pytest.raises(MetadataError, match="connections row 2"):
                parse_excel_connections(
                    MagicMock(),
                    source_path="metadata.xlsx",
                )

    def test_parse_excel_dataflows_transform_merge_and_lists(self) -> None:
        rows = [
            {
                "name": "df",
                "source_connection_name": "src",
                "source_table": "orders",
                "source_filter_expression": "status = 'open'",
                "destination_connection_name": "dst",
                "destination_table": "dim_orders",
                "destination_merge_keys": "id,order_id",
                "transform": '{"schema_hints": [{"column_name": "a", "data_type": "STRING"}]}',
                "transform_select_columns": "id,email",
                "transform_rename_columns": '{"email": "contact_email"}',
                "transform_value_rules": '[{"operation": "trim", "columns": ["email"]}]',
                "transform_hash_columns": '[{"target_column": "row_hash", "columns": ["id"]}]',
                "transform_masking_rules": '[{"method": "nullify", "columns": ["secret"]}]',
                "transform_configure": '{"x": 1}',
                "is_active": "false",
            }
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            out = parse_excel_dataflows(MagicMock())
        assert out[0]["is_active"] is False
        assert out[0]["source"]["filter_expression"] == "status = 'open'"
        assert out[0]["destination"]["merge_keys"] == ["id", "order_id"]
        assert out[0]["transform"]["schema_hints"][0]["column_name"] == "a"
        assert out[0]["transform"]["select_columns"] == ["id", "email"]
        assert out[0]["transform"]["rename_columns"] == {
            "email": "contact_email"
        }
        assert out[0]["transform"]["value_rules"][0]["operation"] == "trim"
        assert out[0]["transform"]["hash_columns"][0]["target_column"] == "row_hash"
        assert out[0]["transform"]["masking_rules"][0]["method"] == "nullify"
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
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2 + i, row) for i, row in enumerate(rows)]):
            out = parse_excel_schema_hints(MagicMock())
        assert len(out) == 1
        assert len(out[0]["hints"]) == 2
        assert out[0]["hints"][1]["column_name"] == "amount"

    def test_parse_excel_schema_hints_accepts_connection_id_reference(self) -> None:
        rows = [
            {
                "connection_id": "c-1",
                "table_name": "orders",
                "column_name": "id",
                "data_type": "INT",
            },
            {
                "connection_id": "c-1",
                "connection_name": "warehouse",
                "table_name": "orders",
                "column_name": "amount",
                "data_type": "DECIMAL",
            },
        ]
        with patch(
            "datacoolie.metadata.documents.excel.excel_sheet_rows",
            return_value=[(2 + i, row) for i, row in enumerate(rows)],
        ):
            out = parse_excel_schema_hints(MagicMock())
        assert out == [
            {
                "connection_id": "c-1",
                "connection_name": "warehouse",
                "table_name": "orders",
                "hints": [
                    {"column_name": "id", "data_type": "INT"},
                    {"column_name": "amount", "data_type": "DECIMAL"},
                ],
            }
        ]

    def test_resolve_connection_inline_dict(self, tmp_path: Path) -> None:
        conn = resolve_connection(
            {
                "connection_id": "inline-1",
                "name": "inline_conn",
                "connection_type": "file",
                "format": "parquet",
            },
            {},
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

    def test_clear_cache_rebuilds_models_from_retained_snapshot(self, tmp_path: Path) -> None:
        data = {
            "connections": MINIMAL_CONFIG["connections"],
            "dataflows": [
                {
                    "dataflow_id": "df-original",
                    "name": "original",
                    "source": {"connection_name": "bronze_adls", "table": "orders"},
                    "destination": {
                        "connection_name": "silver_lakehouse",
                        "table": "orders",
                    },
                }
            ],
            "schema_hints": [],
        }
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())

        loaded = provider.get_dataflows(attach_schema_hints=False)
        loaded[0].name = "mutated-by-caller"
        loaded[0].source.connection.configure["mutated"] = True
        provider.clear_cache()

        rebuilt = provider.get_dataflows(attach_schema_hints=False)
        assert rebuilt[0].name == "original"
        assert "mutated" not in rebuilt[0].source.connection.configure

    def test_parse_excel_connections_branch_matrix(self) -> None:
        rows = [
            {
                "configure": "{}",                 # dict -> cfg update (empty)
                "configure_skip": "",              # None after _cast -> skipped
                "secrets_ref": "",                # _json_cell returns None -> skipped
                "is_active": "true",              # bool branch
                "name": "conn_a",                 # val not None branch
                "connection_type": "file",
            },
            {
                # A fully blank row is ignored by the workbook parser.
            },
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2 + i, row) for i, row in enumerate(rows)]):
            out = parse_excel_connections(MagicMock())
        assert len(out) == 1
        assert out[0]["name"] == "conn_a"
        assert out[0]["is_active"] is True

    def test_parse_excel_dataflows_sparse_rows_cover_branches(self) -> None:
        rows = [
            {
                "transform": "",                           # transform parsed as None
                "source_watermark_columns": "",            # ensure_list empty branch
                "source_table": "",                        # cast none branch
                "source_connection_name": "src",
                "source_query": "SELECT 1",
                "destination_connection_name": "dst",
                "destination_table": "orders",             # non-empty normal field
                "transform_configure": "",                 # json none branch
            },
            {
                # Empty row dict leads to no src/dest/transform and not appended
            },
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            out = parse_excel_dataflows(MagicMock())
        assert len(out) == 1
        assert out[0]["destination"]["table"] == "orders"
        assert out[0]["source"]["query"] == "SELECT 1"

    def test_parse_excel_dataflows_non_prefixed_none_value_skipped(self) -> None:
        rows = [
            {
                "name": "",  # non-prefixed scalar column -> _cast(None) path
            }
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            assert parse_excel_dataflows(MagicMock()) == []

    def test_parse_excel_dataflows_rejects_nonblank_incomplete_row(self) -> None:
        rows = [{"name": "missing-source"}]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            with pytest.raises(MetadataError, match="dataflows row 2"):
                parse_excel_dataflows(
                    MagicMock(),
                    source_path="metadata.xlsx",
                )

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
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2 + i, row) for i, row in enumerate(rows)]):
            with pytest.raises(MetadataError, match="connection_name and table_name"):
                parse_excel_schema_hints(MagicMock())

        invalid_number = {
            "connection_name": "conn_a",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INT",
            "precision": "not-an-integer",
        }
        with patch(
            "datacoolie.metadata.documents.excel.excel_sheet_rows",
            return_value=[(7, invalid_number)],
        ):
            with pytest.raises(MetadataError, match="schema_hints row 7.*precision"):
                parse_excel_schema_hints(MagicMock(), source_path="metadata.xlsx")

    @pytest.mark.parametrize("field", ["precision", "scale", "ordinal_position"])
    @pytest.mark.parametrize("value", ["3.9", 3.9, True, float("nan"), float("inf")])
    def test_parse_excel_schema_hints_rejects_non_integer_numeric_cells(
        self,
        field: str,
        value: Any,
    ) -> None:
        row = {
            "connection_name": "conn_a",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INT",
            field: value,
        }
        with patch(
            "datacoolie.metadata.documents.excel.excel_sheet_rows",
            return_value=[(7, row)],
        ):
            with pytest.raises(MetadataError, match=f"schema_hints row 7.*{field}"):
                parse_excel_schema_hints(MagicMock(), source_path="metadata.xlsx")

    def test_parse_excel_schema_hints_accepts_whole_values_without_float_rounding(self) -> None:
        row = {
            "connection_name": "conn_a",
            "table_name": "orders",
            "column_name": "id",
            "data_type": "INT",
            "precision": "9007199254740993.0",
            "scale": "0.0",
            "ordinal_position": 1.0,
        }
        with patch(
            "datacoolie.metadata.documents.excel.excel_sheet_rows",
            return_value=[(7, row)],
        ):
            result = parse_excel_schema_hints(MagicMock(), source_path="metadata.xlsx")

        hint = result[0]["hints"][0]
        assert hint["precision"] == 9007199254740993
        assert hint["scale"] == 0
        assert hint["ordinal_position"] == 1


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
        data["schema_hints"] = []
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        assert len(provider.get_connections(active_only=True)) == 0
        assert len(provider.get_connections(active_only=False)) == 1

    def test_invalid_connection_raises(self, tmp_path: Path) -> None:
        data = {"connections": [{"name": ""}]}  # empty name → validation error
        path = _write_json_config(tmp_path, data)
        with pytest.raises(MetadataError, match="Invalid connection definition"):
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

    def test_watermark_path_preserves_uri_root(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path="s3://bucket/",
        )

        assert provider._watermark_path("df-1") == (
            f"s3://bucket/bronze2silver_orders_flow_df-1/{WATERMARK_FILE_NAME}"
        )

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
        provider = FileProvider(
            config_path=path,
            platform=platform,
            watermark_base_path=str(tmp_path / "watermarks"),
        )
        with pytest.raises(WatermarkError, match="Cannot read watermark"):
            provider.get_watermark("df-1")

    def test_update_watermark_platform_error_wrapped(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        platform = MagicMock()
        platform.read_file.return_value = json.dumps(MINIMAL_CONFIG)
        platform.write_file.side_effect = RuntimeError("boom")
        provider = FileProvider(
            config_path=path,
            platform=platform,
            watermark_base_path=str(tmp_path / "watermarks"),
        )
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
        provider.initialize()
        assert len(provider._data) > 0
        provider.close()
        assert len(provider._data) == 0

    def test_context_manager(self, tmp_path: Path) -> None:
        path = _write_json_config(tmp_path, MINIMAL_CONFIG)
        with FileProvider(config_path=path, platform=LocalPlatform()) as provider:
            assert len(provider.get_connections()) > 0
        # After exit, internal data cleared
        assert len(provider._data) == 0


class TestFileProviderSchemaHintFields:
    """Cover schema hint optional fields (lines 448, 451, 454)."""

    def test_schema_hint_with_default_value(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data['schema_hints'] = [
            {
                'connection_name': 'silver_lakehouse',
                'table_name': 'orders',
                'hints': [
                    {
                        'column_name': 'status',
                        'data_type': 'STRING',
                        'default_value': 'pending',
                    }
                ],
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        hints = provider.get_schema_hints(connection_id='c-2', table_name='orders')
        assert len(hints) == 1

    def test_schema_hint_with_ordinal_position_and_is_active(self, tmp_path: Path) -> None:
        data = dict(MINIMAL_CONFIG)
        data['schema_hints'] = [
            {
                'connection_name': 'silver_lakehouse',
                'table_name': 'orders',
                'hints': [
                    {
                        'column_name': 'id',
                        'data_type': 'INT',
                        'ordinal_position': '1',
                        'is_active': 'true',
                    }
                ],
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        hints = provider.get_schema_hints(connection_id='c-2', table_name='orders')
        assert len(hints) == 1

    def test_watermark_path_with_stage_and_name(self, tmp_path: Path) -> None:
        """Line 632: _watermark_path uses stage and name when available."""
        data = dict(MINIMAL_CONFIG)
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(
            config_path=path,
            platform=LocalPlatform(),
            watermark_base_path=str(tmp_path / "wm"),
        )
        # df-1 has stage='bronze2silver' and name='orders_flow'
        wm_path = provider._watermark_path('df-1')
        # Path should include stage and name components
        assert 'bronze2silver' in wm_path or 'orders_flow' in wm_path or 'df-1' in wm_path

    def test_bulk_load_populates_schema_hints(self, tmp_path: Path) -> None:
        """Lines 657-692: _bulk_load groups schema hints by (conn_id, schema, table)."""
        data = dict(MINIMAL_CONFIG)
        data['schema_hints'] = [
            {
                'connection_name': 'silver_lakehouse',
                'table_name': 'orders',
                'schema_name': 'dbo',
                'hints': [{'column_name': 'id', 'data_type': 'INT'}],
            }
        ]
        path = _write_json_config(tmp_path, data)
        provider = FileProvider(config_path=path, platform=LocalPlatform())
        # Trigger bulk load by calling get_dataflows
        dataflows = provider.get_dataflows()
        assert dataflows is not None


class TestFileProviderBulkLoadSchemaHintEdgeCases:
    """Cover lines 662, 669, 676, 678, 682, 688-689 in _bulk_load_schema_hints."""

    def test_bulk_load_schema_hints_not_list_raises(self, tmp_path: Path) -> None:
        """Line 662: schema_hints not a list raises MetadataError."""
        config = {**MINIMAL_CONFIG, 'schema_hints': 'invalid'}
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match='schema_hints'):
            fp.get_dataflows()

    def test_bulk_load_schema_hint_group_not_dict_raises(self, tmp_path: Path) -> None:
        """Line 669: schema_hints group not a dict raises MetadataError."""
        config = {**MINIMAL_CONFIG, 'schema_hints': ['not_a_dict']}
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError):
            fp.get_dataflows()

    def test_bulk_load_schema_hint_missing_conn_or_table_raises(self, tmp_path: Path) -> None:
        """A schema-hint group must identify both its connection and table."""
        config = {
            **MINIMAL_CONFIG,
            'schema_hints': [
                {'hints': [{'column_name': 'id', 'data_type': 'INT'}]},
            ],
        }
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="missing connection and table"):
            fp.get_dataflows()

    def test_bulk_load_schema_hint_conn_ref_is_valid_id(self, tmp_path: Path) -> None:
        """Line 678: when conn_ref is a valid connection_id use directly."""
        config = {
            **MINIMAL_CONFIG,
            'schema_hints': [
                {
                    'connection_id': 'c-2',
                    'table_name': 'orders',
                    'hints': [{'column_name': 'id', 'data_type': 'INT'}],
                },
            ],
        }
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        dfs = fp.get_dataflows()
        assert dfs is not None

    def test_bulk_load_schema_hint_unknown_conn_name_raises(self, tmp_path: Path) -> None:
        """A schema-hint group must reference a configured connection."""
        config = {
            **MINIMAL_CONFIG,
            'schema_hints': [
                {
                    'connection_name': 'nonexistent_conn',
                    'table_name': 'orders',
                    'hints': [{'column_name': 'id', 'data_type': 'INT'}],
                },
            ],
        }
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match="unknown connection"):
            fp.get_dataflows()

    def test_bulk_load_schema_hint_invalid_hint_raises(self, tmp_path: Path) -> None:
        """Lines 688-689: invalid hint fields raise MetadataError."""
        config = {
            **MINIMAL_CONFIG,
            'schema_hints': [
                {
                    'connection_name': 'bronze_adls',
                    'table_name': 'orders',
                    'hints': [{'invalid_field': 'bad', 'another': 'bad'}],
                },
            ],
        }
        path = _write_json_config(tmp_path, config)
        fp = FileProvider(config_path=path, platform=LocalPlatform())
        with pytest.raises(MetadataError, match='Invalid schema hint'):
            fp.get_dataflows()


class TestFileProviderSchemaHintsOptionalFields:
    """Cover lines 448, 451, 454: optional fields in _parse_schema_hints_from_rows."""

    def test_parse_excel_schema_hints_with_optional_fields(self) -> None:
        """Lines 448, 451, 454: default_value, ordinal_position, is_active set."""
        rows = [
            {
                'connection_name': 'c1',
                'table_name': 'tbl',
                'column_name': 'col1',
                'data_type': 'VARCHAR',
                'default_value': 'N/A',
                'ordinal_position': '3',
                'is_active': 'true',
            },
        ]
        with patch("datacoolie.metadata.documents.excel.excel_sheet_rows", return_value=[(2, row) for row in rows]):
            out = parse_excel_schema_hints(MagicMock())
        assert len(out) == 1
        hint = out[0]['hints'][0]
        assert hint.get('default_value') == 'N/A'
        assert hint.get('ordinal_position') == 3
        assert hint.get('is_active') is True


class TestFileProviderWatermarkFolderNoDataflow:
    """Cover line 632: _watermark_path when dataflow is None."""

    def test_watermark_path_uses_dataflow_id_when_no_df(self, tmp_path) -> None:
        """Line 632: folder = dataflow_id when get_dataflow_by_id returns None."""
        import json
        config = {
            'connections': [{'connection_id': 'c1', 'name': 'c1', 'connection_type': 'file', 'format': 'parquet'}],
            'dataflows': [],
        }
        path = tmp_path / 'config.json'
        path.write_text(json.dumps(config))
        from datacoolie.platforms.local_platform import LocalPlatform
        from unittest.mock import patch
        provider = FileProvider(config_path=str(path), platform=LocalPlatform())
        provider._watermark_base_path
        provider._watermark_base_path = str(tmp_path / 'wm')
        # Patch get_dataflow_by_id to return None — should use dataflow_id as folder
        with patch.object(provider, 'get_dataflow_by_id', return_value=None):
            result = provider._watermark_path(dataflow_id='df-xyz')
        assert 'df-xyz' in result
