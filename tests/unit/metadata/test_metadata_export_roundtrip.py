"""Regression tests for metadata preparation artifacts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest

from datacoolie.metadata.documents.excel import parse_excel


def test_database_only_setup_uses_memory_without_creating_exports(tmp_path, monkeypatch):
    setup = _load_setup_metadata()
    document = {"connections": [], "dataflows": [], "schema_hints": []}
    source = tmp_path / "source.json"
    source.write_text(json.dumps(document))
    original = source.read_bytes()
    output = tmp_path / "unused_exports"
    seeded = []
    monkeypatch.setattr(setup, "seed_db", lambda url, meta, workspace, truncate:
                        seeded.append((meta, workspace, truncate)))
    monkeypatch.setattr("sys.argv", ["setup_metadata", "--json", str(source),
                        "--targets", "db:sqlite", "--output-dir", str(output)])
    monkeypatch.chdir(tmp_path)
    assert setup.main() == 0
    assert seeded == [(document, "local-workspace", False)]
    assert source.read_bytes() == original
    assert not output.exists()
    assert list(tmp_path.iterdir()) == [source]


def test_explicit_file_setup_exports_snapshot_at_requested_destination(tmp_path, monkeypatch):
    setup = _load_setup_metadata()
    document = {"connections": [], "dataflows": [], "schema_hints": []}
    source = tmp_path / "source.json"
    source.write_text(json.dumps(document))
    output = tmp_path / "exports"
    emitted = []
    monkeypatch.setattr(setup, "emit_yaml", lambda meta, path: emitted.append((meta, path)))
    monkeypatch.setattr(setup, "emit_xlsx", lambda meta, path: emitted.append((meta, path)))
    monkeypatch.setattr("sys.argv", ["setup_metadata", "--json", str(source),
                        "--targets", "file", "--output-dir", str(output)])
    assert setup.main() == 0
    assert json.loads((output / source.name).read_text()) == document
    assert emitted == [(document, output / "source.yaml"), (document, output / "source.xlsx")]


def test_database_setup_reads_exact_group_files_and_shared_configuration(tmp_path, monkeypatch):
    setup = _load_setup_metadata()
    one, two, shared = [tmp_path / name for name in ("one.json", "two.json", "connections.json")]
    one.write_text(json.dumps({"dataflows": [{"name": "one"}]}))
    two.write_text(json.dumps({"dataflows": [{"name": "two"}]}))
    shared.write_text(json.dumps({"connections": [{"name": "shared"}]}))
    seeded = []
    monkeypatch.setattr(setup, "seed_db", lambda url, meta, workspace, truncate: seeded.append(meta))
    monkeypatch.setattr("sys.argv", ["setup_metadata", "--json", str(one), "--json", str(two),
                        "--connections-path", str(shared), "--targets", "db:sqlite"])
    assert setup.main() == 0
    assert seeded == [{"connections": [{"name": "shared"}],
                       "dataflows": [{"name": "one"}, {"name": "two"}], "schema_hints": []}]


def test_export_refuses_source_collision_before_writing_or_seeding(tmp_path, monkeypatch):
    setup = _load_setup_metadata()
    source = tmp_path / "group.json"
    source.write_text(json.dumps({"dataflows": [{"name": "one"}]}))
    original = source.read_bytes()
    monkeypatch.setattr(setup, "seed_db", lambda *args, **kwargs: pytest.fail("Must reject before seeding"))
    monkeypatch.setattr("sys.argv", ["setup_metadata", "--json", str(source),
                        "--targets", "file,db:sqlite", "--output-dir", str(tmp_path)])
    assert setup.main() == 2
    assert source.read_bytes() == original
    assert list(tmp_path.iterdir()) == [source]


def _load_setup_metadata() -> ModuleType:
    script = Path(__file__).parents[3] / "usecase-sim" / "scripts" / "bootstrap" / "setup_metadata.py"
    spec = importlib.util.spec_from_file_location("setup_metadata_for_test", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {script}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(
    importlib.util.find_spec("openpyxl") is None,
    reason="openpyxl is required for the Excel export round-trip",
)
def test_emit_xlsx_preserves_source_filter_expression(tmp_path: Path) -> None:
    setup_metadata = _load_setup_metadata()
    metadata = {
        "connections": [
            {
                "name": "source",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/input"},
            },
            {
                "name": "destination",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/output"},
            },
        ],
        "dataflows": [
            {
                "name": "orders",
                "source": {
                    "connection_name": "source",
                    "table": "orders",
                    "filter_expression": "status = 'open'",
                },
                "destination": {
                    "connection_name": "destination",
                    "table": "orders",
                    "load_type": "append",
                },
            }
        ],
    }

    workbook_path = tmp_path / "metadata.xlsx"
    setup_metadata.emit_xlsx(metadata, workbook_path)

    parsed = parse_excel(str(workbook_path))

    assert parsed["dataflows"][0]["source"]["filter_expression"] == "status = 'open'"


@pytest.mark.skipif(
    importlib.util.find_spec("openpyxl") is None,
    reason="openpyxl is required for the Excel identity round-trip",
)
def test_emit_xlsx_preserves_exact_identity_strings_and_workspace_scope(tmp_path: Path) -> None:
    setup_metadata = _load_setup_metadata()
    source_name = " 源 🧭 "
    destination_name = " 目标 🧪 "
    flow_name = " 作业 🚚 "
    metadata = {
        "connections": [
            {
                "name": source_name,
                "connection_id": " source-explicit ",
                "workspace_id": "ws-a",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/input"},
            },
            {
                "name": destination_name,
                "connection_id": " destination-explicit ",
                "workspace_id": "ws-a",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/output"},
            },
        ],
        "dataflows": [
            {
                "name": flow_name,
                "dataflow_id": " flow-explicit ",
                "workspace_id": "ws-a",
                "source": {
                    "connection_name": source_name,
                    "table": "orders",
                },
                "destination": {
                    "connection_name": destination_name,
                    "table": "orders",
                    "load_type": "append",
                },
            }
        ],
        "schema_hints": [{
            "connection_name": source_name,
            "connection_id": " source-explicit ",
            "workspace_id": "ws-a",
            "dataflow_id": " flow-explicit ",
            "table_name": "orders",
            "hints": [{
                "schema_hint_id": " hint-explicit ",
                "column_name": " order_id ",
                "data_type": "BIGINT",
            }],
        }],
    }
    workbook_path = tmp_path / "identity.xlsx"
    setup_metadata.emit_xlsx(metadata, workbook_path)
    parsed = parse_excel(str(workbook_path))

    assert [row["name"] for row in parsed["connections"]] == [
        source_name, destination_name
    ]
    assert [row["connection_id"] for row in parsed["connections"]] == [
        " source-explicit ", " destination-explicit "
    ]
    assert parsed["dataflows"][0]["name"] == flow_name
    assert parsed["dataflows"][0]["dataflow_id"] == " flow-explicit "
    assert parsed["dataflows"][0]["source"]["connection_name"] == source_name
    assert parsed["dataflows"][0]["destination"]["connection_name"] == destination_name
    assert all(row["workspace_id"] == "ws-a" for row in parsed["connections"])
    assert parsed["dataflows"][0]["workspace_id"] == "ws-a"
    assert parsed["schema_hints"][0]["connection_name"] == source_name
    assert parsed["schema_hints"][0]["connection_id"] == " source-explicit "
    assert parsed["schema_hints"][0]["dataflow_id"] == " flow-explicit "
    assert parsed["schema_hints"][0]["hints"][0]["schema_hint_id"] == " hint-explicit "
    assert parsed["schema_hints"][0]["hints"][0]["column_name"] == "order_id"

    from datacoolie.metadata.file_provider import FileProvider
    from datacoolie.platforms.local_platform import LocalPlatform

    provider = FileProvider(
        config_path=str(workbook_path),
        platform=LocalPlatform(),
        watermark_base_path=str(tmp_path / "watermarks"),
    )
    try:
        provider.initialize()
        flow = provider.get_dataflows()[0]
        assert flow.name == flow_name
        assert flow.dataflow_id == " flow-explicit "
        assert flow.workspace_id == "ws-a"
        assert flow.source.connection.name == source_name
        assert flow.source.connection.connection_id == " source-explicit "
    finally:
        provider.close()


def test_seed_and_verify_preserve_explicit_ids_and_id_only_hint_refs(tmp_path: Path) -> None:
    setup = _load_setup_metadata()
    verify_path = Path(__file__).parents[3] / "usecase-sim" / "metadata" / "database" / "verify_metadata.py"
    verify_spec = importlib.util.spec_from_file_location("verify_metadata_for_export_test", verify_path)
    if verify_spec is None or verify_spec.loader is None:
        raise RuntimeError(f"Cannot load {verify_path}")
    verify = importlib.util.module_from_spec(verify_spec)
    sys.modules[verify_spec.name] = verify
    verify_spec.loader.exec_module(verify)

    document = {
        "connections": [
            {
                "name": "same-name",
                "connection_id": "src-explicit",
                "connection_type": "file",
                "format": "parquet",
            },
            {
                "name": "same-name",
                "connection_id": "dst-explicit",
                "connection_type": "file",
                "format": "parquet",
            },
        ],
        "dataflows": [{
            "name": "flow-explicit",
            "dataflow_id": "flow-explicit-id",
            "source": {"connection_id": "src-explicit", "table": "orders"},
            "destination": {
                "connection_id": "dst-explicit",
                "table": "orders",
                "load_type": "append",
            },
        }],
        "schema_hints": [{
            "connection_id": "src-explicit",
            "table_name": "orders",
            "hints": [{
                "schema_hint_id": "hint-explicit",
                "column_name": "id",
                "data_type": "BIGINT",
            }],
        }],
    }
    url = "sqlite:///" + str(tmp_path / "explicit.db").replace("\\", "/")
    setup.seed_db(url, document, "ws-a", truncate=False)
    from sqlalchemy import create_engine

    engine = create_engine(url)
    try:
        assert verify.verify_connections(document, engine, "ws-a").ok()
        assert verify.verify_dataflows(document, engine, "ws-a").ok()
        assert verify.verify_hints(document, engine, "ws-a").ok()
    finally:
        engine.dispose()


def test_seed_generated_hint_ids_use_connection_identity_and_inline_owners() -> None:
    setup = _load_setup_metadata()
    document = {
        "connections": [
            {"name": "same", "connection_id": "source-id", "connection_type": "file", "format": "parquet"},
            {"name": "same", "connection_id": "destination-id", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [{
            "name": "flow",
            "dataflow_id": "flow-id",
            "source": {"connection_id": "source-id", "table": "orders"},
            "destination": {"connection_id": "destination-id", "table": "orders", "load_type": "append"},
        }],
        "schema_hints": [
            {"connection_id": "source-id", "table_name": "orders",
             "hints": [{"column_name": "id", "data_type": "BIGINT"}]},
            {"connection_id": "destination-id", "table_name": "orders",
             "hints": [{"column_name": "id", "data_type": "BIGINT"}]},
        ],
    }
    records = setup._seed_records(document, "ws-a")
    hint_ids = [item["column"]["schema_hint_id"] for item in records["schema_hints"]]
    assert len(hint_ids) == 2
    assert len(set(hint_ids)) == 2

    inline = {
        "connections": [],
        "dataflows": [{
            "name": "inline-flow",
            "source": {"connection": {
                "name": "inline-source", "connection_id": "inline-source-id",
                "connection_type": "file", "format": "parquet",
            }, "table": "orders"},
            "destination": {"connection": {
                "name": "inline-destination", "connection_id": "inline-destination-id",
                "connection_type": "file", "format": "parquet",
            }, "table": "orders", "load_type": "append"},
        }],
        "schema_hints": [],
    }
    inline_records = setup._seed_records(inline, "ws-a")
    assert {row["connection_id"] for row in inline_records["connections"]} == {
        "inline-source-id", "inline-destination-id"
    }
    assert inline_records["dataflows"][0]["source_connection_id"] == "inline-source-id"

    mismatch = json.loads(json.dumps(inline))
    mismatch["dataflows"][0]["source"]["connection"]["name"] = "different"
    with pytest.raises(ValueError, match="conflicts with inline name"):
        setup._seed_records({
            **mismatch,
            "connections": [{
                "name": "inline-source", "connection_id": "inline-source-id",
                "connection_type": "file", "format": "parquet",
            }],
        }, "ws-a")
