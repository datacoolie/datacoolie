"""Regression tests for metadata preparation artifacts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
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
