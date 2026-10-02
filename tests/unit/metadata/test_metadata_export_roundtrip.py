"""Regression tests for metadata preparation artifacts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from datacoolie.metadata.documents.excel import parse_excel


def _load_setup_metadata() -> ModuleType:
    script = Path(__file__).parents[3] / "usecase-sim" / "scripts" / "setup_metadata.py"
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
