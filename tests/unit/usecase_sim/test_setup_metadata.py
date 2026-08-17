"""Regression tests for usecase-sim metadata fan-out."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from datacoolie.metadata.file_provider import FileProvider


REPO_ROOT = Path(__file__).resolve().parents[3]
SETUP_METADATA_PATH = REPO_ROOT / "usecase-sim" / "scripts" / "setup_metadata.py"
SPEC = importlib.util.spec_from_file_location(
    "usecase_sim_setup_metadata",
    SETUP_METADATA_PATH,
)
assert SPEC and SPEC.loader
setup_metadata = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(setup_metadata)


def _minimal_metadata() -> dict:
    return {
        "connections": [
            {"name": "source", "connection_type": "file", "format": "csv"},
            {"name": "destination", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [
            {
                "name": "copy_rows",
                "source": {"connection_name": "source", "table": "rows"},
                "destination": {
                    "connection_name": "destination",
                    "table": "rows",
                    "load_type": "overwrite",
                },
            }
        ],
        "schema_hints": [
            {
                "connection_name": "source",
                "table_name": "rows",
                "hints": [{"column_name": "id", "data_type": "integer"}],
            }
        ],
    }


def test_emit_xlsx_preserves_source_python_function(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    output_path = tmp_path / "metadata.xlsx"
    metadata = {
        "connections": [],
        "dataflows": [
            {
                "name": "function_source",
                "source": {
                    "connection_name": "functions",
                    "python_function": "package.module.read_rows",
                },
                "destination": {
                    "connection_name": "output",
                    "table": "rows",
                    "load_type": "overwrite",
                },
            }
        ],
        "schema_hints": [],
    }

    setup_metadata.emit_xlsx(metadata, output_path)
    parsed = FileProvider._parse_excel(str(output_path))

    assert parsed["dataflows"][0]["source"]["python_function"] == (
        "package.module.read_rows"
    )


def test_oracle_schema_guards_use_user_tables_uppercase_names() -> None:
    schema = (
        REPO_ROOT / "usecase-sim" / "metadata" / "database" / "schema_oracle.sql"
    ).read_text(encoding="utf-8")

    for table in setup_metadata._REQUIRED_METADATA_TABLES:
        assert f"table_name = '{table.upper()}'" in schema
        assert f"table_name = '{table}'" not in schema


def test_validate_required_tables_reports_missing_table() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        for table in sorted(
            setup_metadata._REQUIRED_METADATA_TABLES - {"dc_framework_watermarks"}
        ):
            conn.execute(text(f"CREATE TABLE {table} (id TEXT)"))

    with pytest.raises(RuntimeError, match="missing tables: dc_framework_watermarks"):
        setup_metadata._validate_required_tables(engine)


def test_seed_db_validates_expected_records_and_is_idempotent(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'metadata.db'}"
    metadata = _minimal_metadata()

    setup_metadata.seed_db(database_url, metadata, "test-workspace", truncate=True)
    setup_metadata.seed_db(database_url, metadata, "test-workspace", truncate=False)

    engine = create_engine(database_url)
    setup_metadata._validate_seeded_metadata(engine, metadata, "test-workspace")


def test_validate_seeded_metadata_reports_missing_record(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'metadata.db'}"
    metadata = _minimal_metadata()
    setup_metadata.seed_db(database_url, metadata, "test-workspace", truncate=True)
    engine = create_engine(database_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM dc_framework_dataflows "
                "WHERE workspace_id = 'test-workspace'"
            )
        )

    with pytest.raises(RuntimeError, match="dataflows: copy_rows"):
        setup_metadata._validate_seeded_metadata(engine, metadata, "test-workspace")
