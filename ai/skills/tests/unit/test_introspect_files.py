"""test_introspect_files.py — Unit tests for file/folder introspection script.

Uses actual fixture files at tests/fixtures/files/.
"""
import csv
import io
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import introspect_files

FIXTURES = Path(__file__).parent.parent / "fixtures" / "files"


# ---------------------------------------------------------------------------
# Arrow type mapping
# ---------------------------------------------------------------------------

class TestMapArrowType:
    def test_int32(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.int32())[0] == "integer"

    def test_int64(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.int64())[0] == "long"

    def test_float64(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.float64())[0] == "double"

    def test_string(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.string())[0] == "string"

    def test_boolean(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.bool_())[0] == "boolean"

    def test_date32(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.date32())[0] == "date"

    def test_timestamp(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.timestamp("us"))[0] == "timestamp"

    def test_timestamp_tz(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.timestamp("us", tz="UTC"))[0] == "timestamp_tz"

    def test_decimal(self):
        import pyarrow as pa
        canonical, _, prec, scl = introspect_files._map_arrow_type(pa.decimal128(10, 2))
        assert canonical == "decimal(10,2)"
        assert prec == "10"
        assert scl == "2"

    def test_list_type(self):
        import pyarrow as pa
        assert introspect_files._map_arrow_type(pa.list_(pa.int32()))[0] == "array"

    def test_struct_type(self):
        import pyarrow as pa
        t = pa.struct([pa.field("x", pa.int32())])
        assert introspect_files._map_arrow_type(t)[0] == "struct"


# ---------------------------------------------------------------------------
# Delta type mapping
# ---------------------------------------------------------------------------

class TestMapDeltaType:
    def test_primitive_long(self):
        from deltalake import DeltaTable
        dt = DeltaTable(str(FIXTURES / "delta_products"))
        field = dt.schema().fields[0]
        canonical, _, _, _ = introspect_files._map_delta_type(field.type)
        assert canonical == "long"

    def test_string_fallback(self):
        # Simulate a type with .type attribute
        from unittest.mock import MagicMock
        mock = MagicMock()
        mock.type = "string"
        canonical, _, _, _ = introspect_files._map_delta_type(mock)
        assert canonical == "string"


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

class TestDetectFormat:
    def test_parquet_extension(self):
        assert introspect_files._detect_format_from_path("data/sales.parquet") == "parquet"

    def test_csv_extension(self):
        assert introspect_files._detect_format_from_path("data/products.csv") == "csv"

    def test_json_extension(self):
        assert introspect_files._detect_format_from_path("data/users.json") == "json"

    def test_jsonl_extension(self):
        assert introspect_files._detect_format_from_path("data/events.jsonl") == "json"

    def test_unknown_extension(self):
        assert introspect_files._detect_format_from_path("data/readme.md") is None

    def test_avro_extension(self):
        assert introspect_files._detect_format_from_path("data/inventory.avro") == "avro"


# ---------------------------------------------------------------------------
# CSV header contract
# ---------------------------------------------------------------------------

class TestCsvContract:
    def test_header_has_14_columns(self):
        assert len(introspect_files.CSV_HEADER) == 14


# ---------------------------------------------------------------------------
# Schema extraction — Parquet
# ---------------------------------------------------------------------------

class TestSchemaParquet:
    @pytest.fixture
    def parquet_path(self):
        return str(FIXTURES / "sales.parquet")

    def test_outputs_csv(self, parquet_path, capsys):
        introspect_files.cmd_schema(
            path=parquet_path,
            fmt="parquet",
            source="test",
            table="sales",
            output_path=None,
            storage_options=None,
        )
        output = capsys.readouterr().out
        reader = csv.reader(io.StringIO(output))
        rows = list(reader)
        assert rows[0] == introspect_files.CSV_HEADER
        assert len(rows) > 1
        # All rows have source=test
        assert all(r[0] == "test" for r in rows[1:])
        # All rows have table=sales
        assert all(r[2] == "sales" for r in rows[1:])

    def test_ordinal_positions_sequential(self, parquet_path, capsys):
        introspect_files.cmd_schema(
            path=parquet_path, fmt="parquet", source="t", table="t",
            output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        reader = csv.reader(io.StringIO(output))
        rows = list(reader)
        ordinals = [int(r[11]) for r in rows[1:]]
        assert ordinals == list(range(1, len(ordinals) + 1))


# ---------------------------------------------------------------------------
# Schema extraction — CSV
# ---------------------------------------------------------------------------

class TestSchemaCsv:
    def test_csv_fixture(self, capsys):
        path = str(FIXTURES / "products.csv")
        introspect_files.cmd_schema(
            path=path, fmt="csv", source="test", table="products",
            output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        reader = csv.reader(io.StringIO(output))
        rows = list(reader)
        assert len(rows) > 1
        # Has "inferred from sample rows" in notes
        assert any("inferred" in r[13] for r in rows[1:])


# ---------------------------------------------------------------------------
# Schema extraction — JSON
# ---------------------------------------------------------------------------

class TestSchemaJson:
    def test_jsonl_fixture(self, capsys):
        """Test with JSONL fixture (one object per line — pyarrow-compatible)."""
        path = str(FIXTURES / "events.jsonl")
        if not Path(path).exists():
            pytest.skip("events.jsonl fixture missing")
        introspect_files.cmd_schema(
            path=path, fmt="json", source="test", table="events",
            output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        reader = csv.reader(io.StringIO(output))
        rows = list(reader)
        assert len(rows) > 1


# ---------------------------------------------------------------------------
# Schema extraction — Delta
# ---------------------------------------------------------------------------

class TestSchemaDelta:
    def test_delta_fixture(self, capsys):
        path = str(FIXTURES / "delta_products")
        introspect_files.cmd_schema(
            path=path, fmt="delta", source="test", table="delta_products",
            output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        reader = csv.reader(io.StringIO(output))
        rows = list(reader)
        assert len(rows) > 1
        # Check canonical types are clean (no PrimitiveType wrapper)
        types = [r[4] for r in rows[1:]]
        assert not any("PrimitiveType" in t for t in types)


# ---------------------------------------------------------------------------
# Structure subcommand
# ---------------------------------------------------------------------------

class TestStructure:
    def test_structure_fixtures(self, capsys):
        introspect_files.cmd_structure(
            path=str(FIXTURES), output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        assert "# Folder Structure" in output
        assert "## Tree" in output
        assert "## Summary" in output

    def test_structure_detects_delta(self, capsys):
        introspect_files.cmd_structure(
            path=str(FIXTURES), output_path=None, storage_options=None,
        )
        output = capsys.readouterr().out
        assert "Delta table" in output


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_structure_minimal(self):
        args = introspect_files.parse_args(["structure", "--path", "/data"])
        assert args.command == "structure"
        assert args.path == "/data"

    def test_schema_minimal(self):
        args = introspect_files.parse_args([
            "schema", "--path", "/data/f.parquet", "--source", "s", "--table", "t",
        ])
        assert args.command == "schema"
        assert args.format == "auto"

    def test_schema_with_format(self):
        args = introspect_files.parse_args([
            "schema", "--path", "/data", "--format", "delta", "--source", "s", "--table", "t",
        ])
        assert args.format == "delta"
