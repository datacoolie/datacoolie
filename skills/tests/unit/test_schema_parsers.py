"""Unit tests for _schema_parsers — golden schema assertions per format."""
import io
import json
import struct
import tempfile
import textwrap
from pathlib import Path

import pytest

from _schema_parsers import (
    infer_json_type,
    schema_from_bytes,
    schema_from_csv_path,
    schema_from_json_path,
    schema_from_jsonl_path,
    schema_from_parquet_fileobj,
    schema_from_excel_path,
    schema_from_avro_fileobj,
)


# ---------------------------------------------------------------------------
# infer_json_type
# ---------------------------------------------------------------------------

class TestInferJsonType:
    def test_bool(self):
        # bool check must come before int (bool is subclass of int)
        assert infer_json_type(True) == "boolean"
        assert infer_json_type(False) == "boolean"

    def test_int(self):
        assert infer_json_type(42) == "integer"

    def test_float(self):
        assert infer_json_type(3.14) == "decimal"

    def test_list(self):
        assert infer_json_type([1, 2, 3]) == "array"

    def test_dict(self):
        assert infer_json_type({"a": 1}) == "object"

    def test_string(self):
        assert infer_json_type("hello") == "string"
        assert infer_json_type("") == "string"

    def test_none_is_string(self):
        assert infer_json_type(None) == "string"


# ---------------------------------------------------------------------------
# schema_from_bytes
# ---------------------------------------------------------------------------

class TestSchemaFromBytes:
    def test_csv(self):
        content = b"id,name,age\n1,Alice,30\n2,Bob,25\n"
        schema = schema_from_bytes(content, ".csv")
        names = [f["name"] for f in schema]
        assert names == ["id", "name", "age"]
        assert all(f["data_type"] == "string" for f in schema)
        assert [f["ordinal"] for f in schema] == [1, 2, 3]

    def test_tsv(self):
        content = b"col_a\tcol_b\tcol_c\nval1\tval2\tval3\n"
        schema = schema_from_bytes(content, ".tsv")
        assert [f["name"] for f in schema] == ["col_a", "col_b", "col_c"]

    def test_json_array(self):
        data = [{"user_id": 1, "active": True, "score": 9.5}]
        content = json.dumps(data).encode()
        schema = schema_from_bytes(content, ".json")
        type_map = {f["name"]: f["data_type"] for f in schema}
        assert type_map["user_id"] == "integer"
        assert type_map["active"] == "boolean"
        assert type_map["score"] == "decimal"

    def test_json_dict(self):
        data = {"key": "value", "count": 5}
        content = json.dumps(data).encode()
        schema = schema_from_bytes(content, ".json")
        type_map = {f["name"]: f["data_type"] for f in schema}
        assert type_map["key"] == "string"
        assert type_map["count"] == "integer"

    def test_jsonl(self):
        lines = '{"event": "click", "user_id": 42, "amount": 1.5}\n{"event": "view"}\n'
        schema = schema_from_bytes(lines.encode(), ".jsonl")
        type_map = {f["name"]: f["data_type"] for f in schema}
        assert type_map["event"] == "string"
        assert type_map["user_id"] == "integer"
        assert type_map["amount"] == "decimal"

    def test_unknown_extension_returns_empty(self):
        schema = schema_from_bytes(b"data", ".xyz")
        assert schema == []

    def test_corrupt_csv_no_header(self):
        # Empty bytes — should return empty, not error key
        schema = schema_from_bytes(b"", ".csv")
        assert schema == [] or all("error" not in f for f in schema)


# ---------------------------------------------------------------------------
# schema_from_csv_path
# ---------------------------------------------------------------------------

class TestSchemaFromCsvPath:
    def test_basic_csv(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("product_id,name,price,category\n1,Widget,9.99,hardware\n")
        schema = schema_from_csv_path(f)
        assert [s["name"] for s in schema] == ["product_id", "name", "price", "category"]

    def test_tsv_via_delimiter(self, tmp_path):
        f = tmp_path / "data.tsv"
        f.write_text("col_x\tcol_y\n1\t2\n")
        schema = schema_from_csv_path(f, delimiter="\t")
        assert [s["name"] for s in schema] == ["col_x", "col_y"]

    def test_header_only_no_data_rows(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("a,b,c\n")
        schema = schema_from_csv_path(f)
        assert len(schema) == 3

    def test_missing_file_returns_error(self, tmp_path):
        schema = schema_from_csv_path(tmp_path / "nonexistent.csv")
        assert len(schema) == 1 and "error" in schema[0]


# ---------------------------------------------------------------------------
# schema_from_json_path
# ---------------------------------------------------------------------------

class TestSchemaFromJsonPath:
    def test_array_of_objects(self, tmp_path):
        f = tmp_path / "records.json"
        f.write_text(json.dumps([{"id": 1, "name": "Alice", "active": True}]))
        schema = schema_from_json_path(f)
        type_map = {s["name"]: s["data_type"] for s in schema}
        assert type_map == {"id": "integer", "name": "string", "active": "boolean"}

    def test_single_object(self, tmp_path):
        f = tmp_path / "obj.json"
        f.write_text(json.dumps({"host": "localhost", "port": 5432}))
        schema = schema_from_json_path(f)
        names = [s["name"] for s in schema]
        assert "host" in names and "port" in names

    def test_empty_array_returns_empty(self, tmp_path):
        f = tmp_path / "empty.json"
        f.write_text("[]")
        schema = schema_from_json_path(f)
        assert schema == []


# ---------------------------------------------------------------------------
# schema_from_jsonl_path
# ---------------------------------------------------------------------------

class TestSchemaFromJsonlPath:
    def test_reads_first_line_only(self, tmp_path):
        f = tmp_path / "events.jsonl"
        f.write_text('{"event_id": "e1", "ts": 1000, "payload": {}}\n{"event_id": "e2"}\n')
        schema = schema_from_jsonl_path(f)
        type_map = {s["name"]: s["data_type"] for s in schema}
        assert type_map["event_id"] == "string"
        assert type_map["ts"] == "integer"
        assert type_map["payload"] == "object"
        assert len(schema) == 3  # only 3 fields from first line

    def test_blank_first_line_returns_empty(self, tmp_path):
        f = tmp_path / "blank.jsonl"
        f.write_text("\n{\"a\": 1}\n")
        schema = schema_from_jsonl_path(f)
        assert schema == []


# ---------------------------------------------------------------------------
# schema_from_parquet_fileobj — uses actual fixture file
# ---------------------------------------------------------------------------

class TestSchemaFromParquetFileobj:
    FIXTURE = (
        Path(__file__).parent.parent
        / "fixtures" / "files"
        / "delta_products"
    )

    def test_parquet_file_from_fixture(self):
        # Find any .parquet file in delta_products/
        parquet_files = list(self.FIXTURE.glob("**/*.parquet"))
        if not parquet_files:
            pytest.skip("No parquet fixture found")
        with open(parquet_files[0], "rb") as fobj:
            schema = schema_from_parquet_fileobj(fobj)
        names = [s["name"] for s in schema]
        assert "product_id" in names
        assert "name" in names
        assert "price" in names
        assert len(schema) == 5  # product_id, name, price, category, in_stock


# ---------------------------------------------------------------------------
# schema_from_excel_path — uses actual fixture file
# ---------------------------------------------------------------------------

class TestSchemaFromExcelPath:
    FIXTURE = Path(__file__).parent.parent / "fixtures" / "files" / "departments.xlsx"

    def test_excel_golden_schema(self):
        if not self.FIXTURE.exists():
            pytest.skip("departments.xlsx fixture not generated")
        schema = schema_from_excel_path(self.FIXTURE)
        names = [s["name"] for s in schema]
        assert names == ["dept_id", "dept_name", "manager", "budget", "headcount"]
        assert [s["ordinal"] for s in schema] == [1, 2, 3, 4, 5]

    def test_excel_all_string_types(self):
        """Excel parser reads all columns as 'string' (header-only inference)."""
        if not self.FIXTURE.exists():
            pytest.skip("departments.xlsx fixture not generated")
        schema = schema_from_excel_path(self.FIXTURE)
        assert all(s["data_type"] == "string" for s in schema)


# ---------------------------------------------------------------------------
# schema_from_avro_fileobj — uses actual fixture file
# ---------------------------------------------------------------------------

class TestSchemaFromAvroFileobj:
    FIXTURE = Path(__file__).parent.parent / "fixtures" / "files" / "inventory.avro"

    def test_avro_golden_schema(self):
        if not self.FIXTURE.exists():
            pytest.skip("inventory.avro fixture not generated")
        with open(self.FIXTURE, "rb") as fobj:
            schema = schema_from_avro_fileobj(fobj)
        names = [s["name"] for s in schema]
        assert names == ["item_id", "warehouse", "quantity", "unit_price", "reorder_level"]

    def test_avro_ordinals(self):
        if not self.FIXTURE.exists():
            pytest.skip("inventory.avro fixture not generated")
        with open(self.FIXTURE, "rb") as fobj:
            schema = schema_from_avro_fileobj(fobj)
        assert [s["ordinal"] for s in schema] == [1, 2, 3, 4, 5]

    def test_avro_data_types(self):
        if not self.FIXTURE.exists():
            pytest.skip("inventory.avro fixture not generated")
        with open(self.FIXTURE, "rb") as fobj:
            schema = schema_from_avro_fileobj(fobj)
        type_map = {s["name"]: s["data_type"] for s in schema}
        assert type_map["item_id"] == "int"
        assert type_map["warehouse"] == "string"
        assert type_map["unit_price"] == "double"


# ---------------------------------------------------------------------------
# schema_from_bytes — roundtrip via avro fileobj for bytes path
# ---------------------------------------------------------------------------

class TestSchemaFromBytesAvro:
    FIXTURE = Path(__file__).parent.parent / "fixtures" / "files" / "inventory.avro"

    def test_bytes_avro_roundtrip(self):
        if not self.FIXTURE.exists():
            pytest.skip("inventory.avro fixture not generated")
        content = self.FIXTURE.read_bytes()
        schema = schema_from_bytes(content, ".avro")
        assert len(schema) == 5
        assert schema[0]["name"] == "item_id"
