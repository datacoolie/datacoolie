"""test_introspect_lakehouse.py — Unit tests for lakehouse catalog introspection.

Tests use mocked HTTP responses (Iceberg REST) and mocked PyHive/subprocess
calls (Hive, Unity, Glue) so no real services are needed.
"""
import csv
import io
import json
from unittest.mock import MagicMock, patch

import pytest

import introspect_lakehouse


# ---------------------------------------------------------------------------
# CSV contract header
# ---------------------------------------------------------------------------

EXPECTED_HEADER = [
    "source", "schema", "table", "column", "type", "format",
    "precision", "scale", "nullable", "pk", "fk",
    "ordinal_position", "row_estimate", "notes",
]


class TestCsvContract:
    """Verify the header constant matches the cross-script contract."""

    def test_header_matches(self):
        assert introspect_lakehouse.CSV_HEADER == EXPECTED_HEADER

    def test_header_length(self):
        assert len(introspect_lakehouse.CSV_HEADER) == 14


# ---------------------------------------------------------------------------
# Iceberg type mapping
# ---------------------------------------------------------------------------

class TestMapIcebergType:
    """Test canonical type mapping from Iceberg JSON types."""

    def test_string(self):
        assert introspect_lakehouse._map_iceberg_type("string") == ("string", "", "", "")

    def test_int(self):
        assert introspect_lakehouse._map_iceberg_type("int") == ("integer", "", "", "")

    def test_long(self):
        assert introspect_lakehouse._map_iceberg_type("long") == ("long", "", "", "")

    def test_boolean(self):
        assert introspect_lakehouse._map_iceberg_type("boolean") == ("boolean", "", "", "")

    def test_float(self):
        assert introspect_lakehouse._map_iceberg_type("float") == ("float", "", "", "")

    def test_double(self):
        assert introspect_lakehouse._map_iceberg_type("double") == ("double", "", "", "")

    def test_date(self):
        assert introspect_lakehouse._map_iceberg_type("date") == ("date", "", "", "")

    def test_timestamp(self):
        assert introspect_lakehouse._map_iceberg_type("timestamp") == ("timestamp", "", "", "")

    def test_timestamptz(self):
        assert introspect_lakehouse._map_iceberg_type("timestamptz") == ("timestamp_tz", "", "", "")

    def test_uuid(self):
        assert introspect_lakehouse._map_iceberg_type("uuid") == ("string", "uuid", "", "")

    def test_binary(self):
        assert introspect_lakehouse._map_iceberg_type("binary") == ("binary", "", "", "")

    def test_decimal(self):
        result = introspect_lakehouse._map_iceberg_type({"type": "decimal", "precision": 10, "scale": 2})
        assert result == ("decimal(10,2)", "", "10", "2")

    def test_list(self):
        result = introspect_lakehouse._map_iceberg_type({"type": "list", "element": "string"})
        assert result == ("array<string>", "", "", "")

    def test_map(self):
        result = introspect_lakehouse._map_iceberg_type({"type": "map", "key": "string", "value": "int"})
        assert result == ("struct<map<string,integer>>", "", "", "")

    def test_struct(self):
        type_obj = {"type": "struct", "fields": [{"name": "x", "type": "int"}]}
        canonical, fmt, _, _ = introspect_lakehouse._map_iceberg_type(type_obj)
        assert canonical == "struct"

    def test_unknown_string(self):
        assert introspect_lakehouse._map_iceberg_type("custom_type") == ("custom_type", "", "", "")

    def test_fixed(self):
        result = introspect_lakehouse._map_iceberg_type({"type": "fixed", "length": 16})
        assert result == ("binary", "", "16", "")


# ---------------------------------------------------------------------------
# Hive type mapping
# ---------------------------------------------------------------------------

class TestMapHiveType:
    """Test canonical type mapping from Hive type strings."""

    def test_int(self):
        assert introspect_lakehouse._map_hive_type("int") == ("integer", "", "", "")

    def test_bigint(self):
        assert introspect_lakehouse._map_hive_type("bigint") == ("long", "", "", "")

    def test_string(self):
        assert introspect_lakehouse._map_hive_type("string") == ("string", "", "", "")

    def test_decimal(self):
        assert introspect_lakehouse._map_hive_type("decimal(10,2)") == ("decimal(10,2)", "", "10", "2")

    def test_varchar(self):
        assert introspect_lakehouse._map_hive_type("varchar(255)") == ("string", "", "255", "")

    def test_char(self):
        assert introspect_lakehouse._map_hive_type("char(10)") == ("string", "", "10", "")

    def test_array(self):
        canonical, fmt, _, _ = introspect_lakehouse._map_hive_type("array<string>")
        assert canonical == "array"

    def test_map(self):
        canonical, fmt, _, _ = introspect_lakehouse._map_hive_type("map<string,int>")
        assert canonical == "struct"

    def test_boolean(self):
        assert introspect_lakehouse._map_hive_type("boolean") == ("boolean", "", "", "")

    def test_date(self):
        assert introspect_lakehouse._map_hive_type("date") == ("date", "", "", "")

    def test_timestamp(self):
        assert introspect_lakehouse._map_hive_type("timestamp") == ("timestamp", "", "", "")

    def test_tinyint(self):
        assert introspect_lakehouse._map_hive_type("tinyint") == ("byte", "", "", "")


# ---------------------------------------------------------------------------
# Iceberg REST introspection (mocked)
# ---------------------------------------------------------------------------

MOCK_NAMESPACES_RESPONSE = {
    "namespaces": [["sales"], ["analytics"]],
}

MOCK_TABLES_RESPONSE = {
    "identifiers": [
        {"namespace": ["sales"], "name": "transactions"},
        {"namespace": ["sales"], "name": "customers"},
    ],
}

MOCK_TABLE_METADATA = {
    "metadata": {
        "current-schema": {
            "fields": [
                {"id": 1, "name": "txn_id", "type": "long", "required": True},
                {"id": 2, "name": "amount", "type": {"type": "decimal", "precision": 10, "scale": 2}, "required": True},
                {"id": 3, "name": "txn_date", "type": "date", "required": False},
                {"id": 4, "name": "region", "type": "string", "required": False},
            ],
            "identifier-field-ids": [1],
        },
        "default-partition-spec": {
            "fields": [
                {"source-id": 4, "field-id": 1000, "name": "region", "transform": "identity"},
                {"source-id": 3, "field-id": 1001, "name": "txn_date_month", "transform": "month"},
            ],
        },
    },
}


class TestIntrospectIceberg:
    """Test Iceberg REST catalog introspection with mocked HTTP."""

    @patch("introspect_lakehouse.requests.get")
    def test_discovers_namespaces_and_tables(self, mock_get):
        """Full flow: list namespaces → list tables → get table metadata."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/v1/namespaces" in url and "/tables" not in url:
                resp.json.return_value = {"namespaces": [["sales"]]}
            elif "/tables/" in url:
                resp.json.return_value = MOCK_TABLE_METADATA
            elif "/tables" in url:
                resp.json.return_value = MOCK_TABLES_RESPONSE
            return resp

        mock_get.side_effect = side_effect
        rows = introspect_lakehouse.introspect_iceberg("http://localhost:8181", "test-lh")

        # Should have 4 columns × 2 tables = 8 rows
        assert len(rows) == 8
        # Check first row
        row = rows[0]
        assert row[0] == "test-lh"      # source
        assert row[1] == "sales"         # schema (namespace)
        assert row[2] == "transactions"  # table
        assert row[3] == "txn_id"        # column
        assert row[4] == "long"          # type
        assert row[9] == "true"          # pk (identifier field)
        assert len(row) == 14

    @patch("introspect_lakehouse.requests.get")
    def test_partition_notes(self, mock_get):
        """Partition columns get notes with transform type."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/v1/namespaces" in url and "/tables" not in url:
                resp.json.return_value = {"namespaces": [["sales"]]}
            elif "/tables/" in url:
                resp.json.return_value = MOCK_TABLE_METADATA
            elif "/tables" in url:
                resp.json.return_value = {
                    "identifiers": [{"namespace": ["sales"], "name": "transactions"}],
                }
            return resp

        mock_get.side_effect = side_effect
        rows = introspect_lakehouse.introspect_iceberg("http://localhost:8181", "test-lh")

        # region (field_id=4) should have partition:identity note
        region_row = [r for r in rows if r[3] == "region"][0]
        assert "partition:identity" in region_row[13]

        # txn_date (field_id=3) should have partition:month note
        date_row = [r for r in rows if r[3] == "txn_date"][0]
        assert "partition:month" in date_row[13]

    @patch("introspect_lakehouse.requests.get")
    def test_namespace_filter(self, mock_get):
        """When namespaces are provided, skip namespace listing."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/tables/" in url:
                resp.json.return_value = MOCK_TABLE_METADATA
            elif "/tables" in url:
                resp.json.return_value = MOCK_TABLES_RESPONSE
            return resp

        mock_get.side_effect = side_effect
        rows = introspect_lakehouse.introspect_iceberg(
            "http://localhost:8181", "test-lh", namespaces=["sales"],
        )
        # Should not call /v1/namespaces (only /tables and /tables/{name})
        calls = [c.args[0] for c in mock_get.call_args_list]
        assert not any("/v1/namespaces" == c for c in calls)
        assert len(rows) > 0

    @patch("introspect_lakehouse.requests.get")
    def test_csv_row_width(self, mock_get):
        """Every row has exactly 14 fields."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/v1/namespaces" in url and "/tables" not in url:
                resp.json.return_value = {"namespaces": [["sales"]]}
            elif "/tables/" in url:
                resp.json.return_value = MOCK_TABLE_METADATA
            elif "/tables" in url:
                resp.json.return_value = MOCK_TABLES_RESPONSE
            return resp

        mock_get.side_effect = side_effect
        rows = introspect_lakehouse.introspect_iceberg("http://localhost:8181", "lh")
        for row in rows:
            assert len(row) == 14, f"Row has {len(row)} fields: {row}"


# ---------------------------------------------------------------------------
# Hive introspection (mocked)
# ---------------------------------------------------------------------------

class TestIntrospectHive:
    """Test Hive introspection with mocked PyHive."""

    @patch("introspect_lakehouse.hive", create=True)
    def test_discovers_databases_and_tables(self, mock_hive_module):
        """Full flow: SHOW DATABASES → SHOW TABLES → DESCRIBE."""
        with patch.dict("sys.modules", {"pyhive": MagicMock(), "pyhive.hive": mock_hive_module}):
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_hive_module.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor

            # Simulate cursor.execute + fetchall sequences
            call_count = [0]
            def fake_execute(sql, *args, **kwargs):
                pass

            def fake_fetchall():
                call_count[0] += 1
                if call_count[0] == 1:  # SHOW DATABASES
                    return [("testdb",)]
                elif call_count[0] == 2:  # SHOW TABLES
                    return [("customers",)]
                elif call_count[0] == 3:  # DESCRIBE
                    return [
                        ("customer_id", "int", ""),
                        ("name", "string", ""),
                        ("email", "varchar(255)", ""),
                    ]
                return []

            mock_cursor.execute = fake_execute
            mock_cursor.fetchall = fake_fetchall

            # Need to mock the import inside the function
            with patch("introspect_lakehouse.hive", mock_hive_module, create=True):
                # Directly import and patch
                import importlib
                introspect_lakehouse.introspect_hive.__globals__["hive"] = mock_hive_module
                # But introspect_hive does `from pyhive import hive` internally
                # Let's patch sys.modules
                with patch.dict("sys.modules", {"pyhive": MagicMock(hive=mock_hive_module), "pyhive.hive": mock_hive_module}):
                    rows = introspect_lakehouse.introspect_hive("localhost:10000", "hive-test")

            assert len(rows) == 3
            assert rows[0][0] == "hive-test"
            assert rows[0][1] == "testdb"
            assert rows[0][2] == "customers"
            assert rows[0][3] == "customer_id"
            assert rows[0][4] == "integer"


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

class TestParseArgs:
    """Test CLI argument parsing."""

    def test_iceberg_args(self):
        args = introspect_lakehouse.parse_args(
            ["--iceberg", "http://localhost:8181", "--source", "my-lh"]
        )
        assert args.iceberg == "http://localhost:8181"
        assert args.source == "my-lh"

    def test_hive_args(self):
        args = introspect_lakehouse.parse_args(
            ["--hive", "localhost:10000", "--source", "hive-dw"]
        )
        assert args.hive == "localhost:10000"
        assert args.source == "hive-dw"

    def test_unity_args(self):
        args = introspect_lakehouse.parse_args(
            ["--unity", "--source", "unity-prod", "--catalog", "main"]
        )
        assert args.unity is True
        assert args.catalog == "main"

    def test_glue_args(self):
        args = introspect_lakehouse.parse_args(
            ["--glue", "--source", "glue-dw", "--database", "analytics", "--region", "us-east-1"]
        )
        assert args.glue is True
        assert args.database == "analytics"
        assert args.region == "us-east-1"

    def test_mutual_exclusion(self):
        """Cannot specify both --iceberg and --hive."""
        with pytest.raises(SystemExit):
            introspect_lakehouse.parse_args(
                ["--iceberg", "http://x", "--hive", "y:10000", "--source", "x"]
            )

    def test_source_required(self):
        """--source is mandatory."""
        with pytest.raises(SystemExit):
            introspect_lakehouse.parse_args(["--iceberg", "http://x"])

    def test_namespace_filter(self):
        args = introspect_lakehouse.parse_args(
            ["--iceberg", "http://localhost:8181", "--source", "lh", "--namespaces", "sales,analytics"]
        )
        assert args.namespaces == "sales,analytics"

    def test_table_filter(self):
        args = introspect_lakehouse.parse_args(
            ["--iceberg", "http://localhost:8181", "--source", "lh", "--tables", "t1,t2"]
        )
        assert args.tables == "t1,t2"


# ---------------------------------------------------------------------------
# main() CLI output
# ---------------------------------------------------------------------------

class TestCliOutput:
    """Test that main() writes proper CSV to --output."""

    @patch("introspect_lakehouse.requests.get")
    def test_main_writes_csv(self, mock_get, tmp_path):
        """main() produces a valid CSV file with header + data rows."""
        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/v1/namespaces" in url and "/tables" not in url:
                resp.json.return_value = {"namespaces": [["ns"]]}
            elif "/tables/" in url:
                resp.json.return_value = {
                    "metadata": {
                        "current-schema": {
                            "fields": [
                                {"id": 1, "name": "col1", "type": "string", "required": False},
                            ],
                        },
                    },
                }
            elif "/tables" in url:
                resp.json.return_value = {
                    "identifiers": [{"namespace": ["ns"], "name": "tbl1"}],
                }
            return resp

        mock_get.side_effect = side_effect
        out = tmp_path / "out.csv"
        introspect_lakehouse.main([
            "--iceberg", "http://localhost:8181",
            "--source", "test",
            "--output", str(out),
        ])

        content = out.read_text()
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        assert rows[0] == EXPECTED_HEADER
        assert len(rows) == 2  # header + 1 data row
        assert rows[1][0] == "test"
        assert rows[1][3] == "col1"
        assert len(rows[1]) == 14


# ---------------------------------------------------------------------------
# Cross-script contract validation
# ---------------------------------------------------------------------------

class TestCrossScriptContract:
    """Verify CSV header matches other introspection scripts."""

    def test_matches_db_header(self):
        import introspect_db
        assert introspect_lakehouse.CSV_HEADER == introspect_db.CSV_HEADER

    def test_matches_files_header(self):
        import introspect_files
        assert introspect_lakehouse.CSV_HEADER == introspect_files.CSV_HEADER

    def test_matches_api_header(self):
        import introspect_api
        assert introspect_lakehouse.CSV_HEADER == introspect_api.CSV_HEADER
