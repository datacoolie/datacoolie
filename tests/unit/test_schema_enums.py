"""CI test: ensure JSON Schema enum values match datacoolie constants.py.

If someone adds a new enum value to constants.py without updating the schema,
this test will fail.
"""

import json
import sys
from pathlib import Path

import pytest

# Resolve paths relative to this test file
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCHEMA_PATH = REPO_ROOT / "ai" / "skills" / "datacoolie-metadata" / "schemas" / "0.1.0" / "metadata.schema.json"

# Import constants from datacoolie
sys.path.insert(0, str(REPO_ROOT / "src"))
from datacoolie.core.constants import (
    ConnectionType,
    DatabaseAuthType,
    Format,
    LoadType,
    ProcessingMode,
    DatabaseType,
)


@pytest.fixture(scope="module")
def schema():
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def get_schema_enum(schema: dict, path: list[str]) -> list[str]:
    """Navigate schema $defs to find enum values at a dotted path."""
    node = schema
    for key in path:
        node = node[key]
    return node.get("enum", [])


class TestSchemaEnumsMatchConstants:
    """Schema enum arrays must match the values in constants.py."""

    def test_connection_type(self, schema):
        schema_values = set(get_schema_enum(schema, ["$defs", "Connection", "properties", "connection_type"]))
        code_values = {e.value for e in ConnectionType}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"

    def test_format(self, schema):
        schema_values = set(get_schema_enum(schema, ["$defs", "Connection", "properties", "format"]))
        code_values = {e.value for e in Format}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"

    def test_load_type(self, schema):
        schema_values = set(get_schema_enum(schema, ["$defs", "Destination", "properties", "load_type"]))
        code_values = {e.value for e in LoadType}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"

    def test_processing_mode(self, schema):
        schema_values = set(get_schema_enum(schema, ["$defs", "DataFlow", "properties", "processing_mode"]))
        code_values = {e.value for e in ProcessingMode}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"

    def test_database_type(self, schema):
        defs = schema.get("$defs", {})
        db_config = defs.get("ConnectionConfigureDatabase", {})
        db_enum = (
            db_config.get("then", {})
            .get("properties", {})
            .get("configure", {})
            .get("properties", {})
            .get("database_type", {})
            .get("enum")
        )
        assert db_enum is not None, "Could not find database_type enum in schema"
        schema_values = set(db_enum)
        code_values = {e.value for e in DatabaseType}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"

    def test_database_auth_type(self, schema):
        defs = schema.get("$defs", {})
        db_config = defs.get("ConnectionConfigureDatabase", {})
        auth_enum = (
            db_config.get("then", {})
            .get("properties", {})
            .get("configure", {})
            .get("properties", {})
            .get("auth_type", {})
            .get("enum")
        )
        assert auth_enum is not None, "Could not find auth_type enum in schema"
        schema_values = set(auth_enum)
        code_values = {e.value for e in DatabaseAuthType}
        assert schema_values == code_values, f"Mismatch: schema={schema_values - code_values}, code={code_values - schema_values}"
