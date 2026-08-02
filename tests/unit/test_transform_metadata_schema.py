"""Contract tests for transformer metadata in the published JSON Schema."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
AI_SCHEMA_PATH = (
    REPO_ROOT
    / "ai"
    / "skills"
    / "datacoolie-metadata"
    / "schemas"
    / "0.1.0"
    / "metadata.schema.json"
)


@pytest.fixture(scope="module")
def validator() -> Draft202012Validator:
    schema = json.loads(AI_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _metadata(transform: dict) -> dict:
    return {
        "dataflows": [
            {
                "name": "transform_contract",
                "source": {"connection_name": "source"},
                "destination": {
                    "connection_name": "destination",
                    "table": "output",
                },
                "transform": deepcopy(transform),
            }
        ]
    }


def _is_valid(validator: Draft202012Validator, transform: dict) -> bool:
    return not list(validator.iter_errors(_metadata(transform)))


@pytest.mark.parametrize(
    "transform",
    [
        {"select_columns": ["id", "email"]},
        {"drop_columns": ["debug_payload"]},
        {"rename_columns": {"id": "customer_id", "email": "contact_email"}},
        {"value_rules": [{"operation": "trim", "columns": ["name"]}]},
        {"value_rules": [{"operation": "case", "columns": ["name"], "mode": "lower"}]},
        {
            "value_rules": [
                {
                    "operation": "regex_replace",
                    "columns": ["phone"],
                    "pattern": "[^0-9]",
                    "replacement": "",
                    "order": 0,
                }
            ]
        },
        {"value_rules": [{"operation": "empty_to_null", "columns": ["country"]}]},
        {
            "value_rules": [
                {"operation": "fill_null", "columns": ["country"], "value": "unknown"}
            ]
        },
        {
            "value_rules": [
                {
                    "operation": "map",
                    "columns": ["status"],
                    "mapping": {"A": "active"},
                    "on_unmapped": "null",
                }
            ]
        },
        {
            "hash_columns": [
                {
                    "target_column": "business_hash",
                    "columns": ["customer_id", "country"],
                    "algorithm": "sha256",
                    "serialization": "dc_hash_v1",
                }
            ]
        },
        {"masking_rules": [{"method": "redact", "columns": ["email"], "value": "***"}]},
        {"masking_rules": [{"method": "nullify", "columns": ["secret"]}]},
        {
            "masking_rules": [
                {
                    "method": "partial",
                    "columns": ["phone"],
                    "keep_start": 2,
                    "keep_end": 2,
                    "mask_char": "#",
                }
            ]
        },
        {
            "masking_rules": [
                {"method": "numeric_bucket", "columns": ["age"], "bucket_size": 10}
            ]
        },
        {
            "masking_rules": [
                {"method": "date_truncate", "columns": ["birth_date"], "unit": "month"}
            ]
        },
        {"configure": {"missing_column_policy": "error"}},
        {"configure": {"missing_column_policy": "ignore"}},
    ],
)
def test_valid_transform_contracts(
    validator: Draft202012Validator, transform: dict
) -> None:
    assert _is_valid(validator, transform)


@pytest.mark.parametrize(
    "transform",
    [
        {"select_columns": ["id"], "drop_columns": ["debug"]},
        {"select_columns": ["id", "id"]},
        {"rename_columns": {"id": ""}},
        {"unknown_transform": True},
        {"value_rules": [{"operation": "trim", "columns": []}]},
        {"value_rules": [{"operation": "trim", "columns": ["name", "name"]}]},
        {"value_rules": [{"operation": "trim", "columns": ["name"], "order": -1}]},
        {
            "value_rules": [
                {"operation": "trim", "columns": ["name"], "unexpected": True}
            ]
        },
        {"value_rules": [{"operation": "case", "columns": ["name"]}]},
        {"value_rules": [{"operation": "case", "columns": ["name"], "mode": None}]},
        {"value_rules": [{"operation": "regex_replace", "columns": ["phone"]}]},
        {
            "value_rules": [
                {"operation": "regex_replace", "columns": ["phone"], "pattern": None}
            ]
        },
        {
            "value_rules": [
                {"operation": "fill_null", "columns": ["country"], "value": None}
            ]
        },
        {"value_rules": [{"operation": "map", "columns": ["status"], "mapping": {}}]},
        {
            "value_rules": [
                {"operation": "map", "columns": ["status"], "mapping": {"A": 1}}
            ]
        },
        {
            "value_rules": [
                {
                    "operation": "map",
                    "columns": ["status"],
                    "mapping": {"A": "active"},
                    "on_unmapped": "drop",
                }
            ]
        },
        {"hash_columns": [{"target_column": "row_hash", "columns": []}]},
        {"hash_columns": [{"target_column": "row_hash", "columns": ["id", "id"]}]},
        {
            "hash_columns": [
                {"target_column": "row_hash", "columns": ["id"], "algorithm": "md5"}
            ]
        },
        {
            "hash_columns": [
                {
                    "target_column": "row_hash",
                    "columns": ["id"],
                    "serialization": "json",
                }
            ]
        },
        {
            "hash_columns": [
                {"target_column": "row_hash", "columns": ["id"], "salt": "secret"}
            ]
        },
        {"masking_rules": [{"method": "redact", "columns": ["email"]}]},
        {
            "masking_rules": [
                {"method": "partial", "columns": ["phone"], "keep_start": -1}
            ]
        },
        {
            "masking_rules": [
                {"method": "partial", "columns": ["phone"], "mask_char": "**"}
            ]
        },
        {"masking_rules": [{"method": "numeric_bucket", "columns": ["age"]}]},
        {
            "masking_rules": [
                {"method": "numeric_bucket", "columns": ["age"], "bucket_size": None}
            ]
        },
        {"masking_rules": [{"method": "date_truncate", "columns": ["birth_date"]}]},
        {
            "masking_rules": [
                {"method": "date_truncate", "columns": ["birth_date"], "unit": None}
            ]
        },
        {"masking_rules": [{"method": "tokenize", "columns": ["email"]}]},
        {"configure": {"missing_column_policy": "warn"}},
    ],
)
def test_invalid_transform_contracts(
    validator: Draft202012Validator, transform: dict
) -> None:
    assert not _is_valid(validator, transform)
