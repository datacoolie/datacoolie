"""Round-trip coverage for transformer metadata in the metadata skill."""

from __future__ import annotations

from pathlib import Path

import openpyxl

from _loaders import load_file
from convert import to_excel


SELECT_TRANSFORM = {
    "select_columns": ["id", "email"],
    "rename_columns": {"email": "contact_email"},
    "value_rules": [
        {
            "operation": "regex_replace",
            "columns": ["email"],
            "pattern": "[ ]+",
            "replacement": "",
            "order": 10,
        },
        {
            "operation": "map",
            "columns": ["status"],
            "mapping": {"A": "active", "I": "inactive"},
            "on_unmapped": "null",
            "order": 20,
        },
    ],
    "hash_columns": [
        {
            "target_column": "row_hash",
            "columns": ["id", "email"],
            "algorithm": "sha256",
            "serialization": "dc_hash_v1",
        }
    ],
    "masking_rules": [
        {
            "method": "partial",
            "columns": ["email"],
            "keep_start": 2,
            "keep_end": 3,
            "mask_char": "#",
        }
    ],
    "configure": {"missing_column_policy": "ignore"},
}

DROP_TRANSFORM = {
    "drop_columns": ["debug_payload", "raw_secret"],
    "value_rules": [{"operation": "trim", "columns": ["name"]}],
    "masking_rules": [{"method": "nullify", "columns": ["secret"]}],
    "configure": {"missing_column_policy": "error"},
}


def test_excel_round_trip_preserves_new_transform_fields(tmp_path: Path) -> None:
    metadata = {
        "connections": [
            {"name": "src", "connection_type": "file", "format": "parquet"},
            {"name": "dst", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [
            {
                "name": "select_transform_case",
                "source": {"connection_name": "src", "table": "input"},
                "destination": {"connection_name": "dst", "table": "select_output"},
                "transform": SELECT_TRANSFORM,
            },
            {
                "name": "drop_transform_case",
                "source": {"connection_name": "src", "table": "input"},
                "destination": {"connection_name": "dst", "table": "drop_output"},
                "transform": DROP_TRANSFORM,
            },
        ],
    }
    output = tmp_path / "metadata.xlsx"

    to_excel(metadata, output)

    loaded = load_file(output)
    transforms = {item["name"]: item["transform"] for item in loaded["dataflows"]}
    assert transforms == {
        "select_transform_case": SELECT_TRANSFORM,
        "drop_transform_case": DROP_TRANSFORM,
    }


def test_flat_excel_transform_columns_parse_new_fields(tmp_path: Path) -> None:
    output = tmp_path / "flat-transform.xlsx"
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "dataflows"
    worksheet.append(
        [
            "name",
            "source_connection_name",
            "source_table",
            "destination_connection_name",
            "destination_table",
            "transform_select_columns",
            "transform_drop_columns",
            "transform_rename_columns",
            "transform_value_rules",
            "transform_hash_columns",
            "transform_masking_rules",
            "transform_configure",
        ]
    )
    worksheet.append(
        [
            "transform_case",
            "src",
            "input",
            "dst",
            "output",
            "id,email",
            None,
            '{"email": "contact_email"}',
            '[{"operation": "regex_replace", "columns": ["email"], "pattern": "[ ]+", "replacement": "", "order": 10}, {"operation": "map", "columns": ["status"], "mapping": {"A": "active", "I": "inactive"}, "on_unmapped": "null", "order": 20}]',
            '[{"target_column": "row_hash", "columns": ["id", "email"], "algorithm": "sha256", "serialization": "dc_hash_v1"}]',
            '[{"method": "partial", "columns": ["email"], "keep_start": 2, "keep_end": 3, "mask_char": "#"}]',
            '{"missing_column_policy": "ignore"}',
        ]
    )
    worksheet.append(
        [
            "drop_transform_case",
            "src",
            "input",
            "dst",
            "output",
            None,
            "debug_payload,raw_secret",
            None,
            '[{"operation": "trim", "columns": ["name"]}]',
            None,
            '[{"method": "nullify", "columns": ["secret"]}]',
            '{"missing_column_policy": "error"}',
        ]
    )
    workbook.save(output)

    transforms = {
        item["name"]: item["transform"] for item in load_file(output)["dataflows"]
    }
    assert transforms == {
        "transform_case": SELECT_TRANSFORM,
        "drop_transform_case": DROP_TRANSFORM,
    }
