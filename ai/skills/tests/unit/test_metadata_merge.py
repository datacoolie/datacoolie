"""Tests for canonical modular metadata resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from merge import merge_metadata


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _flow(name: str, stage: str) -> dict[str, object]:
    return {
        "name": name,
        "stage": stage,
        "source": {"connection_name": "source"},
        "destination": {"connection_name": "destination"},
    }


def _metadata_root(tmp_path: Path) -> Path:
    root = tmp_path / "metadata"
    _write(
        root / "connections.json",
        {
            "$schema": "https://datacoolie.github.io/datacoolie/schema/0.1.0/metadata.schema.json",
            "connections": [
                {"name": "source", "configure": {"host": "base", "port": 1}},
                {"name": "destination", "configure": {"base_path": "base"}},
            ],
        },
    )
    _write(
        root / "schema_hints.json",
        {
            "schema_hints": [
                {
                    "connection_name": "source",
                    "table_name": "orders",
                    "hints": [{"column_name": "id", "data_type": "long"}],
                }
            ]
        },
    )
    return root


def test_merge_default_stage_file_and_environment_overlay(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", {"dataflows": [_flow("orders", "bronze")]})
    _write(
        root / "environments/test.json",
        {
            "connections": [{"name": "source", "configure": {"host": "test"}}],
            "dataflows": [
                {"name": "orders", "source": {"filter_expression": "active = 1"}}
            ],
            "schema_hints": [
                {
                    "connection_name": "source",
                    "table_name": "orders",
                    "hints": [{"column_name": "id", "format": "integer"}],
                }
            ],
        },
    )

    result = merge_metadata(root, "test")

    assert result["connections"][0]["configure"] == {"host": "test", "port": 1}
    assert result["dataflows"][0]["stage"] == "bronze"
    assert result["dataflows"][0]["source"]["filter_expression"] == "active = 1"
    assert result["schema_hints"][0]["hints"][0] == {
        "column_name": "id",
        "data_type": "long",
        "format": "integer",
    }


def test_merge_supports_all_five_organizational_layouts(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows.json", [_flow("root", "root_stage")])
    _write(
        root / "dataflows/source_branch.json",
        [_flow("branch_a", "source_a"), _flow("branch_b", "source_b")],
    )
    _write(root / "dataflows/silver.json", {"dataflows": [_flow("silver", "silver")]})
    _write(
        root / "dataflows/source2bronze/source2bronze_erp.json",
        {"dataflows": [_flow("erp", "source2bronze_erp")]},
    )
    _write(
        root / "dataflows/gold/customer.json",
        _flow("customer", "gold"),
    )

    result = merge_metadata(root, "dev")

    assert {item["name"]: item["stage"] for item in result["dataflows"]} == {
        "root": "root_stage",
        "branch_a": "source_a",
        "branch_b": "source_b",
        "silver": "silver",
        "customer": "gold",
        "erp": "source2bronze_erp",
    }


@pytest.mark.parametrize(
    ("relative_path", "document"),
    [
        ("dataflows.json", [_flow("root", "root_stage")]),
        ("dataflows/branch.json", [_flow("branch", "branch_stage")]),
        ("dataflows/stage.json", {"dataflows": [_flow("stage", "stage")]}),
        (
            "dataflows/branch/stage.json",
            {"dataflows": [_flow("branch_stage", "stage")]},
        ),
        ("dataflows/stage/dataflow.json", _flow("single", "stage")),
    ],
)
def test_each_dataflow_layout_resolves_independently(
    tmp_path: Path, relative_path: str, document: object
) -> None:
    root = _metadata_root(tmp_path)
    _write(root / relative_path, document)

    result = merge_metadata(root, "dev")

    assert result["dataflows"]
    assert all(item["name"] and item["stage"] for item in result["dataflows"])


@pytest.mark.parametrize(
    ("dataflow", "message"),
    [
        ({"stage": "bronze"}, "non-empty name"),
        ({"name": "orders"}, "non-empty stage"),
        ({"name": " ", "stage": "bronze"}, "non-empty name"),
        ({"name": "orders", "stage": " "}, "non-empty stage"),
    ],
)
def test_merge_requires_explicit_non_empty_name_and_stage(
    tmp_path: Path, dataflow: dict[str, object], message: str
) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/anything.json", dataflow)

    with pytest.raises(ValueError, match=message):
        merge_metadata(root, "dev")


def test_merge_rejects_duplicate_names_across_fragments(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows.json", [_flow("orders", "bronze")])
    _write(root / "dataflows/branch/orders.json", _flow("orders", "silver"))

    with pytest.raises(ValueError, match="Duplicate dataflow name 'orders'"):
        merge_metadata(root, "dev")


def test_merge_rejects_invalid_fragment_shape(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bad.json", "not a dataflow")

    with pytest.raises(ValueError, match="array or an object containing 'dataflows'"):
        merge_metadata(root, "dev")


def test_merge_rejects_missing_dataflow_sources(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)

    with pytest.raises(ValueError, match="No canonical dataflow JSON"):
        merge_metadata(root, "dev")


def test_merge_rejects_new_overlay_dataflow_without_stage(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(root / "environments/dev.json", {"dataflows": [{"name": "new_flow"}]})

    with pytest.raises(ValueError, match="resolved dataflows.*non-empty stage"):
        merge_metadata(root, "dev")


def test_merge_rejects_yaml_and_unified_only_layout(tmp_path: Path) -> None:
    root = tmp_path / "metadata"
    root.mkdir()
    (root / "metadata.json").write_text("{}", encoding="utf-8")
    (root / "connections.yaml").write_text("connections: []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="connections file not found"):
        merge_metadata(root, "dev")


def test_merge_rejects_unknown_overlay_keys(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(root / "environments/dev.json", {"engine": "spark"})

    with pytest.raises(ValueError, match="Unsupported overlay keys"):
        merge_metadata(root, "dev")
