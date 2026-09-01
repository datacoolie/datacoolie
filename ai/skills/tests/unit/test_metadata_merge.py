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


def test_selector_patches_use_canonical_snapshot_and_exact_overrides_win(
    tmp_path: Path,
) -> None:
    root = _metadata_root(tmp_path)
    _write(
        root / "dataflows/bronze.json",
        {
            "dataflows": [
                _flow("orders", "bronze"),
                _flow("customers", "bronze"),
                _flow("summary", "silver"),
            ]
        },
    )
    _write(
        root / "environments/test.json",
        {
            "patches": [
                {
                    "match": {
                        "type": "dataflows",
                        "where": {
                            "stage": "bronze",
                            "source": {"connection_name": "source"},
                        },
                    },
                    "patch": {"source": {"connection_name": "patched_source"}},
                },
                {
                    "match": {
                        "type": "dataflows",
                        "where": {
                            "stage": "bronze",
                            "source": {"connection_name": "source"},
                        },
                    },
                    "patch": {"destination": {"table": "patched_table"}},
                },
            ],
            "dataflows": [
                {"name": "orders", "destination": {"table": "exact_orders"}}
            ],
        },
    )

    result = merge_metadata(root, "test")
    flows = {item["name"]: item for item in result["dataflows"]}

    assert flows["orders"]["source"]["connection_name"] == "patched_source"
    assert flows["customers"]["source"]["connection_name"] == "patched_source"
    assert flows["orders"]["destination"]["table"] == "exact_orders"
    assert flows["customers"]["destination"]["table"] == "patched_table"
    assert "table" not in flows["summary"]["destination"]


def test_later_selector_patch_wins_at_same_leaf(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(
        root / "environments/test.json",
        {
            "patches": [
                {
                    "match": {"type": "connections", "where": {"name": "source"}},
                    "patch": {"configure": {"host": "first"}},
                },
                {
                    "match": {"type": "connections", "where": {"name": "source"}},
                    "patch": {"configure": {"host": "second"}},
                },
            ]
        },
    )

    result = merge_metadata(root, "test")

    assert result["connections"][0]["configure"] == {"host": "second", "port": 1}


def test_global_and_dataflow_local_schema_hint_patches_are_isolated(
    tmp_path: Path,
) -> None:
    root = _metadata_root(tmp_path)
    _write(
        root / "schema_hints.json",
        {
            "schema_hints": [
                {
                    "connection_name": "source",
                    "schema_name": None,
                    "table_name": "orders",
                    "hints": [
                        {"column_name": "id", "data_type": "long"},
                        {
                            "column_name": "amount",
                            "data_type": "decimal",
                            "precision": 18,
                            "scale": 2,
                        },
                    ],
                }
            ]
        },
    )
    flows = [_flow("orders", "bronze"), _flow("customers", "bronze")]
    for flow in flows:
        flow["transform"] = {
            "schema_hints": [{"column_name": "id", "data_type": "long"}]
        }
    _write(root / "dataflows/bronze.json", flows)
    _write(
        root / "environments/test.json",
        {
            "patches": [
                {
                    "match": {"type": "dataflows", "where": {"stage": "bronze"}},
                    "patch": {
                        "transform": {
                            "schema_hints": [
                                {
                                    "column_name": "amount",
                                    "data_type": "decimal",
                                    "precision": 16,
                                    "scale": 2,
                                }
                            ]
                        }
                    },
                },
                {
                    "match": {
                        "type": "schema_hints",
                        "where": {
                            "connection_name": "source",
                            "schema_name": None,
                            "table_name": "orders",
                            "column_name": "amount",
                        },
                    },
                    "patch": {"precision": 20},
                },
            ]
        },
    )

    result = merge_metadata(root, "test")

    global_hints = {
        item["column_name"]: item for item in result["schema_hints"][0]["hints"]
    }
    assert global_hints["amount"]["precision"] == 20
    for flow in result["dataflows"]:
        local_hints = {
            item["column_name"]: item
            for item in flow["transform"]["schema_hints"]
        }
        assert local_hints["id"] == {"column_name": "id", "data_type": "long"}
        assert local_hints["amount"]["precision"] == 16


@pytest.mark.parametrize(
    ("patches", "message"),
    [
        ({}, "patches must be an array"),
        ([{}], "invalid keys"),
        (
            [{"match": {"type": "unknown", "where": {"name": "orders"}}, "patch": {"stage": "x"}}],
            "match.type must be one of",
        ),
        (
            [{"match": {"type": ["dataflows"], "where": {"name": "orders"}}, "patch": {"stage": "x"}}],
            "match.type must be one of",
        ),
        (
            [{"match": {"type": "dataflows", "where": {}}, "patch": {"stage": "x"}}],
            "where must be a non-empty object",
        ),
        (
            [{"match": {"type": "dataflows", "where": {"stage": ["bronze"]}}, "patch": {"stage": "x"}}],
            "must not contain arrays",
        ),
        (
            [{"match": {"type": "dataflows", "where": {"stage": "bronze"}}, "patch": {}}],
            "patch must be a non-empty object",
        ),
        (
            [{"match": {"type": "dataflows", "where": {"stage": "bronze"}}, "patch": {"name": "renamed"}}],
            "immutable identity fields: name",
        ),
    ],
)
def test_selector_patch_shape_is_validated(
    tmp_path: Path, patches: object, message: str
) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(root / "environments/dev.json", {"patches": patches})

    with pytest.raises(ValueError, match=message):
        merge_metadata(root, "dev")


def test_selector_patch_rejects_zero_matches_before_exact_additions(
    tmp_path: Path,
) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(
        root / "environments/dev.json",
        {
            "patches": [
                {
                    "match": {"type": "dataflows", "where": {"name": "new_flow"}},
                    "patch": {"stage": "patched"},
                }
            ],
            "dataflows": [_flow("new_flow", "new_stage")],
        },
    )

    with pytest.raises(ValueError, match=r"patches\[0\].*matched zero"):
        merge_metadata(root, "dev")


def test_global_schema_hint_patch_rejects_identity_fields(tmp_path: Path) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(
        root / "environments/dev.json",
        {
            "patches": [
                {
                    "match": {
                        "type": "schema_hints",
                        "where": {
                            "connection_name": "source",
                            "schema_name": None,
                            "table_name": "orders",
                            "column_name": "id",
                        },
                    },
                    "patch": {"column_name": "renamed"},
                }
            ]
        },
    )

    with pytest.raises(ValueError, match="immutable identity fields: column_name"):
        merge_metadata(root, "dev")


def test_exact_global_schema_hint_override_wins_after_selector_patch(
    tmp_path: Path,
) -> None:
    root = _metadata_root(tmp_path)
    _write(root / "dataflows/bronze.json", [_flow("orders", "bronze")])
    _write(
        root / "environments/dev.json",
        {
            "patches": [
                {
                    "match": {
                        "type": "schema_hints",
                        "where": {
                            "connection_name": "source",
                            "schema_name": None,
                            "table_name": "orders",
                            "column_name": "id",
                        },
                    },
                    "patch": {"data_type": "integer"},
                }
            ],
            "schema_hints": [
                {
                    "connection_name": "source",
                    "table_name": "orders",
                    "hints": [{"column_name": "id", "data_type": "string"}],
                }
            ],
        },
    )

    result = merge_metadata(root, "dev")

    assert result["schema_hints"][0]["hints"][0]["data_type"] == "string"
