"""Selector admission and active projection for exact file metadata."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]


def _load_bindings():
    sys.path.insert(0, str(PRODUCT_ROOT / "usecase-sim"))
    try:
        from runner.metadata_bindings import BoundFileProvider, MetadataBindingError
    finally:
        sys.path.pop(0)
    return BoundFileProvider, MetadataBindingError


def _document() -> dict:
    return {
        "connections": [
            {
                "name": "source",
                "connection_id": "source-id",
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "__ROOT__"},
            },
            {
                "name": "csv_destination",
                "connection_id": "csv-destination-id",
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "__ROOT__"},
            },
            {
                "name": "delta_destination",
                "connection_id": "delta-destination-id",
                "connection_type": "lakehouse",
                "format": "delta",
                "configure": {"base_path": "__ROOT__"},
            },
            {
                "name": "other_delta_destination",
                "connection_id": "other-delta-destination-id",
                "connection_type": "lakehouse",
                "format": "delta",
                "configure": {"base_path": "__ROOT__"},
            },
        ],
        "dataflows": [
            {
                "name": "active_csv",
                "dataflow_id": "active-csv-id",
                "stage": "read",
                "source": {"connection_name": "source", "table": "input"},
                "destination": {
                    "connection_name": "csv_destination",
                    "table": "csv_output",
                },
            },
            {
                "name": "inactive_delta",
                "dataflow_id": "inactive-delta-id",
                "stage": "read",
                "is_active": False,
                "source": {"connection_name": "source", "table": "input"},
                "destination": {
                    "connection_name": "delta_destination",
                    "table": "inactive_delta_output",
                },
            },
            {
                "name": "delta_a",
                "dataflow_id": "delta-a-id",
                "stage": "read",
                "source": {"connection_name": "source", "table": "input"},
                "destination": {
                    "connection_name": "delta_destination",
                    "table": "delta_a_output",
                },
            },
            {
                "name": "delta_other",
                "dataflow_id": "delta-other-id",
                "stage": "read",
                "source": {"connection_name": "source", "table": "input"},
                "destination": {
                    "connection_name": "other_delta_destination",
                    "table": "other_delta_output",
                },
            },
        ],
    }


def _provider(tmp_path, document, selectors):
    BoundFileProvider, _ = _load_bindings()
    config = tmp_path / "metadata.json"
    config.write_text(json.dumps(document), encoding="utf-8")
    from datacoolie.platforms.local_platform import LocalPlatform

    provider = BoundFileProvider(
        config_path=str(config),
        platform=LocalPlatform(),
        dataflows=selectors,
    )
    provider.initialize()
    return provider


def test_selector_resolves_inactive_rows_before_active_projection(tmp_path):
    selectors = ["active_csv", "inactive_delta", "delta_a", "delta_other"]
    provider = _provider(tmp_path, _document(), selectors)
    try:
        assert [
            flow.name
            for flow in provider.get_dataflows(
                stage="read", active_only=False, attach_schema_hints=False
            )
        ] == selectors
        assert [
            flow.name
            for flow in provider.get_dataflows(
                stage="read", active_only=True, attach_schema_hints=False
            )
        ] == ["active_csv", "delta_a", "delta_other"]
    finally:
        provider.close()


def test_selector_keeps_wrong_stage_diagnostic(tmp_path):
    _, error = _load_bindings()
    provider = _provider(tmp_path, _document(), ["inactive_delta"])
    try:
        with pytest.raises(error, match="incompatible with stage selection"):
            provider.get_dataflows(
                stage="write", active_only=True, attach_schema_hints=False
            )
    finally:
        provider.close()


def test_unknown_selector_is_rejected_against_full_authored_inventory(tmp_path):
    BoundFileProvider, error = _load_bindings()
    config = tmp_path / "metadata.json"
    config.write_text(json.dumps(_document()), encoding="utf-8")
    from datacoolie.platforms.local_platform import LocalPlatform

    provider = BoundFileProvider(
        config_path=str(config),
        platform=LocalPlatform(),
        dataflows=["missing"],
    )
    with pytest.raises(error, match="Unknown dataflow selector"):
        provider.initialize()


def test_maintenance_intersects_selectors_with_lakehouse_connection_filter(tmp_path):
    selectors = ["active_csv", "inactive_delta", "delta_a", "delta_other"]
    provider = _provider(tmp_path, _document(), selectors)
    try:
        selected = provider.get_maintenance_dataflows(
            connection="delta_destination",
            active_only=False,
            attach_schema_hints=False,
        )
        assert [flow.name for flow in selected] == ["inactive_delta", "delta_a"]
        assert [flow.destination.connection.name for flow in selected] == [
            "delta_destination",
            "delta_destination",
        ]
        assert [flow.destination.connection.connection_id for flow in selected] == [
            "delta-destination-id",
            "delta-destination-id",
        ]
        assert [
            flow.name
            for flow in provider.get_maintenance_dataflows(
                connection="delta_destination",
                active_only=True,
                attach_schema_hints=False,
            )
        ] == ["delta_a"]
        assert [
            flow.name
            for flow in provider.get_maintenance_dataflows(
                connection="other-delta-destination-id",
                active_only=True,
                attach_schema_hints=False,
            )
        ] == ["delta_other"]
        assert provider.get_maintenance_dataflows(
            connection="csv_destination", attach_schema_hints=False
        ) == []
    finally:
        provider.close()


def test_canonical_maintenance_selection_is_exact_and_destination_deduped():
    scenarios = json.loads(
        (PRODUCT_ROOT / "usecase-sim/scenarios/scenarios.json").read_text(
            encoding="utf-8"
        )
    )
    scenario = scenarios["local_polars_delta_maintenance"]

    def root_relative(value):
        return PRODUCT_ROOT / value.removeprefix("./")

    source_rows = []
    for value in scenario["metadata_path"]:
        source_rows.extend(
            json.loads(root_relative(value).read_text(encoding="utf-8"))[
                "dataflows"
            ]
        )
    authored_by_name = {row["name"]: row for row in source_rows}
    selectors = scenario["dataflows"]
    expected_all = [
        name
        for name in selectors
        if authored_by_name[name]["destination"].get("connection_name")
        == scenario["connection"]
    ]
    expected_active = [
        name for name in expected_all if authored_by_name[name].get("is_active", True)
    ]
    assert len(selectors) == 129
    assert len(expected_all) == 18
    assert len(expected_active) == 17

    BoundFileProvider, _ = _load_bindings()
    from datacoolie.platforms.local_platform import LocalPlatform

    metadata_paths = [str(root_relative(value)) for value in scenario["metadata_path"]]
    provider = BoundFileProvider(
        config_path=metadata_paths,
        connections_path=str(root_relative(scenario["connections_path"])),
        schema_hints_path=str(root_relative(scenario["schema_hints_path"])),
        platform=LocalPlatform(),
        dataflows=selectors,
    )
    provider.initialize()
    try:
        selected_all = provider.get_maintenance_dataflows(
            connection=scenario["connection"],
            active_only=False,
            attach_schema_hints=False,
        )
        selected_active = provider.get_maintenance_dataflows(
            connection=scenario["connection"],
            active_only=True,
            attach_schema_hints=False,
        )
        assert [flow.name for flow in selected_all] == expected_all
        assert [flow.name for flow in selected_active] == expected_active
        assert all(
            flow.destination.connection.name == scenario["connection"]
            for flow in selected_all
        )
        assert len(
            {flow.destination.connection.connection_id for flow in selected_all}
        ) == 1
        assert len({flow.destination.table for flow in selected_active}) == 15
    finally:
        provider.close()
