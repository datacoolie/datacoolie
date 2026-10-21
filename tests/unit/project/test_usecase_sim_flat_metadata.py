"""Canonical flat ownership and exact authored selection contracts."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM))

from runner.metadata_documents import load_metadata  # noqa: E402
from runner.metadata_sources import group_document, recipe_document  # noqa: E402


def test_flat_group_names_and_effective_provider_ids_are_unique():
    from datacoolie.utils.identity import name_to_uuid

    names, ids = [], []
    groups = sorted(path for path in (SIM / "metadata/file").glob("*.json")
                    if path.stem not in {"connections", "schema_hints"})
    assert len(groups) == 13
    for path in groups:
        raw = json.loads(path.read_text())
        assert set(raw) == {"dataflows"}
        for flow in raw["dataflows"]:
            names.append(flow["name"])
            ids.append(flow.get("dataflow_id") or name_to_uuid(flow["name"]))
    assert len(names) == len(set(names)) == 372
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("name", ["local_use_cases", "aws_use_cases", "transformer_features"])
def test_export_workflow_projects_exact_order_and_resolvable_shared_connections(name):
    path = SIM / "scenarios/workflows" / f"{name}.json"
    before = {source: source.read_bytes() for source in (SIM / "metadata/file").glob("*.json")}
    manifest = json.loads(path.read_text())
    result = load_metadata(path, root=SIM)
    assert [flow["name"] for flow in result["dataflows"]] == manifest["flow_order"]
    connections = {row["name"] for row in result["connections"]}
    assert all(flow[side]["connection_name"] in connections
               for flow in result["dataflows"] for side in ("source", "destination"))
    assert {source: source.read_bytes() for source in before} == before


def test_recovery_snapshots_bind_one_immutable_flow_and_keep_engine_output_scopes():
    keys = ["api_incremental_continuation", "api_incremental_continuation_spark",
            "api_incremental_recovery", "api_incremental_recovery_fail",
            "api_incremental_recovery_spark", "api_incremental_recovery_spark_fail"]
    documents = {key: recipe_document("incremental", key) for key in keys}
    canonical = next(row for row in group_document("incremental")["dataflows"]
                     if row["name"] == "api_incremental_recovery_validation")
    for document in documents.values():
        flow = document["dataflows"][0]
        assert tuple(flow.get(key) for key in ("name", "stage", "dataflow_id")) == tuple(
            canonical.get(key) for key in ("name", "stage", "dataflow_id"))
    def destination(document):
        return next(row for row in document["connections"] if row["name"] == "api_recovery_dest")["configure"]["base_path"]
    assert destination(documents[keys[0]]) != destination(documents[keys[1]])
    assert documents["api_incremental_recovery_fail"]["dataflows"][0]["source"]["configure"]["max_pages"] == 1
    assert documents["api_incremental_recovery"]["dataflows"][0]["source"]["configure"]["max_pages"] > 1


def test_focused_projection_is_fresh_and_does_not_change_canonical_source():
    path = SIM / "metadata/file/merge.json"
    before = path.read_bytes()
    one = recipe_document("merge", "merge", tokens={"__CASE_ROOT__": "C:/case-one"})
    two = recipe_document("merge", "merge", tokens={"__CASE_ROOT__": "C:/case-two"})
    one["dataflows"][0]["source"].clear()
    assert two["dataflows"][0]["source"]
    assert one["connections"][0]["configure"]["base_path"] != two["connections"][0]["configure"]["base_path"]
    assert path.read_bytes() == before


def test_datatype_qualification_recipe_projection_is_exact_and_fresh():
    canonical_paths = (
        SIM / "metadata/file/schema_types.json",
        SIM / "metadata/file/connections.json",
        SIM / "metadata/file/schema_hints.json",
    )
    before = {path: path.read_bytes() for path in canonical_paths}
    expected_flows = (
        "datatype_decimal_and_date",
        "datatype_unhinted_csv",
        "datatype_unhinted_parquet",
        "datatype_unhinted_json",
        "datatype_unhinted_jsonl",
        "datatype_matrix_postgresql",
        "datatype_matrix_mysql",
        "datatype_matrix_mssql",
        "datatype_matrix_oracle",
        "datatype_matrix_sqlite",
        "datatype_matrix_spark_sql",
        "datatype_database_postgresql",
        "datatype_database_mysql",
        "datatype_database_sqlite",
        "datatype_database_mssql",
        "datatype_database_oracle",
    )
    expected_connections = (
        "datatype_qualification_parquet_source",
        "datatype_qualification_csv_source",
        "datatype_qualification_unhinted_parquet_source",
        "datatype_qualification_json_source",
        "datatype_qualification_jsonl_source",
        "datatype_qualification_postgresql_source",
        "datatype_qualification_mysql_source",
        "datatype_qualification_mssql_source",
        "datatype_qualification_oracle_source",
        "datatype_qualification_sqlite_source",
        "datatype_qualification_spark_source",
        "datatype_qualification_postgresql_db_source",
        "datatype_qualification_mysql_db_source",
        "datatype_qualification_sqlite_db_source",
        "datatype_qualification_mssql_db_source",
        "datatype_qualification_oracle_db_source",
        "datatype_qualification_destination",
    )
    expected_tables = (
        "datatype_decimal",
        "datatype_unhinted_csv",
        "datatype_unhinted_parquet",
        "datatype_unhinted_json",
        "datatype_unhinted_jsonl",
        "datatype_matrix_postgresql",
        "datatype_matrix_mysql",
        "datatype_matrix_mssql",
        "datatype_matrix_oracle",
        "datatype_matrix_sqlite",
        "datatype_matrix_spark_sql",
    )
    expected_db_owners = (
        ("datatype_database_postgresql", "datatype_qualification_postgresql_db_source"),
        ("datatype_database_mysql", "datatype_qualification_mysql_db_source"),
        ("datatype_database_sqlite", "datatype_qualification_sqlite_db_source"),
        ("datatype_database_mssql", "datatype_qualification_mssql_db_source"),
        ("datatype_database_oracle", "datatype_qualification_oracle_db_source"),
    )

    projection = recipe_document("schema_types", "datatype_qualification")
    from datacoolie.utils.identity import name_to_uuid

    expected_ids = tuple(
        flow.get("dataflow_id") or name_to_uuid(flow["name"])
        for flow in projection["dataflows"]
    )
    assert len(set(expected_ids)) == 16
    assert len(projection["dataflows"]) == 16
    assert tuple(flow["name"] for flow in projection["dataflows"]) == expected_flows
    assert len(projection["connections"]) == 17
    assert tuple(connection["name"] for connection in projection["connections"]) == expected_connections
    assert tuple(
        flow["destination"]["table"]
        for flow in projection["dataflows"]
        if flow.get("stage") == "datatype_qualification"
    ) == expected_tables
    assert tuple(
        (flow["name"], flow["source"]["connection_name"])
        for flow in projection["dataflows"]
        if flow.get("stage") == "datatype_database_qualification"
    ) == expected_db_owners

    projection["connections"][0]["configure"]["base_path"] = "mutated"
    projection["dataflows"][0]["source"]["table"] = "mutated"
    refreshed = recipe_document("schema_types", "datatype_qualification")
    assert refreshed["connections"][0]["configure"]["base_path"] != "mutated"
    assert refreshed["dataflows"][0]["source"]["table"] != "mutated"
    assert tuple(
        flow.get("dataflow_id") or name_to_uuid(flow["name"])
        for flow in refreshed["dataflows"]
    ) == expected_ids
    assert {path: path.read_bytes() for path in canonical_paths} == before
