"""Regression coverage for API evidence safety, nullability, and partial scope."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import introspect_api
from _input_safety import validate_nonsecret_locator


def _openapi_response(properties: dict, required: list[str] | None = None) -> dict:
    schema = {"type": "object", "properties": properties}
    if required is not None:
        schema["required"] = required
    return {
        "openapi": "3.0.3",
        "paths": {
            "/items": {
                "get": {
                    "responses": {
                        "200": {
                            "content": {"application/json": {"schema": schema}},
                        },
                    },
                },
            },
        },
    }


@pytest.mark.parametrize("query_key", [
    "sig",
    "SIG",
    "%73%69%67",
    "s%69g",
    "%2573%2569%2567",
])
def test_azure_sas_sig_variants_are_rejected_before_network(query_key):
    locator = f"https://example.test/openapi.json?{query_key}=synthetic-secret"
    with patch("introspect_api.requests.get") as request:
        with pytest.raises(ValueError, match="secret-bearing") as raised:
            validate_nonsecret_locator(locator, "API locator")
    assert "synthetic-secret" not in str(raised.value)
    request.assert_not_called()


@pytest.mark.parametrize("query", [
    "api-version=2023-11-03",
    "x-ms-version=2023-11-03&format=json",
    "sp=r&sr=b&sv=2023-11-03",
    "key=an-ordinary-control",
])
def test_nonsecret_api_controls_remain_allowed(query):
    locator = f"https://example.test/openapi.json?{query}"
    assert validate_nonsecret_locator(locator, "API locator") == locator


def test_openapi_requiredness_is_separate_from_value_nullability():
    spec = _openapi_response(
        {
            "optional_nonnullable": {"type": "string"},
            "optional_nullable": {"type": "string", "nullable": True},
            "required_nonnullable": {"type": "integer"},
            "required_nullable": {"type": "integer", "nullable": True},
        },
        required=["required_nonnullable", "required_nullable"],
    )

    rows = {row["column"]: row for row in introspect_api.parse_openapi(spec, "api")}

    assert rows["optional_nonnullable"]["nullable"] == "false"
    assert rows["optional_nullable"]["nullable"] == "true"
    assert rows["required_nonnullable"]["nullable"] == "false"
    assert rows["required_nullable"]["nullable"] == "true"
    assert "presence=optional" in rows["optional_nullable"]["notes"]
    assert "presence=required" in rows["required_nullable"]["notes"]


def test_openapi_31_nullable_union_keeps_native_type_and_type_mapping():
    spec = _openapi_response({
        "name": {"type": ["string", "null"]},
        "count": {"type": ["integer", "null"]},
    })

    rows = {row["column"]: row for row in introspect_api.parse_openapi(spec, "api")}

    assert rows["name"]["data_type"] == "string"
    assert rows["name"]["nullable"] == "true"
    assert rows["name"]["native_type"] == "string|null"
    assert rows["count"]["data_type"] == "integer"
    assert rows["count"]["native_type"] == "integer|null"


def test_openapi_nested_nullable_object_and_array_wrappers_are_retained_in_notes():
    spec = _openapi_response({
        "payload": {
            "type": ["object", "null"],
            "properties": {"id": {"type": "string"}},
        },
        "records": {
            "type": ["array", "null"],
            "items": {
                "type": "object",
                "properties": {"id": {"type": "integer"}},
            },
        },
    }, required=["payload", "records"])

    rows = {row["column"]: row for row in introspect_api.parse_openapi(spec, "api")}

    payload_id = rows["payload.id"]
    assert payload_id["nullable"] == "false"
    assert "object_presence=required" in payload_id["notes"]
    assert "object_nullable=true" in payload_id["notes"]
    assert "object_native_type=object|null" in payload_id["notes"]

    record_id = rows["records[].id"]
    assert record_id["nullable"] == "false"
    assert "array_presence=required" in record_id["notes"]
    assert "array_nullable=true" in record_id["notes"]
    assert "array_element_nullable=false" in record_id["notes"]


def test_openapi_unsupported_union_keeps_usable_rows_and_reports_partial():
    issues: list[str] = []
    spec = _openapi_response({
        "usable": {"type": "string"},
        "ambiguous": {"type": ["string", "integer", "null"]},
    })

    rows = introspect_api.parse_openapi(spec, "api", issues)

    assert [row["column"] for row in rows] == ["usable"]
    assert any("multiple non-null" in issue for issue in issues)


def test_openapi_external_ref_and_depth_limit_are_partial_without_guessing():
    issues: list[str] = []
    properties: dict = {
        "usable": {"type": "boolean"},
        "external": {"$ref": "models.yaml#/Thing"},
    }
    nested = {"type": "object", "properties": {"leaf": {"type": "string"}}}
    for index in range(introspect_api.MAX_SCHEMA_DEPTH + 2):
        nested = {"type": "object", "properties": {f"level_{index}": nested}}
    properties["deep"] = nested

    rows = introspect_api.parse_openapi(_openapi_response(properties), "api", issues)

    assert any(row["column"] == "usable" for row in rows)
    assert not any("external" in row["column"] for row in rows)
    assert not any("leaf" in row["column"] for row in rows)
    assert any("External OpenAPI" in issue for issue in issues)
    assert any("depth exceeded" in issue for issue in issues)


def _graphql_scalar(name: str = "String") -> dict:
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _graphql_non_null(inner: dict) -> dict:
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


def _graphql_list(inner: dict) -> dict:
    return {"kind": "LIST", "name": None, "ofType": inner}


@patch("introspect_api.requests.post")
def test_graphql_outer_and_element_nullability_are_distinct(mock_post):
    scalar = _graphql_scalar()
    wrappers = {
        "scalar": scalar,
        "scalar_required": _graphql_non_null(scalar),
        "list": _graphql_list(scalar),
        "list_elements_required": _graphql_list(_graphql_non_null(scalar)),
        "list_required": _graphql_non_null(_graphql_list(scalar)),
        "list_and_elements_required": _graphql_non_null(
            _graphql_list(_graphql_non_null(scalar)),
        ),
    }
    response = MagicMock()
    response.json.return_value = {"data": {"__schema": {"types": [{
        "name": "Customer",
        "kind": "OBJECT",
        "fields": [
            {"name": name, "type": type_obj}
            for name, type_obj in wrappers.items()
        ],
    }]}}}
    mock_post.return_value = response

    rows = {row["column"]: row for row in introspect_api.parse_graphql(
        "https://example.test/graphql", "api",
    )}

    assert rows["scalar"]["nullable"] == "true"
    assert rows["scalar_required"]["nullable"] == "false"
    assert rows["list"]["nullable"] == "true"
    assert rows["list_elements_required"]["nullable"] == "true"
    assert rows["list_required"]["nullable"] == "false"
    assert rows["list_and_elements_required"]["nullable"] == "false"
    assert rows["list"]["native_type"] == "[String]"
    assert rows["list_elements_required"]["native_type"] == "[String!]"
    assert rows["list_required"]["native_type"] == "[String]!"
    assert rows["list_and_elements_required"]["native_type"] == "[String!]!"
    assert rows["list_elements_required"]["data_type"] == "array<string>"


def test_cli_partial_status_binds_digest_and_object_scope(tmp_path: Path):
    spec_path = tmp_path / "partial.json"
    output_path = tmp_path / "observations.csv"
    status_path = tmp_path / "status.json"
    spec_path.write_text(json.dumps(_openapi_response({
        "usable": {"type": "string"},
        "ambiguous": {"type": ["string", "integer", "null"]},
    })), encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        introspect_api.main([
            "--spec", str(spec_path),
            "--source", "api",
            "--output", str(output_path),
            "--status-output", str(status_path),
        ])

    assert raised.value.code == introspect_api.PARTIAL_EXIT_CODE
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["status"] == "partial"
    assert payload["output_sha256"]
    assert payload["scope"]["kind"] == "objects"
    assert payload["scope"]["objects"] == [["api", "endpoint", "", "", "/items", "GET"]]
    assert payload["details"]["bounds"] == {
        "max_depth": introspect_api.MAX_SCHEMA_DEPTH,
        "request_timeout_seconds": introspect_api.API_REQUEST_TIMEOUT_SECONDS,
    }


def test_cli_complete_status_uses_source_scope(tmp_path: Path):
    spec_path = tmp_path / "complete.json"
    output_path = tmp_path / "observations.csv"
    status_path = tmp_path / "status.json"
    spec_path.write_text(json.dumps(_openapi_response({"id": {"type": "integer"}})), encoding="utf-8")

    introspect_api.main([
        "--spec", str(spec_path),
        "--source", "api",
        "--output", str(output_path),
        "--status-output", str(status_path),
    ])

    payload = json.loads(status_path.read_text(encoding="utf-8"))
    assert payload["status"] == "complete"
    assert payload["scope"]["kind"] == "source"
    assert payload["scope"]["objects"] == [["api", "endpoint", "", "", "/items", "GET"]]


@pytest.mark.parametrize("alias", ["output", "status-output"])
def test_cli_rejects_local_spec_output_alias(tmp_path: Path, alias: str):
    spec_path = tmp_path / "source.json"
    spec_path.write_text(json.dumps(_openapi_response({"id": {"type": "integer"}})), encoding="utf-8")
    args = ["--spec", str(spec_path), "--source", "api", f"--{alias}", str(spec_path)]

    with pytest.raises(SystemExit, match="distinct"):
        introspect_api.main(args)
