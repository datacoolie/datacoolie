"""Full normalized model parity across JSON/YAML/Excel/DB/API hydration."""

from __future__ import annotations
import copy
import importlib.util
import json
from pathlib import Path
import httpx
from datacoolie.metadata.api_provider import APIProvider
from datacoolie.metadata.database_provider import DatabaseProvider
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.platforms.local_platform import LocalPlatform


def _setup_module():
    path = (
        Path(__file__).resolve().parents[3]
        / "usecase-sim/scripts/bootstrap/setup_metadata.py"
    )
    spec = importlib.util.spec_from_file_location("a9_metadata_export", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _document():
    return {
        "connections": [
            {
                "name": "parity_src",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/input"},
            },
            {
                "name": "parity_dst",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "data/output"},
            },
        ],
        "dataflows": [
            {
                "name": "parity_z",
                "description": "non-empty export description",
                "stage": "parity",
                "group_number": 1,
                "execution_order": 2,
                "source": {
                    "connection_name": "parity_src",
                    "table": "orders",
                    "filter_expression": "status = 'open'",
                    "watermark_columns": ["modified_at"],
                },
                "transform": {
                    "additional_columns": [
                        {"column": "batch", "expression": "'parity'"}
                    ],
                    "schema_hints": [{"column_name": "id", "data_type": "BIGINT"}],
                },
                "destination": {
                    "connection_name": "parity_dst",
                    "table": "orders",
                    "load_type": "append",
                },
            },
            {
                "name": "parity_a",
                "stage": "parity",
                "group_number": 1,
                "execution_order": 1,
                "source": {"connection_name": "parity_src", "table": "other"},
                "destination": {
                    "connection_name": "parity_dst",
                    "table": "other",
                    "load_type": "overwrite",
                },
            },
        ],
    }


def _models(provider):
    provider.initialize()
    values = []
    for flow in provider.get_dataflows(stage="parity"):
        payload = flow.model_dump()
        # Workspace identity and operational binding are dimensions, not ETL rules.
        payload["workspace_id"] = None
        for role in ["source", "destination"]:
            payload[role]["connection"]["workspace_id"] = None
        values.append(payload)
    return sorted(values, key=lambda flow: (flow["execution_order"], flow["name"]))


def test_json_yaml_excel_database_api_full_model_parity(tmp_path):
    document = _document()
    setup = _setup_module()
    json_path = tmp_path / "metadata.json"
    json_path.write_text(json.dumps(document))
    yaml_path = tmp_path / "metadata.yaml"
    excel_path = tmp_path / "metadata.xlsx"
    setup.emit_yaml(document, yaml_path)
    setup.emit_xlsx(document, excel_path)
    providers = [
        FileProvider(
            config_path=str(path),
            platform=LocalPlatform(),
            watermark_base_path=str(tmp_path / "watermarks"),
        )
        for path in [json_path, yaml_path, excel_path]
    ]
    connection_string = "sqlite:///" + str(tmp_path / "metadata.db").replace("\\", "/")
    setup.seed_db(connection_string, document, "a9-parity", truncate=False)
    providers.append(
        DatabaseProvider(connection_string=connection_string, workspace_id="a9-parity")
    )
    try:
        expected = _models(providers[0])
        assert [flow["name"] for flow in expected] == ["parity_a", "parity_z"]
        assert expected[1]["description"] == "non-empty export description"
        for provider in providers[1:]:
            assert _models(provider) == expected
        connections = [
            copy.deepcopy(expected[0][role]["connection"])
            for role in ["source", "destination"]
        ]

        def handler(request):
            if request.url.path.endswith("/connections"):
                data = connections
            elif request.url.path.endswith("/dataflows"):
                data = copy.deepcopy(expected)
            elif request.url.path.endswith("/schema-hints"):
                data = []
            else:
                return httpx.Response(404, json={"error": "not found"})
            return httpx.Response(
                200, json={"data": data, "pagination": {"page": 1, "total_pages": 1}}
            )

        api = APIProvider(
            base_url="https://parity.example", api_key="", workspace_id="a9-parity"
        )
        api._client = httpx.Client(
            base_url="https://parity.example", transport=httpx.MockTransport(handler)
        )
        api._client_owned = True
        providers.append(api)
        assert _models(api) == expected
    finally:
        for provider in providers:
            provider.close()
