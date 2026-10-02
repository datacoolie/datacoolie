from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import tomllib

import pytest

from datacoolie import __version__
from datacoolie.cli.main import main
from datacoolie.cli.render import render
from datacoolie.project.build import build_project, verify_build
from datacoolie.project.build.functions import plan_function_packaging
from datacoolie.project.config import load_project_config, project_config_from_mapping
from datacoolie.project.documents import load_snapshot
from datacoolie.project.errors import (
    ProjectConfigError,
    ProjectDependencyError,
    ProjectError,
    ProjectValidationError,
)
from datacoolie.project.scaffold import init_project
from datacoolie.project.schema import LATEST_SCHEMA_URL, resolve_metadata_schema
from datacoolie.project.validation.artifacts import validate_artifact
from datacoolie.project.validation.metadata import validate_metadata_document
from datacoolie.project.validation.project import validate_project
from datacoolie.platforms.local_platform import LocalPlatform
from datacoolie.orchestration.preparation.query import resolve_query


def _metadata(root: Path) -> None:
    (root / "metadata" / "dataflows").mkdir(parents=True, exist_ok=True)
    (root / "metadata" / "environments").mkdir(parents=True, exist_ok=True)
    (root / "metadata" / "connections.json").write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "csv",
                        "configure": {"base_path": "input"},
                    },
                    {
                        "name": "target",
                        "connection_type": "file",
                        "format": "parquet",
                        "configure": {"base_path": "output"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (root / "metadata" / "schema_hints.json").write_text(
        '{"schema_hints": []}\n', encoding="utf-8"
    )
    (root / "metadata" / "dataflows" / "orders.json").write_text(
        json.dumps(
            {
                "dataflows": [
                    {
                        "name": "orders",
                        "stage": "ingest2bronze",
                        "source": {
                            "connection_name": "source",
                            "query": "sql/orders.sql",
                        },
                        "destination": {
                            "connection_name": "target",
                            "table": "orders",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (root / "sql" / "orders.sql").write_text(
        "select * from orders\n", encoding="utf-8"
    )
    (root / "functions" / "helpers.py").write_text("VALUE = 1\n", encoding="utf-8")


def test_cli_aliases_share_the_public_entrypoint() -> None:
    pyproject = Path(__file__).resolve().parents[3] / "pyproject.toml"
    document = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    scripts = document["project"]["scripts"]
    assert scripts["dc"] == "datacoolie.cli.main:main"
    assert scripts["datacoolie"] == scripts["dc"]


def test_cli_parser_errors_use_json_envelope_and_exit_two(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--format", "json", "metadata", "convert"]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["datacoolie_version"] == __version__
    assert payload["ok"] is False
    assert payload["error"]["code"] == "usage.invalid"
    assert payload["data"] is None
    assert "--input" in payload["error"]["message"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["validate", "--sql-base-path", "sql"],
        ["validate", "--artifact-base-path", "artifact"],
        ["validate", "--metadata-path", "metadata", "--only", "metadata"],
        ["validate", "--artifact-path", "artifact", "--only", "metadata"],
    ],
)
def test_validate_rejects_options_outside_their_scope(
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--format", "json", *arguments]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "usage.invalid"
    assert "only" in payload["error"]["message"] or "require" in payload["error"]["message"]


def test_standalone_schema_failure_keeps_service_skips_and_cli_scope_exclusions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata = tmp_path / "metadata.json"
    metadata.write_text(
        json.dumps(
            {
                "connections": [],
                "dataflows": [],
                "schema_hints": [],
                "unexpected": True,
            }
        ),
        encoding="utf-8",
    )

    assert main(["--format", "json", "validate", "--metadata-path", str(metadata)]) == 1

    payload = json.loads(capsys.readouterr().out)
    details = payload["data"]["details"]
    assert payload["error"]["code"] == "validation.failed"
    assert details["limited_scope"] is True
    assert details["not_checked"] == [
        "model-constraints",
        "identity-uniqueness",
        "query-references",
        "project-config",
        "resource-existence",
        "other-environments",
    ]


def test_standalone_validation_rejects_whitespace_dataflow_identity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata = tmp_path / "metadata.json"
    metadata.write_text(
        json.dumps(
            {
                "connections": [{"name": "source"}, {"name": "target"}],
                "dataflows": [
                    {
                        "name": "   ",
                        "source": {"connection_name": "source"},
                        "destination": {
                            "connection_name": "target",
                            "table": "orders",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert main(["--format", "json", "validate", "--metadata-path", str(metadata)]) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert any(
        "non-empty dataflow_id or name" in item["message"]
        for item in payload["data"]["errors"]
    )


def test_standalone_success_reports_only_cli_scope_exclusions(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata = tmp_path / "metadata.json"
    metadata.write_text(
        json.dumps({"connections": [], "dataflows": [], "schema_hints": []}),
        encoding="utf-8",
    )

    assert main(["--format", "json", "validate", "--metadata-path", str(metadata)]) == 0

    payload = json.loads(capsys.readouterr().out)
    details = payload["data"]["details"]
    assert payload["ok"] is True
    assert details["not_checked"] == [
        "project-config",
        "resource-existence",
        "other-environments",
    ]


def test_standalone_latest_schema_marker_reports_local_resolved_contract(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata = tmp_path / "metadata.json"
    metadata.write_text(
        json.dumps(
            {
                "$schema": LATEST_SCHEMA_URL,
                "connections": [],
                "dataflows": [],
                "schema_hints": [],
            }
        ),
        encoding="utf-8",
    )

    assert main(["--format", "json", "validate", "--metadata-path", str(metadata)]) == 0

    payload = json.loads(capsys.readouterr().out)
    details = payload["data"]["details"]
    resolved = resolve_metadata_schema()
    assert payload["ok"] is True
    assert details["schema_version"] == resolved.descriptor.version
    assert details["schema_url"] == resolved.descriptor.public_url


def test_json_success_uses_one_envelope_for_non_validation_commands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--format", "json", "inspect", "capabilities"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 1
    assert payload["ok"] is True
    assert set(payload) == {"schema_version", "datacoolie_version", "ok", "data"}
    assert payload["datacoolie_version"] == __version__
    assert payload["data"]["datacoolie_version"] == __version__
    assert isinstance(payload["data"]["registrations"], dict)


def test_text_errors_are_reported_on_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--format", "text", "metadata", "convert"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Error [usage.invalid]" in captured.err


def test_parser_error_uses_last_explicit_format_flag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--format", "text", "--format", "json", "metadata", "convert"]) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["error"]["code"] == "usage.invalid"


def test_json_serialization_failure_returns_internal_error_without_partial_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr("datacoolie.cli.main.execute", lambda _args: {"value": object()})
    assert main(["--format", "json", "inspect", "capabilities"]) == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "internal.error"
    assert payload["datacoolie_version"] == __version__
    assert captured.out.strip().startswith("{")
    assert captured.out.strip().endswith("}")


def test_text_renderer_marks_nested_list_items_and_empty_collections() -> None:
    output = StringIO()
    render(
        {
            "items": [{"name": "first"}, {"name": "second"}],
            "empty": [],
        },
        requested="text",
        stream=output,
    )
    text = output.getvalue()
    assert text.count("-\n") == 2
    assert "name: first" in text
    assert "name: second" in text
    assert "empty:\n  []" in text


def test_validation_failure_keeps_complete_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "invalid"
    init_project(project)
    _metadata(project)
    dataflows = []
    for index in range(7):
        dataflows.append(
            {
                "name": f"missing_{index}",
                "stage": "sql",
                "source": {
                    "connection_name": "source",
                    "table": "orders",
                    "query": f"missing_{index}.sql",
                },
                "destination": {
                    "connection_name": "target",
                    "table": f"orders_{index}",
                },
            }
        )
    (project / "metadata" / "dataflows" / "orders.json").write_text(
        json.dumps({"dataflows": dataflows}),
        encoding="utf-8",
    )
    assert main(["--format", "json", "validate", "--project-dir", str(project)]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"]["code"] == "validation.failed"
    assert payload["datacoolie_version"] == __version__
    diagnostics = payload["data"]["errors"]
    assert len(diagnostics) >= 7
    assert all(item["code"] == "query.missing" for item in diagnostics)
    assert all(item["path"] == f"dataflows[{index}].source.query" for index, item in enumerate(diagnostics))
    assert main(["--format", "json", "build", "--project-dir", str(project)]) == 1
    build_payload = json.loads(capsys.readouterr().out)
    assert build_payload["error"]["code"] == "validation.failed"
    assert build_payload["datacoolie_version"] == __version__
    assert len(build_payload["data"]["errors"]) >= 7


def test_build_reports_malformed_query_as_validation_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "invalid-query"
    init_project(project)
    _metadata(project)
    dataflow_path = project / "metadata" / "dataflows" / "orders.json"
    dataflow = json.loads(dataflow_path.read_text(encoding="utf-8"))
    dataflow["dataflows"][0]["source"]["query"] = "/absolute/orders.sql"
    dataflow_path.write_text(json.dumps(dataflow), encoding="utf-8")

    with pytest.raises(ProjectValidationError) as caught:
        build_project(load_project_config(project))

    details = caught.value.details
    assert isinstance(details, dict)
    assert any(item["code"] == "query.invalid" for item in details["errors"])


def test_build_dry_run_exposes_shared_plan_without_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "preview"
    init_project(project)
    _metadata(project)
    config = load_project_config(project)
    preview = build_project(config, dry_run=True)
    assert preview["status"] == "dry_run"
    assert preview["plan"]["components"]["metadata"]["layout"] == "single"
    assert preview["plan"]["components"]["sql"][0]["source_file_count"] == 1
    assert preview["not_performed"] == [
        "metadata-serialization-and-round-trip",
        "function-packaging",
        "assembled-artifact-verification",
        "artifact-publication",
    ]
    assert not (project / ".builds").exists()


def test_dry_run_checks_selected_codec_before_creating_build_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "codec-preview"
    init_project(project)
    _metadata(project)
    config = load_project_config(project)
    import builtins

    original_import = builtins.__import__

    def missing_openpyxl(name: str, *args: object, **kwargs: object):
        if name == "openpyxl":
            raise ImportError("test missing openpyxl")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_openpyxl)
    with pytest.raises(ProjectDependencyError, match="openpyxl"):
        build_project(config, metadata_format="excel", dry_run=True)
    assert not (project / ".builds").exists()


def test_metadata_input_missing_codec_is_a_typed_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "metadata.yml"
    source.write_text("connections: []\n", encoding="utf-8")
    import builtins

    original_import = builtins.__import__

    def missing_yaml(name: str, *args: object, **kwargs: object):
        if name == "yaml":
            raise ImportError("test missing yaml")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing_yaml)
    from datacoolie.project.documents import decode_file

    with pytest.raises(ProjectDependencyError, match="PyYAML"):
        decode_file(source)


def test_function_wheel_planner_reports_missing_frontend_without_writing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "functions"
    root.mkdir()
    (root / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        """[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"

[project]
name = "test-functions"
version = "0.1.0"
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "datacoolie.project.build.functions.importlib.util.find_spec",
        lambda name: None if name == "build" else object(),
    )
    with pytest.raises(ProjectDependencyError, match=r"build.*package"):
        plan_function_packaging(root, "auto")
    with pytest.raises(ProjectError, match="must be one of"):
        plan_function_packaging(root, "unsupported")


def test_init_validate_and_build(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    result = init_project(project)
    assert "datacoolie.yml" in result["created"]
    assert (project / "metadata" / "dataflows").is_dir()
    gitignore = (project / ".gitignore").read_text(encoding="utf-8")
    assert ".runtime/" in gitignore
    assert ".releases/" in gitignore
    _metadata(project)
    config = load_project_config(project)
    validation = validate_project(config)
    assert validation.ok, validation.errors
    built = build_project(config)
    assert (Path(built["build_path"]) / "dev" / "metadata" / "metadata.json").is_file()
    assert (Path(built["build_path"]) / "dev" / "sql" / "orders.sql").is_file()
    assert (Path(built["build_path"]) / "dev" / "functions" / "helpers.py").is_file()
    artifact_report = validate_artifact(built["build_path"])
    assert artifact_report.ok, artifact_report.errors
    assert verify_build(built["current_path"])["build_id"] == built["build_id"]
    current_report = validate_artifact(built["current_path"])
    assert current_report.ok, current_report.errors
    assert current_report.details["current_comparison"]["ok"] is True
    root_manifest = json.loads((Path(built["build_path"]) / "manifest.json").read_text(encoding="utf-8"))
    assert all("deployment_path" not in record for record in root_manifest["environments"].values())
    assert isinstance(root_manifest["functions_artifact"], list)
    assert not (Path(built["current_path"]) / "build.json").exists()
    assert not (Path(built["current_path"]) / "SHA256SUMS").exists()
    changed_file = Path(built["current_path"]) / "dev" / "metadata" / "metadata.json"
    changed_file.write_text(changed_file.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    drift_report = validate_artifact(built["current_path"])
    assert not drift_report.ok
    assert any(item.code == "current.hash_mismatch" for item in drift_report.errors)
    standalone_report = validate_artifact(Path(built["build_path"]) / "dev")
    assert standalone_report.ok, standalone_report.errors


def test_same_second_build_id_reuses_identical_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A same-second identity collision is reuse, not a false mismatch."""

    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    from datacoolie.project.build import publisher

    fixed = publisher.datetime(2026, 9, 15, 12, 0, 0, tzinfo=publisher.timezone.utc)

    class FixedDateTime(publisher.datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def]
            return fixed if tz is not None else fixed.replace(tzinfo=None)

    monkeypatch.setattr(publisher, "datetime", FixedDateTime)
    project = tmp_path / "same-second"
    init_project(project)
    _metadata(project)
    config = load_project_config(project)

    first = build_project(config)
    second = build_project(config)

    assert first["status"] == "created"
    assert second["status"] == "reused"
    assert second["build_id"] == first["build_id"]


def test_cli_json_and_standalone_conversion(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    assert main(["--format", "json", "init", str(project)]) == 0
    captured = json.loads(capsys.readouterr().out)["data"]
    assert captured["project"] == project.name
    source = project / "metadata" / "connections.json"
    output = tmp_path / "connections.yml"
    assert main(
        [
            "--format",
            "json",
            "metadata",
            "convert",
            "--input",
            str(source),
            "--output",
            str(output),
            "--to",
            "yaml",
        ]
    ) == 0
    conversion = json.loads(capsys.readouterr().out)["data"]
    assert conversion["status"] == "converted"
    assert output.is_file()


def test_snapshot_excludes_environment_overlay(tmp_path: Path) -> None:
    root = tmp_path / "metadata"
    root.mkdir()
    (root / "connections.json").write_text('{"connections": []}\n', encoding="utf-8")
    (root / "environments").mkdir()
    (root / "environments" / "dev.json").write_text('{}\n', encoding="utf-8")
    snapshot = load_snapshot(root)
    assert [item.relative_path for item in snapshot.documents] == ["connections.json"]


def test_environment_overlay_and_split_projection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    init_project(project)
    _metadata(project)
    (project / "metadata" / "environments" / "dev.json").write_text(
        json.dumps({"connections": [{"name": "source", "configure": {"base_path": "dev-input"}}]}),
        encoding="utf-8",
    )
    config = load_project_config(project)
    built = build_project(config, metadata_layout="split")
    root = Path(built["build_path"]) / "dev" / "metadata"
    assert (root / "connections.json").is_file()
    assert (root / "schema_hints.json").is_file()
    projected = json.loads((root / "connections.json").read_text(encoding="utf-8"))
    assert projected["connections"][0]["configure"]["base_path"] == "dev-input"


def test_split_projection_preserves_top_level_metadata_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "split-extra"
    init_project(project)
    _metadata(project)
    connections = json.loads(
        (project / "metadata" / "connections.json").read_text(encoding="utf-8")
    )
    connections["$schema"] = (
        "https://datacoolie.github.io/datacoolie/schema/0.2.0/metadata.schema.json"
    )
    connections["extensions"] = {"owner": "analytics"}
    (project / "metadata" / "connections.json").write_text(
        json.dumps(connections),
        encoding="utf-8",
    )
    built = build_project(load_project_config(project), metadata_layout="split")
    projected = json.loads(
        (
            Path(built["build_path"])
            / "dev"
            / "metadata"
            / "metadata.json"
        ).read_text(encoding="utf-8")
    )
    assert projected["$schema"] == (
        "https://datacoolie.github.io/datacoolie/schema/0.2.0/metadata.schema.json"
    )
    assert projected["extensions"] == {"owner": "analytics"}
    assert len(projected["dataflows"]) == 1


def test_preserve_layout_keeps_source_boundaries(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    init_project(project)
    _metadata(project)
    config_path = project / "datacoolie.yml"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace("layout: single", "layout: preserve"),
        encoding="utf-8",
    )
    config = load_project_config(project)
    built = build_project(config)
    metadata_root = Path(built["build_path"]) / "dev" / "metadata"
    assert (metadata_root / "connections.json").is_file()
    assert (metadata_root / "dataflows" / "orders.json").is_file()


def test_metadata_excel_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "metadata.json"
    source.write_text(
        json.dumps(
            {
                "connections": [
                    {
                        "name": "source",
                        "connection_type": "file",
                        "format": "csv",
                        "configure": {"base_path": "input", "read_options": {"header": True}},
                    }
                ],
                "dataflows": [],
                "schema_hints": [],
            }
        ),
        encoding="utf-8",
    )
    from datacoolie.project.documents import decode_file, encode_document

    document = decode_file(source)
    target = tmp_path / "metadata.xlsx"
    target.write_bytes(encode_document(document, "excel"))
    assert decode_file(target) == document


def test_schema_name_null_is_representation_equivalent(tmp_path: Path) -> None:
    source = tmp_path / "metadata.json"
    source.write_text(
        json.dumps(
            {
                "connections": [{"name": "source", "connection_type": "file", "format": "csv"}],
                "schema_hints": [{"connection_name": "source", "table_name": "orders", "schema_name": None, "hints": [{"column_name": "id", "data_type": "INTEGER"}]}],
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "metadata.xlsx"
    assert main(["--format", "json", "metadata", "convert", "--input", str(source), "--output", str(target), "--to", "excel"]) == 0


def test_build_excel_projection_validates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    init_project(project)
    _metadata(project)
    config = load_project_config(project)
    built = build_project(config, metadata_format="excel")
    report = validate_artifact(built["build_path"])
    assert report.ok, report.errors
    assert (Path(built["build_path"]) / "dev" / "metadata" / "metadata.xlsx").is_file()


def test_functions_auto_uses_declared_wheel_backend(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "orders"
    init_project(project)
    _metadata(project)
    (project / "functions" / "pyproject.toml").write_text(
        """[build-system]
requires = [\"setuptools>=61\"]
build-backend = \"setuptools.build_meta\"

[project]
name = \"orders-functions\"
version = \"0.1.0\"
""",
        encoding="utf-8",
    )
    config = load_project_config(project)
    built = build_project(config)
    function_root = Path(built["build_path"]) / "dev" / "functions"
    assert len(list(function_root.glob("*.whl"))) == 1


def test_multi_root_build_routes_sql_and_records_function_modes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "multi-root"
    init_project(project)
    metadata = project / "metadata"
    (metadata / "connections.json").write_text(
        json.dumps(
            {
                "connections": [
                    {"name": "source", "connection_type": "file", "format": "csv"},
                    {"name": "target", "connection_type": "file", "format": "parquet"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (metadata / "dataflows" / "queries.json").write_text(
        json.dumps(
            {
                "dataflows": [
                    {
                        "name": "orders",
                        "stage": "sql",
                        "source": {"connection_name": "source", "query": "sql1/orders.sql"},
                        "destination": {"connection_name": "target", "table": "orders"},
                    },
                    {
                        "name": "customers",
                        "stage": "sql",
                        # A configured nested root is addressed by its leaf
                        # prefix when the runner supplies sql_base_path.
                        "source": {"connection_name": "source", "query": "sql2/customers.sql"},
                        "destination": {"connection_name": "target", "table": "customers"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (metadata / "schema_hints.json").write_text('{"schema_hints": []}\n', encoding="utf-8")
    (project / "sql1").mkdir()
    (project / "shared" / "sql2").mkdir(parents=True)
    (project / "sql1" / "orders.sql").write_text("select 1\n", encoding="utf-8")
    (project / "shared" / "sql2" / "customers.sql").write_text("select 2\n", encoding="utf-8")
    (project / "functions" / "loaders").mkdir(parents=True)
    (project / "functions" / "loaders" / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project / "functions" / "loaders" / "reader.py").write_text("VALUE = 2\n", encoding="utf-8")
    (project / "functions" / "writers").mkdir(parents=True)
    (project / "functions" / "writers" / "csv_writer.py").write_text("VALUE = 3\n", encoding="utf-8")

    config_value = {
        "schema_version": 1,
        "project": {"name": "multi-root"},
        "components": {
            "metadata": {"path": "metadata"},
            "sql": [{"path": "sql1"}, {"path": "shared/sql2"}],
            "functions": [
                {"path": "functions/loaders", "packaging": "auto"},
                {"path": "functions/writers", "packaging": "auto"},
            ],
        },
        "environments": {
            "dev": {"platform": "local"},
            "prod": {"platform": "local"},
        },
    }
    import yaml

    (project / "datacoolie.yml").write_text(yaml.safe_dump(config_value, sort_keys=False), encoding="utf-8")
    config = load_project_config(project)
    built = build_project(config)
    build_root = Path(built["build_path"])
    assert validate_artifact(build_root).ok
    manifests = [
        json.loads((build_root / env / "manifest.json").read_text(encoding="utf-8"))
        for env in ("dev", "prod")
    ]
    build_manifest = json.loads((build_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifests[0]["build_id"] == manifests[1]["build_id"] == built["build_id"]
    assert manifests[0]["created_at"] == manifests[1]["created_at"]
    assert [item["packaging"] for item in manifests[0]["components"]["functions"]] == ["zip", "copy"]
    assert [item["packaging"] for item in build_manifest["functions_artifact"]] == ["zip", "copy"]
    assert all("effective_mode" not in item and "mode" not in item for item in build_manifest["functions_artifact"])
    assert (build_root / "dev" / "functions" / "loaders" / "loaders.zip").is_file()
    assert (build_root / "dev" / "functions" / "writers" / "csv_writer.py").is_file()
    assert resolve_query(
        "shared/sql2/customers.sql",
        LocalPlatform(),
        artifact_base_path=str(build_root / "dev"),
    ) == "select 2\n"
    assert resolve_query(
        "sql2/customers.sql",
        LocalPlatform(),
        sql_base_path=str(build_root / "dev" / "shared" / "sql2"),
    ) == "select 2\n"


def test_project_config_rejects_build_output_component_root() -> None:
    with pytest.raises(ProjectConfigError, match="reserved for build output"):
        project_config_from_mapping(
            {
                "project": {"name": "invalid"},
                "components": {"metadata": {"path": ".builds/metadata"}},
                "environments": {"dev": {}},
            }
        )


@pytest.mark.parametrize("reserved", [".runtime", ".releases"])
def test_project_config_rejects_mutable_state_component_root(reserved: str) -> None:
    with pytest.raises(ProjectConfigError, match="mutable runtime/release state"):
        project_config_from_mapping(
            {
                "project": {"name": "invalid"},
                "components": {"metadata": {"path": f"{reserved}/metadata"}},
                "environments": {"dev": {}},
            }
        )


def test_project_config_rejects_case_colliding_environment_names() -> None:
    with pytest.raises(ProjectConfigError, match="Duplicate environment"):
        project_config_from_mapping(
            {
                "project": {"name": "invalid"},
                "components": {"metadata": {"path": "metadata"}},
                "environments": {"Dev": {}, "dev": {}},
            }
        )


def test_empty_sql_component_rejects_shorthand_query_before_build() -> None:
    config = project_config_from_mapping(
        {
            "project": {"name": "no-sql"},
            "components": {
                "metadata": {"path": "metadata"},
                "sql": [],
            },
            "environments": {"dev": {}},
        },
    )
    assert config.sql == ()
    metadata = {
        "connections": [
            {"name": "source", "connection_type": "file", "format": "csv"},
            {"name": "target", "connection_type": "file", "format": "parquet"},
        ],
        "dataflows": [
            {
                "name": "orders",
                "source": {
                    "connection_name": "source",
                    "table": "orders",
                    "query": "orders.sql",
                },
                "destination": {"connection_name": "target", "table": "orders"},
            }
        ],
        "schema_hints": [],
    }
    report = validate_metadata_document(metadata, sql_root=())
    assert not report.ok
    assert any(error.code == "query.base_missing" for error in report.errors)


def test_inspect_environment_artifact_reports_environment_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "datacoolie.project.scaffold._latest_agents",
        lambda **_: "# latest agents\n",
    )
    project = tmp_path / "inspect-env"
    init_project(project)
    _metadata(project)
    built = build_project(load_project_config(project))
    environment_root = Path(built["build_path"]) / "dev"

    from datacoolie.project.inspection import inspect_artifact

    report = inspect_artifact(environment_root)
    assert report["artifact_type"] == "datacoolie_environment"
    assert report["environment"] == "dev"
    assert report["limited_scope"] is True
    assert report["environments"]["dev"]["components"]["metadata"]["path"] == "metadata"


def test_inspect_metadata_filters_require_a_section(
    tmp_path: Path,
) -> None:
    metadata = tmp_path / "metadata"
    metadata.mkdir()
    (metadata / "connections.json").write_text('{"connections": []}\n', encoding="utf-8")
    with pytest.raises(ProjectValidationError, match="require --section"):
        from datacoolie.project.inspection import inspect_metadata

        inspect_metadata(metadata_path=metadata, name="source")
