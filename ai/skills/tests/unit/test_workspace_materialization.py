"""Tests for immutable DataCoolie workspace builds."""

from __future__ import annotations

import ast
import inspect
import json
import shutil
import subprocess
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

import design_approval
import materialize as build_tool
import validate_build as build_validation
import validate_functions


RUNNER_TEMPLATE = (
    Path(__file__).resolve().parents[3]
    / "skills/datacoolie-build/templates/runners/run_local_polars.py.example"
)
BUILD_SKILL_DIR = Path(__file__).resolve().parents[2] / "datacoolie-build"


def _workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    fake_datacoolie = types.ModuleType("datacoolie")
    fake_datacoolie.platform_registry = types.SimpleNamespace(  # type: ignore[attr-defined]
        list_plugins=lambda: ["local"]
    )
    fake_datacoolie.engine_registry = types.SimpleNamespace(  # type: ignore[attr-defined]
        list_plugins=lambda: ["polars", "spark"]
    )
    monkeypatch.setitem(__import__("sys").modules, "datacoolie", fake_datacoolie)
    monkeypatch.setattr(build_tool, "_datacoolie_version", lambda: "test-version")
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 11, tzinfo=timezone.utc),
    )

    workspace = tmp_path / "example_dcws"
    (workspace / "metadata/dataflows").mkdir(parents=True)
    (workspace / "metadata/environments").mkdir()
    (workspace / "runners").mkdir()
    (workspace / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "project": {"name": "example", "workspace_name": "example_dcws"},
                "environments": {
                    "dev": {"platform": "local"},
                    "test": {"platform": "local"},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    connections = {
        "$schema": "https://datacoolie.github.io/datacoolie/schema/0.1.0/metadata.schema.json",
        "connections": [
            {
                "name": "source",
                "connection_type": "file",
                "format": "csv",
                "configure": {"base_path": "input"},
            },
            {
                "name": "destination",
                "connection_type": "file",
                "format": "parquet",
                "configure": {"base_path": "output"},
            },
        ],
    }
    (workspace / "metadata/connections.json").write_text(
        json.dumps(connections), encoding="utf-8"
    )
    flow = {
        "name": "orders",
        "stage": "bronze",
        "processing_mode": "batch",
        "source": {"connection_name": "source", "table": "orders"},
        "destination": {
            "connection_name": "destination",
            "table": "orders",
            "load_type": "full_load",
        },
    }
    (workspace / "metadata/dataflows/bronze.json").write_text(
        json.dumps({"dataflows": [flow]}), encoding="utf-8"
    )
    (workspace / "metadata/environments/test.json").write_text(
        json.dumps(
            {
                "patches": [
                    {
                        "match": {
                            "type": "dataflows",
                            "where": {"stage": "bronze"},
                        },
                        "patch": {"destination": {"load_type": "overwrite"}},
                    }
                ],
                "connections": [
                    {"name": "destination", "configure": {"base_path": "test-output"}}
                ]
            }
        ),
        encoding="utf-8",
    )
    for engine in ("polars", "spark"):
        (workspace / f"runners/run_local_{engine}.py").write_text(
            f"ENGINE = {engine!r}\n", encoding="utf-8"
        )
    (workspace / "runners/replay_local_polars.py").write_text(
        "OPERATION = 'replay'\n", encoding="utf-8"
    )
    (workspace / "runners/maintenance_local_spark.ipynb").write_text(
        "{}\n", encoding="utf-8"
    )
    return workspace


def _add_function_source(workspace: Path, *, wheel: bool = False) -> None:
    connections_path = workspace / "metadata/connections.json"
    connections = json.loads(connections_path.read_text(encoding="utf-8"))
    source = next(item for item in connections["connections"] if item["name"] == "source")
    source.update({"connection_type": "function", "format": "function", "configure": {}})
    connections_path.write_text(json.dumps(connections), encoding="utf-8")

    flow_path = workspace / "metadata/dataflows/bronze.json"
    flows = json.loads(flow_path.read_text(encoding="utf-8"))
    flows["dataflows"][0]["source"] = {
        "connection_name": "source",
        "python_function": "example_functions.sources.load_orders",
    }
    flow_path.write_text(json.dumps(flows), encoding="utf-8")

    if wheel:
        package = workspace / "functions/src/example_functions"
        package.mkdir(parents=True)
        (workspace / "functions/pyproject.toml").write_text(
            """[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "example-functions"
version = "1.0.0"

[tool.setuptools.packages.find]
where = ["src"]
""",
            encoding="utf-8",
        )
    else:
        package = workspace / "functions/example_functions"
        package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "sources.py").write_text(
        "def load_orders(engine, source, watermark_start, watermark_end): return None\n",
        encoding="utf-8",
    )


def test_materialize_creates_one_project_named_zip_and_validates_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    _add_function_source(workspace)

    result = build_tool.materialize(workspace=workspace)
    manifest = build_tool.verify_build(Path(result["build_dir"]))

    assert manifest["functions_artifact"] == {
        "format": "zip",
        "import_prefix": "example_functions",
        "distribution": None,
        "version": None,
        "path": "functions/example_functions.zip",
        "sha256": manifest["functions_artifact"]["sha256"],
    }
    assert (Path(result["build_dir"]) / "functions/example_functions.zip").is_file()
    shutil.move(workspace / "functions", workspace / "authoring-functions-away")
    current = Path(result["current_dir"])
    validation = validate_functions.validate_metadata_files(
        current / "functions/example_functions.zip",
        [current / "dev/metadata/metadata.json"],
    )
    assert validation["functions"] == ["example_functions.sources.load_orders"]


def test_materialize_rejects_invalid_function_signature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    _add_function_source(workspace)
    (workspace / "functions/example_functions/sources.py").write_text(
        "def load_orders(engine): return None\n", encoding="utf-8"
    )

    with pytest.raises(subprocess.CalledProcessError):
        build_tool.materialize(workspace=workspace)


def test_materialize_creates_one_pure_python_wheel_and_rejects_version_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    _add_function_source(workspace, wheel=True)

    first = build_tool.materialize(workspace=workspace)
    first_manifest = build_tool.verify_build(Path(first["build_dir"]))
    artifact = first_manifest["functions_artifact"]
    assert artifact["format"] == "wheel"
    assert artifact["distribution"] == "example-functions"
    assert artifact["version"] == "1.0.0"
    assert artifact["import_prefix"] == "example_functions"
    assert artifact["path"].endswith("-py3-none-any.whl")

    (workspace / "functions/src/example_functions/sources.py").write_text(
        "def load_orders(engine, source, watermark_start, watermark_end): return engine\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="advance the package version"):
        build_tool.materialize(workspace=workspace)


def test_wheel_packaging_is_byte_stable_for_unchanged_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    _add_function_source(workspace, wheel=True)

    first = build_tool._package_functions(workspace, tmp_path / "dist-1", None)
    second = build_tool._package_functions(workspace, tmp_path / "dist-2", None)

    assert first is not None and second is not None
    assert build_tool._sha256(first[0]) == build_tool._sha256(second[0])
    assert first[1] == second[1]
    assert not (workspace / "functions/build").exists()
    assert not list((workspace / "functions").rglob("*.egg-info"))


def test_materialize_builds_all_environments_and_engines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)

    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    manifest = build_tool.verify_build(build_dir)

    assert result["reused"] is False
    assert build_dir == workspace / ".builds" / "artifacts" / result["build_id"]
    assert result["build_id"].startswith("260808-091011-")
    assert len(result["build_id"]) == 26
    assert manifest["content_digest"].startswith(result["build_id"].rsplit("-", 1)[1])
    assert len(manifest["content_digest"]) == 64
    assert manifest["created_at"] == "2026-08-08T09:10:11Z"
    assert manifest["design"] is None
    assert set(manifest["environments"]) == {"dev", "test"}
    for environment in ("dev", "test"):
        runners = build_dir / environment / "runners"
        assert (runners / "run_local_polars.py").is_file()
        assert (runners / "run_local_spark.py").is_file()
        assert (runners / "replay_local_polars.py").is_file()
        assert (runners / "maintenance_local_spark.ipynb").is_file()
        assert (build_dir / environment / "metadata/metadata.json").is_file()
    assert manifest["schema_version"] == 3
    assert manifest["functions_artifact"] is None
    assert not (build_dir / "functions").exists()
    assert not any(path.is_symlink() for path in build_dir.rglob("*"))
    current_dir = workspace / ".builds/current"
    descriptor = json.loads((current_dir / "build.json").read_text(encoding="utf-8"))
    assert descriptor == {
        "schema_version": 1,
        "artifact_type": "current_build",
        "build_id": result["build_id"],
    }
    assert not (current_dir / "manifest.json").exists()
    assert not (current_dir / "SHA256SUMS").exists()
    assert build_tool.verify_current_build(current_dir)["build_id"] == result["build_id"]
    for artifact in manifest["artifacts"]:
        relative = Path(artifact["path"])
        assert (current_dir / relative).read_bytes() == (build_dir / relative).read_bytes()

    test_metadata = json.loads(
        (build_dir / "test/metadata/metadata.json").read_text(encoding="utf-8")
    )
    destination = next(
        item for item in test_metadata["connections"] if item["name"] == "destination"
    )
    assert destination["configure"]["base_path"] == "test-output"
    assert test_metadata["dataflows"][0]["destination"]["load_type"] == "overwrite"
    dev_metadata = json.loads(
        (build_dir / "dev/metadata/metadata.json").read_text(encoding="utf-8")
    )
    assert dev_metadata["dataflows"][0]["destination"]["load_type"] == "full_load"


def test_materialize_exposes_no_environment_selector() -> None:
    assert "environments" not in inspect.signature(build_tool.materialize).parameters
    source = Path(build_tool.__file__).read_text(encoding="utf-8")
    assert '"--environment"' not in source


@pytest.mark.parametrize(
    ("layout", "roles", "filenames"),
    [
        ("single", {"config_path"}, {"metadata.json"}),
        (
            "split-connections",
            {"config_path", "connections_path"},
            {"dataflows.json", "connections.json"},
        ),
        (
            "split-all",
            {"config_path", "connections_path", "schema_hints_path"},
            {"dataflows.json", "connections.json", "schema_hints.json"},
        ),
    ],
)
def test_materialize_supports_exact_metadata_layouts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    layout: str,
    roles: set[str],
    filenames: set[str],
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    (workspace / "metadata/schema_hints.json").write_text(
        json.dumps({
            "schema_hints": [{
                "connection_name": "source",
                "table_name": "orders",
                "hints": [{"column_name": "id", "data_type": "long"}],
            }]
        }),
        encoding="utf-8",
    )
    result = build_tool.materialize(
        workspace=workspace, metadata_layout=layout
    )
    build_dir = Path(result["build_dir"])
    manifest = build_tool.verify_build(build_dir)
    metadata = manifest["environments"]["dev"]["metadata"]

    assert metadata["layout"] == layout
    assert set(metadata["files"]) == roles
    assert {Path(item["path"]).name for item in metadata["files"].values()} == filenames
    assert {path.name for path in (build_dir / "dev/metadata").iterdir()} == filenames
    primary = json.loads(
        (build_dir / metadata["files"]["config_path"]["path"]).read_text(encoding="utf-8")
    )
    if layout == "split-all":
        hints = json.loads(
            (build_dir / metadata["files"]["schema_hints_path"]["path"]).read_text(
                encoding="utf-8"
            )
        )
        assert "schema_hints" not in primary
        assert hints["schema_hints"]
    else:
        assert primary["schema_hints"]


def test_metadata_layout_changes_build_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    single = build_tool.materialize(
        workspace=workspace, metadata_layout="single"
    )
    split = build_tool.materialize(
        workspace=workspace,
        metadata_layout="split-connections",
    )

    assert single["build_id"] != split["build_id"]


def test_materialize_rejects_unknown_metadata_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="Unsupported metadata layout"):
        build_tool.materialize(
            workspace=workspace, metadata_layout="automatic"
        )


def test_current_descriptor_schema_and_projection_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = json.loads(
        (BUILD_SKILL_DIR / "schemas/current-build.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    descriptor_path = workspace / ".builds/current/build.json"
    Draft202012Validator(schema).validate(
        json.loads(descriptor_path.read_text(encoding="utf-8"))
    )
    assert (workspace / ".builds/current/test/metadata/metadata.json").is_file()

    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )
    second = build_tool.materialize(workspace=workspace)
    assert second["build_id"] != first["build_id"]
    assert json.loads(descriptor_path.read_text(encoding="utf-8"))["build_id"] == second["build_id"]
    assert (workspace / ".builds/current/dev/metadata/metadata.json").is_file()
    assert (workspace / ".builds/current/test/metadata/metadata.json").is_file()
    assert build_tool.verify_current_build(workspace / ".builds/current")["build_id"] == (
        second["build_id"]
    )


def test_current_projection_rejects_unknown_build_and_runtime_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    current_dir = workspace / ".builds/current"
    descriptor_path = current_dir / "build.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["build_id"] = "260808-091012-000000000000"
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
    with pytest.raises(ValueError, match="Incomplete build"):
        build_tool.verify_current_build(current_dir)

    descriptor["build_id"] = result["build_id"]
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
    (current_dir / "dev/metadata/metadata.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match build"):
        build_tool.verify_current_build(current_dir)
    assert build_tool.verify_build(Path(result["build_dir"]))["build_id"] == result["build_id"]


def test_validate_build_cli_accepts_current_directly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    monkeypatch.setattr(
        sys,
        "argv",
        ["validate_build.py", "--build-dir", str(workspace / ".builds/current")],
    )

    assert build_validation.main() == 0
    assert f"OK: verified build {result['build_id']}" in capsys.readouterr().out


def test_current_projection_rejects_symlinked_state_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    build_tool.materialize(workspace=workspace)
    current_root = workspace.resolve() / ".builds/current"
    original_is_symlink = Path.is_symlink

    def report_current_as_symlink(path: Path) -> bool:
        return path == current_root or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", report_current_as_symlink)
    with pytest.raises(ValueError, match="Current build path must not be a symlink"):
        build_tool.verify_current_build(current_root)


def test_current_projection_swap_failure_preserves_previous_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    current_root = workspace / ".builds/current"
    original_descriptor = (current_root / "build.json").read_bytes()
    original_rename = Path.rename

    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )

    def fail_current_swap(source: Path, target: Path) -> Path:
        if (
            Path(target) == current_root
            and source.name.startswith(".current-")
            and not source.name.startswith(".current-backup-")
        ):
            raise OSError("simulated current projection swap failure")
        return original_rename(source, target)

    monkeypatch.setattr(Path, "rename", fail_current_swap)
    with pytest.raises(OSError, match="simulated current projection swap failure"):
        build_tool.materialize(workspace=workspace)

    assert (current_root / "build.json").read_bytes() == original_descriptor
    assert build_tool.verify_current_build(current_root)["build_id"] == first["build_id"]


def test_materialized_runner_preserves_verified_durable_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    durable = workspace / "runners/run_local_polars.py"
    source = RUNNER_TEMPLATE.read_text(encoding="utf-8")
    durable.write_text(source, encoding="utf-8")

    result = build_tool.materialize(
        workspace=workspace,
        runner_names=["run_local_polars.py"],
    )
    generated = Path(result["build_dir"]) / "dev/runners/run_local_polars.py"

    assert generated.read_bytes() == durable.read_bytes()
    ast.parse(generated.read_text(encoding="utf-8"))

    calls: list[str | list[str] | None] = []

    class Driver:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def __enter__(self) -> "Driver":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def run(self, *, stage: str | list[str] | None) -> types.SimpleNamespace:
            calls.append(stage)
            return types.SimpleNamespace(failed=0)

    def module(name: str, **attributes: object) -> types.ModuleType:
        result = types.ModuleType(name)
        for key, value in attributes.items():
            setattr(result, key, value)
        return result

    fake_modules = {
        "datacoolie.core": module("datacoolie.core"),
        "datacoolie.core.models": module(
            "datacoolie.core.models", DataCoolieRunConfig=lambda **kwargs: kwargs
        ),
        "datacoolie.engines": module("datacoolie.engines"),
        "datacoolie.engines.polars_engine": module(
            "datacoolie.engines.polars_engine", PolarsEngine=lambda **kwargs: kwargs
        ),
        "datacoolie.metadata": module("datacoolie.metadata"),
        "datacoolie.metadata.file_provider": module(
            "datacoolie.metadata.file_provider", FileProvider=lambda **kwargs: kwargs
        ),
        "datacoolie.orchestration": module("datacoolie.orchestration"),
        "datacoolie.orchestration.driver": module(
            "datacoolie.orchestration.driver", DataCoolieDriver=Driver
        ),
        "datacoolie.platforms": module("datacoolie.platforms"),
        "datacoolie.platforms.local_platform": module(
            "datacoolie.platforms.local_platform", LocalPlatform=object
        ),
    }
    for name, fake_module in fake_modules.items():
        monkeypatch.setitem(sys.modules, name, fake_module)

    namespace = {"__name__": "generated_runner_test"}
    exec(compile(generated.read_text(encoding="utf-8"), str(generated), "exec"), namespace)
    namespace["parse_args"] = lambda: types.SimpleNamespace(
        stage=None,
        metadata_path="metadata.json",
        connections_path=None,
        schema_hints_path=None,
        watermark_base_path=".runtime/dev/watermarks",
        base_log_path=".runtime/dev/logs",
        dry_run=False,
        max_workers=1,
    )
    assert namespace["main"]() == 0
    assert calls == [None]


def test_materialized_manifest_binds_approved_design(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    architecture = workspace / "architecture" / "current.md"
    architecture.parent.mkdir(parents=True)
    architecture.write_text(
        "---\nartifact_type: architecture\n---\n# Architecture\n",
        encoding="utf-8",
    )
    receipt = design_approval.record_approval(
        workspace=workspace,
        architecture=architecture,
        approved_by="owner",
        approval_reference="current session",
        approved_scope="material design",
    )

    result = build_tool.materialize(workspace=workspace)
    manifest = build_tool.verify_build(Path(result["build_dir"]))
    assert manifest["design"] == {
        "architecture_path": "architecture/current.md",
        "architecture_sha256": design_approval.sha256_file(architecture),
        "approval_receipt": receipt.relative_to(workspace).as_posix(),
    }


def test_build_id_uses_invocation_time_and_content_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )
    second = build_tool.materialize(workspace=workspace)
    assert second["build_id"] != first["build_id"]
    assert second["build_id"].startswith("260808-091012-")
    assert second["reused"] is False
    assert build_tool.verify_build(Path(first["build_dir"]))["content_digest"] == (
        build_tool.verify_build(Path(second["build_dir"]))["content_digest"]
    )

    runner = workspace / "runners/run_local_polars.py"
    runner.write_text("ENGINE = 'polars'\nREVISION = 2\n", encoding="utf-8")
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 13, tzinfo=timezone.utc),
    )
    third = build_tool.materialize(workspace=workspace)
    assert third["build_id"].startswith("260808-091013-")
    assert Path(first["build_dir"]).is_dir()


def test_every_environment_overlay_participates_in_build_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    first_manifest = build_tool.verify_build(Path(first["build_dir"]))

    (workspace / "metadata/environments/test.json").write_text(
        json.dumps(
            {"connections": [{"name": "destination", "configure": {"base_path": "changed"}}]}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )

    second = build_tool.materialize(workspace=workspace)
    second_manifest = build_tool.verify_build(Path(second["build_dir"]))
    assert second["build_id"] != first["build_id"]
    assert (
        second_manifest["environments"]["test"]["metadata"]["sha256"]
        != first_manifest["environments"]["test"]["metadata"]["sha256"]
    )
    assert (
        second_manifest["environments"]["dev"]["metadata"]["sha256"]
        == first_manifest["environments"]["dev"]["metadata"]["sha256"]
    )


def test_invalid_configured_environment_blocks_complete_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    config = yaml.safe_load((workspace / "config.yaml").read_text(encoding="utf-8"))
    config["environments"]["test"]["platform"] = "not_installed_here"
    (workspace / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="test=not_installed_here"):
        build_tool.materialize(workspace=workspace)


def test_materialization_rejects_unregistered_runner_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    (workspace / "runners/run_local_unknown.py").write_text("pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unregistered engine"):
        build_tool.materialize(
            workspace=workspace,
            runner_names=["run_local_unknown.py"],
        )


def test_materialization_tooling_identity_covers_runtime_helpers() -> None:
    paths = {item["path"] for item in build_tool._tooling_entries()}
    assert "scripts/_loaders.py" in paths
    assert "scripts/requirements.txt" in paths
    assert "scripts/validate_functions.py" in paths
    assert "schemas/current-build.schema.json" in paths


def test_checksum_tampering_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    (build_dir / "dev/metadata/metadata.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="does not match generated bytes"):
        build_tool.verify_build(build_dir)


def test_content_digest_tampering_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    manifest_path = build_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["content_digest"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="content digest mismatch"):
        build_tool.verify_build(build_dir)


def test_legacy_build_manifest_schema_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    manifest_path = build_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = 1
    manifest["functions"] = []
    manifest.pop("functions_artifact")
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported build manifest schema"):
        build_tool.verify_build(build_dir)


def test_invalid_date_prefixed_build_id_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    invalid_dir = build_dir.parent / "not-a-build-id"
    build_dir.rename(invalid_dir)
    manifest_path = invalid_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["build_id"] = "not-a-build-id"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid build ID"):
        build_tool.verify_build(invalid_dir)


def test_build_id_must_match_creation_date_and_content_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    suffix = result["build_id"].split("-", 1)[1]

    wrong_date = build_dir.parent / f"260809-{suffix}"
    shutil.copytree(build_dir, wrong_date)
    date_manifest_path = wrong_date / "manifest.json"
    date_manifest = json.loads(date_manifest_path.read_text(encoding="utf-8"))
    date_manifest["build_id"] = wrong_date.name
    date_manifest_path.write_text(json.dumps(date_manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="creation date mismatch"):
        build_tool.verify_build(wrong_date)

    wrong_digest = build_dir.parent / "260808-091011-000000000000"
    shutil.copytree(build_dir, wrong_digest)
    digest_manifest_path = wrong_digest / "manifest.json"
    digest_manifest = json.loads(digest_manifest_path.read_text(encoding="utf-8"))
    digest_manifest["build_id"] = wrong_digest.name
    digest_manifest_path.write_text(json.dumps(digest_manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="ID/content digest mismatch"):
        build_tool.verify_build(wrong_digest)


def test_short_build_id_collision_never_overwrites_existing_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    original = Path(first["build_dir"])
    original_manifest = (original / "manifest.json").read_bytes()

    runner = workspace / "runners/run_local_polars.py"
    runner.write_text("ENGINE = 'polars'\nREVISION = 2\n", encoding="utf-8")
    monkeypatch.setattr(build_tool, "_build_id", lambda content_digest, created_at: first["build_id"])

    with pytest.raises(RuntimeError, match="Build ID collision"):
        build_tool.materialize(workspace=workspace)
    assert (original / "manifest.json").read_bytes() == original_manifest


def test_build_directory_symlink_is_rejected_before_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    alias = tmp_path / "aliased-build"
    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == alias or original_is_symlink(path),
    )

    with pytest.raises(ValueError, match="Build path must not be a symlink"):
        build_tool.verify_build(alias)


def test_requested_runner_must_cover_configured_environments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    (workspace / "runners/run_cloud_spark.py").write_text("pass\n", encoding="utf-8")
    with pytest.raises(ValueError, match="No runner compatible"):
        build_tool.materialize(
            workspace=workspace,
            runner_names=["run_cloud_spark.py"],
        )


@pytest.mark.parametrize(
    "runner_name",
    [
        "run_local_polars.py",
        "replay_local_polars.py",
        "maintenance_local_spark.ipynb",
    ],
)
def test_operational_runner_names_match_environment_platform(runner_name: str) -> None:
    build_tool._validate_runner_name(runner_name, "local")


@pytest.mark.parametrize(
    ("runner_name", "platform", "provider"),
    [
        ("run_fabric_polars_azure_sdk.py", "fabric", "azure_sdk"),
        ("run_databricks_polars_sdk.py", "databricks", "sdk"),
    ],
)
def test_external_platform_runner_names_encode_backend_variant(
    runner_name: str,
    platform: str,
    provider: str,
) -> None:
    assert build_tool._runner_identity(runner_name, platform, {"polars"}) == {
        "operation": "run",
        "engine": "polars",
        "provider": provider,
    }


@pytest.mark.parametrize(
    "runner_name",
    ["execute_local_polars.py", "replay_cloud_polars.py", "maintenance_local_.py"],
)
def test_invalid_operational_runner_names_are_rejected(runner_name: str) -> None:
    with pytest.raises(ValueError, match="must match"):
        build_tool._validate_runner_name(runner_name, "local")


def _write_build_receipt(
    workspace: Path,
    build_dir: Path,
    *,
    receipt_id: str = "verification-1",
    status: str = "succeeded",
) -> Path:
    manifest = build_tool.verify_build(build_dir)
    artifacts = {item["path"]: item["sha256"] for item in manifest["artifacts"]}
    environment = "dev"
    runner_path = f"{environment}/runners/run_local_polars.py"
    metadata = manifest["environments"][environment]["metadata"]
    checks = [
        {
            "name": "generated-artifact-validation",
            "status": "passed" if status == "succeeded" else "failed",
            "evidence": "immutable build and generated slice validator",
        }
    ]
    function_artifact = manifest["functions_artifact"]
    if function_artifact is not None:
        checks.extend(
            [
                {
                    "name": "functions-artifact-import",
                    "status": "passed" if status == "succeeded" else "failed",
                    "evidence": "isolated import",
                },
            ]
        )
    receipt = {
        "schema_version": 4,
        "artifact_type": "build_verification",
        "receipt_id": receipt_id,
        "status": status,
        "build_id": manifest["build_id"],
        "environment": environment,
        "platform": manifest["environments"][environment]["platform"],
        "datacoolie_version": manifest["datacoolie_version"],
        "runner": {"path": runner_path, "sha256": artifacts[runner_path]},
        "metadata": metadata,
        "functions_artifact": function_artifact,
        "operation": "run",
        "stage": "bronze",
        "execution_reference": "pytest generated build execution",
        "base_log_path": ".runtime/dev/logs",
        "watermark_base_path": ".runtime/dev/watermarks",
        "checks": checks,
        "started_at": "2026-08-08T09:10:11Z",
        "finished_at": "2026-08-08T09:11:11Z",
        "unresolved_issues": [] if status == "succeeded" else ["generated execution failed"],
    }
    receipt_path = (
        workspace
        / ".builds"
        / "evidence"
        / manifest["build_id"]
        / environment
        / f"{receipt_id}.json"
    )
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    return receipt_path


def test_successful_build_receipt_matches_exact_generated_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)

    receipt = build_validation.validate_receipt(
        build_dir, receipt_path, require_success=True
    )
    assert receipt["build_id"] == result["build_id"]
    current_receipt = build_validation.validate_receipt(
        workspace / ".builds/current", receipt_path, require_success=True
    )
    assert current_receipt["build_id"] == result["build_id"]


def test_build_host_runtime_execution_is_optional_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["checks"].append({
        "name": "generated-runtime-execution",
        "status": "skipped",
        "evidence": "exact runner requires its target execution host",
    })
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    assert build_validation.validate_receipt(
        build_dir, receipt_path, require_success=True
    )["status"] == "succeeded"

    receipt["checks"][-1]["status"] = "failed"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid build verification receipt"):
        build_validation.validate_receipt(build_dir, receipt_path)


def test_failed_build_receipt_is_evidence_but_not_releasable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(
        workspace, build_dir, receipt_id="verification-failed", status="failed"
    )

    assert build_validation.validate_receipt(build_dir, receipt_path)["status"] == "failed"
    with pytest.raises(ValueError, match="successful build verification receipt"):
        build_validation.validate_receipt(
            build_dir, receipt_path, require_success=True
        )


def test_build_receipt_v3_is_audit_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["schema_version"] = 3
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(ValueError, match="schema_version"):
        build_validation.validate_receipt(build_dir, receipt_path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("base_log_path", ".builds/build/dev/logs", "outside .builds"),
        ("finished_at", "2026-08-08T08:11:11Z", "must not precede"),
    ],
)
def test_build_receipt_rejects_invalid_runtime_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
    message: str,
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt[field] = value
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        build_validation.validate_receipt(build_dir, receipt_path)


def test_build_receipt_rejects_artifact_hash_and_filename_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace)
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["runner"]["sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        build_validation.validate_receipt(build_dir, receipt_path)

    receipt["runner"]["sha256"] = build_tool._sha256(
        build_dir / receipt["runner"]["path"]
    )
    receipt["receipt_id"] = "another-id"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="filename must match"):
        build_validation.validate_receipt(build_dir, receipt_path)
