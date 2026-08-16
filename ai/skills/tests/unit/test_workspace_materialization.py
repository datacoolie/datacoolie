"""Tests for immutable DataCoolie workspace builds."""

from __future__ import annotations

import ast
import json
import shutil
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
    (workspace / "functions").mkdir()
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
    (workspace / "functions/__init__.py").write_text("", encoding="utf-8")
    (workspace / "functions/transforms.py").write_text(
        "def clean(frame): return frame\n", encoding="utf-8"
    )
    return workspace


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
        assert (build_dir / environment / "metadata.json").is_file()
    assert (build_dir / "dist/functions.zip").is_file()
    assert not any(path.is_symlink() for path in build_dir.rglob("*"))
    for environment in ("dev", "test"):
        pointer = json.loads(
            (workspace / ".builds" / "current" / f"{environment}.json").read_text(
                encoding="utf-8"
            )
        )
        assert pointer == {
            "schema_version": 1,
            "artifact_type": "current_build",
            "environment": environment,
            "build_id": result["build_id"],
        }
        assert build_tool.resolve_current_build(workspace, environment) == build_dir

    test_metadata = json.loads(
        (build_dir / "test/metadata.json").read_text(encoding="utf-8")
    )
    destination = next(
        item for item in test_metadata["connections"] if item["name"] == "destination"
    )
    assert destination["configure"]["base_path"] == "test-output"


def test_current_pointer_schema_and_environment_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = json.loads(
        (BUILD_SKILL_DIR / "schemas/current-build.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace)
    test_pointer_path = workspace / ".builds/current/test.json"
    test_pointer = json.loads(test_pointer_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(test_pointer)

    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )
    second = build_tool.materialize(workspace=workspace, environments=["dev"])
    assert second["build_id"] != first["build_id"]
    assert json.loads(test_pointer_path.read_text(encoding="utf-8"))["build_id"] == (
        first["build_id"]
    )
    assert build_tool.resolve_current_build(workspace, "dev") == Path(second["build_dir"])


def test_current_pointer_rejects_wrong_environment_and_unknown_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
    pointer_path = workspace / ".builds/current/dev.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["environment"] = "qa"
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    with pytest.raises(ValueError, match="environment does not match"):
        build_tool.resolve_current_build(workspace, "dev")

    pointer["environment"] = "dev"
    pointer["build_id"] = "260808-091012-000000000000"
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    with pytest.raises(ValueError, match="Incomplete build"):
        build_tool.resolve_current_build(workspace, "dev")
    assert build_tool.verify_build(Path(result["build_dir"]))["build_id"] == result["build_id"]


def test_current_pointer_rejects_symlinked_state_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    build_tool.materialize(workspace=workspace, environments=["dev"])
    current_root = workspace.resolve() / ".builds/current"
    original_is_symlink = Path.is_symlink

    def report_current_as_symlink(path: Path) -> bool:
        return path == current_root or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", report_current_as_symlink)
    with pytest.raises(ValueError, match="Current-build path must not be a symlink"):
        build_tool.resolve_current_build(workspace, "dev")


def test_current_pointer_atomic_failure_preserves_previous_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace, environments=["dev"])
    pointer_path = workspace / ".builds/current/dev.json"
    original_pointer = pointer_path.read_bytes()
    original_replace = build_tool.os.replace

    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )

    def fail_current_replace(source: str, target: Path) -> None:
        if Path(target) == pointer_path:
            raise OSError("simulated pointer replace failure")
        original_replace(source, target)

    monkeypatch.setattr(build_tool.os, "replace", fail_current_replace)
    with pytest.raises(OSError, match="simulated pointer replace failure"):
        build_tool.materialize(workspace=workspace, environments=["dev"])

    assert pointer_path.read_bytes() == original_pointer
    assert build_tool.resolve_current_build(workspace, "dev") == Path(first["build_dir"])


def test_materialized_runner_preserves_verified_durable_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    durable = workspace / "runners/run_local_polars.py"
    source = RUNNER_TEMPLATE.read_text(encoding="utf-8")
    durable.write_text(source, encoding="utf-8")

    result = build_tool.materialize(
        workspace=workspace,
        environments=["dev"],
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

    result = build_tool.materialize(workspace=workspace, environments=["dev"])
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
    first = build_tool.materialize(workspace=workspace, environments=["dev"])
    monkeypatch.setattr(
        build_tool,
        "_utc_now",
        lambda: datetime(2026, 8, 8, 9, 10, 12, tzinfo=timezone.utc),
    )
    second = build_tool.materialize(workspace=workspace, environments=["dev"])
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
    third = build_tool.materialize(workspace=workspace, environments=["dev"])
    assert third["build_id"].startswith("260808-091013-")
    assert Path(first["build_dir"]).is_dir()


def test_unselected_environment_changes_do_not_invalidate_subset_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    first = build_tool.materialize(workspace=workspace, environments=["dev"])

    (workspace / "metadata/environments/test.json").write_text(
        json.dumps(
            {"connections": [{"name": "destination", "configure": {"base_path": "changed"}}]}
        ),
        encoding="utf-8",
    )
    config = yaml.safe_load((workspace / "config.yaml").read_text(encoding="utf-8"))
    config["environments"]["test"]["platform"] = "not_installed_here"
    (workspace / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )

    second = build_tool.materialize(workspace=workspace, environments=["dev"])
    assert second["build_id"] == first["build_id"]
    assert second["reused"] is True

    (workspace / "metadata/environments/dev.json").write_text(
        json.dumps(
            {"connections": [{"name": "destination", "configure": {"base_path": "dev-changed"}}]}
        ),
        encoding="utf-8",
    )
    third = build_tool.materialize(workspace=workspace, environments=["dev"])
    assert third["build_id"] != first["build_id"]


def test_materialization_rejects_unregistered_runner_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    (workspace / "runners/run_local_unknown.py").write_text("pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unregistered engine"):
        build_tool.materialize(
            workspace=workspace,
            environments=["dev"],
            runner_names=["run_local_unknown.py"],
        )


def test_materialization_tooling_identity_covers_runtime_helpers() -> None:
    paths = {item["path"] for item in build_tool._tooling_entries()}
    assert "scripts/_loaders.py" in paths
    assert "scripts/requirements.txt" in paths
    assert "schemas/current-build.schema.json" in paths


def test_checksum_tampering_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
    build_dir = Path(result["build_dir"])
    (build_dir / "dev/metadata.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Checksum mismatch"):
        build_tool.verify_build(build_dir)


def test_content_digest_tampering_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
    build_dir = Path(result["build_dir"])
    manifest_path = build_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["content_digest"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="content digest mismatch"):
        build_tool.verify_build(build_dir)


def test_invalid_date_prefixed_build_id_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
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
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
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
    first = build_tool.materialize(workspace=workspace, environments=["dev"])
    original = Path(first["build_dir"])
    original_manifest = (original / "manifest.json").read_bytes()

    runner = workspace / "runners/run_local_polars.py"
    runner.write_text("ENGINE = 'polars'\nREVISION = 2\n", encoding="utf-8")
    monkeypatch.setattr(build_tool, "_build_id", lambda content_digest, created_at: first["build_id"])

    with pytest.raises(RuntimeError, match="Build ID collision"):
        build_tool.materialize(workspace=workspace, environments=["dev"])
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


def test_requested_runner_must_match_selected_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    (workspace / "runners/run_cloud_spark.py").write_text("pass\n", encoding="utf-8")
    with pytest.raises(ValueError, match="No runner compatible"):
        build_tool.materialize(
            workspace=workspace,
            environments=["dev"],
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
    metadata_path = manifest["environments"][environment]["metadata"]
    checks = [
        {
            "name": "generated-runtime-execution",
            "status": "passed" if status == "succeeded" else "failed",
            "evidence": ".runtime/dev/logs/run.log",
        }
    ]
    receipt = {
        "schema_version": 1,
        "artifact_type": "build_verification",
        "receipt_id": receipt_id,
        "status": status,
        "build_id": manifest["build_id"],
        "environment": environment,
        "platform": manifest["environments"][environment]["platform"],
        "datacoolie_version": manifest["datacoolie_version"],
        "runner": {"path": runner_path, "sha256": artifacts[runner_path]},
        "metadata": {"path": metadata_path, "sha256": artifacts[metadata_path]},
        "functions": [
            {"path": path, "sha256": artifacts[path]} for path in manifest["functions"]
        ],
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
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(workspace, build_dir)

    receipt = build_validation.validate_receipt(
        build_dir, receipt_path, require_success=True
    )
    assert receipt["build_id"] == result["build_id"]


def test_failed_build_receipt_is_evidence_but_not_releasable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = _workspace(tmp_path, monkeypatch)
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
    build_dir = Path(result["build_dir"])
    receipt_path = _write_build_receipt(
        workspace, build_dir, receipt_id="verification-failed", status="failed"
    )

    assert build_validation.validate_receipt(build_dir, receipt_path)["status"] == "failed"
    with pytest.raises(ValueError, match="successful build verification receipt"):
        build_validation.validate_receipt(
            build_dir, receipt_path, require_success=True
        )


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
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
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
    result = build_tool.materialize(workspace=workspace, environments=["dev"])
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
