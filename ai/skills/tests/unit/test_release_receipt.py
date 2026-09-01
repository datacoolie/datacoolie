"""Tests for immutable, authorization-bound release evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import validate_release


SKILL_DIR = Path(__file__).parent.parent.parent / "datacoolie-release"
BUILD_SCHEMA = SKILL_DIR.parent / "datacoolie-build/schemas/build-verification-receipt.schema.json"
PROVISION_SCHEMA = SKILL_DIR.parent / "datacoolie-provision/schemas/provision-receipt.schema.json"


def _artifact(path: Path, workspace: Path) -> dict[str, str]:
    return {
        "path": path.relative_to(workspace).as_posix(),
        "sha256": validate_release.sha256_file(path),
    }


def _metadata_set(files: dict[str, Path], relative_paths: dict[str, str], layout: str) -> dict:
    file_artifacts = {
        role: {"path": relative_paths[role], "sha256": validate_release.sha256_file(path)}
        for role, path in files.items()
    }
    return {
        "layout": layout,
        "files": file_artifacts,
        "sha256": validate_release.canonical_digest(
            {role: item["sha256"] for role, item in file_artifacts.items()}
        ),
    }


def _workspace(
    tmp_path: Path, *, with_functions: bool = False, metadata_layout: str = "single"
) -> Path:
    workspace = tmp_path / "project_dcws"
    build_dir = workspace / ".builds" / "artifacts" / "candidate"
    artifacts = []
    environments = {}
    layout_files = {
        "single": {"config_path": "metadata.json"},
        "split-connections": {
            "config_path": "dataflows.json",
            "connections_path": "connections.json",
        },
        "split-all": {
            "config_path": "dataflows.json",
            "connections_path": "connections.json",
            "schema_hints_path": "schema_hints.json",
        },
    }[metadata_layout]
    for environment in ("dev", "qa"):
        metadata_files = {
            role: build_dir / environment / "metadata" / filename
            for role, filename in layout_files.items()
        }
        runner = build_dir / environment / "runners" / "run_target.py"
        next(iter(metadata_files.values())).parent.mkdir(parents=True)
        runner.parent.mkdir(parents=True)
        for role, metadata in metadata_files.items():
            metadata.write_text(
                json.dumps({"environment": environment, "role": role}), encoding="utf-8"
            )
        runner.write_text("print('run')\n", encoding="utf-8")
        runner_relative = runner.relative_to(build_dir).as_posix()
        metadata_relatives = {
            role: path.relative_to(build_dir).as_posix()
            for role, path in metadata_files.items()
        }
        artifacts.extend(
            {"path": metadata_relatives[role], "sha256": validate_release.sha256_file(path)}
            for role, path in metadata_files.items()
        )
        artifacts.append({"path": runner_relative, "sha256": validate_release.sha256_file(runner)})
        environments[environment] = {
            "platform": "target-platform",
            "metadata": _metadata_set(metadata_files, metadata_relatives, metadata_layout),
            "runners": [runner_relative],
        }
    function_artifact = None
    if with_functions:
        function_path = build_dir / "functions/example_functions.zip"
        function_path.parent.mkdir(parents=True)
        function_path.write_bytes(b"verified project functions")
        function_relative = function_path.relative_to(build_dir).as_posix()
        function_artifact = {
            "format": "zip",
            "path": function_relative,
            "sha256": validate_release.sha256_file(function_path),
            "import_prefix": "example_functions",
            "distribution": None,
            "version": None,
        }
        artifacts.append({"path": function_relative, "sha256": function_artifact["sha256"]})
    manifest = {
        "schema_version": 3,
        "build_id": "candidate",
        "created_at": "2026-08-10T00:00:00Z",
        "input_digest": "1" * 64,
        "datacoolie_version": "0.1.3",
        "environments": environments,
        "functions_artifact": function_artifact,
        "artifacts": artifacts,
    }
    manifest["content_digest"] = validate_release.canonical_digest({
        "input_digest": manifest["input_digest"],
        "artifacts": manifest["artifacts"],
    })
    build_id = f"260810-000000-{manifest['content_digest'][:12]}"
    manifest["build_id"] = build_id
    (build_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    build_dir.rename(build_dir.with_name(build_id))
    build_dir = build_dir.with_name(build_id)
    checksum_lines = [
        f"{validate_release.sha256_file(path)}  {path.relative_to(build_dir).as_posix()}"
        for path in sorted(build_dir.rglob("*"))
        if path.is_file()
    ]
    (build_dir / "SHA256SUMS").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")

    for environment in environments:
        runner_path = build_dir / environment / "runners" / "run_target.py"
        checks = [{
            "name": "generated-artifact-validation",
            "status": "passed",
            "evidence": "immutable build and resolved slice validator",
        }]
        if function_artifact is not None:
            checks.extend([
                {
                    "name": "functions-artifact-import",
                    "status": "passed",
                    "evidence": "isolated import",
                },
            ])
        build_receipt = {
            "schema_version": 4,
            "artifact_type": "build_verification",
            "receipt_id": "build-check",
            "status": "succeeded",
            "build_id": build_id,
            "environment": environment,
            "platform": "target-platform",
            "datacoolie_version": "0.1.3",
            "runner": {
                "path": runner_path.relative_to(build_dir).as_posix(),
                "sha256": validate_release.sha256_file(runner_path),
            },
            "metadata": manifest["environments"][environment]["metadata"],
            "functions_artifact": function_artifact,
            "operation": "run",
            "stage": None,
            "execution_reference": "generated runner integration test",
            "base_log_path": f".runtime/{environment}/logs",
            "watermark_base_path": f".runtime/{environment}/watermarks",
            "checks": checks,
            "started_at": "2026-08-10T00:00:00Z",
            "finished_at": "2026-08-10T00:00:01Z",
            "unresolved_issues": [],
        }
        Draft202012Validator(
            json.loads(BUILD_SCHEMA.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        ).validate(build_receipt)
        receipt_path = (
            workspace / ".builds" / "evidence" / build_id / environment / "build-check.json"
        )
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    return workspace


def _build_id(workspace: Path) -> str:
    builds = [
        path.name
        for path in (workspace / ".builds" / "artifacts").iterdir()
        if path.is_dir()
    ]
    assert len(builds) == 1
    return builds[0]


def _write_release(
    workspace: Path,
    *,
    environment: str,
    release_id: str,
    action: str = "deploy",
    status: str = "active",
    source_release: Path | None = None,
) -> tuple[Path, dict]:
    build_id = _build_id(workspace)
    build_dir = workspace / ".builds" / "artifacts" / build_id
    runner = build_dir / environment / "runners" / "run_target.py"
    build_receipt = (
        workspace / ".builds" / "evidence" / build_id / environment / "build-check.json"
    )
    failed = status == "failed"
    manifest = json.loads((build_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest_metadata = manifest["environments"][environment]["metadata"]
    metadata_files = {
        role: build_dir / item["path"]
        for role, item in manifest_metadata["files"].items()
    }
    manifest_function = manifest["functions_artifact"]
    release_function = None
    attachment = None
    if manifest_function is not None:
        release_function = {
            **manifest_function,
            "path": (
                Path(".builds/artifacts") / build_id / manifest_function["path"]
            ).as_posix(),
        }
        attachment = {
            "method": "target-job-library",
            "target_reference": (
                f"target/candidates/{environment}/run_target/{release_id}/functions/library"
            ),
            "fresh_session_required": True,
        }
    release_metadata = _metadata_set(
        metadata_files,
        {
            role: (Path(".builds/artifacts") / build_id / item["path"]).as_posix()
            for role, item in manifest_metadata["files"].items()
        },
        manifest_metadata["layout"],
    )
    payload = {
        "schema_version": 7,
        "artifact_type": "release_receipt",
        "release_id": release_id,
        "action": action,
        "status": status,
        "build_id": build_id,
        "environment": environment,
        "platform": "target-platform",
        "manifest": _artifact(build_dir / "manifest.json", workspace),
        "build_receipt": _artifact(build_receipt, workspace),
        "runner": _artifact(runner, workspace),
        "metadata": release_metadata,
        "functions_artifact": release_function,
        "functions_attachment": attachment,
        "provision_receipt": None,
        "provision_requirements": None,
        "source_release": _artifact(source_release, workspace) if source_release else None,
        "previous_active_release_id": "unhealthy-release" if action == "rollback" else None,
        "authorization": {
            "reference": "exact target authorization",
            "source": "target_policy",
            "action": action,
            "environment": environment,
            "build_id": build_id,
            "deployment_intent_sha256": "0" * 64,
        },
        "target": {
            "identity": f"target-environment/{environment}",
            "activation_mechanism": "target-native-association",
            "runner_deployment_kind": "external-python",
            "runner_runtime_kind": "python",
            "protection": "standard",
            "candidate_reference": f"target/candidates/{environment}/run_target/{release_id}",
            "current_reference": f"target/active/{environment}/run_target/current",
            "status": "partial" if failed else "active",
        },
        "deployment_marker": {
            "sha256": "0" * 64,
            "observed_sha256": None,
            "observed_build_id": None if failed else build_id,
            "observed_release_id": None if failed else release_id,
        },
        "runtime_paths": {
            "base_log_path": f"target/control/{environment}/logs",
            "watermark_base_path": f"target/control/{environment}/watermarks",
        },
        "qualification_scope": {
            "base_log_path": f"target/control/{environment}/qualification/{release_id}/logs",
            "watermark_base_path": (
                f"target/control/{environment}/qualification/{release_id}/watermarks"
            ),
            "destination_effect": "isolated",
        },
        "runtime_state": {
            "action": "initialize",
            "reference": "target observation: no active watermark state",
        },
        "transfers": [
            {
                "source": _artifact(runner, workspace),
                "target_reference": (
                    f"target/candidates/{environment}/run_target/{release_id}/run_target.py"
                ),
                "observed_sha256": (
                    None if failed else validate_release.sha256_file(runner)
                ),
                "status": "failed" if failed else "succeeded",
                "evidence": "partial transfer" if failed else "target-side digest",
            },
        ],
        "verification": [
            {"name": "build-preflight", "status": "passed", "evidence": "validator"},
            {
                "name": "resource-readiness",
                "status": "passed",
                "evidence": "required resources observed",
            },
            {
                "name": "environment-isolation",
                "status": "passed",
                "evidence": f"isolated target paths for {environment}",
            },
            {
                "name": "shared-component-compatibility",
                "status": "passed",
                "evidence": "metadata and functions projection is compatible with target runners",
            },
            {
                "name": "runtime-state-preflight",
                "status": "passed",
                "evidence": "no active watermark state",
            },
            {
                "name": "candidate-artifact-integrity",
                "status": "passed",
                "evidence": "candidate matches exact runner slice",
            },
            {
                "name": "deployment-marker-integrity",
                "status": "failed" if failed else "passed",
                "evidence": "exact candidate and current marker identity",
            },
            {
                "name": "candidate-runtime-qualification",
                "status": "failed" if failed else "passed",
                "evidence": "isolated target runner execution",
                "method": "isolated-smoke",
            },
            {
                "name": "activation-preflight",
                "status": "failed" if failed else "passed",
                "evidence": "expected active reference rechecked",
            },
            {
                "name": "target-observation",
                "status": "failed" if failed else "passed",
                "evidence": "observable target state",
            },
        ],
        "started_at": "2026-08-10T00:00:00Z",
        "finished_at": "2026-08-10T00:00:01Z",
        "unresolved_issues": ["partial transfer"] if failed else [],
    }
    for role, metadata in metadata_files.items():
        payload["transfers"].append({
            "source": _artifact(metadata, workspace),
            "target_reference": (
                f"target/candidates/{environment}/run_target/{release_id}/metadata/{metadata.name}"
            ),
            "observed_sha256": (
                None if failed else validate_release.sha256_file(metadata)
            ),
            "status": "failed" if failed else "succeeded",
            "evidence": f"{role}: partial transfer" if failed else f"{role}: target-side digest",
        })
    if release_function is not None:
        function_source = build_dir / manifest_function["path"]
        payload["transfers"].append({
            "source": _artifact(function_source, workspace),
            "target_reference": (
                f"target/candidates/{environment}/run_target/{release_id}/functions/{function_source.name}"
            ),
            "observed_sha256": (
                None if failed else validate_release.sha256_file(function_source)
            ),
            "status": "failed" if failed else "succeeded",
            "evidence": "partial transfer" if failed else "target-side digest",
        })
        payload["verification"].extend([
            {
                "name": "functions-artifact-integrity",
                "status": "passed",
                "evidence": "target digest",
            },
            {
                "name": "functions-attachment",
                "status": "passed",
                "evidence": attachment["target_reference"],
            },
            {
                "name": "functions-target-import",
                "status": "passed",
                "evidence": "target import probe",
            },
            {
                "name": "functions-target-execution",
                "status": "passed",
                "evidence": "function-backed target qualification",
            },
            {
                "name": "functions-session-activation",
                "status": "passed",
                "evidence": "fresh target session",
            },
        ])
    payload["deployment_marker"]["sha256"] = (
        validate_release.deployment_marker_sha256(payload)
    )
    if not failed:
        payload["deployment_marker"]["observed_sha256"] = payload["deployment_marker"][
            "sha256"
        ]
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path = workspace / ".releases" / environment / f"{release_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path, payload


def _attach_provision(workspace: Path, payload: dict) -> None:
    environment = payload["environment"]
    requirements = workspace / "architecture" / "current.md"
    requirements.parent.mkdir(parents=True, exist_ok=True)
    requirements.write_text("# Target requirements\n", encoding="utf-8")
    evidence = workspace / "provision" / "evidence" / environment
    plan = evidence / "plans" / "release-plan.json"
    plan.parent.mkdir(parents=True, exist_ok=True)
    plan.write_text('{"actions":["create-target"]}\n', encoding="utf-8")
    plan_artifact = _artifact(plan, workspace)
    provision = {
        "schema_version": 1,
        "artifact_type": "provision_receipt",
        "receipt_id": "provision-release",
        "operation": "apply",
        "status": "succeeded",
        "environment": environment,
        "platform": payload["platform"],
        "requirements": _artifact(requirements, workspace),
        "plan": plan_artifact,
        "authorizations": [{
            "scope": "apply",
            "reference": "current-session plan approval",
            "environment": environment,
            "plan_sha256": plan_artifact["sha256"],
        }],
        "tool_versions": {"provisioner": "1.0.0"},
        "state": {"backend_reference": "state/qa", "status": "updated"},
        "actions": [{
            "resource": "target",
            "action": "create",
            "status": "succeeded",
            "data_bearing": False,
            "evidence": "target observed",
        }],
        "resource_outputs": [],
        "verification": [{
            "name": "resource-observation",
            "status": "passed",
            "evidence": "target exists",
        }],
        "started_at": "2026-08-10T00:00:00Z",
        "finished_at": "2026-08-10T00:00:01Z",
        "unresolved_issues": [],
    }
    Draft202012Validator(
        json.loads(PROVISION_SCHEMA.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    ).validate(provision)
    provision_path = evidence / "receipts" / "provision-release.json"
    provision_path.parent.mkdir(parents=True, exist_ok=True)
    provision_path.write_text(json.dumps(provision), encoding="utf-8")
    payload["provision_receipt"] = _artifact(provision_path, workspace)
    payload["provision_requirements"] = provision["requirements"]
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )


def _set_incomplete_phase(payload: dict, status: str) -> None:
    assert status in {"prepared", "staged", "qualified"}
    payload["status"] = status
    payload["target"]["status"] = "not_staged" if status == "prepared" else "staged"
    if status == "prepared":
        payload["deployment_marker"]["observed_sha256"] = None
        payload["deployment_marker"]["observed_build_id"] = None
        payload["deployment_marker"]["observed_release_id"] = None
    payload["finished_at"] = None
    for transfer in payload["transfers"]:
        if status == "prepared":
            transfer["status"] = "pending"
            transfer["observed_sha256"] = None
            transfer["evidence"] = None
    pending_checks = {
        "candidate-artifact-integrity",
        "deployment-marker-integrity",
        "candidate-runtime-qualification",
        "activation-preflight",
        "target-observation",
        "functions-artifact-integrity",
        "functions-attachment",
        "functions-target-import",
        "functions-target-execution",
        "functions-session-activation",
    }
    if status == "staged":
        pending_checks.remove("candidate-artifact-integrity")
        pending_checks.remove("deployment-marker-integrity")
    elif status == "qualified":
        pending_checks -= {
            "candidate-artifact-integrity",
            "deployment-marker-integrity",
            "candidate-runtime-qualification",
            "functions-artifact-integrity",
            "functions-attachment",
            "functions-target-import",
            "functions-target-execution",
        }
    for check in payload["verification"]:
        if check["name"] in pending_checks:
            check["status"] = "pending"
            check["evidence"] = None
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )


def test_schema_and_template_are_valid() -> None:
    schema = json.loads(
        (SKILL_DIR / "schemas/release-receipt.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator.check_schema(schema)
    template = json.loads(
        (SKILL_DIR / "templates/release-receipt.json.example").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(template)
    assert template["authorization"]["deployment_intent_sha256"] == (
        validate_release.deployment_intent_sha256(template)
    )


@pytest.mark.parametrize("metadata_layout", ["single", "split-connections", "split-all"])
def test_successful_deploy_validates_exact_build_slice(
    tmp_path: Path, metadata_layout: str
) -> None:
    workspace = _workspace(tmp_path, metadata_layout=metadata_layout)
    path, _ = _write_release(workspace, environment="qa", release_id="release-1")
    receipt = validate_release.validate_receipt(workspace, path, require_success=True)
    assert receipt["build_id"] == _build_id(workspace)
    assert receipt["metadata"]["layout"] == metadata_layout


@pytest.mark.parametrize("status", ["prepared", "staged", "qualified"])
def test_incomplete_release_phases_are_valid_but_not_active(
    tmp_path: Path, status: str
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(
        workspace, environment="qa", release_id=f"release-{status}"
    )
    _set_incomplete_phase(payload, status)
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert validate_release.validate_receipt(workspace, path)["status"] == status
    with pytest.raises(ValueError, match="active release receipt"):
        validate_release.validate_receipt(workspace, path, require_success=True)


def test_active_release_requires_candidate_runtime_qualification(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["verification"] = [
        check
        for check in payload["verification"]
        if check["name"] != "candidate-runtime-qualification"
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid release receipt"):
        validate_release.validate_receipt(workspace, path)


def test_successful_function_release_binds_attachment_and_target_checks(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path, with_functions=True)
    path, receipt = _write_release(
        workspace, environment="qa", release_id="release-functions"
    )

    validated = validate_release.validate_receipt(workspace, path, require_success=True)
    assert validated["functions_artifact"]["import_prefix"] == "example_functions"

    receipt["functions_attachment"]["target_reference"] = "target/libraries/substitute"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="deployment intent"):
        validate_release.validate_receipt(workspace, path)


@pytest.mark.parametrize(
    "check_name",
    [
        "functions-attachment",
        "functions-target-import",
        "functions-target-execution",
        "functions-session-activation",
    ],
)
def test_function_release_requires_target_qualification_checks(
    tmp_path: Path, check_name: str
) -> None:
    workspace = _workspace(tmp_path, with_functions=True)
    path, receipt = _write_release(
        workspace, environment="qa", release_id="release-functions"
    )
    receipt["verification"] = [
        check
        for check in receipt["verification"]
        if check["name"] != check_name
    ]
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid release receipt"):
        validate_release.validate_receipt(workspace, path)


def test_failed_observation_preserves_active_unhealthy_target_state(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-bad")
    payload["status"] = "failed"
    payload["target"]["status"] = "active_unhealthy"
    for check in payload["verification"]:
        if check["name"] == "target-observation":
            check["status"] = "failed"
            check["evidence"] = "active health signal failed"
    payload["unresolved_issues"] = ["active target requires reconciliation"]
    path.write_text(json.dumps(payload), encoding="utf-8")

    validated = validate_release.validate_receipt(workspace, path)
    assert validated["target"]["status"] == "active_unhealthy"


def test_authorization_is_bound_to_exact_build_action_and_target(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["authorization"]["build_id"] = "260810-000001-bbbbbbbbbbbb"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="Authorization build_id"):
        validate_release.validate_receipt(workspace, path)

    path, payload = _write_release(workspace, environment="qa", release_id="release-2")
    payload["target"]["identity"] = "another-target"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="deployment intent"):
        validate_release.validate_receipt(workspace, path)

    path, payload = _write_release(workspace, environment="qa", release_id="release-3")
    payload["transfers"][0]["target_reference"] = "another/target/path"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="deployment intent"):
        validate_release.validate_receipt(workspace, path)


def test_authorization_binds_runtime_paths_and_state_intent(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-path")
    payload["runtime_paths"]["watermark_base_path"] = "target/control/prod/watermarks"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="deployment intent"):
        validate_release.validate_receipt(workspace, path)

    path, payload = _write_release(workspace, environment="qa", release_id="release-state")
    payload["runtime_state"] = {
        "action": "preserve",
        "reference": "release-previous",
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="deployment intent"):
        validate_release.validate_receipt(workspace, path)


@pytest.mark.parametrize(
    "check_name",
    [
        "resource-readiness",
        "environment-isolation",
        "runtime-state-preflight",
        "shared-component-compatibility",
    ],
)
def test_success_requires_control_and_state_preflight(
    tmp_path: Path, check_name: str
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(
        workspace, environment="qa", release_id=f"release-{check_name}"
    )
    payload["verification"] = [
        check for check in payload["verification"] if check["name"] != check_name
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=check_name):
        validate_release.validate_receipt(workspace, path)


@pytest.mark.parametrize("state_action", ["migrate", "reset", "replay"])
def test_state_changing_intent_requires_current_session_authorization(
    tmp_path: Path, state_action: str
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(
        workspace, environment="qa", release_id=f"release-{state_action}"
    )
    payload["runtime_state"] = {
        "action": state_action,
        "reference": f"plans/{state_action}-state.md",
    }
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="current_session"):
        validate_release.validate_receipt(workspace, path)

    payload["authorization"]["source"] = "current_session"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert validate_release.validate_receipt(workspace, path)["status"] == "active"


def test_schema_v1_receipt_is_audit_only(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-v1")
    payload["schema_version"] = 1
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        validate_release.validate_receipt(workspace, path)


def test_schema_v2_receipt_is_audit_only(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-v2")
    payload["schema_version"] = 2
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        validate_release.validate_receipt(workspace, path)


def test_schema_v3_receipt_is_audit_only(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-v3")
    payload["schema_version"] = 3
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        validate_release.validate_receipt(workspace, path)


def test_schema_v4_receipt_is_audit_only(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-v4")
    payload["schema_version"] = 4
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        validate_release.validate_receipt(workspace, path)


@pytest.mark.parametrize("schema_version", [5, 6])
def test_pre_v7_release_receipt_is_audit_only(
    tmp_path: Path, schema_version: int
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(
        workspace, environment="qa", release_id=f"release-v{schema_version}"
    )
    payload["schema_version"] = schema_version
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        validate_release.validate_receipt(workspace, path)


def test_production_requires_current_session_authorization(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-prod")
    payload["target"]["protection"] = "production"
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="current_session|current-session authorization"):
        validate_release.validate_receipt(workspace, path)


def test_post_mutation_observation_does_not_change_authorized_intent(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    _, payload = _write_release(workspace, environment="qa", release_id="release-1")
    expected = validate_release.deployment_intent_sha256(payload)
    payload["target"]["status"] = "failed"
    payload["deployment_marker"]["observed_sha256"] = "f" * 64
    payload["deployment_marker"]["observed_build_id"] = None
    payload["deployment_marker"]["observed_release_id"] = None
    payload["transfers"][0]["status"] = "failed"
    payload["transfers"][0]["observed_sha256"] = None
    payload["transfers"][0]["evidence"] = "different transfer observation"
    payload["verification"][1]["evidence"] = "different observation"
    assert validate_release.deployment_intent_sha256(payload) == expected


def test_target_current_and_candidate_references_must_be_distinct(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["target"]["current_reference"] = payload["target"]["candidate_reference"]
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="must be distinct"):
        validate_release.validate_receipt(workspace, path)


def test_build_addressed_candidate_is_rejected(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["target"]["candidate_reference"] = (
        f"target/candidates/qa/run_target/{payload['build_id']}"
    )
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="release-addressed, not build-addressed"):
        validate_release.validate_receipt(workspace, path)


def test_target_assigned_opaque_candidate_identity_is_supported(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["target"]["candidate_reference"] = "fabric-item://opaque-candidate-identity"
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert validate_release.validate_receipt(workspace, path)["status"] == "active"


@pytest.mark.parametrize("version_field", ["build_id", "release_id"])
def test_target_current_reference_is_version_independent(
    tmp_path: Path, version_field: str
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["target"]["current_reference"] = (
        f"target/active/qa/run_target/{payload[version_field]}"
    )
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="stable and version-independent"):
        validate_release.validate_receipt(workspace, path)


def test_target_current_requires_exact_deployment_marker(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["deployment_marker"]["observed_release_id"] = "another-release"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="wrong target release ID"):
        validate_release.validate_receipt(workspace, path)


def test_qualification_paths_are_isolated_from_active_runtime_state(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["qualification_scope"]["watermark_base_path"] = payload["runtime_paths"][
        "watermark_base_path"
    ]
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="Qualification watermarks"):
        validate_release.validate_receipt(workspace, path)


def test_build_receipt_must_be_successful_and_hash_bound(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    build_receipt_path = workspace / payload["build_receipt"]["path"]
    build_receipt = json.loads(build_receipt_path.read_text(encoding="utf-8"))
    build_receipt["status"] = "failed"
    build_receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    payload["build_receipt"] = _artifact(build_receipt_path, workspace)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="Build receipt status"):
        validate_release.validate_receipt(workspace, path)


def test_release_validates_exact_provision_handoff(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    _attach_provision(workspace, payload)
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert validate_release.validate_receipt(workspace, path)["status"] == "active"

    provision_path = workspace / payload["provision_receipt"]["path"]
    provision = json.loads(provision_path.read_text(encoding="utf-8"))
    provision["authorizations"][0]["plan_sha256"] = "f" * 64
    provision_path.write_text(json.dumps(provision), encoding="utf-8")
    payload["provision_receipt"] = _artifact(provision_path, workspace)
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="plan-bound apply authorization"):
        validate_release.validate_receipt(workspace, path)


def test_release_rejects_incomplete_successful_build_receipt(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    build_receipt_path = workspace / payload["build_receipt"]["path"]
    build_receipt = json.loads(build_receipt_path.read_text(encoding="utf-8"))
    build_receipt.pop("datacoolie_version", None)
    build_receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    payload["build_receipt"] = _artifact(build_receipt_path, workspace)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="datacoolie_version"):
        validate_release.validate_receipt(workspace, path)


def test_release_requires_generated_artifact_proof_not_build_runtime(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    build_receipt_path = workspace / payload["build_receipt"]["path"]
    build_receipt = json.loads(build_receipt_path.read_text(encoding="utf-8"))
    build_receipt["checks"].append({
        "name": "generated-runtime-execution",
        "status": "skipped",
        "evidence": "runner requires its target execution host",
    })
    build_receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    payload["build_receipt"] = _artifact(build_receipt_path, workspace)
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert validate_release.validate_receipt(workspace, path)["status"] == "active"

    build_receipt["checks"] = []
    build_receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    payload["build_receipt"] = _artifact(build_receipt_path, workspace)
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="generated-artifact-validation"):
        validate_release.validate_receipt(workspace, path)


def test_release_rejects_untracked_build_files(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, _ = _write_release(workspace, environment="qa", release_id="release-1")
    build_dir = workspace / ".builds" / "artifacts" / _build_id(workspace)
    (build_dir / "undeclared.txt").write_text("not in checksums\n", encoding="utf-8")
    with pytest.raises(ValueError, match="untracked or missing files"):
        validate_release.validate_receipt(workspace, path)


def test_failed_partial_release_cannot_satisfy_success_gate(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, _ = _write_release(
        workspace, environment="qa", release_id="release-failed", status="failed"
    )
    assert validate_release.validate_receipt(workspace, path)["status"] == "failed"
    with pytest.raises(ValueError, match="active release receipt"):
        validate_release.validate_receipt(workspace, path, require_success=True)


def test_target_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["transfers"][0]["observed_sha256"] = "f" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="Observed target SHA-256"):
        validate_release.validate_receipt(workspace, path)


def test_identical_existing_candidate_is_an_idempotent_transfer(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-retry")
    for transfer in payload["transfers"]:
        transfer["status"] = "skipped"
        transfer["evidence"] = "existing candidate has the exact observed SHA-256"
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert validate_release.validate_receipt(workspace, path)["status"] == "active"


def test_promotion_requires_exact_successful_source_release(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    source, _ = _write_release(workspace, environment="dev", release_id="release-dev")
    target, _ = _write_release(
        workspace,
        environment="qa",
        release_id="release-promote",
        action="promote",
        source_release=source,
    )
    assert validate_release.validate_receipt(workspace, target)["action"] == "promote"


def test_failed_source_release_and_latest_selector_are_rejected(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    source, _ = _write_release(
        workspace, environment="qa", release_id="release-candidate", status="failed"
    )
    rollback, _ = _write_release(
        workspace,
        environment="qa",
        release_id="release-rollback",
        action="rollback",
        source_release=source,
    )
    with pytest.raises(ValueError, match="active release receipt"):
        validate_release.validate_receipt(workspace, rollback)

    latest = rollback.with_name("latest.json")
    latest.write_bytes(rollback.read_bytes())
    with pytest.raises(ValueError, match="latest and globs are forbidden"):
        validate_release.validate_receipt(workspace, latest)


def test_release_rejects_current_projection_as_build_identity(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    current = workspace / ".builds" / "current" / "build.json"
    current.parent.mkdir(parents=True)
    current.write_text(json.dumps({
        "schema_version": 1,
        "artifact_type": "current_build",
        "build_id": payload["build_id"],
    }), encoding="utf-8")
    payload["manifest"] = _artifact(current, workspace)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="exact build manifest"):
        validate_release.validate_receipt(workspace, path)


def test_current_selector_resolves_once_to_the_prepared_build(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    descriptor = workspace / ".builds" / "current" / "build.json"
    descriptor.parent.mkdir(parents=True)
    descriptor.write_text(json.dumps({
        "schema_version": 1,
        "artifact_type": "current_build",
        "build_id": payload["build_id"],
    }), encoding="utf-8")

    validated = validate_release.validate_receipt(
        workspace, path, build_selector="current"
    )
    assert validated["build_id"] == payload["build_id"]
    assert validate_release.validate_receipt(
        workspace, path, build_selector=".builds/current"
    )["build_id"] == payload["build_id"]

    descriptor.write_text(json.dumps({
        "schema_version": 1,
        "artifact_type": "current_build",
        "build_id": "260810-000001-bbbbbbbbbbbb",
    }), encoding="utf-8")
    assert validate_release.validate_receipt(workspace, path)["build_id"] == payload["build_id"]
    with pytest.raises(ValueError, match="Resolved immutable build does not exist"):
        validate_release.validate_receipt(workspace, path, build_selector="current")


def test_explicit_build_selector_must_match_prepared_release(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    assert validate_release.validate_receipt(
        workspace, path, build_selector=payload["build_id"]
    )["status"] == "active"
    with pytest.raises(ValueError, match="Resolved immutable build does not exist"):
        validate_release.validate_receipt(
            workspace, path, build_selector="260810-000001-bbbbbbbbbbbb"
        )
