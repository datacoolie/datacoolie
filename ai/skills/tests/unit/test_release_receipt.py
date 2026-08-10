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


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "project_dcws"
    build_dir = workspace / ".builds" / "candidate"
    artifacts = []
    environments = {}
    for environment in ("dev", "qa"):
        metadata = build_dir / environment / "metadata.json"
        runner = build_dir / environment / "runners" / "run_target.py"
        runner.parent.mkdir(parents=True)
        metadata.write_text(json.dumps({"environment": environment}), encoding="utf-8")
        runner.write_text("print('run')\n", encoding="utf-8")
        metadata_relative = metadata.relative_to(build_dir).as_posix()
        runner_relative = runner.relative_to(build_dir).as_posix()
        artifacts.extend([
            {"path": metadata_relative, "sha256": validate_release.sha256_file(metadata)},
            {"path": runner_relative, "sha256": validate_release.sha256_file(runner)},
        ])
        environments[environment] = {
            "platform": "target-platform",
            "metadata": metadata_relative,
            "runners": [runner_relative],
        }
    manifest = {
        "schema_version": 1,
        "build_id": "candidate",
        "created_at": "2026-08-10T00:00:00Z",
        "input_digest": "1" * 64,
        "datacoolie_version": "0.1.3",
        "environments": environments,
        "functions": [],
        "artifacts": artifacts,
    }
    manifest["content_digest"] = validate_release.canonical_digest({
        "input_digest": manifest["input_digest"],
        "artifacts": manifest["artifacts"],
    })
    build_id = f"260810-{manifest['content_digest'][:12]}"
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
        metadata_path = build_dir / environment / "metadata.json"
        runner_path = build_dir / environment / "runners" / "run_target.py"
        build_receipt = {
            "schema_version": 1,
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
            "metadata": {
                "path": metadata_path.relative_to(build_dir).as_posix(),
                "sha256": validate_release.sha256_file(metadata_path),
            },
            "functions": [],
            "operation": "run",
            "stage_plan": [],
            "execution_reference": "generated runner integration test",
            "base_log_path": f".runtime/{environment}/logs",
            "watermark_base_path": f".runtime/{environment}/watermarks",
            "checks": [{
                "name": "generated-runtime-execution",
                "status": "passed",
                "evidence": f".runtime/{environment}/logs/run.log",
            }],
            "started_at": "2026-08-10T00:00:00Z",
            "finished_at": "2026-08-10T00:00:01Z",
            "unresolved_issues": [],
        }
        Draft202012Validator(
            json.loads(BUILD_SCHEMA.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        ).validate(build_receipt)
        receipt_path = (
            workspace / ".evidence" / "builds" / build_id / environment / "build-check.json"
        )
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    return workspace


def _build_id(workspace: Path) -> str:
    builds = [path.name for path in (workspace / ".builds").iterdir() if path.is_dir()]
    assert len(builds) == 1
    return builds[0]


def _write_release(
    workspace: Path,
    *,
    environment: str,
    release_id: str,
    action: str = "deploy",
    status: str = "succeeded",
    source_release: Path | None = None,
) -> tuple[Path, dict]:
    build_id = _build_id(workspace)
    build_dir = workspace / ".builds" / build_id
    metadata = build_dir / environment / "metadata.json"
    runner = build_dir / environment / "runners" / "run_target.py"
    build_receipt = (
        workspace / ".evidence" / "builds" / build_id / environment / "build-check.json"
    )
    failed = status == "failed"
    payload = {
        "schema_version": 1,
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
        "metadata": _artifact(metadata, workspace),
        "functions": [],
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
            "protection": "standard",
            "release_reference": f"target/releases/{build_id}/{environment}",
            "activation_reference": None if failed else f"target/active/{environment}",
            "status": "partial" if failed else "active",
        },
        "transfers": [{
            "source": _artifact(metadata, workspace),
            "target_reference": f"target/releases/{build_id}/{environment}/metadata.json",
            "observed_sha256": None if failed else validate_release.sha256_file(metadata),
            "status": "failed" if failed else "succeeded",
            "evidence": "partial transfer" if failed else "target-side digest",
        }],
        "verification": [
            {"name": "build-preflight", "status": "passed", "evidence": "validator"},
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
    evidence = workspace / ".evidence" / "provision" / environment
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
    provision_path = evidence / "provision-release.json"
    provision_path.write_text(json.dumps(provision), encoding="utf-8")
    payload["provision_receipt"] = _artifact(provision_path, workspace)
    payload["provision_requirements"] = provision["requirements"]
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


def test_successful_deploy_validates_exact_build_slice(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, _ = _write_release(workspace, environment="qa", release_id="release-1")
    receipt = validate_release.validate_receipt(workspace, path, require_success=True)
    assert receipt["build_id"] == _build_id(workspace)


def test_authorization_is_bound_to_exact_build_action_and_target(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["authorization"]["build_id"] = "260810-bbbbbbbbbbbb"
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
    payload["target"]["activation_reference"] = "observed/after/mutation"
    payload["target"]["status"] = "failed"
    payload["transfers"][0]["status"] = "failed"
    payload["transfers"][0]["observed_sha256"] = None
    payload["transfers"][0]["evidence"] = "different transfer observation"
    payload["verification"][1]["evidence"] = "different observation"
    assert validate_release.deployment_intent_sha256(payload) == expected


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
    assert validate_release.validate_receipt(workspace, path)["status"] == "succeeded"

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


def test_release_requires_generated_runtime_proof(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    build_receipt_path = workspace / payload["build_receipt"]["path"]
    build_receipt = json.loads(build_receipt_path.read_text(encoding="utf-8"))
    build_receipt["checks"] = []
    build_receipt_path.write_text(json.dumps(build_receipt), encoding="utf-8")
    payload["build_receipt"] = _artifact(build_receipt_path, workspace)
    payload["authorization"]["deployment_intent_sha256"] = (
        validate_release.deployment_intent_sha256(payload)
    )
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="generated-runtime-execution"):
        validate_release.validate_receipt(workspace, path)


def test_release_rejects_untracked_build_files(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, _ = _write_release(workspace, environment="qa", release_id="release-1")
    build_dir = workspace / ".builds" / _build_id(workspace)
    (build_dir / "undeclared.txt").write_text("not in checksums\n", encoding="utf-8")
    with pytest.raises(ValueError, match="untracked or missing files"):
        validate_release.validate_receipt(workspace, path)


def test_failed_partial_release_cannot_satisfy_success_gate(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, _ = _write_release(
        workspace, environment="qa", release_id="release-failed", status="failed"
    )
    assert validate_release.validate_receipt(workspace, path)["status"] == "failed"
    with pytest.raises(ValueError, match="successful release receipt"):
        validate_release.validate_receipt(workspace, path, require_success=True)


def test_target_digest_mismatch_is_rejected(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    path, payload = _write_release(workspace, environment="qa", release_id="release-1")
    payload["transfers"][0]["observed_sha256"] = "f" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="Observed target SHA-256"):
        validate_release.validate_receipt(workspace, path)


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
    with pytest.raises(ValueError, match="successful release receipt"):
        validate_release.validate_receipt(workspace, rollback)

    latest = rollback.with_name("latest.json")
    latest.write_bytes(rollback.read_bytes())
    with pytest.raises(ValueError, match="latest and globs are forbidden"):
        validate_release.validate_receipt(workspace, latest)
