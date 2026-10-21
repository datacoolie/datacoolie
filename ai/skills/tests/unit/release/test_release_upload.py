from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


SKILL_DIR = Path(__file__).resolve().parents[3] / "datacoolie-release"
SCRIPT = SKILL_DIR / "scripts" / "upload_local.py"


def _module():
    spec = importlib.util.spec_from_file_location("upload_local", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source(tmp_path: Path, build_id: str = "build-1") -> Path:
    source = tmp_path / "retained" / "dev"
    (source / "metadata").mkdir(parents=True)
    (source / "manifest.json").write_text(
        json.dumps(
            {
                "artifact_type": "datacoolie_environment",
                "build_id": build_id,
                "environment": "dev",
            }
        ),
        encoding="utf-8",
    )
    (source / "metadata" / "metadata.json").write_text('{"dataflows": []}\n', encoding="utf-8")
    (source / "runners" / "run.py").parent.mkdir(parents=True)
    (source / "runners" / "run.py").write_text("print('ok')\n", encoding="utf-8")
    return source


def test_local_upload_maps_artifact_then_current_and_keeps_unknown_files(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path)
    deployment = tmp_path / "target with spaces" / "測試"
    old = deployment / "current" / "old.txt"
    old.parent.mkdir(parents=True)
    old.write_text("retain", encoding="utf-8")

    record = module.upload_environment(
        source,
        deployment,
        build_id="build-1",
        environment="dev",
        release_id="release-1",
    )

    assert record["status"] == "success"
    assert (deployment / "artifacts" / "build-1" / "metadata" / "metadata.json").is_file()
    assert (deployment / "current" / "manifest.json").is_file()
    assert old.read_text(encoding="utf-8") == "retain"
    assert record["uploads"]["artifact"]["files"] == record["uploads"]["current"]["files"] == 3


def test_artifact_failure_skips_current(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path)
    calls: list[Path] = []

    def fail_first(source_path: Path, target: Path, deployment: Path) -> None:
        calls.append(target)
        raise module.UploadError("simulated artifact failure")

    record = module.upload_environment(
        source,
        tmp_path / "target",
        build_id="build-1",
        environment="dev",
        release_id="release-1",
        copy=fail_first,
    )

    assert record["status"] == "failed"
    assert record["uploads"]["artifact"]["status"] == "failed"
    assert record["uploads"]["current"]["status"] == "skipped"
    assert len(calls) == 1


def test_local_upload_rejects_environment_mismatch(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path)
    manifest = source / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "artifact_type": "datacoolie_environment",
                "build_id": "build-1",
                "environment": "prod",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(module.UploadError, match="environment"):
        module.upload_environment(
            source,
            tmp_path / "target",
            build_id="build-1",
            environment="dev",
            release_id="release-1",
        )


def test_current_failure_is_partial_and_same_source_can_retry(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path)
    count = 0

    def fail_current(source_path: Path, target: Path, deployment: Path) -> None:
        nonlocal count
        count += 1
        if target.parts[-2:] == ("current", "manifest.json"):
            raise module.UploadError("simulated current failure")
        module._copy_file(source_path, target, deployment)

    partial = module.upload_environment(
        source,
        tmp_path / "target",
        build_id="build-1",
        environment="dev",
        release_id="release-1",
        copy=fail_current,
    )
    assert partial["status"] == "partial_failure"
    assert partial["uploads"]["artifact"]["status"] == "success"
    assert partial["uploads"]["current"]["status"] == "failed"

    success = module.upload_environment(
        source,
        tmp_path / "target",
        build_id="build-1",
        environment="dev",
        release_id="release-2",
    )
    assert success["status"] == "success"
    assert count > 0


@pytest.mark.parametrize("value", ["", "s3://bucket/path"])
def test_local_adapter_rejects_non_local_destination(tmp_path: Path, value: str) -> None:
    module = _module()
    with pytest.raises(module.UploadError):
        module.upload_environment(
            _source(tmp_path),
            value,
            build_id="build-1",
            environment="dev",
            release_id="release-1",
        )


def test_cli_persists_preflight_failure_with_skipped_phases(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    record_path = tmp_path / "receipt.json"
    exit_code = module.main(
        [
            "--source",
            str(tmp_path / "missing"),
            "--deployment-path",
            str(tmp_path / "target"),
            "--build-id",
            "build-1",
            "--environment",
            "dev",
            "--release-id",
            "release-1",
            "--record",
            str(record_path),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["preflight"]["status"] == "failed"
    assert output["status"] == "failed"
    assert output["release_id"] is None
    assert output["uploads"] == {
        "artifact": {"status": "skipped", "files": 0},
        "current": {"status": "skipped", "files": 0},
    }
    assert output["record"]["status"] == "written"
    assert json.loads(record_path.read_text(encoding="utf-8")) == output


def test_cli_record_write_failure_preserves_transfer_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    source = _source(tmp_path)

    def fail_write(path: Path, value: dict) -> None:
        raise module.UploadError("record store token=do-not-print unavailable")

    monkeypatch.setattr(module, "_write_record", fail_write)
    exit_code = module.main(
        [
            "--source",
            str(source),
            "--deployment-path",
            str(tmp_path / "target"),
            "--build-id",
            "build-1",
            "--environment",
            "dev",
            "--release-id",
            "release-1",
            "--record",
            str(tmp_path / "receipt.json"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["preflight"] == {"status": "passed"}
    assert output["status"] == "success"
    assert output["uploads"]["artifact"]["status"] == "success"
    assert output["record"]["status"] == "failed"
    assert "<redacted>" in output["record"]["error"]
    assert "do-not-print" not in json.dumps(output)


def test_v2_phase_errors_are_bounded_without_changing_v1_helper_result(tmp_path: Path) -> None:
    module = _module()
    source = _source(tmp_path)
    raw_error = "token=synthetic " + ("x" * 600)

    def fail_first(source_path: Path, target: Path, deployment: Path) -> None:
        raise module.UploadError(raw_error)

    v1_result = module.upload_environment(
        source,
        tmp_path / "target",
        build_id="build-1",
        environment="dev",
        release_id="release-1",
        copy=fail_first,
    )
    assert v1_result["uploads"]["artifact"]["error"] == raw_error
    v2_result = module._v2_record(
        release_id=v1_result["release_id"],
        build_id=v1_result["build_id"],
        environment=v1_result["environment"],
        deployment_path=v1_result["deployment_path"],
        source=v1_result["source"],
        preflight={"status": "passed"},
        status=v1_result["status"],
        uploads=v1_result["uploads"],
    )
    error = v2_result["uploads"]["artifact"]["error"]
    assert len(error) <= 512
    assert "token=synthetic" not in error
    assert "token=<redacted>" in error


@pytest.mark.parametrize("alias", ["source", "current"])
def test_cli_rejects_record_aliases_without_overwriting_upload_bytes(
    tmp_path: Path, alias: str, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    source = _source(tmp_path)
    deployment = tmp_path / "target"
    if alias == "source":
        record_path = source / "manifest.json"
    else:
        record_path = deployment / "current" / "manifest.json"
        record_path.parent.mkdir(parents=True)
        record_path.write_text("target-current-bytes", encoding="utf-8")
    before = record_path.read_bytes()

    exit_code = module.main(
        [
            "--source",
            str(source),
            "--deployment-path",
            str(deployment),
            "--build-id",
            "build-1",
            "--environment",
            "dev",
            "--release-id",
            "release-1",
            "--record",
            str(record_path),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert output["preflight"]["status"] == "failed"
    assert output["preflight"]["error"]
    assert output["uploads"] == {
        "artifact": {"status": "skipped", "files": 0},
        "current": {"status": "skipped", "files": 0},
    }
    assert output["record"]["status"] == "failed"
    assert record_path.read_bytes() == before


def test_cli_preflight_is_not_repeated_before_transfer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _module()
    source = _source(tmp_path)
    original = module._prepare_upload
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "_prepare_upload", counted)
    assert module.main(
        [
            "--source",
            str(source),
            "--deployment-path",
            str(tmp_path / "target"),
            "--build-id",
            "build-1",
            "--environment",
            "dev",
            "--release-id",
            "release-1",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert calls == 1
    assert output["preflight"] == {"status": "passed"}
    assert output["status"] == "success"
