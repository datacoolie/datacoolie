"""Private coordinates, immutable evidence and fail-closed workspace delivery."""
import copy
import hashlib
import json
import os
import stat
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "usecase-sim"))
from runner import runtime_workspace as ws

TOKEN = "a" * 32
SCRIPT = "usecase-sim/scripts/features/replay/validate_replay_window_safety.py"


@pytest.fixture
def coordinate(tmp_path, monkeypatch):
    monkeypatch.setattr(ws, "WORKSPACES", tmp_path / "workspaces")
    monkeypatch.setattr(ws, "EXPORTS", tmp_path / "exports")
    monkeypatch.setattr(ws, "in_container", lambda: True)
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    monkeypatch.setenv(ws.ROOT_ENV, "")
    monkeypatch.delenv(ws.ROOT_ENV, raising=False)
    return ws.WORKSPACES / TOKEN / "runtime"


def owned(root):
    root.mkdir(parents=True)
    (root.parent / "owner.json").write_text(json.dumps({
        "schema_version": 1, "attempt_id": TOKEN, "runtime_root": str(root)}))


def test_override_requires_existing_matching_owner(coordinate, monkeypatch):
    monkeypatch.setenv(ws.ROOT_ENV, str(coordinate))
    with pytest.raises(FileNotFoundError):
        ws.runtime_root(Path("default"))
    owned(coordinate)
    assert ws.runtime_root(Path("default")) == coordinate
    (coordinate.parent / "owner.json").write_text('{}')
    with pytest.raises(ValueError, match="ownership mismatch"):
        ws.runtime_root(Path("default"))


@pytest.mark.parametrize("requested", ["relative", "/tmp/outside", "/tmp/../escaped"])
def test_override_rejects_unowned_coordinates(coordinate, monkeypatch, requested):
    monkeypatch.setenv(ws.ROOT_ENV, requested)
    with pytest.raises(ValueError):
        ws.runtime_root(Path("default"))


def test_default_and_control_keep_canonical_path(coordinate, monkeypatch):
    default = Path("default")
    assert ws.runtime_root(default) == default
    owned(coordinate)
    monkeypatch.setenv(ws.ROOT_ENV, str(coordinate))
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "sequential")
    with pytest.raises(ValueError):
        ws.runtime_root(default)


def test_source_fingerprint_uses_case_sensitive_portable_relative_order(tmp_path):
    from cases.receipts import worktree_fingerprint
    root = tmp_path / "usecase-sim"
    docker = root / "docker"
    docker.mkdir(parents=True)
    files = {"Dockerfile.spark": b"FROM pinned", "docker-compose.yml": b"services: {}"}
    digest = hashlib.sha256()
    for name, content in files.items():
        (docker / name).write_bytes(content)
        digest.update(("usecase-sim/docker/" + name).encode())
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    assert worktree_fingerprint(root) == digest.hexdigest()


def test_explicit_root_maps_under_same_child_coordinate(coordinate):
    requested = ws.PRODUCT / "usecase-sim/.runtime/data/window_123"
    assert ws.mapped_arguments(["--root", str(requested)], coordinate) == [
        "--root", str(coordinate / "data/window_123")]
    for arguments in (["--root"], ["--root", str(ws.PRODUCT)],
                      ["--root", str(requested), "--root", str(requested)]):
        with pytest.raises(ValueError):
            ws.mapped_arguments(arguments, coordinate)


def test_only_declared_standalone_lane_is_eligible(monkeypatch):
    scenario = {"engine": "spark", "platform": "local", "skip_pipeline": True,
                "runtime_workspace": "linux_fresh", "validation": {
                    "in_container": True, "script": SCRIPT}}
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    assert ws.enabled(scenario, use_docker=True)
    assert not ws.enabled(scenario, use_docker=False)
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "sequential")
    assert not ws.enabled(scenario, use_docker=True)
    for changed in ({"setup": {"script": "anything"}}, {"skip_pipeline": False},
                    {"engine": "polars"}, {"runtime_workspace": "unknown"}):
        with pytest.raises(ValueError):
            ws.enabled(scenario | changed, use_docker=True)


def seed_export(coordinate, destination, exit_code=0):
    for name in ("data", "logs", "watermarks"):
        (coordinate / name).mkdir(parents=True, exist_ok=True)
        (coordinate / name / "evidence.json").write_text('{"original_linux_path":"/tmp/example"}')
    (coordinate / "logs/empty").mkdir()
    destination.mkdir(parents=True)
    return ws.export_runtime(coordinate, destination, token=TOKEN, exit_code=exit_code,
                             revision="source", runtime={"python": "native"}, elapsed=1)


def rewrite_archive(archive_path, mutate):
    replacement = archive_path.with_name("runtime-rewrite.zip")
    with zipfile.ZipFile(archive_path) as source, zipfile.ZipFile(
        replacement, "w", compression=zipfile.ZIP_DEFLATED
    ) as target:
        for info in source.infolist():
            copied = copy.copy(info)
            payload = source.read(info)
            payload = mutate(copied, payload)
            target.writestr(copied, payload)
    replacement.replace(archive_path)


def test_complete_export_keeps_native_bytes_and_detects_changes(coordinate, tmp_path):
    destination = tmp_path / "export"
    manifest = seed_export(coordinate, destination)
    assert ws.verify_export(destination, TOKEN, revision="source") == manifest
    assert (coordinate.parent / "runtime.zip").read_bytes() == (destination / "runtime.zip").read_bytes()
    with zipfile.ZipFile(destination / "runtime.zip") as archive:
        assert archive.read("runtime/logs/evidence.json") == (coordinate / "logs/evidence.json").read_bytes()
        assert "runtime/logs/empty/" in archive.namelist()
    assert manifest["directories"] == ["data", "logs", "logs/empty", "watermarks"]
    assert manifest["files"][0]["mtime_ns"] == (coordinate / "data/evidence.json").stat().st_mtime_ns
    with pytest.raises(ValueError):
        ws.verify_export(destination, TOKEN, revision="different")
    def tamper(info, payload):
        if info.filename == "runtime/data/evidence.json":
            return b"x" * len(payload)
        return payload
    rewrite_archive(destination / "runtime.zip", tamper)
    with pytest.raises(ValueError):
        ws.verify_export(destination, TOKEN)


def test_failed_validator_cannot_publish_passing_evidence(coordinate, tmp_path):
    destination = tmp_path / "export"
    seed_export(coordinate, destination, exit_code=2)
    with pytest.raises(ValueError):
        ws.verify_export(destination, TOKEN)


def test_host_verification_uses_linux_coordinate_syntax(coordinate, tmp_path, monkeypatch):
    destination = tmp_path / "export"
    manifest = seed_export(coordinate, destination)
    monkeypatch.setattr(ws, "WORKSPACES", Path("/tmp/datacoolie-usecase-sim/workspaces"))
    monkeypatch.setattr(ws, "EXPORTS", Path("/tmp/datacoolie-usecase-sim/exports"))
    manifest["runtime_root"] = "/tmp/datacoolie-usecase-sim/workspaces/" + TOKEN + "/runtime"
    manifest["archive_path"] = "/tmp/datacoolie-usecase-sim/exports/" + TOKEN + "/runtime.zip"
    (destination / "manifest.json").write_text(json.dumps(manifest))
    assert ws.verify_export(destination, TOKEN) == manifest


def test_archive_copy_failure_never_publishes_completion(coordinate, tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("export write failed")
    monkeypatch.setattr(ws.shutil, "copyfile", fail)
    destination = tmp_path / "export"
    with pytest.raises(OSError, match="export write failed"):
        seed_export(coordinate, destination)
    assert not (destination / "manifest.json").exists()


@pytest.mark.parametrize("member", ["runtime/../escape", "runtime/C:payload", "runtime/data/C:/foo"])
def test_unsafe_archive_member_set_is_rejected(coordinate, tmp_path, member):
    destination = tmp_path / "export"
    seed_export(coordinate, destination)
    with zipfile.ZipFile(destination / "runtime.zip", "a") as archive:
        archive.writestr(member, b"unsafe")
    with pytest.raises(ValueError, match="unsafe member"):
        ws.verify_export(destination, TOKEN)


def test_corrupt_archive_is_rejected(coordinate, tmp_path):
    destination = tmp_path / "export"
    seed_export(coordinate, destination)
    (destination / "runtime.zip").write_bytes(b"not a zip archive")
    with pytest.raises(ValueError, match="workspace archive"):
        ws.verify_export(destination, TOKEN)


def test_archive_member_modes_must_match_names(coordinate, tmp_path):
    destination = tmp_path / "export"
    seed_export(coordinate, destination)

    def wrong_file_mode(info, payload):
        if info.filename == "runtime/data/evidence.json":
            info.external_attr = (stat.S_IFDIR | 0o755) << 16 | 0x10
        return payload

    rewrite_archive(destination / "runtime.zip", wrong_file_mode)
    with pytest.raises(ValueError, match="invalid file member"):
        ws.verify_export(destination, TOKEN)


def test_archive_directory_mode_must_match_name(coordinate, tmp_path):
    destination = tmp_path / "export"
    seed_export(coordinate, destination)

    def wrong_directory_mode(info, payload):
        if info.filename == "runtime/logs/empty/":
            info.external_attr = (stat.S_IFREG | 0o644) << 16
        return payload

    rewrite_archive(destination / "runtime.zip", wrong_directory_mode)
    with pytest.raises(ValueError, match="invalid directory member"):
        ws.verify_export(destination, TOKEN)


def test_manifest_requires_canonical_runtime_roots(coordinate, tmp_path):
    destination = tmp_path / "export"
    seed_export(coordinate, destination)
    manifest = json.loads((destination / "manifest.json").read_text())
    manifest["directories"].remove("logs")
    (destination / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="workspace export"):
        ws.verify_export(destination, TOKEN)


def test_wrapper_sets_coordinate_before_import_and_preserves_owner_registration(coordinate, monkeypatch):
    monkeypatch.setenv("DATACOOLIE_SIM_OWNER_DIR", "host-visible-owner")
    monkeypatch.setattr(sys, "argv", ["wrapper", "--workspace-id", TOKEN,
        "--script", SCRIPT, "--", "--engine", "spark", "--root",
        str(ws.PRODUCT / "usecase-sim/.runtime/data/new-case")])
    def validator(*args, **kwargs):
        assert ws.runtime_root(Path("default")) == coordinate
        assert os.environ["DATACOOLIE_SIM_OWNER_DIR"] == "host-visible-owner"
        assert sys.argv[-1] == str(coordinate / "data/new-case")
        for name in ("data", "logs", "watermarks"):
            (coordinate / name).mkdir()
        (coordinate / "logs/native.json").write_text("unchanged")
        raise SystemExit(2)
    monkeypatch.setattr(ws.runpy, "run_path", validator)
    assert ws.main() == 2
    with zipfile.ZipFile(ws.EXPORTS / TOKEN / "runtime.zip") as archive:
        assert archive.read("runtime/logs/native.json") == b"unchanged"
    with pytest.raises(ValueError):
        ws.verify_export(ws.EXPORTS / TOKEN, TOKEN)


def request():
    return {"engine": "spark", "metadata_type": "file", "platform": "local",
            "skip_pipeline": True, "runtime_workspace": "linux_fresh",
            "validation": {"in_container": True,
                "script": "usecase-sim/scripts/features/incremental/validate_api_date.py",
                "args": ["--engine", "spark"]}}


def test_dispatcher_preserves_old_roots_in_workspace_lane(tmp_path, monkeypatch):
    from runner import run_scenario as dispatcher
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    def forbidden(*args):
        pytest.fail("fresh workspace must not pre-clean historical paths")
    monkeypatch.setattr(dispatcher, "_pre_clean_paths", forbidden)
    monkeypatch.setattr(dispatcher, "_pre_clean_job_logs", forbidden)
    monkeypatch.setattr(dispatcher, "_validate_scenario_result", lambda *args, **kwargs: (0, "PASS"))
    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", lambda *args, **kwargs: None)
    assert dispatcher._run_scenario("native", {"native": request()},
        {dispatcher.DOCKER_SPARK_CONTAINER: True})[0] == 0


def test_dispatcher_refuses_missing_export_after_zero_validator_exit(tmp_path, monkeypatch):
    import subprocess
    from runner import run_scenario as dispatcher
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    console = tmp_path / "native.console.log"
    console.write_text("")
    commands = []
    def run(command, **kwargs):
        commands.append(command)
        completed = subprocess.CompletedProcess(command, 0, "", "")
        completed.execution = {"command": command}
        return completed
    monkeypatch.setattr(dispatcher, "run_owned", run)
    token = dispatcher._attempt.set(TOKEN)
    try:
        code, status = dispatcher._validate_scenario_result(request(), 0, console, use_docker=True)
        assert code == 1 and "workspace export" in status
        assert any("runtime_workspace.py" in item for item in commands[0])
        dispatcher._write_qualification_receipt("native", request(), final_exit_code=code,
            final_status=status, runtime_verified=False, execution_mode="docker", container="spark")
        receipt = json.loads((tmp_path / "native.receipt.json").read_text())
        assert receipt["validation_command"] == commands[0]
        assert receipt["command"] == commands[0]
        assert receipt["status"] == "blocked" and not receipt["runtime_verified"]
        assert "runtime_workspace" not in receipt
    finally:
        dispatcher._attempt.reset(token)


def test_dispatcher_export_timeout_never_becomes_pass(tmp_path, monkeypatch):
    import subprocess
    from runner import run_scenario as dispatcher
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    console = tmp_path / "native.console.log"
    console.write_text("")
    def timeout(command, **kwargs):
        error = subprocess.TimeoutExpired(command, 1)
        error.execution = {"command": command}
        raise error
    monkeypatch.setattr(dispatcher, "run_owned", timeout)
    token = dispatcher._attempt.set(TOKEN)
    try:
        code, status = dispatcher._validate_scenario_result(request(), 0, console, use_docker=True)
        assert code == 1 and "timed out" in status
    finally:
        dispatcher._attempt.reset(token)


@pytest.mark.parametrize("error", [InterruptedError("cancelled"), RuntimeError("teardown failed")])
def test_aborted_workspace_retains_requested_wrapper_and_docker_identity(tmp_path, monkeypatch, error):
    from runner import run_scenario as dispatcher
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    monkeypatch.setattr(dispatcher, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path / "logs/scenarios")
    monkeypatch.setattr(dispatcher, "_docker_spark_running", lambda container: True)
    monkeypatch.setattr(dispatcher, "_ensure_service_running", lambda service: None)
    launched = []
    def cancel(command, **kwargs):
        launched.append(command)
        raise error
    monkeypatch.setattr(dispatcher, "run_owned", cancel)
    assert dispatcher.run_scenarios(["native"], {"native": request()}) == 1
    receipt = json.loads((dispatcher.SCENARIO_LOG_DIR / "native.receipt.json").read_text())
    assert receipt["execution_mode"] == "docker"
    assert receipt["container"] == dispatcher.DOCKER_SPARK_CONTAINER
    assert receipt["status"] == "blocked" and not receipt["runtime_verified"]
    assert receipt["validation_command"] == launched[0]
    assert receipt["validation_launch"]["status"] == "launch_requested"
    assert receipt["validation_execution"] is None
