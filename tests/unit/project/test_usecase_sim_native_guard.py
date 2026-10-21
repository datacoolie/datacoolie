"""Native guard prerequisites are verified before simulator seed or execution."""
from __future__ import annotations

import importlib.util
import errno
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import zipfile

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM))
from runner import native_guard as guard  # noqa: E402


def _artifact(directory):
    native = guard.native
    directory.mkdir(parents=True, exist_ok=True)
    jar = directory / native.JAR_NAME
    values = {"helper_abi": native.HELPER_ABI, "source_fingerprint": native.source_fingerprint(),
              "runtime_jar_sha256": native.RUNTIME_JAR_SHA256, "spark_version": native.SPARK_VERSION,
              "iceberg_version": native.ICEBERG_VERSION, "java_release": native.JAVA_RELEASE}
    with zipfile.ZipFile(jar, "w") as archive:
        for name in native.HELPER_CLASSES:
            archive.writestr(name.replace(".", "/") + ".class", b"test-class")
        archive.writestr(native.BUILD_RESOURCE, "".join(f"{k}={v}\n" for k, v in values.items()))
    manifest = native.GuardBuildManifest(
        manifest_version=1, helper_abi=native.HELPER_ABI, helper_class=native.HELPER_CLASS,
        helper_classes=native.HELPER_CLASSES, source_fingerprint=native.source_fingerprint(),
        spark_version=native.SPARK_VERSION, iceberg_version=native.ICEBERG_VERSION,
        java_release=native.JAVA_RELEASE, runtime_jar_name=native.RUNTIME_JAR_NAME,
        runtime_jar_sha256=native.RUNTIME_JAR_SHA256, helper_jar_name=native.JAR_NAME,
        helper_jar_sha256=native.sha256_file(jar))
    (directory / native.MANIFEST_NAME).write_text(json.dumps(manifest.as_dict()), encoding="utf-8")
    return manifest


@pytest.fixture
def cache(tmp_path, monkeypatch):
    monkeypatch.setattr(guard, "DEPENDENCIES_DIR", tmp_path / "dependencies")
    path = guard.DEPENDENCIES_DIR / "native-maintenance" / (
        f"{guard.native.source_fingerprint()}-{len(guard.native.RUNTIME_CLASSES)}classes") / "manifest.json"
    monkeypatch.setattr(guard, "runtime_inputs", lambda: (tmp_path / "runtime.jar", tmp_path / "spark-jars"))
    return path


def test_missing_build_is_published_and_reused_without_compilation(cache, monkeypatch):
    calls = []
    def build(directory, **kwargs):
        calls.append(directory)
        return _artifact(directory)
    monkeypatch.setattr(guard.native, "build_guard", build)
    assert guard.prepare(cache)["action"] == "built"
    before = {p.name: p.read_bytes() for p in cache.parent.iterdir()}
    assert guard.prepare(cache)["action"] == "reused"
    assert len(calls) == 1 and calls[0] != cache.parent
    assert {p.name: p.read_bytes() for p in cache.parent.iterdir()} == before


def test_compiler_failure_cannot_publish_or_seed(cache, monkeypatch):
    def fail(directory, **kwargs):
        (directory / "guard.jar").write_bytes(b"partial")
        raise guard.native.NativeBuildError("compiler failed")
    monkeypatch.setattr(guard.native, "build_guard", fail)
    with pytest.raises(guard.native.NativeBuildError, match="compiler failed"):
        guard.prepare(cache)
    assert not cache.parent.exists()
    assert list(cache.parent.parent.iterdir()) == []


@pytest.mark.parametrize("mutation", ["incomplete", "source", "coverage", "jar", "embedded"])
def test_invalid_existing_artifact_is_preserved_and_rejected(cache, monkeypatch, mutation):
    _artifact(cache.parent)
    raw = json.loads(cache.read_text())
    if mutation == "incomplete":
        del raw["runtime_classes"]
    elif mutation == "source":
        raw["source_fingerprint"] = "wrong"
    elif mutation == "coverage":
        raw["runtime_classes"] = raw["runtime_classes"][:-1]
    elif mutation == "jar":
        cache.with_name("guard.jar").write_bytes(b"corrupt")
    else:
        jar = cache.with_name("guard.jar")
        with zipfile.ZipFile(jar, "w") as archive:
            for name in guard.native.HELPER_CLASSES:
                archive.writestr(name.replace(".", "/") + ".class", b"test")
            archive.writestr(guard.native.BUILD_RESOURCE, "source_fingerprint=wrong\n")
        raw["helper_jar"]["sha256"] = guard.native.sha256_file(jar)
    cache.write_text(json.dumps(raw))
    before = {p.name: p.read_bytes() for p in cache.parent.iterdir()}
    monkeypatch.setattr(guard.native, "build_guard", lambda *_a, **_k: pytest.fail("invalid cache rebuilt"))
    with pytest.raises(guard.native.NativeProvenanceError):
        guard.prepare(cache)
    assert {p.name: p.read_bytes() for p in cache.parent.iterdir()} == before


def test_empty_existing_directory_is_not_repaired(cache):
    cache.parent.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        guard.prepare(cache)
    assert not cache.exists()


def test_competing_publication_preserves_winner(cache, monkeypatch):
    monkeypatch.setattr(guard.native, "build_guard", lambda directory, **kwargs: _artifact(directory))
    publish = guard._publish
    winner = {}
    def race(source, destination):
        _artifact(destination)
        winner.update({p.name: p.read_bytes() for p in destination.iterdir()})
        publish(source, destination)
    monkeypatch.setattr(guard, "_publish", race)
    assert guard.prepare(cache)["action"] == "reused"
    assert {p.name: p.read_bytes() for p in cache.parent.iterdir()} == winner


def test_publication_rechecks_canonical_path(cache, monkeypatch):
    monkeypatch.setattr(guard.native, "build_guard", lambda directory, **kwargs: _artifact(directory))
    publish = guard._publish
    check = guard._manifest_path
    published = False
    def checked(value):
        if published:
            raise ValueError("Native guard path must not contain symlinks")
        return check(value)
    def publish_then_change(source, destination):
        nonlocal published
        publish(source, destination)
        published = True
    monkeypatch.setattr(guard, "_manifest_path", checked)
    monkeypatch.setattr(guard, "_publish", publish_then_change)
    with pytest.raises(ValueError, match="symlinks"):
        guard.prepare(cache)


def test_publish_refuses_existing_empty_directory(tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    (source / "keep").write_bytes(b"keep")
    with pytest.raises(OSError):
        guard._publish(source, destination)
    assert (source / "keep").is_file() and list(destination.iterdir()) == []


@pytest.mark.parametrize("code", [errno.EINVAL, errno.ENOSYS, errno.ENOTSUP])
def test_unsupported_mount_uses_exclusive_manifest_last_publication(tmp_path, monkeypatch, code):
    source, destination = tmp_path / "source", tmp_path / "destination"
    _artifact(source)
    class UnsupportedRename:
        def __call__(self, *args):
            guard.ctypes.set_errno(code)
            return -1
    monkeypatch.setattr(guard, "os", SimpleNamespace(name="posix", fsencode=guard.os.fsencode,
                                                    fsync=guard.os.fsync))
    monkeypatch.setattr(guard, "sys", SimpleNamespace(platform="linux"))
    monkeypatch.setattr(guard.ctypes, "CDLL", lambda *a, **k: SimpleNamespace(renameat2=UnsupportedRename()))
    assert guard._publish(source, destination) == "exclusive-reservation"
    guard.verify_artifact(destination / "manifest.json")
    before = {p.name: p.read_bytes() for p in destination.iterdir()}
    with pytest.raises(FileExistsError):
        guard._publish(source, destination)
    assert {p.name: p.read_bytes() for p in destination.iterdir()} == before


@pytest.mark.parametrize("failed_name", ["guard.jar", "manifest.json"])
def test_reserved_publication_failure_keeps_incomplete_coordinate(cache, monkeypatch, tmp_path, failed_name):
    source = tmp_path / "source"
    _artifact(source)
    cache.parent.parent.mkdir(parents=True)
    copy = guard.shutil.copyfileobj
    def fail_copy(reader, writer):
        if Path(writer.name).name != failed_name:
            return copy(reader, writer)
        writer.write(b"partial")
        raise OSError(errno.EIO, "copy failed")
    monkeypatch.setattr(guard.shutil, "copyfileobj", fail_copy)
    with pytest.raises(guard.native.NativeBuildError, match="copy failed"):
        guard._publish_reserved(source, cache.parent)
    assert cache.with_name(failed_name).read_bytes() == b"partial"
    with pytest.raises((FileNotFoundError, json.JSONDecodeError)):
        guard.prepare(cache)


def test_owned_publication_error_is_not_accepted_as_race_winner(cache, monkeypatch):
    monkeypatch.setattr(guard.native, "build_guard", lambda directory, **kwargs: _artifact(directory))
    def fail_after_copy(source, destination):
        _artifact(destination)
        raise OSError(errno.EIO, "fsync failed")
    monkeypatch.setattr(guard, "_publish", fail_after_copy)
    with pytest.raises(OSError, match="fsync failed"):
        guard.prepare(cache)


@pytest.mark.parametrize("issue", ["missing", "digest", "version"])
def test_runtime_inputs_fail_without_fetching_dependencies(tmp_path, monkeypatch, issue):
    jars = tmp_path / "pyspark/jars"
    jars.mkdir(parents=True)
    fake = SimpleNamespace(__version__="wrong" if issue == "version" else guard.native.SPARK_VERSION,
                           __file__=str(jars.parent / "__init__.py"))
    monkeypatch.setitem(sys.modules, "pyspark", fake)
    monkeypatch.setattr(guard, "spark_dependencies_dir", lambda *_: tmp_path / "ivy")
    monkeypatch.setattr(guard.Path, "home", lambda: tmp_path)
    if issue == "digest":
        (jars / guard.native.RUNTIME_JAR_NAME).write_bytes(b"wrong")
    # Host fixtures never inspect container cache coordinates.
    with pytest.raises((guard.native.NativeBuildError, guard.native.NativeProvenanceError)):
        guard.runtime_inputs()


def _dispatcher():
    spec = importlib.util.spec_from_file_location("guard_dispatcher_test", SIM / "runner/run_scenario.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("docker", [False, True])
def test_preflight_uses_executing_context_and_current_sidecar(tmp_path, monkeypatch, docker):
    dispatcher = _dispatcher()
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    token = dispatcher._attempt.set("current")
    observed = []
    def run(command, **kwargs):
        observed.append(command)
        return SimpleNamespace(returncode=0, stdout="verified", stderr="", execution={"command": command})
    monkeypatch.setattr(dispatcher, "run_owned", run)
    scenario = {"engine": "spark", "maintenance_guard_manifest": "manifest",
                "setup": {"script": "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"}}
    try:
        assert dispatcher._prepare_native_guard("case", scenario, use_docker=docker, container="actual") == (0, "PASS")
        command = observed[0]
        assert (command[:4] == ["docker", "exec", "actual", "python3"]) if docker else command[0] == sys.executable
        assert command[-2:] == ["--manifest", "manifest"]
        receipt = json.loads((tmp_path / "case.prerequisite.process.json").read_text())
        assert receipt["attempt_id"] == "current" and receipt["actual_exit_code"] == 0
    finally:
        dispatcher._attempt.reset(token)


def test_failed_preflight_prevents_setup_cleanup_and_invocation(tmp_path, monkeypatch):
    dispatcher = _dispatcher()
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    scenario = {"engine": "spark"}
    monkeypatch.setattr(dispatcher, "_fresh_maintenance_scenario", lambda value: value)
    monkeypatch.setattr(dispatcher, "_prepare_native_guard", lambda *_a, **_k: (1, "FAIL (prerequisite)"))
    for name in ("_run_scenario_setup", "_pre_clean_paths", "_maintenance_container_runtime", "_run_with_tee"):
        monkeypatch.setattr(dispatcher, name, lambda *_a, **_k: pytest.fail("work admitted after prerequisite failure"))
    receipts = []
    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", lambda *_a, **kwargs: receipts.append(kwargs))
    assert dispatcher._run_scenario("case", {"case": scenario}, {"datacoolie-spark": True})[0] == 1
    assert receipts[0]["final_status"] == "FAIL (prerequisite)"


def test_authored_recipes_follow_source_changes_without_mutating_pins(monkeypatch):
    scenarios = json.loads((SIM / "scenarios/scenarios.json").read_text())
    selected = {name: value for name, value in scenarios.items()
                if value.get("maintenance_guard_manifest") == guard.MANIFEST_TOKEN}
    assert set(selected) == {
        "local_spark_iceberg_maintenance", "local_spark_iceberg_expiration_effect",
        "aws_spark_iceberg_maintenance", "aws_spark_iceberg_expiration_effect",
    }
    before = json.dumps(selected)
    coordinates = []
    for fingerprint in ("a" * 64, "b" * 64):
        monkeypatch.setattr(guard.native, "source_fingerprint", lambda: fingerprint)
        for authored in selected.values():
            resolved = guard.resolve_scenario(authored)
            coordinate = resolved["maintenance_guard_manifest"]
            assert f"/{fingerprint}-{len(guard.native.RUNTIME_CLASSES)}classes/" in coordinate
            assert guard.MANIFEST_TOKEN not in json.dumps(resolved)
            for validation in [resolved["validation"],
                               *[step["validation"] for step in resolved["invocations"]]]:
                args = validation["args"]
                assert args[args.index("--maintenance-guard-manifest") + 1] == coordinate
            assert guard._manifest_path(coordinate) == guard.current_manifest_path().resolve()
        coordinates.append(coordinate)
    assert coordinates[0] != coordinates[1]
    assert json.dumps(selected) == before
    pinned = {"maintenance_guard_manifest": coordinates[0]}
    assert guard.resolve_scenario(pinned) is pinned
    with pytest.raises(ValueError, match="canonical source coordinate"):
        guard._manifest_path(pinned["maintenance_guard_manifest"])


@pytest.mark.parametrize("docker", [False, True])
def test_resolved_coordinate_mismatch_blocks_seed_and_replaces_stale_console(tmp_path, monkeypatch, docker):
    dispatcher = _dispatcher()
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    scenarios = json.loads((SIM / "scenarios/scenarios.json").read_text())
    scenario = scenarios["local_spark_iceberg_maintenance"]
    monkeypatch.setattr(guard.native, "source_fingerprint", lambda: "a" * 64)
    commands = []

    def execute_in_different_context(command, **kwargs):
        commands.append(command)
        coordinate = command[command.index("--manifest") + 1]
        assert "/" + "a" * 64 + "-" in coordinate
        with monkeypatch.context() as context:
            context.setattr(guard.native, "source_fingerprint", lambda: "b" * 64)
            with pytest.raises(ValueError, match="canonical source coordinate") as error:
                guard._manifest_path(coordinate)
        return SimpleNamespace(returncode=1, stdout="", stderr=str(error.value),
                               execution={"command": command})

    monkeypatch.setattr(dispatcher, "run_owned", execute_in_different_context)
    for method in ("_run_scenario_setup", "_pre_clean_paths", "_maintenance_container_runtime", "_run_with_tee"):
        monkeypatch.setattr(dispatcher, method, lambda *_a, **_k: pytest.fail("work after coordinate mismatch"))
    receipts = []
    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", lambda *_a, **kwargs: receipts.append(kwargs))
    console = tmp_path / "case.console.log"
    console.write_text("old successful execution")
    token = dispatcher._attempt.set("new-attempt")
    try:
        assert dispatcher._run_scenario("case", {"case": scenario}, {"datacoolie-spark": docker})[0] == 1
    finally:
        dispatcher._attempt.reset(token)
    assert len(commands) == 1
    assert receipts[0]["final_status"] == "FAIL (native guard prerequisite)"
    current_console = console.read_text()
    assert "new-attempt" in current_console and "canonical source coordinate" in current_console
    assert "old successful execution" not in current_console


def test_preflight_timeout_records_current_outcome(tmp_path, monkeypatch):
    dispatcher = _dispatcher()
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    token = dispatcher._attempt.set("current")
    def timeout(command, **kwargs):
        error = subprocess.TimeoutExpired(command, kwargs["timeout"], output="partial")
        error.execution = {"command": command, "timed_out": True}
        raise error
    monkeypatch.setattr(dispatcher, "run_owned", timeout)
    scenario = {"engine": "spark", "maintenance_guard_manifest": "manifest",
                "setup": {"script": "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"}}
    try:
        assert dispatcher._prepare_native_guard("case", scenario, use_docker=False, container="") == (
            124, "FAIL (native guard prerequisite timed out)")
        receipt = json.loads((tmp_path / "case.prerequisite.process.json").read_text())
        assert receipt["attempt_id"] == "current" and receipt["actual_exit_code"] == 124
        assert receipt["timed_out"] is True
        assert (tmp_path / "case.prerequisite.log").read_text() == "partial"
    finally:
        dispatcher._attempt.reset(token)
