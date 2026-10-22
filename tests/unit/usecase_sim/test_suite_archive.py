"""Focused tests for durable scenario-suite evidence retention."""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
if str(SIM) not in sys.path:
    sys.path.insert(0, str(SIM))

from runner import suite_archive, runtime_workspace  # noqa: E402
from runner.suite_archive import SuiteArchiveError, finalize_suite_archive  # noqa: E402


def _write_json(path: Path, payload: dict) -> bytes:
    raw = (json.dumps(payload, indent=2) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def _rolling_root(tmp_path: Path) -> tuple[Path, Path]:
    log_dir = tmp_path / "usecase-sim" / ".runtime" / "logs" / "scenarios"
    log_dir.mkdir(parents=True)
    timing = _write_json(
        log_dir / "suite.timing.json",
        {"suite_id": "suite-one", "selected": ["leaf"], "results": {"leaf": 0}},
    )
    _write_json(
        log_dir / "leaf.receipt.json",
        {
            "schema_version": 1,
            "attempt_id": "attempt-leaf",
            "scenario": "leaf",
            "status": "passed",
            "result": "PASS",
        },
    )
    return log_dir, timing


def test_successive_suites_keep_independent_bytes(tmp_path):
    log_dir, timing = _rolling_root(tmp_path)
    first = finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], suite_id="suite-one", results={"leaf": 0})
    second_timing = _write_json(log_dir / "suite.timing.json", {"suite_id": "suite-two"})
    _write_json(log_dir / "leaf.receipt.json", {"attempt_id": "second", "scenario": "leaf"})
    second = finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], suite_id="suite-two", results={"leaf": 0})
    assert (first.archive_dir / "evidence/suite.timing.json").read_bytes() == timing
    assert (second.archive_dir / "evidence/suite.timing.json").read_bytes() == second_timing
    assert json.loads(second.latest_pointer.read_bytes())["suite_id"] == "suite-two"


@pytest.mark.parametrize("workspace", [{}, {"complete": True}, {"complete": True, "manifest_path": "missing"}])
def test_declared_native_workspace_needs_complete_references(tmp_path, workspace):
    log_dir, _ = _rolling_root(tmp_path)
    _write_json(log_dir / "leaf.receipt.json", {"attempt_id": "attempt-leaf", "runtime_workspace": workspace})
    with pytest.raises(SuiteArchiveError):
        finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], suite_id="suite-one")
    assert not (log_dir / "suites/latest-retained.json").exists()


def test_bundle_attempt_mismatch_rejects_stale_receipt(tmp_path):
    log_dir, _ = _rolling_root(tmp_path)
    _write_json(log_dir / "bundle.receipt.json", {"steps": [{"receipt": "leaf.receipt.json", "ref": "leaf", "attempt_id": "stale"}]})
    with pytest.raises(SuiteArchiveError, match="attempt mismatch"):
        finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], bundle_names=["bundle"], suite_id="suite-one")


@pytest.mark.parametrize("operation", ["mkdir", "open", "write", "stage"])
def test_archive_io_failures_are_normalized_and_locks_cleaned(tmp_path, monkeypatch, operation):
    log_dir, _ = _rolling_root(tmp_path)
    root = log_dir / "suites"
    root.mkdir()
    def fail(*args, **kwargs):
        raise OSError("synthetic disk failure")
    if operation == "mkdir":
        monkeypatch.setattr(Path, "mkdir", fail)
    elif operation == "stage":
        monkeypatch.setattr(suite_archive.tempfile, "mkdtemp", fail)
    else:
        monkeypatch.setattr(suite_archive.os, operation, fail)
    with pytest.raises(SuiteArchiveError):
        finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], suite_id="suite-one")
    assert not (root / ".archive.lock").exists()
    assert not (root / "latest-retained.json").exists()


def test_collector_parses_captured_bytes_instead_of_rereading(tmp_path, monkeypatch):
    log_dir, _ = _rolling_root(tmp_path)
    collector = suite_archive._Collector(log_dir, tmp_path / "capture")
    original = collector.retain
    def retain_then_replace(source, **kwargs):
        result = original(source, **kwargs)
        source.write_text("not json", encoding="utf-8")
        return result
    monkeypatch.setattr(collector, "retain", retain_then_replace)
    payload = collector.enqueue(log_dir / "leaf.receipt.json", original_path="leaf.receipt.json", kind="leaf")
    assert payload["attempt_id"] == "attempt-leaf"


def test_captured_timing_must_belong_to_suite(tmp_path, monkeypatch):
    log_dir, _ = _rolling_root(tmp_path)
    original = suite_archive._Collector.retain
    def replace_timing_before_capture(self, source, **kwargs):
        if kwargs["kind"] == "suite":
            _write_json(source, {"suite_id": "foreign-suite"})
        return original(self, source, **kwargs)
    monkeypatch.setattr(suite_archive._Collector, "retain", replace_timing_before_capture)
    with pytest.raises(SuiteArchiveError, match="another suite"):
        finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["leaf"], suite_id="suite-one")
    assert not (log_dir / "suites/latest-retained.json").exists()


def test_archive_retains_receipts_after_rolling_files_are_removed(tmp_path):
    log_dir, timing = _rolling_root(tmp_path)

    result = finalize_suite_archive(
        scenario_log_dir=log_dir,
        selected_names=["leaf"],
        suite_id="suite-one",
        results={"leaf": 0},
    )

    retained_timing = result.archive_dir / "evidence/suite.timing.json"
    retained_receipt = result.archive_dir / "evidence/receipts/leaves/leaf.receipt.json"
    assert retained_timing.read_bytes() == timing
    assert retained_receipt.is_file()
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["retained_status"] == "accepted"
    assert manifest["execution_status"] == "passed"
    assert any(
        item["original_path"] == str(log_dir / "leaf.receipt.json")
        and item["retained_path"] == "evidence/receipts/leaves/leaf.receipt.json"
        for item in manifest["path_mappings"]
    )

    (log_dir / "suite.timing.json").unlink()
    (log_dir / "leaf.receipt.json").unlink()
    assert retained_timing.read_bytes() == timing
    assert retained_receipt.read_bytes()


@pytest.mark.parametrize("mutation", [None, "workspace-attempt", "manifest-attempt", "extra-member", "missing-hash"])
def test_archive_closes_bundle_leaf_and_native_workspace_references(tmp_path, mutation):
    log_dir = tmp_path / "logs" / "scenarios"
    log_dir.mkdir(parents=True)
    token = "a" * 32
    native = tmp_path / "native" / token
    native.mkdir(parents=True)
    runtime = tmp_path / "workspaces" / token / "runtime"
    for directory in ("data", "logs", "watermarks"):
        (runtime / directory).mkdir(parents=True)
    member = b"retained native payload\n"
    (runtime / "payload.txt").write_bytes(member)
    runtime_workspace.export_runtime(runtime, native, token=token, exit_code=0,
                                     revision="revision", runtime={}, elapsed=0.1)
    archive_path = native / "runtime.zip"
    manifest_path = native / "manifest.json"
    manifest_payload = json.loads(manifest_path.read_bytes())
    manifest_payload["runtime_root"] = runtime_workspace._native_posix(runtime_workspace.WORKSPACES / token / "runtime")
    manifest_bytes = _write_json(manifest_path, manifest_payload)

    _write_json(
        log_dir / "suite.timing.json",
        {"suite_id": "suite-native", "selected": ["bundle.leaf"], "results": {}},
    )
    _write_json(
        log_dir / "bundle.leaf.receipt.json",
        {
            "attempt_id": token,
            "status": "passed",
            "runtime_workspace": {
                "attempt_id": token,
                "manifest_path": str(manifest_path),
                "archive_path": str(archive_path),
                "complete": True,
            },
        },
    )
    _write_json(
        log_dir / "bundle.receipt.json",
        {
            "kind": "bundle",
            "steps": [
                {
                    "ref": "bundle.leaf",
                    "receipt": str(log_dir / "bundle.leaf.receipt.json"),
                    "attempt_id": token,
                }
            ],
        },
    )

    if mutation is not None:
        if mutation == "workspace-attempt":
            receipt_path = log_dir / "bundle.leaf.receipt.json"
            payload = json.loads(receipt_path.read_bytes())
            payload["runtime_workspace"]["attempt_id"] = "b" * 32
            _write_json(receipt_path, payload)
        elif mutation == "manifest-attempt":
            manifest_payload["attempt_id"] = "b" * 32
            _write_json(manifest_path, manifest_payload)
        elif mutation == "missing-hash":
            manifest_payload["files"][0].pop("sha256")
            _write_json(manifest_path, manifest_payload)
        else:
            with zipfile.ZipFile(archive_path, "a") as archive:
                archive.writestr("runtime/../unexpected", b"unexpected")
        pointer = log_dir / "suites/latest-retained.json"
        _write_json(pointer, {"suite_id": "previous"})
        before = pointer.read_bytes()
        with pytest.raises(SuiteArchiveError):
            finalize_suite_archive(scenario_log_dir=log_dir, selected_names=["bundle.leaf"],
                                   bundle_names=["bundle"], suite_id="suite-native")
        assert pointer.read_bytes() == before
        return

    result = finalize_suite_archive(
        scenario_log_dir=log_dir,
        selected_names=["bundle.leaf"],
        bundle_names=["bundle"],
        suite_id="suite-native",
        results={"bundle.leaf": 0},
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    retained_paths = {item["retained_path"] for item in manifest["path_mappings"]}
    assert "evidence/receipts/bundles/bundle.receipt.json" in retained_paths
    assert "evidence/receipts/leaves/bundle.leaf.receipt.json" in retained_paths
    assert f"evidence/native/{token}/manifest.json" in retained_paths
    assert f"evidence/native/{token}/runtime.zip" in retained_paths
    retained_manifest = result.archive_dir / f"evidence/native/{token}/manifest.json"
    assert retained_manifest.read_bytes() == manifest_bytes

    manifest_path.unlink()
    archive_path.unlink()
    assert retained_manifest.is_file()
    with zipfile.ZipFile(result.archive_dir / f"evidence/native/{token}/runtime.zip") as archive:
        assert archive.read("runtime/payload.txt") == member


def test_missing_or_corrupt_reference_does_not_change_latest_pointer(tmp_path):
    log_dir, _ = _rolling_root(tmp_path)
    first = finalize_suite_archive(
        scenario_log_dir=log_dir,
        selected_names=["leaf"],
        suite_id="suite-one",
        results={"leaf": 0},
    )
    pointer_bytes = first.latest_pointer.read_bytes()

    _write_json(
        log_dir / "suite.timing.json",
        {"suite_id": "suite-two", "selected": ["leaf"], "results": {"leaf": 0}},
    )
    (log_dir / "leaf.receipt.json").write_bytes(b"{broken\n")
    with pytest.raises(SuiteArchiveError, match="corrupt leaf"):
        finalize_suite_archive(
            scenario_log_dir=log_dir,
            selected_names=["leaf"],
            suite_id="suite-two",
            results={"leaf": 0},
        )
    assert first.latest_pointer.read_bytes() == pointer_bytes


def test_aborted_execution_is_retained_with_separate_execution_status(tmp_path):
    log_dir, _ = _rolling_root(tmp_path)
    _write_json(
        log_dir / "suite.timing.json",
        {"suite_id": "suite-aborted", "selected": ["leaf"], "aborted": True},
    )
    _write_json(
        log_dir / "leaf.receipt.json",
        {
            "attempt_id": "attempt-aborted",
            "status": "blocked",
            "result": "BLOCKED (suite aborted)",
        },
    )
    result = finalize_suite_archive(
        scenario_log_dir=log_dir,
        selected_names=["leaf"],
        suite_id="suite-aborted",
        results={"leaf": 1},
        aborted=True,
        error="synthetic abort",
    )
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["execution_status"] == "aborted"
    assert manifest["retained_status"] == "accepted"
    assert manifest["attempts"] == [
        {
            "scenario": "leaf",
            "attempt_id": "attempt-aborted",
            "status": "blocked",
            "result": "BLOCKED (suite aborted)",
            "receipt": str(log_dir / "leaf.receipt.json"),
        }
    ]


def test_collision_and_publication_failure_leave_pointer_unchanged(tmp_path, monkeypatch):
    log_dir, _ = _rolling_root(tmp_path)
    first = finalize_suite_archive(
        scenario_log_dir=log_dir,
        selected_names=["leaf"],
        suite_id="suite-one",
        results={"leaf": 0},
    )
    pointer_bytes = first.latest_pointer.read_bytes()

    with pytest.raises(SuiteArchiveError, match="collision"):
        finalize_suite_archive(
            scenario_log_dir=log_dir,
            selected_names=["leaf"],
            suite_id="suite-one",
            results={"leaf": 0},
        )
    assert first.latest_pointer.read_bytes() == pointer_bytes

    _write_json(
        log_dir / "suite.timing.json",
        {"suite_id": "suite-two", "selected": ["leaf"], "results": {"leaf": 0}},
    )
    original_write = suite_archive._write_json_atomic

    def fail_pointer(path, payload):
        if path.name == "latest-retained.json":
            raise SuiteArchiveError("synthetic pointer publication failure")
        return original_write(path, payload)

    monkeypatch.setattr(suite_archive, "_write_json_atomic", fail_pointer)
    with pytest.raises(SuiteArchiveError, match="pointer publication"):
        finalize_suite_archive(
            scenario_log_dir=log_dir,
            selected_names=["leaf"],
            suite_id="suite-two",
            results={"leaf": 0},
        )
    assert first.latest_pointer.read_bytes() == pointer_bytes
    assert not (first.latest_pointer.parent / "suite-two").exists()


def test_conflicting_archive_writer_is_rejected(tmp_path):
    log_dir, _ = _rolling_root(tmp_path)
    suites = log_dir / "suites"
    suites.mkdir()
    lock = suites / ".archive.lock"
    lock.write_text("another-writer\n", encoding="utf-8")
    with pytest.raises(SuiteArchiveError, match="conflicting"):
        finalize_suite_archive(
            scenario_log_dir=log_dir,
            selected_names=["leaf"],
            suite_id="suite-one",
            results={"leaf": 0},
        )
