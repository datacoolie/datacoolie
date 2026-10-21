"""Log selection must retain the writer's identity/session and failure oracles."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "usecase-sim"))
from runner import runtime_logs
from datacoolie.logging.persistence.layout import build_job_stem


@pytest.fixture(autouse=True)
def optimized(monkeypatch):
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")


def _write(root, job, *, prefix="job", session="current", part=None, name=None,
           record_job=None, payload=None):
    category = {"job": "execution_logs/job_run_log", "dataflow": "execution_logs/dataflow_run_log",
                "system": "system_logs"}[prefix]
    stem = build_job_stem(datetime(2026, 10, 8, tzinfo=timezone.utc), job_id=job,
                          job_num=1, job_index=0)
    suffix = "" if part is None else f"_part_{part:08d}"
    path = root / category / "__run_date=2026-10-08" / (name or f"{prefix}_{stem}{suffix}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"job_id": job if record_job is None else record_job,
              "log_session_id": session, "_type": "job_run_log" if prefix == "job" else "dataflow_run_log"}
    path.write_text(payload if payload is not None else json.dumps(record) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("job", ["ordinary", "with_part_00000001", "a/b%? Việt", ".dot.", " padded ", ""])
@pytest.mark.parametrize("prefix,part", [("job", None), ("dataflow", None), ("dataflow", 1), ("system", 2)])
def test_snapshot_batch_and_encoded_ids_match_full_scan(tmp_path, job, prefix, part):
    _write(tmp_path, job, prefix=prefix, part=part)
    _write(tmp_path, "unrelated", prefix=prefix, part=part)
    category = "system_logs" if prefix == "system" else "execution_logs"
    selected = list(runtime_logs.records(tmp_path, job, category, "current"))
    assert len(selected) == 1
    assert selected == list(runtime_logs.records(tmp_path, job, category, "current", full_scan=True))


def test_snapshot_id_ending_in_batch_suffix_cannot_hide_either_interpretation(tmp_path):
    # These have the same canonical basename; JSON determines which job owns it.
    path = _write(tmp_path, "parent", prefix="dataflow", part=1)
    assert not list(runtime_logs.records(tmp_path, "parent_part_00000001", "execution_logs"))
    path.write_text(json.dumps({"job_id": "parent_part_00000001", "log_session_id": "s"}))
    assert len(list(runtime_logs.records(tmp_path, "parent_part_00000001", "execution_logs"))) == 1
    assert not list(runtime_logs.records(tmp_path, "parent", "execution_logs"))


@pytest.mark.parametrize("prefix", ["dataflow", "system"])
@pytest.mark.parametrize("job", ["parent", "parent_part_100000000"])
def test_batch_sequence_minimum_width_and_snapshot_ambiguity(tmp_path, prefix, job):
    category = "system_logs" if prefix == "system" else "execution_logs"
    # Both writers use :08d, which permits sequences wider than eight digits.
    _write(tmp_path, job, prefix=prefix, part=100000000)
    _write(tmp_path, job, prefix=prefix)
    selected = list(runtime_logs.records(tmp_path, job, category))
    assert len(selected) == 2
    assert selected == list(runtime_logs.records(tmp_path, job, category, full_scan=True))


@pytest.mark.parametrize("name", ["legacy.json", "custom_20261008_000000_1_0_other.json",
    "job_20261399_999999_1_0_other.json", "job_20261008_000000_x_y_other.json",
    "job_20261008_000000_-1_0_other.json", "job_20261008_000000_0_0_other.json",
    "job_20261008_000000_1_1_other.json", "job_20261008_000000_01_0_other.json",
    "job_20261008_000000_1_00_other.json", "job_20261008_000000_1_-1_other.json",
    "job_20261008_000000_1_0_other%zz.json"])
def test_unknown_or_malformed_names_keep_content_fallback(tmp_path, name):
    _write(tmp_path, "wanted", name=name)
    assert len(list(runtime_logs.records(tmp_path, "wanted", "execution_logs"))) == 1


def test_json_identity_and_sessions_remain_authoritative(tmp_path):
    _write(tmp_path, "wanted", record_job="different")
    assert not list(runtime_logs.records(tmp_path, "wanted", "execution_logs"))
    _write(tmp_path, "wanted", session="old")
    assert runtime_logs.job_sessions(tmp_path, "wanted") == {"old"}
    with pytest.raises(AssertionError, match="current job snapshot"):
        runtime_logs.require_job_logs(tmp_path, "wanted", "current")
    # Same job may own several sessions; no suffix or in-memory cache chooses one.
    _write(tmp_path, "wanted", name="legacy-current.json", session="current")
    _write(tmp_path, "wanted", prefix="system", session="current")
    assert runtime_logs.job_sessions(tmp_path, "wanted") == {"old", "current"}
    runtime_logs.require_job_logs(tmp_path, "wanted", "current")
    _write(tmp_path, "wanted", name="duplicate.json", session="current")
    with pytest.raises(AssertionError, match="got 2"):
        runtime_logs.require_job_logs(tmp_path, "wanted", "current")


def test_later_snapshot_rewrite_and_new_batches_are_visible(tmp_path):
    path = _write(tmp_path, "wanted", prefix="dataflow", session="old")
    assert len(list(runtime_logs.records(tmp_path, "wanted", "execution_logs", "old"))) == 1
    path.write_text(json.dumps({"job_id": "wanted", "log_session_id": "new"}))
    _write(tmp_path, "wanted", prefix="dataflow", session="new", part=1)
    assert not list(runtime_logs.records(tmp_path, "wanted", "execution_logs", "old"))
    assert len(list(runtime_logs.records(tmp_path, "wanted", "execution_logs", "new"))) == 2


def test_no_cross_category_pruning_and_malformed_json_remains_ignored(tmp_path):
    _write(tmp_path, "wanted", prefix="system", name="job_20261008_000000_1_0_other.json")
    _write(tmp_path, "wanted", prefix="system", part=1, payload="malformed\n[]\n{}\n")
    assert len(list(runtime_logs.records(tmp_path, "wanted", "system_logs"))) == 1


def test_file_reads_are_bounded_to_current_job_and_unknown_layout(tmp_path, monkeypatch):
    wanted = _write(tmp_path, "wanted")
    legacy = _write(tmp_path, "wanted", name="legacy.json")
    for index in range(120):
        _write(tmp_path, f"other-{index}")
    original = Path.read_text
    opened = []

    def observe(path, *args, **kwargs):
        opened.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", observe)
    selected = list(runtime_logs.records(tmp_path, "wanted", "execution_logs"))
    assert set(opened) == {wanted, legacy}
    opened.clear()
    assert selected == list(runtime_logs.records(tmp_path, "wanted", "execution_logs", full_scan=True))
    assert len(opened) == 122
    opened.clear()
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "sequential")
    assert selected == list(runtime_logs.records(tmp_path, "wanted", "execution_logs"))
    assert len(opened) == 122


def test_reviewed_local_validators_have_only_fresh_admission():
    from runner.admission import declaration

    product = Path(__file__).resolve().parents[3]
    registry = json.loads((product / "usecase-sim/scenarios/scenarios.json").read_text())
    reviewed = [s for s in registry.values() if (s.get("validation") or {}).get("script") in {
        "usecase-sim/scripts/features/read/validate_existing_local.py",
        "usecase-sim/scripts/features/incremental/validate_existing_lookback.py"}]
    assert len(reviewed) == 15
    assert all(declaration(s, product) == ({"polars": 1}, []) for s in reviewed)


@pytest.mark.parametrize("current", [False, True])
def test_standalone_timing_requires_current_attempt_process_evidence(tmp_path, monkeypatch, current):
    from runner import run_scenario as dispatcher

    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", tmp_path)
    monkeypatch.setattr(dispatcher, "worktree_fingerprint", lambda *_: "frozen-source")
    monkeypatch.setattr(dispatcher, "runtime_identity", lambda: {})
    token = dispatcher._attempt.set("current")
    try:
        (tmp_path / "case.console.log").write_text('SIM_RUNTIME_TIMING={"log_lookup_s":1.5}\n')
        (tmp_path / "case.console.validation.process.json").write_text(json.dumps({
            "attempt_id": "current" if current else "earlier", "returncode": 0}))
        dispatcher._write_qualification_receipt("case", {"engine": "polars", "skip_pipeline": True},
            final_exit_code=0, final_status="PASS")
        receipt = json.loads((tmp_path / "case.receipt.json").read_text())
        assert ("validation_runtime_timing" in receipt) is current
        if current:
            assert receipt["validation_runtime_timing"] == {"log_lookup_s": 1.5}
    finally:
        dispatcher._attempt.reset(token)
