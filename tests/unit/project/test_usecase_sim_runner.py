"""Local contract tests for the usecase-sim runner boundary."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_DIR = PRODUCT_ROOT / "usecase-sim" / "runner"


def _load_dispatcher():
    spec = importlib.util.spec_from_file_location(
        "scenario_dispatcher_test", RUNNER_DIR / "run_scenario.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_runner():
    spec = importlib.util.spec_from_file_location(
        "usecase_runner_test", RUNNER_DIR / "run.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(RUNNER_DIR))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def _load_script(name: str):
    script = PRODUCT_ROOT / "usecase-sim" / "scripts" / name
    relocated = PRODUCT_ROOT / "usecase-sim" / "scripts" / "features" / "replay" / name
    if not script.exists() and relocated.exists():
        script = relocated
    spec = importlib.util.spec_from_file_location(f"{name.replace('.', '_')}_test", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_bound_parser():
    # The simulator is an executable boundary rather than an installed package.
    # Import its helper with only the runner directory temporarily on sys.path.
    sys.path.insert(0, str(RUNNER_DIR))
    try:
        from _runner_utils import parse_replay_bound
    finally:
        sys.path.pop(0)
    return parse_replay_bound


def test_direct_runner_main_anchors_cwd_before_parsing(tmp_path, monkeypatch) -> None:
    module = _load_runner()
    observed: list[Path] = []

    def stop_after_parse():
        observed.append(Path.cwd())
        raise RuntimeError("stop after cwd assertion")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module, "parse_args", stop_after_parse)
    with pytest.raises(RuntimeError, match="stop after cwd assertion"):
        module.main()
    assert observed == [PRODUCT_ROOT]


def test_file_provider_receives_explicit_watermark_root() -> None:
    module = _load_runner()
    watermark_root = PRODUCT_ROOT / "usecase-sim" / ".runtime" / "test-watermarks"
    args = type(
        "Args",
        (),
        {
            "metadata_path": "usecase-sim/metadata/file/baseline/local_use_cases.json",
            "sql_base_path": None,
            "watermark_path": str(watermark_root),
        },
    )()
    provider = module._build_metadata("file", args)
    assert Path(provider.watermark_base_path) == watermark_root
    assert not provider.is_initialized


@pytest.mark.parametrize(
    ("platform", "expected_log", "expected_watermark"),
    [
        ("local", "usecase-sim/.runtime/logs", "usecase-sim/.runtime/watermarks"),
        ("aws", "s3://datacoolie-test/logs", "s3://datacoolie-test/watermarks"),
    ],
)
def test_direct_runner_runtime_defaults_follow_platform(
    platform, expected_log, expected_watermark, monkeypatch
) -> None:
    module = _load_runner()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--engine",
            "polars",
            "--metadata-source",
            "file",
            "--metadata-path",
            "usecase-sim/metadata/file/baseline/local_use_cases.json",
            "--platform",
            platform,
            "--stage",
            "read__csv",
        ],
    )
    args = module.parse_args()
    if platform == "aws":
        assert args.log_path == expected_log
        assert args.watermark_path == expected_watermark
    else:
        assert Path(args.log_path).resolve() == module.LOG_DIR.resolve()
        assert Path(args.watermark_path).resolve() == module.WATERMARKS_DIR.resolve()


def test_direct_runner_state_path_selects_matching_watermark_root() -> None:
    module = _load_runner()
    args = type(
        "Args",
        (),
        {
            "platform": "aws",
            "log_path": None,
            "watermark_path": None,
            "state_base_path": "s3://datacoolie-test/state/topic",
        },
    )()
    module._apply_runtime_defaults(args)
    assert args.log_path == "s3://datacoolie-test/logs"
    assert args.watermark_path == "s3://datacoolie-test/state/topic/watermarks"


@pytest.mark.parametrize("use_docker", [False, True])
def test_scenario_command_uses_shared_log_root(use_docker) -> None:
    module = _load_dispatcher()
    scenario = {
        "engine": "spark",
        "metadata_type": "file",
        "platform": "local",
        "metadata_path": "usecase-sim/metadata/file/baseline/local_use_cases.json",
        "stage": "read__csv",
    }
    command = module.build_command(
        "custom-log",
        scenario,
        use_docker=use_docker,
        spark_container="datacoolie-spark",
    )
    log_index = command.index("--log-path") + 1
    expected = "/datacoolie/usecase-sim/.runtime/logs" if use_docker else str(module.LOG_DIR.resolve())
    assert command[log_index] == expected


def test_all_local_registry_invocations_use_shared_log_root() -> None:
    module = _load_dispatcher()
    registry = module.load_scenarios(module.SCENARIOS_PATH)
    for name, scenario in registry.items():
        if scenario.get("platform", "local") != "local" or scenario.get("skip_pipeline"):
            continue
        for invocation in module._scenario_invocations(scenario):
            effective = module._invocation_scenario(scenario, invocation)
            for docker in (False, True):
                command = module.build_command(name, effective, use_docker=docker)
                expected = "/datacoolie/usecase-sim/.runtime/logs" if docker else str(module.LOG_DIR.resolve())
                assert command[command.index("--log-path") + 1] == expected, name


def test_scenario_rejects_topic_log_override() -> None:
    module = _load_dispatcher()
    with pytest.raises(ValueError, match="runtime/logs"):
        module._scenario_log_root({"log_base_path": str(module.LOG_DIR / "topic")})


def test_job_log_cleanup_preserves_other_jobs_and_receipts(tmp_path, monkeypatch) -> None:
    module = _load_dispatcher()
    monkeypatch.setattr(module, "LOG_DIR", tmp_path)
    for relative, job in (("execution_logs/own.json", "own"), ("system_logs/other.json", "other"),
                          ("scenarios/receipt.json", "own"), ("legacy/own.json", "own")):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"job_id": job}), encoding="utf-8")
    module._pre_clean_job_logs({"job_id": "own"})
    assert not (tmp_path / "execution_logs/own.json").exists()
    assert (tmp_path / "system_logs/other.json").exists()
    assert (tmp_path / "scenarios/receipt.json").exists()
    assert (tmp_path / "legacy/own.json").exists()


def test_shared_log_validation_rejects_stale_session(tmp_path) -> None:
    module = _load_dispatcher()
    from runner.runtime_logs import job_sessions, require_job_logs

    job_root = tmp_path / "execution_logs/job_run_log"
    job_root.mkdir(parents=True)
    (job_root / "old.json").write_text(json.dumps({
        "job_id": "job", "log_session_id": "old", "_type": "job_run_log"
    }))
    system = tmp_path / "system_logs"
    system.mkdir()
    (system / "old.json").write_text(json.dumps({"job_id": "job", "log_session_id": "old"}))
    (job_root / "malformed.json").write_text("malformed")
    assert job_sessions(tmp_path, "job") == {"old"}
    with pytest.raises(AssertionError, match="current job snapshot"):
        require_job_logs(tmp_path, "job", "current")
    console = tmp_path / "console.log"
    console.write_text("")
    assert module._validate_scenario_result({"validation": {"require_log_session": True}}, 0, console)[0] == 1


def test_aws_scenario_uses_object_store_runtime_defaults() -> None:
    module = _load_dispatcher()
    scenario = {
        "engine": "polars",
        "metadata_type": "file",
        "platform": "aws",
        "metadata_path": "usecase-sim/metadata/file/baseline/local_use_cases.json",
        "stage": "read__csv",
    }
    command = module.build_command("aws-defaults", scenario)
    assert command[command.index("--log-path") + 1] == "s3://datacoolie-test/logs"
    assert command[command.index("--watermark-path") + 1] == "s3://datacoolie-test/watermarks"


def test_replay_preparation_resets_watermark_variants(tmp_path, monkeypatch) -> None:
    module = _load_script("features/replay/prepare_replay_surface_sync.py")
    data_root = tmp_path / "data" / "replay_surface_sync"
    watermark_root = tmp_path / "watermarks" / "replay_surface_sync"
    log_root = tmp_path / "logs" / "replay_surface_sync"
    monkeypatch.setattr(module, "BASE", data_root)
    monkeypatch.setattr(module, "WATERMARK_BASE", watermark_root)
    monkeypatch.setattr(module, "RECEIPT_BASE", log_root)

    module.prepare("c1", "polars")
    stale = watermark_root / "c1" / "polars" / "variant" / "stale.json"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("stale", encoding="utf-8")
    module.prepare("c1", "polars")

    assert not stale.exists()
    assert (data_root / "c1" / "polars" / "metadata").is_dir()
    assert (log_root / "c1" / "polars" / "setup_receipt.json").is_file()


def test_cleanup_validates_all_targets_before_mutating(tmp_path, monkeypatch) -> None:
    module = _load_dispatcher()
    data = tmp_path / "data"
    watermarks = tmp_path / "watermarks"
    output = data / "case"
    output.mkdir(parents=True)
    sentinel = output / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(module, "DATA_DIR", data)
    monkeypatch.setattr(module, "WATERMARKS_DIR", watermarks)
    scenario = {"pre_clean_paths": [str(output), str(tmp_path / "outside")]}
    with pytest.raises(ValueError, match="strictly below"):
        module._pre_clean_paths(scenario)
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_log_cleanup_rejects_roots_outside_log_area(tmp_path, monkeypatch) -> None:
    module = _load_dispatcher()
    logs = tmp_path / "logs"
    external = tmp_path / "external"
    monkeypatch.setattr(module, "LOG_DIR", logs)
    scenario = {
        "state_base_path": str(external),
        "derive_log_paths_from_state": True,
        "job_id": "test-job",
    }
    with pytest.raises(ValueError, match="runtime/logs"):
        module._pre_clean_job_logs(scenario)


def test_simulator_runner_converts_integer_cli_bounds() -> None:
    parse_replay_bound = _load_bound_parser()

    assert parse_replay_bound("0") == 0
    assert parse_replay_bound("-10") == -10
    assert parse_replay_bound(" +12 ") == 12


def test_simulator_runner_preserves_iso_temporal_bounds() -> None:
    parse_replay_bound = _load_bound_parser()

    assert parse_replay_bound("2024-01-01") == "2024-01-01"
    assert parse_replay_bound("2024-01-01T00:00:00Z") == "2024-01-01T00:00:00Z"


@pytest.mark.parametrize(
    ("name", "child_exit"),
    [
        ("local_polars_startup_failure", 1),
        ("local_polars_transform_dedup_strict", 2),
        ("local_spark_transform_dedup_strict", 2),
        ("local_polars_transform_invalid_fill", 2),
        ("local_spark_transform_invalid_fill", 2),
        ("local_polars_transform_invalid_redact", 2),
        ("local_spark_transform_invalid_redact", 2),
        ("local_polars_transform_sanitizer_collision", 2),
        ("local_spark_transform_sanitizer_collision", 2),
        ("local_polars_api_recovery_fail", 2),
        ("local_spark_api_recovery_fail", 2),
        ("local_polars_qualified_sql_delta_ambiguity", 2),
        ("local_polars_qualified_sql_iceberg_ambiguity", 2),
    ],
)
def test_negative_registry_separates_child_and_final_exit(name, child_exit) -> None:
    """Retain planned child failures without changing their launch commands."""
    module = _load_dispatcher()
    scenario = json.loads(
        (PRODUCT_ROOT / "usecase-sim/scenarios/scenarios.json").read_text()
    )[name]
    invocations = module._scenario_invocations(scenario)
    assert len(invocations) == 1
    assert module._invocation_expected_exit(scenario, invocations[0], 1) == child_exit
    assert scenario["validation"]["expected_exit_code"] == 0
    effective = module._invocation_scenario(scenario, invocations[0])
    assert module.build_command(name, effective) == module.build_command(name, scenario)


@pytest.mark.parametrize(
    ("child_exit", "console_matches", "job_matches", "outer_exit"),
    [(1, True, True, 0), (0, True, True, 1), (124, True, True, 1),
     (1, False, True, 1), (1, True, False, 1)],
)
def test_startup_failure_through_full_dispatcher(
    tmp_path, monkeypatch, child_exit, console_matches, job_matches, outer_exit
) -> None:
    """A planned child failure must also pass final scenario validation."""
    module = _load_dispatcher()
    name = "local_polars_startup_failure"
    scenario = json.loads(
        (PRODUCT_ROOT / "usecase-sim/scenarios/scenarios.json").read_text()
    )[name]
    state = tmp_path / "state"
    logs = state / "logs"
    monkeypatch.setattr(module, "RUNTIME_DIR", state)
    monkeypatch.setattr(module, "LOG_DIR", logs)
    monkeypatch.setattr(module, "SCENARIO_LOG_DIR", logs / "scenarios")
    scenario["state_base_path"] = str(state)
    scenario["validation"]["args"] += ["--log-root", str(logs)]

    def child_receipt(command, console_log, timeout):
        # The child outcome is the fixture; orchestration and log validation
        # use the actual dispatcher and checked-in validator subprocess.
        console_log.write_text(
            "DataCoolie session startup failed" if console_matches else "unexpected",
            encoding="utf-8",
        )
        job_id = scenario["job_id"]
        records = [
            {"job_id": job_id, "log_session_id": "current", "event_name": "session.starting"},
            {"job_id": job_id, "log_session_id": "current", "event_name": "session.startup_failed"},
        ]
        (logs / "system_logs").mkdir(parents=True, exist_ok=True)
        (logs / "system_logs/records.json").write_text(
            "\n".join(json.dumps(record) for record in records), encoding="utf-8"
        )
        if job_matches:
            job_root = logs / "execution_logs/job_run_log"
            job_root.mkdir(parents=True, exist_ok=True)
            (job_root / "job.json").write_text(json.dumps({
                "job_id": job_id, "log_session_id": "current", "_type": "job_run_log", "status": "failed"
            }), encoding="utf-8")
        return child_exit, "fixture child"

    monkeypatch.setattr(module, "_run_with_tee", child_receipt)
    assert module.run_scenarios([name], {name: scenario}) == outer_exit
    receipts = json.loads(
        (module.SCENARIO_LOG_DIR / f"{name}.invocations.json").read_text()
    )
    assert len(receipts) == 1
    assert receipts[0]["expected_exit_code"] == 1
    assert receipts[0]["actual_exit_code"] == child_exit
