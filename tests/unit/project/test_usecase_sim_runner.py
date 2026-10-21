"""Local contract tests for the usecase-sim runner boundary."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_DIR = PRODUCT_ROOT / "usecase-sim" / "runner"


@pytest.fixture(autouse=True)
def _close_test_introduced_log_handlers(caplog):
    del caplog
    root_logger = logging.getLogger()
    existing_handlers = tuple(root_logger.handlers)
    try:
        yield
    finally:
        for handler in list(root_logger.handlers):
            if any(handler is existing for existing in existing_handlers):
                continue
            root_logger.removeHandler(handler)
            handler.close()


def _pin_dispatcher_revision(monkeypatch, module):
    monkeypatch.setattr(module, "worktree_fingerprint", lambda *_: "test-revision")


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


def _load_maintenance():
    spec = importlib.util.spec_from_file_location(
        "maintenance_runner_test", RUNNER_DIR / "maintenance.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(RUNNER_DIR))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


def test_sqlite_metadata_url_uses_the_actual_host_and_container_coordinate():
    dispatcher = _load_dispatcher()
    scenario = {"metadata_type": "database", "metadata_workspace_id": "local-workspace",
        "metadata_db_connection_string": "sqlite:///./usecase-sim/metadata/database/datacoolie_metadata.db"}
    host = dispatcher._metadata_source_args("sqlite", scenario)
    container = dispatcher._metadata_source_args("sqlite", scenario, use_docker=True)
    from runner.runtime_paths import sqlite_metadata_url
    assert host[1].replace("\\", "/") == sqlite_metadata_url()
    assert container[1] == "sqlite:////datacoolie/usecase-sim/metadata/database/datacoolie_metadata.db"


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


def test_maintenance_scenario_transports_native_evidence_and_action_selection():
    module = _load_dispatcher()
    request = {"engine": "polars", "metadata_type": "maintenance", "platform": "local",
        "metadata_path": "fixture.json", "maintenance_deployment": "deployment.json",
        "maintenance_guard_manifest": "manifest.json", "do_compact": False,
        "cleanup_options": {"expire_snapshots": True, "remove_orphan_files": False}, "retention_hours": 168}
    command = module.build_command("isolated", request)
    assert command[command.index("--maintenance-deployment") + 1] == "deployment.json"
    assert command[command.index("--maintenance-guard-manifest") + 1] == "manifest.json"
    assert "--no-compact" in command
    assert json.loads(command[command.index("--cleanup-options") + 1]) == request["cleanup_options"]
    assert command[command.index("--retention-hours") + 1] == "168"


def test_rest_maintenance_reruns_preserve_fixture_and_scope_all_paths():
    module = _load_dispatcher()
    token = "maintenance_t5/case"
    original = {"setup": {"script": "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py",
        "args": ["--descriptor", f"usecase-sim/.runtime/data/{token}/deployment.json"]},
        "metadata_path": f"s3://datacoolie-test/metadata/{token}/metadata.json",
        "invocations": [{"validation": {"args": [f"usecase-sim/.runtime/data/{token}/effect.json"]}}]}
    one = module._fresh_maintenance_scenario(original)
    two = module._fresh_maintenance_scenario(original)
    assert one != two and original["metadata_path"].endswith("/case/metadata.json")
    suffix = Path(one["setup"]["args"][1]).parent.name
    assert one["metadata_path"].endswith(f"/{suffix}/metadata.json")
    assert f"/{suffix}/" in one["invocations"][0]["validation"]["args"][0]
    original["setup"]["args"][1] = "../outside/deployment.json"
    with pytest.raises(ValueError, match="direct maintenance_t5"):
        module._fresh_maintenance_scenario(original)


def test_qualified_rest_receipt_records_executed_container_runtime(tmp_path, monkeypatch):
    module = _load_dispatcher()
    monkeypatch.setattr(module, "SCENARIO_LOG_DIR", tmp_path)
    observed = {"packages": {"pyspark": "3.5.9"}, "platform": "Linux", "container_id": "fixed", "verified_at": "before"}
    monkeypatch.setattr(module, "_maintenance_container_runtime", lambda container: observed)
    request = {"engine": "spark", "_executed_maintenance_runtime": dict(observed),
        "setup": {"script": "usecase-sim/scripts/features/maintenance/prepare_iceberg_rest.py"}}
    module._write_qualification_receipt("native", request, final_exit_code=0,
        final_status="PASS", runtime_verified=False, execution_mode="docker", container="qualified")
    receipt = json.loads((tmp_path / "native.receipt.json").read_text())
    assert receipt["status"] == "passed" and receipt["runtime_verified"] is True
    assert receipt["runtime"]["execution_verified_at"] == "before"
    def reject(container):
        raise ValueError("wrong runtime")
    monkeypatch.setattr(module, "_maintenance_container_runtime", reject)
    module._write_qualification_receipt("native", request, final_exit_code=0,
        final_status="PASS", runtime_verified=False, execution_mode="docker", container="qualified")
    assert json.loads((tmp_path / "native.receipt.json").read_text())["status"] == "blocked"


def test_invocation_validation_failure_stops_later_mutation(tmp_path, monkeypatch):
    module = _load_dispatcher()
    monkeypatch.setattr(module, "SCENARIO_LOG_DIR", tmp_path / "scenarios")
    monkeypatch.setattr(module, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(module, "RUNTIME_DIR", tmp_path)
    called = []
    def child(command, console, timeout):
        called.append(command)
        console.write_text("effect", encoding="utf-8")
        return 0, "PASS"
    monkeypatch.setattr(module, "_run_with_tee", child)
    monkeypatch.setattr(module, "_validate_scenario_result", lambda *args, **kwargs: (1, "FAIL (native capture)"))
    request = {"engine": "polars", "metadata_type": "maintenance", "metadata_path": "fixture.json",
        "invocations": [{"label": "effect", "validation": {"script": "capture.py"}}, {"label": "no_work"}]}
    assert module.run_scenarios(["fresh"], {"fresh": request}) == 1
    assert len(called) == 1
    receipt = json.loads((module.SCENARIO_LOG_DIR / "fresh.invocations.json").read_text())
    assert receipt[0]["validation_status"] == "FAIL (native capture)"


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


@pytest.mark.parametrize(
    ("inherited_mode", "mode_args", "expected_mode"),
    [
        pytest.param("sequential", [], "optimized", id="default-overrides-sequential"),
        pytest.param(
            "optimized",
            ["--execution-mode", "sequential"],
            "sequential",
            id="explicit-sequential-overrides-optimized",
        ),
        pytest.param(
            "sequential",
            ["--execution-mode", "optimized"],
            "optimized",
            id="explicit-optimized-overrides-sequential",
        ),
    ],
)
def test_dispatcher_cli_mode_routes_selection_and_environment(
    monkeypatch, inherited_mode, mode_args, expected_mode
):
    module = _load_dispatcher()
    observed = {}
    monkeypatch.setattr(
        module.os, "chdir", lambda path: observed.setdefault("cwd", path)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_scenario.py", "--scenario", "case", *mode_args],
    )
    monkeypatch.setattr(
        module,
        "load_scenarios",
        lambda path: {"case": {"metadata_type": "file"}},
    )

    def dispatch(names, scenarios):
        observed["names"] = names
        observed["scenario"] = scenarios["case"]
        observed["mode"] = module.os.environ.get(module.MODE_ENV)
        return 0

    monkeypatch.setattr(module, "run_scenarios", dispatch)
    if inherited_mode is None:
        monkeypatch.delenv(module.MODE_ENV, raising=False)
    else:
        monkeypatch.setenv(module.MODE_ENV, inherited_mode)

    with pytest.raises(SystemExit) as exc_info:
        module.main()

    assert exc_info.value.code == 0
    assert observed["cwd"] == module.DATACOOLIE_ROOT
    assert observed["names"] == ["case"]
    assert observed["scenario"] == {"metadata_type": "file"}
    assert observed["mode"] == expected_mode


def test_runtime_profile_without_dispatch_override_remains_sequential(monkeypatch):
    module = _load_dispatcher()
    monkeypatch.delenv(module.MODE_ENV, raising=False)
    from runner.runtime_profile import optimized

    assert optimized() is False


@pytest.mark.parametrize("mode, expected_parallel", [("optimized", True), ("sequential", False)])
def test_dispatcher_mode_reaches_admission_child_and_receipt(
    tmp_path, monkeypatch, mode, expected_parallel
):
    from types import SimpleNamespace

    module = _load_dispatcher()
    scenario_dir = tmp_path / "scenarios"
    monkeypatch.setattr(module, "SCENARIO_LOG_DIR", scenario_dir)
    monkeypatch.setattr(module, "_setup_log_dirs", lambda: scenario_dir.mkdir(parents=True))
    monkeypatch.setattr(module, "scenario_dependencies", lambda *_: {"case": set()})
    _pin_dispatcher_revision(monkeypatch, module)
    monkeypatch.setenv(module.MODE_ENV, mode)

    observed = {}

    def owned_child(*_args, **_kwargs):
        observed["child_mode"] = module.os.environ.get(module.MODE_ENV)
        return SimpleNamespace(returncode=0, stdout="", stderr="", execution={})

    monkeypatch.setattr(module, "run_owned", owned_child)

    def fake_scenario(name, *_args):
        module._run_with_tee(["fake-child"], scenario_dir / f"{name}.console.log", 1)
        (scenario_dir / f"{name}.receipt.json").write_text(
            json.dumps({
                "attempt_id": module._attempt.get(),
                "status": "passed",
                "final_status": "PASS",
                "runtime_verified": True,
                "profile": {},
            }),
            encoding="utf-8",
        )
        return 0, 0.0

    monkeypatch.setattr(module, "_run_scenario", fake_scenario)
    actual_execute = module.execute

    def observe_execute(
        names, declarations, dependencies, run, blocked, *, parallel, cancel=None
    ):
        observed["parallel"] = parallel
        return actual_execute(
            names,
            declarations,
            dependencies,
            run,
            blocked,
            parallel=parallel,
            cancel=cancel,
        )

    monkeypatch.setattr(module, "execute", observe_execute)
    scenarios = {"case": {"engine": "polars", "metadata_type": "file"}}

    assert module.run_scenarios(["case"], scenarios) == 0
    receipt = json.loads((scenario_dir / "case.receipt.json").read_text())
    assert observed == {"child_mode": mode, "parallel": expected_parallel}
    assert receipt["profile"]["optimization_mode"] == mode

@pytest.mark.parametrize("failure", ["engine", "setup", "provider", "driver", None])
def test_runner_stops_spark_once_on_startup_failure_and_success(monkeypatch, failure):
    from types import SimpleNamespace
    import datacoolie.engines

    module = _load_runner()
    monkeypatch.setattr(sys, "argv", ["run.py", "--engine", "spark", "--metadata-source", "file",
                                      "--metadata-path", "missing.json", "--stage", "seed"])
    monkeypatch.setattr(module, "optimized", lambda: False)
    stopped = []
    spark = SimpleNamespace(stop=lambda: stopped.append(True))
    monkeypatch.setattr(module, "setup_platform", lambda *args: object())
    monkeypatch.setattr(module, "build_spark_session", lambda **kwargs: spark)

    def engine(**kwargs):
        if failure == "engine":
            raise ValueError("startup failure")
        return object()

    def setup(*args):
        if failure == "setup":
            raise ValueError("startup failure")

    def provider(*args):
        if failure == "provider":
            raise ValueError("startup failure")
        return object()

    def driver(**kwargs):
        if failure == "driver":
            raise ValueError("startup failure")
        return object()

    monkeypatch.setattr(datacoolie.engines, "SparkEngine", engine)
    monkeypatch.setattr(module, "_run_engine_setup", setup)
    monkeypatch.setattr(module, "_build_metadata", provider)
    monkeypatch.setattr(module, "DataCoolieDriver", driver)
    monkeypatch.setattr(module, "run_and_report", lambda *args, **kwargs: kwargs["cleanup_fn"]())
    if failure:
        with pytest.raises(ValueError, match="startup failure"):
            module.main()
    else:
        module.main()
    assert stopped == [True]


def test_file_provider_receives_explicit_watermark_root() -> None:
    module = _load_runner()
    watermark_root = PRODUCT_ROOT / "usecase-sim" / ".runtime" / "test-watermarks"
    args = type(
        "Args",
        (),
        {
            "metadata_path": "usecase-sim/metadata/file/read.json",
            "sql_base_path": None,
            "watermark_path": str(watermark_root),
        },
    )()
    provider = module._build_metadata("file", args)
    assert Path(provider.watermark_base_path) == watermark_root
    assert not provider.is_initialized


def test_direct_runner_parser_preserves_single_metadata_path_and_orders_repeated_paths(
    monkeypatch,
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
            "group.json",
            "--stage",
            "read__csv",
        ],
    )
    assert module.parse_args().metadata_path == "group.json"

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
            "group-a.json",
            "--metadata-path",
            "group-b.json",
            "--connections-path",
            "connections.json",
            "--schema-hints-path",
            "schema_hints.json",
            "--stage",
            "read__csv",
        ],
    )
    args = module.parse_args()
    assert args.metadata_path == ["group-a.json", "group-b.json"]
    assert args.connections_path == "connections.json"
    assert args.schema_hints_path == "schema_hints.json"


def test_maintenance_parser_preserves_single_metadata_path_and_shared_files(monkeypatch) -> None:
    module = _load_maintenance()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "maintenance.py",
            "--engine",
            "polars",
            "--metadata-path",
            "group-a.json",
            "--metadata-path",
            "group-b.json",
            "--connections-path",
            "connections.json",
            "--schema-hints-path",
            "schema_hints.json",
        ],
    )
    args = module.parse_args()
    assert args.metadata_path == ["group-a.json", "group-b.json"]
    assert args.connections_path == "connections.json"
    assert args.schema_hints_path == "schema_hints.json"


def test_direct_runner_file_provider_receives_ordered_files_and_shared_sections() -> None:
    module = _load_runner()
    args = type(
        "Args",
        (),
        {
            "metadata_path": ["group-a.json", "group-b.json"],
            "connections_path": "connections.json",
            "schema_hints_path": "schema_hints.json",
            "sql_base_path": None,
            "watermark_path": "watermarks",
        },
    )()
    provider = module._build_metadata("file", args)
    assert provider._config_paths == ["group-a.json", "group-b.json"]
    assert provider._connections_path == "connections.json"
    assert provider._schema_hints_path == "schema_hints.json"


def test_direct_runner_keeps_exact_metadata_paths_without_materializing(monkeypatch) -> None:
    module = _load_runner()
    args = type(
        "Args",
        (),
        {
            "metadata_path": ["group-a.json", "group-b.json"],
            "connections_path": None,
            "schema_hints_path": None,
            "metadata_document": None,
            "metadata_bindings": None,
            "dataflow": None,
            "sql_base_path": None,
            "watermark_path": "watermarks",
            "platform": "local",
        },
    )()
    provider = module._build_metadata("file", args)
    assert provider._config_paths == ["group-a.json", "group-b.json"]


def test_dispatcher_transports_metadata_lists_and_shared_files_across_host_and_docker():
    module = _load_dispatcher()
    root = module.DATACOOLIE_ROOT
    scenario = {
        "engine": "spark",
        "metadata_type": "file",
        "platform": "local",
        "metadata_path": [str(root / "metadata" / "group-a.json"), str(root / "metadata" / "group-b.json")],
        "connections_path": str(root / "metadata" / "connections.json"),
        "schema_hints_path": str(root / "metadata" / "schema_hints.json"),
        "stage": "read__csv",
    }
    for use_docker in (False, True):
        command = module.build_command("direct-files", scenario, use_docker=use_docker)
        expected = [
            "/datacoolie/metadata/group-a.json",
            "/datacoolie/metadata/group-b.json",
            "/datacoolie/metadata/connections.json",
            "/datacoolie/metadata/schema_hints.json",
        ] if use_docker else [
            str(root / "metadata" / "group-a.json"),
            str(root / "metadata" / "group-b.json"),
            str(root / "metadata" / "connections.json"),
            str(root / "metadata" / "schema_hints.json"),
        ]
        assert command.count("--metadata-path") == 2
        assert [command[i + 1] for i, value in enumerate(command) if value == "--metadata-path"] == expected[:2]
        assert command[command.index("--connections-path") + 1] == expected[2]
        assert command[command.index("--schema-hints-path") + 1] == expected[3]


def test_runner_rejects_empty_metadata_path_inputs(monkeypatch) -> None:
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
            "",
            "--stage",
            "read__csv",
        ],
    )
    with pytest.raises(SystemExit):
        module.parse_args()


def test_dispatcher_rejects_empty_metadata_path_list() -> None:
    module = _load_dispatcher()
    with pytest.raises(ValueError, match="non-empty"):
        module._metadata_source_args(
            "empty-files", {"metadata_type": "file", "metadata_path": []}
        )


@pytest.mark.parametrize(
    ("platform", "expected_log", "expected_watermark"),
    [
            ("local", "usecase-sim/.runtime/logs", "usecase-sim/.runtime/watermarks/polars"),
            ("aws", "s3://datacoolie-test/logs", "s3://datacoolie-test/watermarks/polars"),
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
            "usecase-sim/metadata/file/read.json",
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
        assert Path(args.watermark_path).resolve() == (module.WATERMARKS_DIR / "polars").resolve()


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
    assert args.watermark_path == "s3://datacoolie-test/watermarks/polars"


@pytest.mark.parametrize("use_docker", [False, True])
def test_scenario_command_uses_shared_log_root(use_docker) -> None:
    module = _load_dispatcher()
    scenario = {
        "engine": "spark",
        "metadata_type": "file",
        "platform": "local",
        "metadata_path": "usecase-sim/metadata/file/read.json",
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
    leaf_registry = module.scenario_leaf_views(registry)
    for name, scenario in leaf_registry.items():
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
        "metadata_path": "usecase-sim/metadata/file/read.json",
        "stage": "read__csv",
    }
    command = module.build_command("aws-defaults", scenario)
    assert command[command.index("--log-path") + 1] == "s3://datacoolie-test/logs"
    assert command[command.index("--watermark-path") + 1] == "s3://datacoolie-test/watermarks/polars"


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
        ("error_messages.local_polars_startup_failure", 1),
        ("error_messages.local_polars_transform_dedup_strict", 2),
        ("error_messages.local_spark_transform_dedup_strict", 2),
        ("error_messages.local_polars_transform_invalid_fill", 2),
        ("error_messages.local_spark_transform_invalid_fill", 2),
        ("error_messages.local_polars_transform_invalid_redact", 2),
        ("error_messages.local_spark_transform_invalid_redact", 2),
        ("error_messages.local_polars_transform_sanitizer_collision", 2),
        ("error_messages.local_spark_transform_sanitizer_collision", 2),
        ("local_polars_api_recovery_fail", 2),
        ("local_spark_api_recovery_fail", 2),
        ("error_messages.local_polars_qualified_sql_delta_ambiguity", 2),
        ("error_messages.local_polars_qualified_sql_iceberg_ambiguity", 2),
    ],
)
def test_negative_registry_separates_child_and_final_exit(name, child_exit) -> None:
    """Retain planned child failures without changing their launch commands."""
    module = _load_dispatcher()
    registry = module.load_scenarios(module.SCENARIOS_PATH)
    scenario = module.scenario_leaf_views(registry)[name]
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
    name = "error_messages.local_polars_startup_failure"
    registry = module.load_scenarios(module.SCENARIOS_PATH)
    scenario = module.scenario_leaf_views(registry)[name]
    state = tmp_path / "state"
    logs = state / "logs"
    monkeypatch.setattr(module, "RUNTIME_DIR", state)
    monkeypatch.setattr(module, "LOG_DIR", logs)
    monkeypatch.setattr(module, "SCENARIO_LOG_DIR", logs / "scenarios")
    _pin_dispatcher_revision(monkeypatch, module)
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
