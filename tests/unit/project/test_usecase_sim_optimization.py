"""Execution measurements and conservative dependency selection."""

import json
import logging
import sys
import subprocess
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "usecase-sim"))
from runner.runtime_profile import metadata_capabilities
from runner.runtime_timing import MARKER, phase, read_runtime_timing, runtime_entrypoint, scope
from runner.admission import resource, overlaps, compatible, declaration, execute
from runner import owned_process


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


def _pin_dispatcher_revision(monkeypatch, dispatcher):
    monkeypatch.setattr(dispatcher, "worktree_fingerprint", lambda *_: "test-revision")


@pytest.mark.parametrize("still_alive", [False, True])
def test_process_exiting_during_identity_check_is_not_a_foreign_live_member(monkeypatch, still_alive):
    class ProcPath:
        def __truediv__(self, part):
            return self

        def read_bytes(self):
            return b""  # A zombie loses its environment after the initial scan.

    scans = iter([[17], [17] if still_alive else []])
    monkeypatch.setattr(owned_process, "Path", lambda value: ProcPath())
    monkeypatch.setattr(owned_process, "_members", lambda group: next(scans))
    signalled = []
    monkeypatch.setattr(owned_process.os, "killpg", lambda *args: signalled.append(args), raising=False)
    if still_alive:
        with pytest.raises(owned_process.TeardownError, match="identity changed"):
            owned_process._signal_owned(17, "token", 15)
        assert not signalled
    else:
        owned_process._signal_owned(17, "token", 15)
        assert not signalled


@pytest.mark.parametrize("vanished", [FileNotFoundError, ProcessLookupError])
def test_vanished_proc_entry_does_not_signal_an_unverified_group(monkeypatch, vanished):
    class ProcPath:
        def __truediv__(self, part):
            return self

        def read_bytes(self):
            raise vanished("process exited during environment read")

    monkeypatch.setattr(owned_process, "Path", lambda value: ProcPath())
    monkeypatch.setattr(owned_process, "_members", lambda group: [17])
    signalled = []
    monkeypatch.setattr(owned_process.os, "killpg", lambda *args: signalled.append(args), raising=False)
    owned_process._signal_owned(17, "token", 15)
    assert not signalled


def test_verified_group_exiting_before_signal_is_already_torn_down(monkeypatch):
    class ProcPath:
        def __truediv__(self, part):
            return self

        def read_bytes(self):
            return b"DATACOOLIE_SIM_PROCESS_TOKEN=token\0"

    monkeypatch.setattr(owned_process, "Path", lambda value: ProcPath())
    monkeypatch.setattr(owned_process, "_members", lambda group: [17])
    signalled = []

    def vanished(group, sig):
        signalled.append((group, sig))
        raise ProcessLookupError("owned group exited before signal")

    monkeypatch.setattr(owned_process.os, "killpg", vanished, raising=False)
    owned_process._signal_owned(17, "token", 15)
    assert signalled == [(17, 15)]


def test_failed_engine_creation_reports_shutdown_without_fake_execution(tmp_path, capsys):
    @runtime_entrypoint
    def run():
        try:
            with phase("engine_creation_s"):
                raise ValueError("startup")
        finally:
            with phase("shutdown_s"):
                pass
    with pytest.raises(ValueError, match="startup"):
        run()
    log = tmp_path / "console"
    log.write_text(capsys.readouterr().out + MARKER + "not-json\n")
    value = read_runtime_timing(log)
    assert value["execution_s"] is None
    assert value["engine_creation_s"] >= 0
    assert value["shutdown_s"] >= 0
    assert value["total_s"] >= value["engine_creation_s"] + value["shutdown_s"]


def test_nested_timing_scopes_do_not_charge_child_phases_twice():
    with scope() as outer:
        with phase("validation_s"):
            with scope() as child:
                with phase("execution_s"):
                    pass
    assert outer.snapshot()["execution_s"] is None
    assert child.snapshot()["validation_s"] is None


def _metadata(tmp_path):
    document = {"connections": [
        {"name": "src", "connection_type": "file", "format": "parquet"},
        {"name": "delta", "connection_id": "delta-id", "connection_type": "lakehouse", "format": "delta"},
        {"name": "iceberg", "connection_type": "lakehouse", "format": "iceberg"},
        {"name": "s3", "connection_type": "lakehouse", "format": "delta",
         "configure": {"base_path": "s3a://fixture/data"}},
    ], "dataflows": [
        {"stage": "delta", "source": {"connection_name": "src"}, "destination": {"connection_name": "delta"}},
        {"stage": "iceberg", "source": {"connection_name": "src"}, "destination": {"connection_name": "iceberg"}},
        {"stage": "s3", "source": {"connection_name": "src"}, "destination": {"connection_name": "s3"}},
    ]}
    path = tmp_path / "metadata.json"
    path.write_text(json.dumps(document))
    return path, document


def test_selected_destination_determines_maintenance_dependencies(tmp_path):
    path, _ = _metadata(tmp_path)
    assert metadata_capabilities(path, connection="delta-id", maintenance=True) == {
        "needs_iceberg": False, "needs_s3": False, "needs_jdbc": False}
    assert metadata_capabilities(path, stage="s3")["needs_s3"] is True
    assert metadata_capabilities(path, maintenance=True)["needs_iceberg"] is True


def test_unknown_or_opaque_metadata_keeps_conservative_dependencies(tmp_path):
    path, document = _metadata(tmp_path)
    document["connections"][0]["connection_type"] = "function"
    path.write_text(json.dumps(document))
    assert metadata_capabilities(path, stage="delta") is None
    assert metadata_capabilities("s3://fixture/metadata.json") is None
    assert metadata_capabilities(path, connection="missing", maintenance=True) is None
    path.write_text('{"dataflows": [null]}')
    assert metadata_capabilities(path) is None


def _provider_capability_documents(tmp_path):
    connections = {
        "connections": [
            {"name": "source_file", "connection_id": "source-file",
             "connection_type": "file", "format": "parquet",
             "configure": {"base_path": str(tmp_path / "input")}},
            {"name": "source_db", "connection_id": "source-db",
             "connection_type": "database", "format": "sql",
             "configure": {"host": "localhost", "port": 5432}},
            {"name": "source_api", "connection_id": "source-api",
             "connection_type": "api", "format": "api",
             "configure": {"base_url": "http://localhost:8080"}},
            {"name": "iceberg", "connection_id": "ice-id",
             "connection_type": "lakehouse", "format": "iceberg",
             "configure": {"base_path": str(tmp_path / "iceberg")}},
            {"name": "s3", "connection_id": "s3-id",
             "connection_type": "lakehouse", "format": "delta",
             "configure": {"base_path": "s3a://fixture/delta"}},
            {"name": "parquet", "connection_id": "parquet-id",
             "connection_type": "file", "format": "parquet",
             "configure": {"base_path": str(tmp_path / "parquet")}},
        ]
    }
    group_one = {"dataflows": [
        {"name": "iceberg_flow", "stage": "iceberg",
         "source": {"connection_name": "source_file", "table": "orders"},
         "destination": {"connection_name": "iceberg", "table": "orders"}},
        {"name": "jdbc_flow", "stage": "jdbc",
         "source": {"connection_name": "source_db", "table": "orders"},
         "destination": {"connection_name": "parquet", "table": "orders"}},
        {"name": "maintenance_db_flow", "stage": "maintenance_db",
         "source": {"connection_name": "source_db", "table": "orders"},
         "destination": {"connection_name": "iceberg", "table": "orders"}},
    ]}
    group_two = {"dataflows": [
        {"name": "s3_flow", "stage": "s3",
         "source": {"connection_name": "source_file", "table": "orders"},
         "destination": {"connection_name": "s3", "table": "orders"}},
        {"name": "api_flow", "stage": "api",
         "source": {"connection_name": "source_api", "table": "orders"},
         "destination": {"connection_name": "iceberg", "table": "orders"}},
    ]}
    scalar = {
        "connections": connections["connections"],
        "schema_hints": [],
        "dataflows": group_one["dataflows"] + group_two["dataflows"],
    }
    connections_path = tmp_path / "connections.json"
    hints_path = tmp_path / "schema_hints.json"
    group_one_path = tmp_path / "group_one.json"
    group_two_path = tmp_path / "group_two.json"
    scalar_path = tmp_path / "scalar.json"
    connections_path.write_text(json.dumps(connections))
    hints_path.write_text(json.dumps({"schema_hints": []}))
    group_one_path.write_text(json.dumps(group_one))
    group_two_path.write_text(json.dumps(group_two))
    scalar_path.write_text(json.dumps(scalar))
    return [group_one_path, group_two_path], connections_path, hints_path, scalar_path


def test_provider_capabilities_match_scalar_for_ordered_groups_and_shared_sections(tmp_path):
    groups, connections, hints, scalar = _provider_capability_documents(tmp_path)
    shared = {"connections_path": connections, "schema_hints_path": hints}

    for stage, expected in (
        ("iceberg", {"needs_iceberg": True, "needs_s3": False, "needs_jdbc": False}),
        ("s3", {"needs_iceberg": False, "needs_s3": True, "needs_jdbc": False}),
        ("jdbc", {"needs_iceberg": False, "needs_s3": False, "needs_jdbc": True}),
    ):
        direct = metadata_capabilities(groups, stage=stage, **shared)
        control = metadata_capabilities(scalar, stage=stage)
        assert direct == expected
        assert direct == control

    maintenance = metadata_capabilities(
        groups,
        connection="ice-id",
        maintenance=True,
        **shared,
    )
    assert maintenance == {
        "needs_iceberg": True,
        "needs_s3": False,
        "needs_jdbc": False,
    }
    assert metadata_capabilities(
        groups,
        stage="api",
        skip_api_sources=True,
        **shared,
    ) is None


def test_provider_capabilities_invalid_models_and_close_failures_are_conservative(
    tmp_path, monkeypatch
):
    groups, connections, hints, _ = _provider_capability_documents(tmp_path)
    invalid = tmp_path / "invalid.json"
    invalid.write_text(json.dumps({"dataflows": [{
        "name": "missing_destination_table",
        "source": {"connection_name": "source_file", "table": "orders"},
        "destination": {"connection_name": "iceberg"},
    }]}))
    before = {path for path in tmp_path.rglob("*")}
    assert metadata_capabilities(
        invalid,
        connections_path=connections,
        schema_hints_path=hints,
    ) is None
    assert {path for path in tmp_path.rglob("*")} == before

    import datacoolie.metadata.file_provider as file_provider_module

    real_provider = file_provider_module.FileProvider

    class CloseFailureProvider(real_provider):
        def close(self):
            super().close()
            raise RuntimeError("close failed")

    monkeypatch.setattr(file_provider_module, "FileProvider", CloseFailureProvider)
    assert metadata_capabilities(
        groups,
        stage="iceberg",
        connections_path=connections,
        schema_hints_path=hints,
    ) is None


def test_resource_aliases_and_ancestors_conflict_but_readers_can_overlap(tmp_path):
    one = resource("s3a://bucket/a/../tables/table", tmp_path)
    two = resource("s3://bucket/tables", tmp_path)
    assert overlaps(one, two)
    local = resource("folder/../data/child", tmp_path)
    parent = resource(str(tmp_path / "data"), tmp_path)
    assert overlaps(local, parent)
    reader = ({"polars": 1}, [(parent, "read")])
    assert compatible(reader, [reader])
    assert not compatible(({"spark": 1}, [(local, "write")]), [reader])
    assert not compatible(reader, [None])
    assert not compatible(None, [reader])
    assert not compatible(reader, [reader, reader])


def test_declared_admission_reads_ordered_files_and_explicit_shared_sections(tmp_path):
    source = tmp_path / "source.json"
    extra = tmp_path / "extra.json"
    connections = tmp_path / "connections.json"
    source.write_text(json.dumps({"dataflows": [{"name": "one", "stage": "run",
        "source": {"connection_name": "source", "table": "orders"},
        "destination": {"connection_name": "target", "table": "orders"}}]}))
    extra.write_text(json.dumps({"dataflows": []}))
    connections.write_text(json.dumps({"connections": [
        {"name": "source", "connection_type": "file", "format": "parquet",
         "configure": {"base_path": str(tmp_path / "input")}},
        {"name": "target", "connection_type": "file", "format": "parquet",
         "configure": {"base_path": str(tmp_path / "output")}}]}))
    scenario = {"engine": "polars", "metadata_type": "file", "stage": "run",
        "metadata_path": [str(source), str(extra)], "connections_path": str(connections),
        "concurrency": {"isolation": "declared", "engines": {"polars": 1},
            "resources": [{"path": str(tmp_path / "input"), "mode": "read"},
                          {"path": str(tmp_path / "output"), "mode": "write"}]}}
    assert declaration(scenario, tmp_path)[0] == {"polars": 1}
    scenario["concurrency"]["resources"].pop()
    with pytest.raises(ValueError, match="Undeclared write"):
        declaration(scenario, tmp_path)
    scenario["connections_path"] = "s3://bucket/connections.json"
    with pytest.raises(ValueError, match="local shared metadata"):
        declaration(scenario, tmp_path)


def test_cleanup_and_nested_engine_declarations_fail_closed(tmp_path):
    root = Path(__file__).resolve().parents[3]
    scenario = json.loads((root / "usecase-sim/scenarios/scenarios.json").read_text())["local_spark_transform_features"]
    _, locks = declaration(scenario, root)
    assert all((resource(path, root), "write") in locks for path in scenario["pre_clean_paths"])
    scenario["concurrency"]["resources"] = []
    with pytest.raises(ValueError, match="Undeclared read"):
        declaration(scenario, root)
    scenario["concurrency"]["engines"] = {"polars": 1}
    with pytest.raises(ValueError, match="scenario's engine"):
        declaration(scenario, tmp_path)
    scenario["concurrency"] = {"isolation": "fresh-local", "engines": {"spark": 1}, "resources": []}
    scenario.pop("pre_clean_paths")
    with pytest.raises(ValueError, match="standalone Polars"):
        declaration(scenario, tmp_path)


@pytest.mark.parametrize("failure", ["teardown", "allocation", "preflight"])
def test_current_failed_attempt_cannot_relabel_an_earlier_pass(tmp_path, monkeypatch, failure):
    import importlib.util
    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location("attempt_dispatcher", root / "usecase-sim/runner/run_scenario.py")
    dispatcher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dispatcher)
    log_root = tmp_path / "logs"
    scenario_root = log_root / "scenarios"
    scenario_root.mkdir(parents=True)
    monkeypatch.setattr(dispatcher, "LOG_DIR", log_root)
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", scenario_root)
    _pin_dispatcher_revision(monkeypatch, dispatcher)
    prior = {"status": "passed", "attempt_id": "old", "revision": "test-revision",
             "runtime_verified": True, "final_status": "PASS", "profile": {}}
    receipt = scenario_root / "case.receipt.json"
    receipt.write_text(json.dumps(prior))
    if failure == "teardown":
        def fail(*_, **__):
            raise owned_process.TeardownError("unconfirmed")
        monkeypatch.setattr(dispatcher, "_run_scenario", fail)
    elif failure == "allocation":
        def fail(*_, **__):
            raise ValueError("could not allocate fresh root")
        monkeypatch.setattr(dispatcher, "_fresh_maintenance_scenario", fail)
    else:
        def fail(*_, **__):
            raise subprocess.TimeoutExpired("service-start", 120)
        monkeypatch.setattr(dispatcher, "_ensure_service_running", fail)
    request = {"engine": "polars", "metadata_type": "file"}
    if failure == "preflight":
        request["services"] = ["mock-api"]
    assert dispatcher.run_scenarios(["case"], {"case": request}) == 1
    current = json.loads(receipt.read_text())
    assert current["attempt_id"] != "old"
    assert current["status"] == ("failed" if failure == "allocation" else "blocked")
    if failure == "preflight":
        assert "suite aborted" in current["final_status"]
        assert json.loads((scenario_root / "suite.timing.json").read_text())["aborted"] is True
    else:
        assert current["timing"]["total_s"] >= 0


def test_scheduler_holds_lock_through_validation_and_preserves_summary_order():
    started = threading.Event()
    validation_finished = threading.Event()
    overlap = threading.Event()
    first = ({"spark": 1}, [("key:table", "write")])
    disjoint = ({"polars": 1}, [("key:other", "write")])
    seen = []

    def run(name):
        if name == "spark":
            started.set()
            assert overlap.wait(3)
            validation_finished.set()
        elif name == "independent":
            assert started.wait(3)
            assert not validation_finished.is_set()
            overlap.set()
        else:
            assert validation_finished.is_set()
        seen.append(name)
        return 0

    results = execute(["spark", "conflicting", "independent"],
                      {"spark": first, "conflicting": first, "independent": disjoint},
                      {}, run, lambda *_: 1, parallel=True)
    assert list(results) == ["spark", "conflicting", "independent"]
    assert seen.index("conflicting") > seen.index("spark")


def test_failed_dependency_blocks_chain_and_unknown_cases_remain_exclusive():
    calls, blocked = [], []
    results = execute(["failure", "recovery", "continuation", "ordinary"],
                      {name: None for name in ("failure", "recovery", "continuation", "ordinary")},
                      {"recovery": {"failure"}, "continuation": {"recovery"}},
                      lambda name: calls.append(name) or (2 if name == "failure" else 0),
                      lambda name, reason: blocked.append((name, reason)) or 1, parallel=True)
    assert calls == ["failure", "ordinary"]
    assert [name for name, _ in blocked] == ["recovery", "continuation"]
    assert results["continuation"] == 1


def test_uncertain_teardown_stops_admission():
    calls, cancelled = [], []
    def run(name):
        calls.append(name)
        raise owned_process.TeardownError("unverified")
    with pytest.raises(owned_process.TeardownError):
        execute(["first", "later"], {"first": None, "later": None}, {}, run,
                lambda *_: 1, parallel=True, cancel=lambda: cancelled.append(True))
    assert calls == ["first"] and cancelled == [True]


def test_structured_docker_identity_does_not_depend_on_flag_offsets():
    command = ["docker", "exec", "-e", "PYTHONUNBUFFERED=1", "-e", "MODE=optimized",
               "qualified-spark", "python3", "run.py"]
    assert owned_process.docker_command(command)[1:] == ("qualified-spark", ["python3", "run.py"])
    with pytest.raises(ValueError, match="Unsupported"):
        owned_process.docker_command(["docker", "exec", "--privileged", "shared", "run.py"])


@pytest.mark.parametrize("parent_exits_early", [False, True])
def test_host_timeout_kills_grandchild_but_preserves_unrelated_process(tmp_path, monkeypatch, parent_exits_early):
    monkeypatch.setattr(owned_process, "ROOT", tmp_path)
    monkeypatch.delenv(owned_process.OWNER_ENV, raising=False)
    owned_process.CANCEL.clear()
    pid_file = tmp_path / "grandchild.pid"
    child = "import time; time.sleep(60)"
    parent = ("import subprocess,sys,time,pathlib; "
              f"p=subprocess.Popen([sys.executable,'-c',{child!r}]); "
              f"pathlib.Path({str(pid_file)!r}).write_text(str(p.pid)); " +
              ("print('parent done',flush=True)" if parent_exits_early else "time.sleep(60)"))
    unrelated = subprocess.Popen([sys.executable, "-c", child])
    try:
        started = time.monotonic()
        if parent_exits_early:
            result = owned_process.run_owned([sys.executable, "-c", parent], cwd=tmp_path, timeout=2, grace=.3)
            assert result.returncode == 0
        else:
            with pytest.raises(subprocess.TimeoutExpired):
                owned_process.run_owned([sys.executable, "-c", parent], cwd=tmp_path, timeout=2, grace=.3)
        assert time.monotonic() - started < 10
        assert unrelated.poll() is None
        pid = int(pid_file.read_text())
        if sys.platform == "win32":
            import ctypes
            from ctypes import wintypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            handle = kernel.OpenProcess(0x100000, False, pid)
            if handle:
                assert kernel.WaitForSingleObject(handle, 3000) == 0
                kernel.CloseHandle(handle)
        else:
            assert not owned_process._members(pid)
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


def test_profile_keeps_explicit_overrides_and_benchmark_control(monkeypatch):
    import importlib.util
    path = Path(__file__).resolve().parents[3] / "usecase-sim/runner/_runner_utils.py"
    spec = importlib.util.spec_from_file_location("optimization_spark_helpers", path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    seen = []
    fake = type("Session", (), {"version": "fake", "sparkContext": type("Context", (), {
        "getConf": lambda self: type("Config", (), {"get": lambda self, key, default=None: default})(),
        "defaultParallelism": 1,
        "_jvm": type("Jvm", (), {"java": type("Java", (), {"lang": type("Lang", (), {"Runtime": type("Runtime", (), {
            "getRuntime": lambda: type("RuntimeValue", (), {"maxMemory": lambda self: 4 * 1024**3})()})})})})()
    })()})()
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    monkeypatch.setattr(helper, "ivy_cache", lambda original, _: original)
    monkeypatch.setattr(helper, "get_or_create_spark_session", lambda **kw: seen.append(kw) or fake)
    monkeypatch.setattr(helper, "_resolve_packages", lambda **_: None)
    helper.build_spark_session(extra_config={"spark.master": "local[1]", "spark.driver.memory": "2g"})
    assert seen[-1]["config"]["spark.master"] == "local[1]"
    assert seen[-1]["config"]["spark.driver.memory"] == "2g"
    helper.build_spark_session(functional_profile=False)
    assert seen[-1]["config"]["spark.driver.memory"] == "64g"
    assert seen[-1]["config"]["spark.master"] == "local[*]"


def test_immutable_fixture_reuse_verifies_bytes_and_invalidates_changes(tmp_path, monkeypatch):
    import importlib.util
    root = Path(__file__).resolve().parents[3]
    path = root / "usecase-sim/scripts/features/read/prepare_polars_qualified_sql.py"
    spec = importlib.util.spec_from_file_location("immutable_fixture_preparation", path)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    monkeypatch.setenv("DATACOOLIE_SIM_EXECUTION_MODE", "optimized")
    helper.prepare_delta_suite("delta-positive", fixture_root=tmp_path)
    files = helper._fixture_files(tmp_path / "delta-positive")
    # Reuse cannot silently rewrite a version/commit file.
    monkeypatch.setattr(helper.pl, "DataFrame", lambda *_: pytest.fail("immutable inputs were rebuilt"))
    helper.prepare_delta_suite("delta-positive", fixture_root=tmp_path)
    assert helper._fixture_files(tmp_path / "delta-positive") == files
    table = tmp_path / "delta-positive" / helper.DELTA_TABLES["delta-positive"][0]
    next(table.glob("*.parquet")).write_bytes(b"changed")
    with pytest.raises(pytest.fail.Exception, match="rebuilt"):
        helper.prepare_delta_suite("delta-positive", fixture_root=tmp_path)
