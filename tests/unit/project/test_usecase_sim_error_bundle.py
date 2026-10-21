from __future__ import annotations

import json
import logging
import sys
import threading
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"
if str(SIM_ROOT) not in sys.path:
    sys.path.insert(0, str(SIM_ROOT))

from cases.catalog import (  # noqa: E402
    load_scenario_registry,
    runtime_identity,
    scenario_leaf_names,
    scenario_leaf_views,
    select_group_scenarios,
)
from cases.receipts import execution_statuses  # noqa: E402
from runner import run_scenario as dispatcher  # noqa: E402


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


def _pin_dispatcher_revision(monkeypatch):
    monkeypatch.setattr(dispatcher, "worktree_fingerprint", lambda *_: "test-revision")


EXPECTED_STEPS = [
    "local_polars_transform_dedup_strict",
    "local_spark_transform_dedup_strict",
    "local_polars_transform_invalid_fill",
    "local_spark_transform_invalid_fill",
    "local_polars_transform_invalid_redact",
    "local_spark_transform_invalid_redact",
    "local_polars_transform_sanitizer_collision",
    "local_spark_transform_sanitizer_collision",
    "local_polars_qualified_sql_delta_ambiguity",
    "local_polars_qualified_sql_iceberg_ambiguity",
    "local_polars_startup_failure",
    "local_polars_iceberg_unsupported_compact",
    "aws_polars_iceberg_unsupported_compact",
    "local_polars_iceberg_unsupported_orphan_files",
    "aws_polars_iceberg_unsupported_orphan_files",
    "local_polars_api_max_pages_negative",
    "local_polars_api_date_precision_negative",
    "local_spark_api_date_precision_negative",
]


def test_registry_exposes_one_canonical_error_bundle_and_qualified_leaves() -> None:
    registry = load_scenario_registry(SIM_ROOT)
    assert "error_messages" in registry
    assert registry["error_messages"]["kind"] == "bundle"
    assert [step["id"] for step in registry["error_messages"]["steps"]] == EXPECTED_STEPS
    assert all(name not in registry for name in EXPECTED_STEPS)
    assert scenario_leaf_names(registry, "error_messages") == [
        f"error_messages.{name}" for name in EXPECTED_STEPS
    ]
    leaves = scenario_leaf_views(registry)
    assert all(f"error_messages.{name}" in leaves for name in EXPECTED_STEPS)
    assert leaves["error_messages.local_spark_transform_dedup_strict"]["engine"] == "spark"


def test_group_selection_returns_only_matching_qualified_error_steps() -> None:
    transform = select_group_scenarios(SIM_ROOT, "transform")
    assert transform[:3] == [
        "local_polars_transform_a9_existing_row_filter",
        "local_polars_transform_features",
        "local_spark_transform_features",
    ]
    assert transform[3:11] == [
        f"error_messages.{name}"
        for name in EXPECTED_STEPS[:8]
    ]
    assert all(not name.startswith("error_messages.local_polars_api") for name in transform)


def test_leaf_command_forwards_repeatable_dataflows_and_bindings() -> None:
    registry = load_scenario_registry(SIM_ROOT)
    scenario = scenario_leaf_views(registry)["error_messages.local_polars_transform_dedup_strict"]
    scenario = dict(scenario)
    scenario["metadata_bindings"] = {"tokens": {"case": "bundle"}}
    command = dispatcher.build_command("error_messages.local_polars_transform_dedup_strict", scenario)
    assert command.count("--dataflow") == 1
    assert command[command.index("--dataflow") + 1] == "transform_dedup__missing_is_strict"
    assert "--metadata-bindings" in command
    assert json.loads(command[command.index("--metadata-bindings") + 1]) == scenario["metadata_bindings"]

    with pytest.raises(ValueError, match="file-backed"):
        dispatcher.build_command(
            "bad",
            {"engine": "polars", "platform": "local", "metadata_type": "api", "metadata_bindings": {}},
        )


def test_bundle_dispatch_runs_all_leaves_after_a_probe_failure_and_records_services(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registry = load_scenario_registry(SIM_ROOT)
    log_dir = tmp_path / "scenarios"
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", log_dir)
    monkeypatch.setattr(dispatcher, "_setup_log_dirs", lambda: log_dir.mkdir(parents=True, exist_ok=True))
    _pin_dispatcher_revision(monkeypatch)
    monkeypatch.setattr(dispatcher, "_docker_spark_running", lambda container: False)
    monkeypatch.setattr(dispatcher, "_cleanup_spark_state", lambda reason: None)
    services: list[str] = []
    monkeypatch.setattr(dispatcher, "_ensure_service_running", services.append)
    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", lambda *args, **kwargs: None)
    monkeypatch.setenv(dispatcher.MODE_ENV, "optimized")
    seen: list[str] = []

    def fake_run(name, scenarios, statuses, last_spark_finish=0.0):
        seen.append(name)
        return (2 if len(seen) == 1 else 0), last_spark_finish

    monkeypatch.setattr(dispatcher, "_run_scenario", fake_run)
    assert dispatcher.run_scenarios(["error_messages"], registry) == 1
    expected = [f"error_messages.{step}" for step in EXPECTED_STEPS]
    assert seen == expected
    assert {"mock-api", "minio", "iceberg-rest"}.issubset(set(services))
    aggregate = json.loads((log_dir / "error_messages.receipt.json").read_text(encoding="utf-8"))
    assert aggregate["kind"] == "bundle"
    assert "profile" not in aggregate
    assert aggregate["results"][expected[0]] == 2
    assert aggregate["results"][expected[-1]] == 0


def test_all_keeps_ordinary_overlap_and_orders_bundle_after_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Optimized --all keeps ordinary admission while serializing probes."""

    def ordinary(name: str) -> dict[str, object]:
        return {
            "engine": "polars",
            "metadata_type": "file",
            "platform": "local",
            "_test_name": name,
            "_admission": ({"polars": 1}, [(f"key:{name}", "write")]),
        }

    first = "error_messages.first"
    second = "error_messages.second"
    registry = {
        "ordinary_a": ordinary("ordinary_a"),
        "ordinary_b": ordinary("ordinary_b"),
        "error_messages": {
            "kind": "bundle",
            "execution": "sequential",
            "steps": [
                {"id": "first", "recipe": {
                    "engine": "polars", "platform": "local", "_test_name": first,
                    "_admission": ({"polars": 1}, [("key:probe:first", "write")]),
                }},
                {"id": "second", "recipe": {
                    "engine": "polars", "platform": "local", "_test_name": second,
                    "_admission": ({"polars": 1}, [("key:probe:second", "write")]),
                }},
            ],
        },
    }
    log_dir = tmp_path / "scenarios"
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", log_dir)
    monkeypatch.setattr(dispatcher, "_setup_log_dirs", lambda: log_dir.mkdir(parents=True, exist_ok=True))
    _pin_dispatcher_revision(monkeypatch)
    monkeypatch.setattr(dispatcher, "scenario_dependencies", lambda *_args: {})
    monkeypatch.setattr(dispatcher, "_write_bundle_receipt", lambda *args, **kwargs: None)
    monkeypatch.setenv(dispatcher.MODE_ENV, "optimized")

    admissions: dict[str, object] = {}

    def fake_declaration(scenario: dict, _root: Path):
        name = scenario.get("_test_name")
        if name:
            admissions[str(name)] = scenario.get("_admission")
        return scenario.get("_admission")

    monkeypatch.setattr(dispatcher, "declaration", fake_declaration)

    def write_receipt(name: str, _scenario: dict, *, final_exit_code: int, final_status: str, **_kwargs):
        (log_dir / f"{name}.receipt.json").write_text(
            json.dumps({
                "attempt_id": dispatcher._attempt.get(),
                "revision": dispatcher.worktree_fingerprint(dispatcher.USECASE_SIM_DIR),
                "status": "passed" if final_exit_code == 0 else "failed",
                "runtime_verified": True,
                "final_status": final_status,
            }),
            encoding="utf-8",
        )

    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", write_receipt)

    ordinary_names = {"ordinary_a", "ordinary_b"}
    active: set[str] = set()
    lock = threading.Lock()
    ordinary_barrier = threading.Barrier(2)
    first_finished = threading.Event()
    seen: list[str] = []

    def fake_run(name: str, _scenarios: dict, _statuses: dict, last_spark_finish: float = 0.0):
        with lock:
            seen.append(name)
            active.add(name)
        try:
            if name in ordinary_names:
                ordinary_barrier.wait(timeout=3)
            elif name == first:
                with lock:
                    assert active == {name}
            else:
                assert first_finished.is_set()
                with lock:
                    assert active == {name}
            rc = 2 if name == first else 0
            write_receipt(name, {}, final_exit_code=rc, final_status="FAIL" if rc else "PASS")
            return rc, last_spark_finish
        finally:
            with lock:
                active.remove(name)
            if name == first:
                first_finished.set()

    monkeypatch.setattr(dispatcher, "_run_scenario", fake_run)

    # ``list(registry)`` is the exact selection returned by the runner's
    # ``--all`` path; the bundle appears alongside ordinary scenarios.
    assert dispatcher.run_scenarios(list(registry), registry) == 1
    # Bundle declarations are intentionally bypassed and replaced with the
    # exclusive ``None`` admission marker; ordinary declarations still enter
    # the scheduler and overlap.
    assert set(admissions) == ordinary_names
    assert seen[-2:] == [first, second]
    assert first_finished.is_set()


def test_reversed_direct_bundle_refs_are_reordered_and_exclusive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registry = load_scenario_registry(SIM_ROOT)
    canonical = [f"error_messages.{step}" for step in EXPECTED_STEPS[:2]]
    captured: dict[str, object] = {}
    log_dir = tmp_path / "scenarios"
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", log_dir)
    monkeypatch.setattr(dispatcher, "_setup_log_dirs", lambda: log_dir.mkdir(parents=True, exist_ok=True))
    _pin_dispatcher_revision(monkeypatch)
    monkeypatch.setattr(dispatcher, "scenario_dependencies", lambda *_args: {})
    monkeypatch.setattr(dispatcher, "_docker_spark_running", lambda _container: False)
    monkeypatch.setattr(dispatcher, "_cleanup_spark_state", lambda _reason: None)
    monkeypatch.setattr(dispatcher, "_write_bundle_receipt", lambda *args, **kwargs: None)
    monkeypatch.setattr(dispatcher, "declaration", lambda *_args: ("declared", []))
    monkeypatch.setenv(dispatcher.MODE_ENV, "optimized")

    def fake_execute(names, declarations, dependencies, run, blocked, *, parallel, cancel):
        captured["names"] = list(names)
        captured["declarations"] = dict(declarations)
        captured["parallel"] = parallel
        return {name: 0 for name in names}

    monkeypatch.setattr(dispatcher, "execute", fake_execute)
    assert dispatcher.run_scenarios(list(reversed(canonical)), registry) == 0
    assert captured["names"] == canonical
    assert captured["parallel"] is True
    assert all(value is None for value in captured["declarations"].values())


def test_receipt_statuses_resolve_leaf_and_profile_neutral_bundle_receipt(tmp_path: Path) -> None:
    root = tmp_path / "usecase-sim"
    directory = root / ".runtime" / "logs" / "scenarios"
    directory.mkdir(parents=True)
    registry = {
        "error_messages": {
            "kind": "bundle",
            "steps": [{"id": "one", "recipe": {"engine": "polars", "platform": "local"}}],
        }
    }
    revision = "r1"
    attempt_id = "attempt-1"
    (directory / "error_messages.one.receipt.json").write_text(
        json.dumps(
            {
                "revision": revision,
                "attempt_id": attempt_id,
                "profile": {"engine": "polars", "platform": "local"},
                "command": ["python", "run.py"],
                "runtime": runtime_identity(),
                "status": "passed",
                "actual_exit_code": 0,
            }
        ),
        encoding="utf-8",
    )
    (directory / "error_messages.receipt.json").write_text(
        json.dumps(
            {
                "kind": "bundle",
                "revision": revision,
                "status": "passed",
                "complete": True,
                "steps": [
                    {
                        "ref": "error_messages.one",
                        "receipt": ".runtime/logs/scenarios/error_messages.one.receipt.json",
                        "result": 0,
                        "attempt_id": attempt_id,
                        "status": "passed",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    statuses = execution_statuses(root, registry, revision=revision)
    assert statuses["error_messages.one"]["status"] == "passed"
    assert statuses["error_messages"]["status"] == "passed"
    assert statuses["error_messages"]["classification"] == "current"


def test_qualified_bundle_leaf_stays_sequential_and_partial_aggregate_is_blocked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registry = load_scenario_registry(SIM_ROOT)
    leaf = "error_messages.local_polars_transform_dedup_strict"
    log_dir = tmp_path / "scenarios"
    monkeypatch.setattr(dispatcher, "SCENARIO_LOG_DIR", log_dir)
    monkeypatch.setattr(dispatcher, "_setup_log_dirs", lambda: log_dir.mkdir(parents=True, exist_ok=True))
    _pin_dispatcher_revision(monkeypatch)
    monkeypatch.setattr(dispatcher, "_write_qualification_receipt", lambda *args, **kwargs: None)
    monkeypatch.setattr(dispatcher, "_run_scenario", lambda *args, **kwargs: (0, 0.0))
    captured: dict[str, object] = {}

    def fake_execute(names, declarations, dependencies, run, blocked, *, parallel, cancel):
        captured["parallel"] = parallel
        return {name: run(name) for name in names}

    monkeypatch.setattr(dispatcher, "execute", fake_execute)
    monkeypatch.setenv(dispatcher.MODE_ENV, "optimized")
    assert dispatcher.run_scenarios([leaf], registry) == 0
    assert captured["parallel"] is True
    aggregate = json.loads((log_dir / "error_messages.receipt.json").read_text(encoding="utf-8"))
    assert aggregate["complete"] is False
    assert aggregate["status"] == "blocked"
    assert aggregate["steps"][0]["ref"] == leaf
    assert aggregate["steps"][0]["attempt_id"]
    assert aggregate["steps"][0]["status"] == "blocked"


def test_unknown_qualified_step_is_rejected_before_log_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = load_scenario_registry(SIM_ROOT)
    monkeypatch.setattr(dispatcher, "_setup_log_dirs", lambda: pytest.fail("log setup must not run"))
    assert dispatcher.run_scenarios(["error_messages.unknown_step"], registry) == 1


def test_existing_incremental_failure_recovery_chain_keeps_order() -> None:
    names = select_group_scenarios(SIM_ROOT, "incremental")
    wanted = [
        "local_polars_api_recovery_fail",
        "local_polars_api_recovery",
        "local_polars_api_continuation",
        "local_spark_api_recovery_fail",
        "local_spark_api_recovery",
        "local_spark_api_continuation",
    ]
    positions = [names.index(name) for name in wanted]
    assert positions == sorted(positions)
