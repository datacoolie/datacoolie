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


def _load_bound_parser():
    # The simulator is an executable boundary rather than an installed package.
    # Import its helper with only the runner directory temporarily on sys.path.
    sys.path.insert(0, str(RUNNER_DIR))
    try:
        from _runner_utils import parse_replay_bound
    finally:
        sys.path.pop(0)
    return parse_replay_bound


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
    scenario["derive_log_paths_from_state"] = True
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
            {"job_id": job_id, "event_name": "session.starting"},
            {"job_id": job_id, "event_name": "session.startup_failed"},
        ]
        if job_matches:
            records.append({"job_id": job_id, "_type": "job_run_log", "status": "failed"})
        (logs / "records.json").write_text(
            "\n".join(json.dumps(record) for record in records), encoding="utf-8"
        )
        return child_exit, "fixture child"

    monkeypatch.setattr(module, "_run_with_tee", child_receipt)
    assert module.run_scenarios([name], {name: scenario}) == outer_exit
    receipts = json.loads(
        (module.SCENARIO_LOG_DIR / f"{name}.invocations.json").read_text()
    )
    assert len(receipts) == 1
    assert receipts[0]["expected_exit_code"] == 1
    assert receipts[0]["actual_exit_code"] == child_exit
