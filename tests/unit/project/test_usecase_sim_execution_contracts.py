"""Guard simulator history oracles independently from the product projection."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def validator():
    script = (
        Path(__file__).resolve().parents[3]
        / "usecase-sim/scripts/features/replay"
        / "validate_replay_window_safety.py"
    )
    spec = importlib.util.spec_from_file_location("window_validator_test", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def history_records(case):
    statuses = (
        ["failed", "succeeded"]
        if case == "retry-after-write"
        else ["skipped"]
        if case == "typed-empty"
        else ["failed"]
        if case == "folder-only"
        else ["succeeded"]
    )
    attempts = []
    for number, status in enumerate(statuses, 1):
        attempts.append(
            {
                "attempt_number": number,
                "status": status,
                "source": {
                    "source_action": {"reader": "SimulatorReader"},
                    "selection": {
                        "mode": "range",
                        "lower": {"seq": 1},
                        "upper": {"seq": 10},
                        "lower_operator": ">=",
                        "upper_operator": "<",
                        "combine_operator": "OR",
                        "upper_origin": "explicit",
                    },
                },
                "transform": None,
                "destination": None,
                "watermark": {
                    "load_status": "loaded" if number == 1 else "reused",
                    "loaded_watermark": {"seq": 1},
                    "merge_base": {"seq": 1},
                    "candidate": {
                        "values": {"seq": 2},
                        "kind": "observed_max",
                    },
                    "watermark_to_save": {"seq": 2},
                    "save_status": "not_attempted",
                    "saved_watermark": None,
                },
            }
        )
    if case == "retry-after-write":
        attempts[-1]["watermark"].update(
            save_status="saved", saved_watermark={"seq": 2}
        )
    destination_metric = None if case == "folder-only" else 0
    metrics = {
        "rows_read": 0 if case == "typed-empty" else 1,
        "rows_written": None if case == "folder-only" else int(statuses[-1] == "succeeded"),
        "rows_inserted": destination_metric,
        "rows_updated": destination_metric,
        "rows_deleted": destination_metric,
        "files_added": destination_metric,
        "files_removed": destination_metric,
        "bytes_added": destination_metric,
        "bytes_removed": destination_metric,
        "bytes_saved": destination_metric,
    }
    run = {
        "log_schema_version": 6,
        "_type": "dataflow_run_log",
        "dataflow_run_id": "shared-run",
        "status": statuses[-1],
        "retry_attempts": len(attempts) - 1,
        "read_request": {
            "watermark_start": {"seq": 1},
            "watermark_end": {"seq": 10},
            "watermark_start_operator": ">=",
            "watermark_end_operator": "<",
            "read_range": None,
            "reference_time": "2026-10-04T00:00:00+00:00",
        },
        "attempts": attempts,
        "metrics": metrics,
    }
    job = {
        "log_schema_version": 6,
        "_type": "job_run_log",
        "total_dataflows": 1,
        "total_succeeded": int(statuses[-1] == "succeeded"),
        "total_failed": int(statuses[-1] == "failed"),
        "total_skipped": int(statuses[-1] == "skipped"),
        "total_rows_read": metrics["rows_read"],
        "total_rows_written": metrics["rows_written"],
    }
    system = {
        "log_schema_version": 6,
        "_type": "system_log",
        "event_name": "retry.scheduled",
        "attempt_number": 1,
        "dataflow_run_id": "shared-run",
    }
    return run, job, system


def attach_records(monkeypatch, validator, run, job, system):
    def records(root, job_id, category, session_id):
        assert job_id == "job" and session_id == "session"
        value = (
            system
            if category == "system_logs"
            else (job if category.endswith("job_run_log") else run)
        )
        return [(Path("fixture.json"), value)]

    monkeypatch.setattr(validator, "records", records)


@pytest.mark.parametrize(
    "case", ["typed-empty", "folder-only", "mixed-folder-row", "retry-after-write"]
)
def test_persisted_history_contract(validator, monkeypatch, case):
    attach_records(monkeypatch, validator, *history_records(case))
    assert (
        validator.validate_history("job", "session", case)["dataflow_run_id"]
        == "shared-run"
    )


@pytest.mark.parametrize("mutation", ["count", "rows", "run_id", "number", "non_array"])
def test_history_oracle_rejects_retry_count_inflation_and_bad_correlation(
    validator, monkeypatch, mutation
):
    run, job, system = history_records("retry-after-write")
    if mutation == "count":
        job["total_dataflows"] = 2
    elif mutation == "rows":
        job["total_rows_read"] = 2
    elif mutation == "run_id":
        system["dataflow_run_id"] = "another-run"
    elif mutation == "number":
        system["attempt_number"] = 2
    else:
        run["attempts"] = {"attempt_number": 1}
    attach_records(monkeypatch, validator, run, job, system)
    with pytest.raises(AssertionError):
        validator.validate_history("job", "session", "retry-after-write")


def test_empty_history_oracle_rejects_destination_participation(validator, monkeypatch):
    run, job, system = history_records("typed-empty")
    attempts = run["attempts"]
    attempts[0]["destination"] = {"rows_deleted": 2}
    attach_records(monkeypatch, validator, run, job, system)
    with pytest.raises(AssertionError):
        validator.validate_history("job", "session", "typed-empty")
