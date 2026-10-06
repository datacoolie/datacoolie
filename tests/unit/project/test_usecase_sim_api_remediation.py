"""Mutation and composition checks for dedicated API remediation cases."""

import copy
import json
import sys
from pathlib import Path

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM / "scripts/features"))
from read.api_rejection_support import assert_rejection  # noqa: E402
from read.validate_api_max_pages import ERROR  # noqa: E402
from incremental.validate_api_date import assert_date_state, EXPECTED  # noqa: E402
from datacoolie.utils.identity import name_to_uuid  # noqa: E402


def _failure():
    return [
        {
            "_type": "dataflow_run_log",
            "dataflow_id": name_to_uuid("api_request__max_pages"),
            "log_session_id": "current",
            "status": "failed",
            "attempts": [
                {"source": {"status": "failed", "message": ERROR}, "destination": None}
            ],
        }
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_error",
        "missing_id",
        "wrong_id",
        "wrong_session",
        "missing_attempt",
        "wrong_exit",
        "null_source",
        "destination_phase",
    ],
)
def test_rejection_oracle_rejects_unrelated_failures(mutation):
    rows = _failure()
    code = 2
    if mutation == "wrong_error":
        rows[0]["attempts"][0]["source"]["message"] = "HTTP 500"
    if mutation == "missing_id":
        rows[0].pop("dataflow_id")
    if mutation == "wrong_id":
        rows[0]["dataflow_id"] = "another-flow"
    if mutation == "wrong_session":
        rows[0]["log_session_id"] = "historical"
    if mutation == "missing_attempt":
        rows[0]["attempts"] = []
    if mutation == "wrong_exit":
        code = 0
    if mutation == "null_source":
        rows[0]["attempts"][0]["source"] = None
    if mutation == "destination_phase":
        rows[0]["attempts"][0]["destination"] = {"status": "failed"}
    with pytest.raises(AssertionError):
        assert_rejection(
            code, rows, flow="api_request__max_pages", session_id="current", error=ERROR
        )


def test_exact_rejection_accepts_expected_failure():
    assert_rejection(
        2, _failure(), flow="api_request__max_pages", session_id="current", error=ERROR
    )


@pytest.mark.parametrize(
    "mutation", ["duplicate", "wrong_row", "wrong_checkpoint", "missing_row"]
)
def test_date_oracle_rejects_wrong_results(mutation):
    rows = copy.deepcopy(EXPECTED)
    checkpoint = {"event_date": EXPECTED[-1]["event_date"].isoformat()}
    if mutation == "duplicate":
        rows.append(rows[0])
    if mutation == "wrong_row":
        rows[0]["category"] = "wrong"
    if mutation == "missing_row":
        rows.pop()
    if mutation == "wrong_checkpoint":
        checkpoint = {"event_date": EXPECTED[0]["event_date"]}
    with pytest.raises(AssertionError):
        assert_date_state(rows, checkpoint, EXPECTED)


def test_moved_flows_excluded_from_broad_workflow_and_owned_once():
    from runner.metadata_composition import load_metadata

    removed = {"api_request__max_pages", "api_wm__date_fmt"}
    composed = load_metadata("metadata/file/workflows/local_use_cases.json", root=SIM)
    assert not removed.intersection(f["name"] for f in composed["dataflows"])
    for name in removed:
        refs = [
            r
            for p in (SIM / "cases").glob("*.json")
            for c in json.loads(p.read_text()).get("cases", [])
            for r in c.get("flows", [])
            if r.get("flow") == name
        ]
        assert len(refs) == 1 and refs[0]["role"] == "primary"


def test_api_cap_authored_controls_preserved():
    d = json.loads((SIM / "metadata/file/read/api_max_pages_negative.json").read_text())
    cfg = d["dataflows"][0]["source"]["configure"]
    assert cfg["max_pages"] == 2 and cfg["page_size"] == 5


def test_date_fixture_exclusive_lower_and_date_hint():
    d = json.loads(
        (SIM / "metadata/file/incremental/api_date_positive.json").read_text()
    )
    f = d["dataflows"][0]
    assert (
        f["source"]["configure"]["range_param_mapping"]["event_date"]["lower"][
            "operator"
        ]
        == ">"
    )
    assert f["transform"]["schema_hints"][0]["data_type"] == "DATE"


def test_native_database_recipe_preserves_authored_mssql_false(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from read import validate_database_profiles as module

    case = SimpleNamespace(
        metadata=tmp_path / "metadata.json", output=tmp_path / "output"
    )
    monkeypatch.setattr(module, "resolve_case_root", lambda *args: case)

    def stop_before_source_setup(*args):
        raise RuntimeError("stop before source setup")

    monkeypatch.setattr(module, "_connection", stop_before_source_setup)
    with pytest.raises(RuntimeError, match="stop before source setup"):
        module.validate(profile="mssql")
    authored = json.loads((SIM / "metadata/file/shared/local.json").read_text())
    source = next(
        c for c in authored["connections"] if c["name"] == "local_mssql_source"
    )
    compiled = json.loads(case.metadata.read_text())
    assert (
        compiled["connections"][0]["configure"]["encrypt"]
        == source["configure"]["encrypt"]
        == "false"
    )
