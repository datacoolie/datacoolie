"""Mutate persisted surface evidence without executing services or ETL."""

from __future__ import annotations

import importlib.util
from copy import deepcopy
from pathlib import Path

import pytest


@pytest.fixture
def surfaces():
    path = Path(__file__).resolve().parents[3] / "usecase-sim/scripts/features/replay/validate_replay_surface_sync.py"
    spec = importlib.util.spec_from_file_location("replay_surfaces_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def c1_evidence():
    state = {"sha256": "same", "files": []}
    saved = {"sha256": "saved", "files": [{"path": "watermark_value.json", "value": {"event_time": "2024-02-01T10:00:00Z"}}]}
    return [
        {"label": "initial", "state_after": deepcopy(state)},
        {"label": "replay_no_save", "state_before": deepcopy(state), "state_after": deepcopy(state), "command": ["--replay-start", "--replay-end"]},
        {"label": "replay_save", "state_after": saved, "command": ["--replay-start", "--replay-end"]},
    ]


@pytest.mark.parametrize("mutant", [None, "no_save", "stale_save", "duplicate", "missing_write"])
def test_c1_oracle_preserves_no_save_and_monotonic_save(surfaces, monkeypatch, tmp_path, mutant):
    output = tmp_path / "output/c1"
    output.mkdir(parents=True)
    runs = c1_evidence()
    ids = [1, 2, 3, 4, 5]
    if mutant == "no_save":
        runs[1]["state_after"]["sha256"] = "changed"
    if mutant == "stale_save":
        runs[2]["state_after"]["files"][0]["value"]["event_time"] = "2024-01-01T00:00:00Z"
    if mutant == "duplicate":
        ids.append(5)
    monkeypatch.setattr(surfaces, "_root", lambda *_: tmp_path)
    monkeypatch.setattr(surfaces, "_ids", lambda *_: ids)
    monkeypatch.setattr(surfaces, "_receipts", lambda *_: runs)
    monkeypatch.setattr(surfaces, "_history_length", lambda *_: 2 if mutant == "missing_write" else 3)
    if mutant:
        with pytest.raises(AssertionError):
            surfaces._validate_c1("polars")
    else:
        assert surfaces._validate_c1("polars")["ids"] == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("mutant", [None, "wrong_link", "offending_dispatch", "unexpected_state", "missing_id"])
def test_c5_oracle_accepts_valid_continuations_and_rejects_conflicts(surfaces, monkeypatch, tmp_path, mutant):
    queries = {
        "opaque": "page=2&mode=opaque&signed=token%2Bopaque",
        "matching": "page=2&mode=matching&from_time=2024-01-15T00%3A00%3A00%2B00%3A00&to_time=2024-01-16T00%3A00%3A00%2B00%3A00&signed=token%2Bmatching",
        "missing": "page=2&mode=missing&signed=token%2Bmissing&from_time=2024-01-15T00%3A00%3A00%2B00%3A00&to_time=2024-01-16T00%3A00%3A00%2B00%3A00",
    }
    trace = [{"path": "/api/orders/surface-next-link", "query": f"mode={mode}"} for mode in ["opaque", "matching", "missing", "conflict", "duplicate"]]
    trace.extend({"path": "/api/orders/surface-next-link", "query": query} for query in queries.values())
    runs = [{"label": mode, "actual_exit_code": 1, "state_after": {"sha256": None}} for mode in ["conflict", "duplicate"]]
    if mutant == "wrong_link":
        trace[-1]["query"] = "page=2&mode=missing&unexpected=1"
    if mutant == "offending_dispatch":
        trace.append({"path": "/api/orders/surface-next-link", "query": "mode=conflict&page=2"})
    if mutant == "unexpected_state":
        runs[0]["state_after"]["sha256"] = "committed"
    monkeypatch.setattr(surfaces, "_root", lambda *_: tmp_path)
    monkeypatch.setattr(surfaces, "_ids", lambda *_: [2101] if mutant == "missing_id" else [2101, 2102])
    monkeypatch.setattr(surfaces, "_receipts", lambda *_: runs)
    monkeypatch.setattr(surfaces, "_trace", lambda: {"requests": trace})
    if mutant:
        with pytest.raises(AssertionError):
            surfaces._validate_c5("polars")
    else:
        assert set(surfaces._validate_c5("polars")["modes"]) == {"opaque", "matching", "missing", "conflict", "duplicate"}
