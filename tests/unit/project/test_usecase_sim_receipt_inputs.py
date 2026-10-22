"""Receipts track current recipe inputs and exact bundle attempts."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM))

from cases.receipts import execution_statuses, runtime_identity, worktree_fingerprint  # noqa: E402
from runner.metadata_sources import metadata_provenance, scenario_metadata_provenance  # noqa: E402


@pytest.mark.parametrize("relative", ["metadata_recipes.json", "workflows/local_use_cases.json", "errors/startup_failure.json"])
def test_recipe_changes_invalidate_worktree_receipts(tmp_path, relative):
    root = tmp_path / "usecase-sim"
    path = root / "scenarios" / relative
    path.parent.mkdir(parents=True)
    path.write_text('{}', encoding="utf-8")
    before = worktree_fingerprint(root)
    path.write_text('{"changed": true}', encoding="utf-8")
    assert worktree_fingerprint(root) != before


def test_bundle_requires_exact_current_leaf_attempts_and_qualification(tmp_path):
    root = tmp_path / "usecase-sim"
    directory = root / ".runtime/logs/scenarios"
    directory.mkdir(parents=True)
    registry = {"errors": {"kind": "bundle", "steps": [
        {"id": "one", "recipe": {"engine": "polars", "platform": "local"}},
    ]}}
    leaf = {"revision": "r1", "attempt_id": "a1", "command": ["python", "run.py"],
            "profile": {"engine": "polars", "platform": "local"},
            "runtime": runtime_identity(), "status": "passed", "actual_exit_code": 0}
    leaf_path = directory / "errors.one.receipt.json"
    bundle_path = directory / "errors.receipt.json"
    leaf_path.write_text(json.dumps(leaf), encoding="utf-8")
    bundle_path.write_text(json.dumps({"revision": "r1", "status": "passed",
                                      "steps": [{"ref": "errors.one", "attempt_id": "a1"}]}), encoding="utf-8")
    assert execution_statuses(root, registry, revision="r1")["errors"]["status"] == "passed"
    leaf_path.write_text(json.dumps({**leaf, "runtime_verified": False}), encoding="utf-8")
    assert execution_statuses(root, registry, revision="r1")["errors"]["status"] == "blocked"
    leaf_path.write_text(json.dumps({**leaf, "attempt_id": "a2", "status": "failed", "actual_exit_code": 2}), encoding="utf-8")
    assert execution_statuses(root, registry, revision="r1")["errors"]["status"] == "historical"
    assert execution_statuses(root, registry, revision="r1")["errors"]["execution_status"] == "failed"


def test_metadata_provenance_hashes_binding_values_without_disclosing_them(tmp_path):
    path = tmp_path / "group.json"
    path.write_text('{}', encoding="utf-8")
    document = {"dataflows": [{"name": "one", "stage": "seed"}],
                "connections": [{"name": "api", "configure": {"headers": {"Authorization": "private-token"}}}]}
    bindings = {"connections": {"api": {"replace": document["connections"][0]}}}
    first = metadata_provenance(document, sources=[str(path)], bindings=bindings)
    assert "private-token" not in json.dumps(first)
    assert first["sources"][0]["sha256"]
    document["connections"][0]["configure"]["headers"]["Authorization"] = "changed-token"
    second = metadata_provenance(document, sources=[str(path)], bindings=bindings)
    assert first["effective_snapshot_sha256"] != second["effective_snapshot_sha256"]
    assert first["binding_fields"] == second["binding_fields"]


def test_scenario_provenance_keeps_exact_stage_selection():
    from datacoolie.utils.identity import name_to_uuid

    records = scenario_metadata_provenance({"metadata_path": "usecase-sim/metadata/file/merge.json",
        "connections_path": "usecase-sim/metadata/file/connections.json",
        "schema_hints_path": "usecase-sim/metadata/file/schema_hints.json",
        "dataflows": ["merge-seed", "merge-upsert"], "stage": "merge_upsert"})
    assert records[0]["dataflows"] == [{"name": "merge-upsert", "stage": "merge_upsert",
                                       "workspace_id": None,
                                       "dataflow_id": name_to_uuid("merge-upsert")}]
    scoped = metadata_provenance(
        {"dataflows": [{"name": "merge-upsert", "workspace_id": "ws-a"}]}
    )
    assert scoped["dataflows"] == [{
        "name": "merge-upsert",
        "stage": None,
        "workspace_id": "ws-a",
        "dataflow_id": name_to_uuid("merge-upsert", "ws-a"),
    }]


@pytest.mark.parametrize("case", ["c4", "c5"])
def test_replay_variant_invocations_preserve_their_own_checkpoint_namespace(case):
    from runner.run_scenario import _invocation_scenario, _scenario_watermark_root

    registry = json.loads((SIM / "scenarios/scenarios.json").read_text(encoding="utf-8"))
    scenario = registry[f"local_polars_replay_surface_sync_{case}"]
    roots = []
    for invocation in scenario["invocations"]:
        effective = _invocation_scenario(scenario, invocation)
        expected = effective["state_base_path"].rstrip("/") + "/watermarks"
        assert _scenario_watermark_root(effective).replace("\\", "/").endswith(expected)
        roots.append(expected)
    assert len(roots) == len(set(roots))
