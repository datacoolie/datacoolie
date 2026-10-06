"""Read-only catalog and group-dispatch contract tests."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


PRODUCT_ROOT = Path(__file__).resolve().parents[3]
SIM_ROOT = PRODUCT_ROOT / "usecase-sim"


def _load_catalog_module():
    path = SIM_ROOT / "cases" / "catalog.py"
    spec = importlib.util.spec_from_file_location("usecase_sim_case_catalog_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_dispatcher_module():
    path = SIM_ROOT / "runner" / "run_scenario.py"
    spec = importlib.util.spec_from_file_location("usecase_sim_runner_catalog_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _fixture_root(tmp_path: Path, *, dedicated: list[str] | None = None):
    root = tmp_path / "usecase-sim"
    cases = root / "cases"
    fixture = root / "metadata" / "demo.json"
    cases.mkdir(parents=True)
    fixture.parent.mkdir(parents=True)
    fixture.write_text(
        json.dumps(
            {
                "connections": [],
                "dataflows": [{"name": "demo_flow"}],
            }
        ),
        encoding="utf-8",
    )
    (root / "assertions.py").write_text("def test_demo():\n    assert True\n", encoding="utf-8")
    scenario_names = ["demo_scenario"] if dedicated is None else dedicated
    scenarios = {
        name: {"engine": "polars", "platform": "local"}
        for name in scenario_names
    }
    (root / "scenarios").mkdir()
    (root / "scenarios" / "scenarios.json").write_text(
        json.dumps(scenarios), encoding="utf-8"
    )
    (cases / "inventory.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "exclusions": [],
                "scripts": [],
                "generated": [],
            }
        ),
        encoding="utf-8",
    )
    (cases / "demo.json").write_text(
        json.dumps(
            {
                "group": "demo",
                "behaviors": [
                    {
                        "id": "demo.behavior",
                        "intent": "demonstrate a flow",
                        "contract": "assertions.py",
                        "coverage": "asserted",
                        "assertion_owners": ["assertions.py::test_demo"],
                    }
                ],
                "cases": [
                    {
                        "id": "demo.case",
                        "behavior": "demo.behavior",
                        "intent": "run the demo flow",
                        "contract": "assertions.py",
                        "flows": [
                            {
                                "fixture": "usecase-sim/metadata/demo.json",
                                "flow": "demo_flow",
                                "role": "primary",
                            }
                        ],
                        "scenarios": scenario_names,
                        "dedicated_scenarios": scenario_names,
                        "preconditions": [],
                        "expected": "the flow completes",
                        "assertion_owners": ["assertions.py::test_demo"],
                        "required_profiles": [
                            {"engine": "polars", "platform": "local"}
                        ],
                        "coverage": "asserted",
                        "dependencies": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return root


def test_audit_discovers_authored_flows_without_reading_runtime(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    runtime = root / ".runtime" / "data"
    runtime.mkdir(parents=True)
    (runtime / "generated.json").write_text(
        json.dumps({"dataflows": [{"name": "must_not_be_discovered"}]}),
        encoding="utf-8",
    )

    report = module.audit_catalog(root)

    assert report["ok"]
    assert report["discovered"]["flows"] == [
        {"fixture": "metadata/demo.json", "flow": "demo_flow"}
    ]


@pytest.mark.parametrize("relative", ["metadata/file/other/demo.json", "platforms/aws/metadata/file/other/demo.json"])
def test_audit_rejects_primary_flow_in_another_group_folder(tmp_path, relative):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    old = root / "metadata/demo.json"
    wrong = root / relative
    wrong.parent.mkdir(parents=True)
    old.replace(wrong)
    path = root / "cases/demo.json"
    value = json.loads(path.read_text())
    value["cases"][0]["flows"][0]["fixture"] = "usecase-sim/" + relative
    path.write_text(json.dumps(value), encoding="utf-8")

    report = module.audit_catalog(root)

    assert not report["ok"]
    assert any(item["code"] == "fixture_group_mismatch" for item in report["errors"])
    assert all("must_not_be_discovered" not in str(item) for item in report["discovered"]["flows"])


def test_audit_reports_unowned_flow_and_duplicate_primary_owner(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    fixture = root / "metadata" / "other.json"
    fixture.write_text(
        json.dumps({"dataflows": [{"name": "orphan_flow"}]}), encoding="utf-8"
    )
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    duplicate = dict(catalog["cases"][0])
    duplicate["id"] = "demo.duplicate"
    duplicate["dedicated_scenarios"] = []
    duplicate["scenarios"] = []
    catalog["cases"].append(duplicate)
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = module.audit_catalog(root)
    codes = {item["code"] for item in report["errors"]}

    assert "unowned_flow" in codes
    assert "duplicate_flow_owner" in codes


def test_audit_rejects_phantom_primary_and_support_flow_refs(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["cases"][0]["flows"] = [
        {
            "fixture": "usecase-sim/metadata/demo.json",
            "flow": "missing_primary",
            "role": "primary",
        },
        {
            "fixture": "usecase-sim/metadata/demo.json",
            "flow": "missing_support",
            "role": "support",
        },
    ]
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = module.audit_catalog(root)
    orphan_refs = [item for item in report["errors"] if item["code"] == "orphan_flow_ref"]

    assert {item["message"].split()[0] for item in orphan_refs} == {"primary", "support"}


def test_group_selection_preserves_failure_recovery_continuation_order(tmp_path):
    module = _load_catalog_module()
    names = ["api_failure", "api_recovery", "api_continuation"]
    root = _fixture_root(tmp_path, dedicated=names)

    selected = module.select_group_scenarios(root, "demo")

    assert selected == names


def test_group_selection_includes_cross_group_dependency_before_group_cases(tmp_path):
    module = _load_catalog_module()
    names = ["api_failure", "api_recovery", "api_continuation"]
    root = _fixture_root(tmp_path, dedicated=names)
    scenarios_path = root / "scenarios" / "scenarios.json"
    scenarios = json.loads(scenarios_path.read_text(encoding="utf-8"))
    scenarios["prerequisite_scenario"] = {"engine": "polars", "platform": "local"}
    scenarios_path.write_text(json.dumps(scenarios), encoding="utf-8")

    demo_path = root / "cases" / "demo.json"
    demo = json.loads(demo_path.read_text(encoding="utf-8"))
    demo["cases"][0]["dependencies"] = ["prerequisite.case"]
    demo_path.write_text(json.dumps(demo), encoding="utf-8")
    (root / "cases" / "prerequisite.json").write_text(
        json.dumps(
            {
                "group": "prerequisite",
                "behaviors": [],
                "cases": [
                    {
                        "id": "prerequisite.case",
                        "dedicated_scenarios": ["prerequisite_scenario"],
                        "dependencies": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert module.select_group_scenarios(root, "demo") == [
        "prerequisite_scenario",
        *names,
    ]


def test_group_selection_rejects_dependency_without_dedicated_recipe(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    demo_path = root / "cases" / "demo.json"
    demo = json.loads(demo_path.read_text(encoding="utf-8"))
    demo["cases"][0]["dependencies"] = ["prerequisite.case"]
    demo_path.write_text(json.dumps(demo), encoding="utf-8")
    (root / "cases" / "prerequisite.json").write_text(
        json.dumps(
            {
                "group": "prerequisite",
                "behaviors": [],
                "cases": [
                    {
                        "id": "prerequisite.case",
                        "dedicated_scenarios": [],
                        "dependencies": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(module.CatalogError, match="without a dedicated scenario"):
        module.select_group_scenarios(root, "demo")


def test_group_selection_rejects_unknown_and_empty_groups(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path, dedicated=[])

    with pytest.raises(module.CatalogError, match="unknown group"):
        module.select_group_scenarios(root, "missing")
    with pytest.raises(module.CatalogError, match="no runnable"):
        module.select_group_scenarios(root, "demo")


def test_unknown_dispatch_rejects_before_log_setup():
    dispatcher = _load_dispatcher_module()
    dispatcher._setup_log_dirs = lambda: pytest.fail("unknown dispatch created log setup")

    assert dispatcher.run_scenarios(["missing_scenario"], {}) == 1


def test_legacy_receipt_is_historical_and_explicit_identity_can_be_current():
    module = _load_catalog_module()
    legacy = module.classify_receipt(
        {"actual_exit_code": 0, "expected_exit_code": 0}
    )
    current = module.classify_receipt(
        {
            "revision": "r1",
            "profile": {"engine": "polars", "platform": "local"},
            "command": ["python", "run"],
            "runtime": {"python": "3.11", "datacoolie": "test"},
            "status": "passed",
            "actual_exit_code": 0,
            "expected_exit_code": 0,
        },
        revision="r1",
        profile={"engine": "polars", "platform": "local"},
        command=["python", "run"],
    )

    assert legacy == {
        "status": "historical",
        "execution_status": "blocked",
        "classification": "historical",
    }
    assert current["status"] == "passed"
    assert current["classification"] == "current"


def test_receipt_requires_final_status_and_worktree_revision(tmp_path):
    module = _load_catalog_module()
    (tmp_path / "usecase-sim" / "runner").mkdir(parents=True)
    (tmp_path / "usecase-sim" / "runner" / "run.py").write_text(
        "print('runner')\n", encoding="utf-8"
    )
    artifacts = tmp_path / "usecase-sim" / "artifacts"
    artifacts.mkdir()
    artifact = artifacts / "query.sql"
    artifact.write_text("select 1\n", encoding="utf-8")
    root = tmp_path / "usecase-sim"
    profile = {"engine": "polars", "platform": "local"}
    command = ["python", "run"]
    revision = module.worktree_fingerprint(root)
    receipt = {
        "revision": revision,
        "profile": profile,
        "command": command,
        "runtime": {"python": "3.11", "datacoolie": "test"},
        "status": "passed",
        "actual_exit_code": 0,
    }
    assert module.classify_receipt(
        receipt, revision=revision, profile=profile, command=command
    )["classification"] == "current"

    docker_receipt = {**receipt, "runtime_verified": False}
    assert module.classify_receipt(
        docker_receipt, revision=revision, profile=profile, command=command
    )["status"] == "blocked"

    missing_status = dict(receipt)
    missing_status.pop("status")
    assert module.classify_receipt(
        missing_status, revision=revision, profile=profile, command=command
    )["status"] == "historical"

    artifact.write_text("select 2\n", encoding="utf-8")
    assert module.worktree_fingerprint(root) != revision
    assert module.classify_receipt(
        receipt, revision=module.worktree_fingerprint(root), profile=profile, command=command
    )["status"] == "historical"

    (root / "runner" / "run.py").write_text("print('changed')\n", encoding="utf-8")
    assert module.worktree_fingerprint(root) != revision

    runtime = module.runtime_identity()
    runtime_receipt = {**receipt, "runtime": runtime}
    assert module.classify_receipt(
        runtime_receipt,
        revision=revision,
        profile=profile,
        command=command,
        runtime=runtime,
    )["classification"] == "current"
    changed_runtime = {**runtime, "python": "0.0"}
    assert module.classify_receipt(
        runtime_receipt,
        revision=revision,
        profile=profile,
        command=command,
        runtime=changed_runtime,
    )["status"] == "historical"

    host_profile = {
        **profile,
        "execution_mode": "host",
        "container": None,
    }
    profiled_receipt = {**runtime_receipt, "profile": host_profile}
    assert module.classify_receipt(
        profiled_receipt,
        revision=revision,
        profile=host_profile,
        command=command,
        runtime=runtime,
    )["classification"] == "current"
    docker_profile = {**host_profile, "execution_mode": "docker", "container": "spark"}
    assert module.classify_receipt(
        profiled_receipt,
        revision=revision,
        profile=docker_profile,
        command=command,
        runtime=runtime,
    )["status"] == "historical"


def test_receipt_records_keep_failure_precedence_over_later_success():
    module = _load_catalog_module()
    profile = {"engine": "polars", "platform": "local"}
    command = ["python", "run"]
    common = {
        "revision": "r1",
        "profile": profile,
        "command": command,
        "runtime": {"python": "3.11", "datacoolie": "test"},
    }
    records = [
        {**common, "status": "failed", "actual_exit_code": 1},
        {**common, "status": "passed", "actual_exit_code": 0},
    ]
    result = module.classify_receipt(
        records, revision="r1", profile=profile, command=command
    )
    assert result["classification"] == "current"
    assert result["status"] == "failed"


def test_owner_resolution_uses_exact_ast_chains_and_allows_directories(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    (root / "assertions.py").write_text(
        """
class TestDemo:
    def test_method(self):
        assert True

def test_function(value):
    assert value

text = 'ghost'
# def comment_ghost(): pass
""",
        encoding="utf-8",
    )
    (root / "validators").mkdir()

    assert module._owner_exists(root, "assertions.py::TestDemo::test_method[param]")[0]
    assert module._owner_exists(root, "assertions.py::test_function[param]")[0]
    assert not module._owner_exists(root, "assertions.py::ghost")[0]
    assert not module._owner_exists(root, "assertions.py::comment_ghost")[0]
    assert not module._owner_exists(root, "assertions.py::")[0]
    assert not module._contract_exists(root, "assertions.py::")[0]
    assert not module._owner_exists(root, "assertions.py::")[0]
    assert module._contract_exists(root, "assertions.py::TestDemo::test_method[param]")[0]
    assert not module._contract_exists(root, "assertions.py::ghost")[0]
    assert not module._owner_exists(root, "validators")[0]
    assert module._contract_exists(root, "validators")[0]
    assert not module._contract_exists(root, "validators::ghost")[0]
    assert not module._contract_exists(root, "validators::")[0]


@pytest.mark.parametrize("contract", ["", "missing_contract.py"])
def test_audit_rejects_empty_or_missing_contract_targets(tmp_path, contract):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["behaviors"][0]["contract"] = contract
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = module.audit_catalog(root)

    assert not report["ok"]
    assert any(item["code"] == "bad_contract" for item in report["errors"])


def test_duplicate_dataflow_name_is_rejected_only_within_one_authored_json(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    fixture = root / "metadata" / "demo.json"
    value = json.loads(fixture.read_text(encoding="utf-8"))
    value["dataflows"].append({"name": "demo_flow"})
    fixture.write_text(json.dumps(value), encoding="utf-8")
    duplicate_report = module.audit_catalog(root)
    assert any(item["code"] == "duplicate_dataflow_name" for item in duplicate_report["errors"])

    value["dataflows"] = [{"name": "demo_flow"}]
    fixture.write_text(json.dumps(value), encoding="utf-8")
    other = root / "metadata" / "other.json"
    other.write_text(json.dumps({"dataflows": [{"name": "demo_flow"}]}), encoding="utf-8")
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["cases"][0]["flows"].append(
        {
            "fixture": "usecase-sim/metadata/other.json",
            "flow": "demo_flow",
            "role": "primary",
        }
    )
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    cross_file_report = module.audit_catalog(root)
    assert not any(item["code"] == "duplicate_dataflow_name" for item in cross_file_report["errors"])


def test_v2_inventory_requires_behavior_checks_and_case_next_action(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    inventory_path = root / "cases" / "inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    inventory["coverage_schema_version"] = 2
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")

    report = module.audit_catalog(root)
    codes = {item["code"] for item in report["errors"]}

    assert "missing_checks" in codes
    assert "missing_case_field" in codes
    assert not module.audit_catalog(root)["ok"]


def test_v2_rejects_unknown_check_cases_family_mismatch_and_contradictions(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    inventory_path = root / "cases" / "inventory.json"
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    inventory["coverage_schema_version"] = 2
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["cases"][0]["next_action"] = "none"
    catalog["behaviors"][0]["checks"] = [
        {
            "id": "demo.behavior.result",
            "acceptance": "the result is observable",
            "preconditions": ["seed the fixture"],
            "case_ids": ["unknown.case"],
            "assertion_owners": [],
            "coverage": "asserted",
            "gap": "",
            "next_action": "none",
        }
    ]
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = module.audit_catalog(root)
    codes = {item["code"] for item in report["errors"]}
    assert "orphan_check_case" in codes
    assert "asserted_check_without_owner" in codes

    catalog["behaviors"][0]["checks"][0].update(
        {"case_ids": ["demo.case"], "coverage": "fixture_only", "gap": "", "next_action": "none"}
    )
    catalog["cases"][0]["behavior"] = "other.behavior"
    catalog["cases"][0]["coverage"] = "fixture_only"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    report = module.audit_catalog(root)
    codes = {item["code"] for item in report["errors"]}
    assert "check_case_family_mismatch" in codes
    assert "missing_check_gap" in codes
    assert "missing_check_action" in codes
    assert "missing_case_gap" in codes
    assert "missing_case_action" in codes

    catalog["cases"][0]["behavior"] = "demo.behavior"
    catalog["cases"][0]["coverage"] = "asserted"
    catalog["behaviors"][0]["checks"][0]["coverage"] = "invalid"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    report = module.audit_catalog(root)
    assert any(item["code"] == "bad_check_coverage" for item in report["errors"])


def test_malformed_case_array_is_reported_without_shape_iteration_crash(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    catalog_path = root / "cases" / "demo.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog["cases"] = {"not": "an array"}
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = module.audit_catalog(root)

    assert not report["ok"]
    assert any(item["code"] == "catalog_field_type" for item in report["errors"])


def test_check_coverage_views_separate_functional_and_measurement_scopes():
    module = _load_catalog_module()
    report = module.audit_catalog(SIM_ROOT)
    performance = module.audit_catalog(SIM_ROOT, "performance")
    merge = module.audit_catalog(SIM_ROOT, "merge")

    assert performance["functional_check_coverage"]["total"] == 0
    assert performance["measurement_check_coverage"]["total"] == 2
    assert merge["measurement_check_coverage"]["total"] == 0
    assert report["functional_check_coverage"]["total"] + report["measurement_check_coverage"]["total"] == report["check_coverage"]["total"]
    compact = module.compact_audit_report(report)
    assert compact["functional_check_coverage"] == report["functional_check_coverage"]
    assert compact["measurement_check_coverage"] == report["measurement_check_coverage"]


def test_group_report_scopes_presentation_but_keeps_global_integrity_errors(tmp_path):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    other = {
        "group": "other",
        "behaviors": [
            {
                "id": "other.behavior",
                "intent": "bad target",
                "contract": "missing.py",
                "coverage": "missing",
                "assertion_owners": [],
            }
        ],
        "cases": [],
    }
    (root / "cases" / "other.json").write_text(json.dumps(other), encoding="utf-8")

    report = module.audit_catalog(root, "demo")

    assert [row["group"] for row in report["groups"]] == ["demo"]
    assert all(cell["group"] == "demo" for cell in report["profiles"])
    assert all(row["group"] == "demo" for row in report["case_ledger"])
    assert report["case_actions"]["total"] == len(report["case_ledger"])
    assert report["validation_scope"] == "all_catalogs"
    assert any(item["group"] == "other" and item["code"] == "bad_contract" for item in report["errors"])


@pytest.mark.parametrize("dependencies, expected_code", [
    (["unknown.case"], "orphan_dependency"),
    ([None], "bad_dependency"),
    ("demo.case", "bad_dependency"),
])
def test_scoped_audit_rejects_dependencies_in_another_group(tmp_path, dependencies, expected_code):
    module = _load_catalog_module()
    root = _fixture_root(tmp_path)
    other = json.loads((root / "cases/demo.json").read_text())
    other["group"] = "other"
    other["behaviors"][0]["id"] = "other.behavior"
    other_case = other["cases"][0]
    other_case.update(id="other.case", behavior="other.behavior", flows=[], scenarios=[], dedicated_scenarios=[], dependencies=dependencies)
    (root / "cases/other.json").write_text(json.dumps(other), encoding="utf-8")
    for group in (None, "demo"):
        report = module.audit_catalog(root, group)
        assert not report["ok"]
        assert any(item["group"] == "other" and item["code"] == expected_code for item in report["errors"])
        if group:
            assert [row["group"] for row in report["groups"]] == ["demo"]
            assert all(row["group"] == "demo" for row in report["profiles"] + report["checks"] + report["case_ledger"])
