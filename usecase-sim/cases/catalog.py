"""Read-only case catalog discovery, validation, and group selection.

The simulator deliberately keeps the catalog as JSON.  This module contains
the small amount of policy needed by both the audit command and the scenario
dispatcher without importing a runner, starting a service, or creating a
runtime artifact.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


_MODULE_DIR = str(Path(__file__).resolve().parents[1])
if _MODULE_DIR not in sys.path:
    sys.path.insert(0, _MODULE_DIR)


VALID_COVERAGE = {
    "missing",
    "fixture_only",
    "asserted",
    "deferred",
    "unsupported",
}
VALID_FLOW_ROLES = {"primary", "support"}
VALID_RECEIPT_STATUSES = {"passed", "failed", "blocked", "not_run"}
_EXCLUDED_PARTS = {".runtime", "cases", "scenarios"}


class CatalogError(ValueError):
    """A catalog cannot be loaded or a group cannot be selected."""


class Diagnostic:
    """One stable, JSON-serializable audit finding."""

    def __init__(
        self,
        code: str,
        message: str,
        path: str | None = None,
        group: str | None = None,
        severity: str = "error",
    ) -> None:
        self.code = code
        self.message = message
        self.path = path
        self.group = group
        self.severity = severity

    def as_dict(self) -> dict[str, str]:
        result = {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }
        if self.path is not None:
            result["path"] = self.path
        if self.group is not None:
            result["group"] = self.group
        return result


def simulator_root(root: str | Path | None = None) -> Path:
    """Resolve a repository root or simulator root to the simulator directory."""

    if root is None:
        return Path(__file__).resolve().parents[1]
    candidate = Path(root).expanduser().resolve()
    if candidate.name == "cases":
        candidate = candidate.parent
    # Accept either the standalone simulator path or the repository root.  A
    # fixture root used by unit tests normally already has cases/scenarios.
    child = candidate / "usecase-sim"
    if child.is_dir() and not (candidate / "scenarios").exists():
        candidate = child
    else:
        component_child = candidate / "datacoolie" / "usecase-sim"
        if component_child.is_dir() and not (candidate / "scenarios").exists():
            candidate = component_child
    return candidate


def _product_root(sim_root: Path) -> Path:
    return sim_root.parent


def _slash(value: str | Path) -> str:
    text = str(value).replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return text


def _relative(sim_root: Path, path: Path) -> str:
    try:
        return _slash(path.relative_to(sim_root))
    except ValueError:
        return _slash(path)


def _catalog_path(sim_root: Path, path: str | Path) -> Path:
    """Resolve a product-relative path used by catalog references."""

    text = _slash(path)
    candidates = []
    product = _product_root(sim_root)
    if text.startswith("usecase-sim/"):
        candidates.append(product / text)
        text = text[len("usecase-sim/") :]
    candidates.extend((sim_root / text, product / text))
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    # Return the canonical simulator-relative candidate for useful diagnostics.
    return (sim_root / text).resolve()


def _read_json(path: Path) -> Any:
    """Read JSON while rejecting duplicate object keys."""

    class DuplicateKey(ValueError):
        pass

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise DuplicateKey(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=pairs)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise CatalogError(f"could not read JSON {path}: {exc}") from exc


def load_scenario_registry(root: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Load the declarative scenario registry without importing the runner."""

    path = simulator_root(root) / "scenarios" / "scenarios.json"
    value = _read_json(path)
    if not isinstance(value, dict):
        raise CatalogError(f"scenario registry must be an object: {_relative(simulator_root(root), path)}")
    if any(not isinstance(item, dict) for item in value.values()):
        raise CatalogError("scenario registry values must be objects")
    return value


def catalog_paths(root: str | Path | None = None) -> list[Path]:
    """Return group catalog JSON files in stable path order."""

    directory = simulator_root(root) / "cases"
    if not directory.is_dir():
        return []
    return sorted(
        path
        for path in directory.glob("*.json")
        if path.name != "inventory.json"
    )


def load_inventory(root: str | Path | None = None) -> dict[str, Any]:
    """Load ``cases/inventory.json`` or a useful empty inventory for fixtures."""

    sim_root = simulator_root(root)
    path = sim_root / "cases" / "inventory.json"
    if not path.exists():
        return {"schema_version": 1, "exclusions": [], "scripts": [], "generated": []}
    value = _read_json(path)
    if not isinstance(value, dict):
        raise CatalogError("inventory.json must contain an object")
    return value


def load_catalogs(root: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Load all group catalogs keyed by their declared group name."""

    catalogs: dict[str, dict[str, Any]] = {}
    for path in catalog_paths(root):
        value = _read_json(path)
        if not isinstance(value, dict):
            raise CatalogError(f"catalog must contain an object: {path}")
        group = value.get("group")
        if not isinstance(group, str) or not group:
            raise CatalogError(f"catalog group is required: {path}")
        if group in catalogs:
            raise CatalogError(f"duplicate catalog group: {group}")
        catalogs[group] = value
    return catalogs


def load_catalog(root: str | Path | None = None, group: str | None = None) -> dict[str, Any]:
    """Load one catalog, or the aggregate catalog map when ``group`` is absent."""

    catalogs = load_catalogs(root)
    if group is None:
        return catalogs
    try:
        return catalogs[group]
    except KeyError as exc:
        raise CatalogError(f"unknown group: {group}") from exc


from cases.discovery import (  # noqa: E402
    discover_authored_json,
    discover_flows,
    discover_scripts,
)
from cases.checklist import (  # noqa: E402
    CHECK_FIELDS,
    VALID_NEXT_ACTIONS,
    aggregate_behavior_coverage,
    case_action_summary,
    case_ledger_rows,
    checklist_rows,
    coverage_summary,
    functional_check_coverage,
    gap_ledger,
    measurement_check_coverage,
)


def _owner_path(sim_root: Path, value: str) -> tuple[Path, str | None]:
    path_text, separator, node = value.partition("::")
    path = _catalog_path(sim_root, path_text)
    return path, node if separator else None


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _node_name(value: str) -> str:
    """Strip a pytest parametrization suffix from one AST declaration name."""

    return value.split("[", 1)[0].strip()


def _ast_declaration_chain_exists(path: Path, node: str) -> tuple[bool, str]:
    names = [_node_name(part) for part in node.split("::")]
    if not names or any(not name for name in names):
        return False, "assertion owner node must contain non-empty declarations"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return False, f"cannot parse assertion owner {path}: {exc.msg}"
    except (OSError, UnicodeError) as exc:
        return False, f"cannot inspect assertion owner {path}: {exc}"

    scope: list[ast.stmt] = list(tree.body)
    for index, name in enumerate(names):
        declaration = next(
            (
                item
                for item in scope
                if isinstance(item, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name == name
            ),
            None,
        )
        if declaration is None:
            return False, f"assertion owner node does not exist: {node}"
        if index < len(names) - 1:
            scope = list(declaration.body)
    return True, ""


def _owner_exists(sim_root: Path, value: object) -> tuple[bool, str]:
    if not isinstance(value, str) or not value.strip():
        return False, "assertion owner must be a non-empty path"
    if "::" in value and not value.partition("::")[2].strip():
        return False, "assertion owner symbol must be non-empty when declared"
    path, node = _owner_path(sim_root, value)
    if "::" in value and (node is None or not node.strip()):
        return False, f"assertion owner node must be non-empty: {value}"
    if not _path_within(path, _product_root(sim_root)):
        return False, f"assertion owner path is outside the product: {value}"
    if not path.is_file():
        return False, f"assertion owner path does not exist: {value}"
    if node:
        ok, message = _ast_declaration_chain_exists(path, node)
        if not ok:
            return False, message
    return True, ""


def _contract_exists(sim_root: Path, value: object) -> tuple[bool, str]:
    """Validate a contract path; a directory is a valid module contract."""

    if not isinstance(value, str) or not value.strip():
        return False, "contract must be a non-empty path"
    if "::" in value and not value.partition("::")[2].strip():
        return False, "contract symbol must be non-empty when declared"
    path, node = _owner_path(sim_root, value)
    if "::" in value and (node is None or not node.strip()):
        return False, f"contract node must be non-empty: {value}"
    if not _path_within(path, _product_root(sim_root)):
        return False, f"contract path is outside the product: {value}"
    if not path.exists():
        return False, f"contract path does not exist: {value}"
    if path.is_dir():
        if node:
            return False, f"contract symbol requires a source file: {value}"
        return True, ""
    if not path.is_file():
        return False, f"contract path is not a file or directory: {value}"
    if node:
        ok, message = _ast_declaration_chain_exists(path, node)
        if not ok:
            return False, message.replace("assertion owner node", "contract node")
    return True, ""


def _scenario_path(sim_root: Path, value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    text = _slash(value)
    if text.startswith("usecase-sim/"):
        return text
    return _slash(text)


def _script_path(sim_root: Path, value: object) -> Path | None:
    if not isinstance(value, str) or not value:
        return None
    return _catalog_path(sim_root, value)


def _profile(value: object) -> tuple[str, str] | None:
    if not isinstance(value, dict):
        return None
    engine = value.get("engine")
    platform = value.get("platform")
    if not isinstance(engine, str) or not engine or not isinstance(platform, str) or not platform:
        return None
    return engine, platform


def _scenario_profiles(scenarios: Mapping[str, Mapping[str, Any]], names: Iterable[str]) -> set[tuple[str, str]]:
    profiles: set[tuple[str, str]] = set()
    for name in names:
        scenario = scenarios.get(name)
        if scenario:
            engine = scenario.get("engine")
            platform = scenario.get("platform", "local")
            if isinstance(engine, str) and isinstance(platform, str):
                profiles.add((engine, platform))
    return profiles


def _normal_flow(flow: object) -> tuple[str, str, str] | None:
    if not isinstance(flow, dict):
        return None
    fixture = flow.get("fixture")
    name = flow.get("flow")
    role = flow.get("role")
    if not all(isinstance(item, str) and item for item in (fixture, name, role)):
        return None
    return _slash(fixture), name, role


def _normal_profile(profile: object) -> tuple[str, str] | None:
    return _profile(profile)


def _normal_scenario_names(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _inventory_exclusions(inventory: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    raw = inventory.get("exclusions", [])
    if not isinstance(raw, list):
        return result
    for item in raw:
        if isinstance(item, dict) and isinstance(item.get("path"), str):
            path = _slash(item["path"])
            result[path] = str(item.get("reason", ""))
            result[path.removeprefix("usecase-sim/")] = str(item.get("reason", ""))
    return result


def _diagnostic(
    errors: list[Diagnostic], code: str, message: str, *, path: str | None = None, group: str | None = None
) -> None:
    errors.append(Diagnostic(code, message, path=path, group=group))


def _generated_flow_provable(source: str, flow: str) -> bool:
    """Require a route name or its complete token family in setup source."""

    if not source:
        return False
    try:
        ast.parse(source)
    except SyntaxError:
        return False
    if flow in source or flow.replace("-", "_") in source:
        return True
    tokens = [token for token in re.split(r"[_-]", flow) if len(token) >= 3]
    if not tokens:
        return False
    return all(
        re.search(rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])", source)
        for token in tokens
    )


def _validate_inventory(
    sim_root: Path,
    inventory: Mapping[str, Any],
    scenarios: Mapping[str, Mapping[str, Any]],
    errors: list[Diagnostic],
) -> None:
    if inventory.get("schema_version") != 1:
        _diagnostic(errors, "inventory_schema", "inventory schema_version must be 1", path="cases/inventory.json")
    if "coverage_schema_version" in inventory and inventory.get("coverage_schema_version") != 2:
        _diagnostic(errors, "coverage_schema", "inventory coverage_schema_version must be 2 when declared", path="cases/inventory.json")
    for key in ("exclusions", "scripts", "generated"):
        if not isinstance(inventory.get(key, []), list):
            _diagnostic(errors, "inventory_field", f"inventory {key} must be an array", path="cases/inventory.json")

    exclusions = inventory.get("exclusions", [])
    for item in exclusions if isinstance(exclusions, list) else []:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str) or not str(item.get("reason", "")).strip():
            _diagnostic(errors, "bad_exclusion", "inventory exclusions require path and reason", path="cases/inventory.json")

    script_entries = inventory.get("scripts", [])
    inventory_paths: set[str] = set()
    for item in script_entries if isinstance(script_entries, list) else []:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            _diagnostic(errors, "bad_script_entry", "script inventory entries require path, group, role, and reason", path="cases/inventory.json")
            continue
        path = _slash(item["path"])
        inventory_paths.add(path.removeprefix("usecase-sim/"))
        if not item.get("group") or not item.get("role") or not str(item.get("reason", "")).strip():
            _diagnostic(errors, "bad_script_entry", f"script inventory entry is incomplete: {path}", path="cases/inventory.json")
        script_path = _script_path(sim_root, path)
        if script_path is None or not script_path.is_file():
            _diagnostic(errors, "orphan_script_ref", f"script inventory path does not exist: {path}", path="cases/inventory.json")
        parts = path.removeprefix("usecase-sim/").split("/")
        if parts[:2] == ["scripts", "features"] and len(parts) > 3 and parts[2] != item.get("group"):
            _diagnostic(errors, "script_group_mismatch", f"feature script folder {parts[2]} differs from owner {item.get('group')}: {path}", path=path)

    excluded = _inventory_exclusions(inventory)
    for path in discover_scripts(sim_root):
        if path not in inventory_paths and path not in excluded:
            _diagnostic(errors, "unowned_script", f"authored script has no inventory owner: {path}", path=path)

    generated = inventory.get("generated", [])
    seen_generated: set[str] = set()
    for item in generated if isinstance(generated, list) else []:
        if not isinstance(item, dict):
            _diagnostic(errors, "bad_generated_entry", "generated entries must be objects", path="cases/inventory.json")
            continue
        scenario_name = item.get("scenario")
        setup_script = item.get("setup_script")
        expected_flows = item.get("expected_flows")
        profiles = item.get("profiles")
        if not isinstance(scenario_name, str) or not scenario_name:
            _diagnostic(errors, "bad_generated_entry", "generated scenario is required", path="cases/inventory.json")
            continue
        if scenario_name in seen_generated:
            _diagnostic(errors, "duplicate_generated", f"generated scenario listed more than once: {scenario_name}", path="cases/inventory.json")
        seen_generated.add(scenario_name)
        scenario = scenarios.get(scenario_name)
        if scenario is None:
            _diagnostic(errors, "orphan_generated_scenario", f"generated route references unknown scenario: {scenario_name}", path="cases/inventory.json")
        if not isinstance(setup_script, str) or not setup_script:
            _diagnostic(errors, "bad_generated_entry", f"generated route requires setup_script: {scenario_name}", path="cases/inventory.json")
        else:
            setup_path = _script_path(sim_root, setup_script)
            if setup_path is None or not setup_path.is_file():
                _diagnostic(errors, "orphan_generated_script", f"generated setup script does not exist: {setup_script}", path="cases/inventory.json")
            if scenario and isinstance(scenario.get("setup"), dict):
                declared = _slash(scenario["setup"].get("script", ""))
                expected = _slash(setup_script)
                if declared and declared != expected and declared.removeprefix("usecase-sim/") != expected.removeprefix("usecase-sim/"):
                    _diagnostic(errors, "generated_route_mismatch", f"generated setup script does not match scenario {scenario_name}: {setup_script}", path="cases/inventory.json")
        if not isinstance(expected_flows, list) or not expected_flows or any(not isinstance(flow, str) or not flow for flow in expected_flows):
            _diagnostic(errors, "bad_generated_entry", f"generated route requires expected_flows: {scenario_name}", path="cases/inventory.json")
        elif len(set(expected_flows)) != len(expected_flows):
            _diagnostic(errors, "duplicate_generated_flow", f"generated route repeats an expected flow: {scenario_name}", path="cases/inventory.json")
        if not isinstance(profiles, list) or not profiles or any(_normal_profile(profile) is None for profile in profiles):
            _diagnostic(errors, "bad_generated_entry", f"generated route requires valid profiles: {scenario_name}", path="cases/inventory.json")
        elif scenario:
            configured = _scenario_profiles({scenario_name: scenario}, [scenario_name])
            declared_profiles = {_normal_profile(profile) for profile in profiles}
            if not configured.issubset(declared_profiles):
                _diagnostic(errors, "generated_profile_mismatch", f"generated route profiles do not cover scenario {scenario_name}", path="cases/inventory.json")


def _validate_group_shape(
    path: Path,
    catalog: Mapping[str, Any],
    group: str,
    all_cases: dict[str, tuple[str, Mapping[str, Any]]],
    all_behaviors: dict[str, tuple[str, Mapping[str, Any]]],
    all_checks: dict[str, tuple[str, Mapping[str, Any]]],
    errors: list[Diagnostic],
    *,
    checklist_enabled: bool = False,
) -> None:
    required_root = ("group", "behaviors", "cases")
    for key in required_root:
        if key not in catalog:
            _diagnostic(errors, "missing_catalog_field", f"catalog requires {key}", path=_relative(path.parent.parent, path), group=group)
    if catalog.get("group") != group:
        _diagnostic(errors, "catalog_group_mismatch", f"declared group does not match {group}", path=_slash(path.name), group=group)
    for key in ("behaviors", "cases"):
        if not isinstance(catalog.get(key), list):
            _diagnostic(errors, "catalog_field_type", f"catalog {key} must be an array", path=_slash(path.name), group=group)

    behaviors = catalog.get("behaviors", [])
    raw_cases = catalog.get("cases", [])
    case_entries = raw_cases if isinstance(raw_cases, list) else []
    local_case_ids = {
        item.get("id")
        for item in case_entries
        if isinstance(item, dict) and isinstance(item.get("id"), str) and item.get("id")
    }
    local_case_behaviors = {
        item.get("id"): item.get("behavior")
        for item in case_entries
        if isinstance(item, dict) and isinstance(item.get("id"), str) and item.get("id")
    }
    for behavior in behaviors if isinstance(behaviors, list) else []:
        if not isinstance(behavior, dict):
            _diagnostic(errors, "bad_behavior", "behavior entries must be objects", path=_slash(path.name), group=group)
            continue
        behavior_id = behavior.get("id")
        if not isinstance(behavior_id, str) or not behavior_id:
            _diagnostic(errors, "bad_behavior", "behavior requires id", path=_slash(path.name), group=group)
            continue
        if behavior_id in all_behaviors:
            _diagnostic(errors, "duplicate_behavior", f"behavior ID appears more than once: {behavior_id}", path=_slash(path.name), group=group)
        all_behaviors[behavior_id] = (group, behavior)
        for key in ("intent", "contract", "coverage", "assertion_owners"):
            if key not in behavior:
                _diagnostic(errors, "missing_behavior_field", f"behavior {behavior_id} requires {key}", path=_slash(path.name), group=group)
        if "contract" in behavior:
            ok, message = _contract_exists(path.parent.parent, behavior.get("contract"))
            if not ok:
                _diagnostic(errors, "bad_contract", f"behavior {behavior_id}: {message}", path=_slash(path.name), group=group)
        coverage = behavior.get("coverage")
        if coverage not in VALID_COVERAGE:
            _diagnostic(errors, "bad_coverage", f"behavior {behavior_id} has invalid coverage: {coverage}", path=_slash(path.name), group=group)
        owners = behavior.get("assertion_owners", [])
        if not isinstance(owners, list):
            _diagnostic(errors, "bad_assertion_owners", f"behavior {behavior_id} assertion_owners must be an array", path=_slash(path.name), group=group)
        else:
            for owner in owners:
                ok, message = _owner_exists(path.parent.parent, owner)
                if not ok:
                    _diagnostic(errors, "bad_assertion_owner", message, path=_slash(path.name), group=group)
        if coverage == "asserted" and not owners:
            _diagnostic(errors, "asserted_without_owner", f"behavior {behavior_id} is asserted without assertion_owners", path=_slash(path.name), group=group)
        if coverage in {"deferred", "unsupported"} and not str(behavior.get("gap", "")).strip():
            _diagnostic(errors, "missing_gap", f"behavior {behavior_id} requires gap for {coverage}", path=_slash(path.name), group=group)

        if checklist_enabled:
            checks = behavior.get("checks")
            if not isinstance(checks, list) or not checks:
                _diagnostic(
                    errors,
                    "missing_checks",
                    f"behavior {behavior_id} requires a non-empty checks array",
                    path=_slash(path.name),
                    group=group,
                )
                checks = []
            local_check_ids: set[str] = set()
            for check in checks:
                if not isinstance(check, dict):
                    _diagnostic(errors, "bad_check", f"behavior {behavior_id} checks must be objects", path=_slash(path.name), group=group)
                    continue
                check_id = check.get("id")
                if not isinstance(check_id, str) or not check_id:
                    _diagnostic(errors, "bad_check", f"behavior {behavior_id} check requires non-empty id", path=_slash(path.name), group=group)
                    continue
                if not check_id.startswith(f"{behavior_id}."):
                    _diagnostic(errors, "check_id_scope", f"check {check_id} must be scoped under behavior {behavior_id}", path=_slash(path.name), group=group)
                if check_id in local_check_ids or check_id in all_checks:
                    _diagnostic(errors, "duplicate_check", f"check ID appears more than once: {check_id}", path=_slash(path.name), group=group)
                local_check_ids.add(check_id)
                all_checks[check_id] = (group, check)
                for key in CHECK_FIELDS:
                    if key not in check:
                        _diagnostic(errors, "missing_check_field", f"check {check_id} requires {key}", path=_slash(path.name), group=group)
                acceptance = check.get("acceptance")
                if not isinstance(acceptance, str) or not acceptance.strip():
                    _diagnostic(errors, "bad_check", f"check {check_id} acceptance must be non-empty", path=_slash(path.name), group=group)
                preconditions = check.get("preconditions")
                if (
                    not isinstance(preconditions, list)
                    or not preconditions
                    or any(not isinstance(item, str) or not item.strip() for item in preconditions)
                ):
                    _diagnostic(errors, "bad_check_preconditions", f"check {check_id} preconditions must be a non-empty string array", path=_slash(path.name), group=group)
                case_ids = check.get("case_ids")
                if not isinstance(case_ids, list) or any(not isinstance(item, str) or not item.strip() for item in case_ids):
                    _diagnostic(errors, "bad_check_case_ids", f"check {check_id} case_ids must be a string array", path=_slash(path.name), group=group)
                else:
                    for case_id in case_ids:
                        if case_id not in local_case_ids:
                            _diagnostic(errors, "orphan_check_case", f"check {check_id} references unknown same-group case: {case_id}", path=_slash(path.name), group=group)
                        elif local_case_behaviors.get(case_id) != behavior_id:
                            _diagnostic(errors, "check_case_family_mismatch", f"check {check_id} references case outside behavior family: {case_id}", path=_slash(path.name), group=group)
                owners = check.get("assertion_owners")
                if not isinstance(owners, list):
                    _diagnostic(errors, "bad_check_owners", f"check {check_id} assertion_owners must be an array", path=_slash(path.name), group=group)
                    owners = []
                for owner in owners:
                    ok, message = _owner_exists(path.parent.parent, owner)
                    if not ok:
                        _diagnostic(errors, "bad_assertion_owner", f"check {check_id}: {message}", path=_slash(path.name), group=group)
                check_coverage = check.get("coverage")
                if check_coverage not in VALID_COVERAGE:
                    _diagnostic(errors, "bad_check_coverage", f"check {check_id} has invalid coverage: {check_coverage}", path=_slash(path.name), group=group)
                gap = check.get("gap")
                if gap is not None and not isinstance(gap, str):
                    _diagnostic(errors, "bad_check_gap", f"check {check_id} gap must be a string", path=_slash(path.name), group=group)
                next_action = check.get("next_action")
                if next_action not in VALID_NEXT_ACTIONS:
                    _diagnostic(errors, "bad_next_action", f"check {check_id} has invalid next_action: {next_action}", path=_slash(path.name), group=group)
                if check_coverage == "asserted" and not owners:
                    _diagnostic(errors, "asserted_check_without_owner", f"check {check_id} is asserted without assertion_owners", path=_slash(path.name), group=group)
                if check_coverage != "asserted":
                    if not isinstance(gap, str) or not gap.strip():
                        _diagnostic(errors, "missing_check_gap", f"check {check_id} requires gap when coverage is {check_coverage}", path=_slash(path.name), group=group)
                    if next_action == "none":
                        _diagnostic(errors, "missing_check_action", f"check {check_id} requires an actionable next_action", path=_slash(path.name), group=group)
            valid_coverages = [
                check.get("coverage")
                for check in checks
                if isinstance(check, dict) and check.get("coverage") in VALID_COVERAGE
            ]
            if checks and len(valid_coverages) == len(checks):
                aggregate = aggregate_behavior_coverage(
                    behavior.get("coverage"),
                    [{"coverage": value} for value in valid_coverages],
                )
                if behavior.get("coverage") != aggregate:
                    _diagnostic(
                        errors,
                        "behavior_coverage_mismatch",
                        f"behavior {behavior_id} coverage {behavior.get('coverage')} does not match check aggregate {aggregate}",
                        path=_slash(path.name),
                        group=group,
                    )

    cases = catalog.get("cases", [])
    for case in cases if isinstance(cases, list) else []:
        if not isinstance(case, dict):
            _diagnostic(errors, "bad_case", "case entries must be objects", path=_slash(path.name), group=group)
            continue
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id:
            _diagnostic(errors, "bad_case", "case requires id", path=_slash(path.name), group=group)
            continue
        if case_id in all_cases:
            _diagnostic(errors, "duplicate_case", f"case ID appears more than once: {case_id}", path=_slash(path.name), group=group)
        all_cases[case_id] = (group, case)
        required = ("behavior", "intent", "contract", "flows", "scenarios", "dedicated_scenarios", "preconditions", "expected", "assertion_owners", "required_profiles", "coverage", "dependencies")
        if checklist_enabled:
            required = (*required, "next_action")
        for key in required:
            if key not in case:
                _diagnostic(errors, "missing_case_field", f"case {case_id} requires {key}", path=_slash(path.name), group=group)
        if "contract" in case:
            ok, message = _contract_exists(path.parent.parent, case.get("contract"))
            if not ok:
                _diagnostic(errors, "bad_contract", f"case {case_id}: {message}", path=_slash(path.name), group=group)
        behavior_id = case.get("behavior")
        if isinstance(behavior_id, str) and behavior_id not in all_behaviors:
            _diagnostic(errors, "orphan_behavior_ref", f"case {case_id} references unknown behavior: {behavior_id}", path=_slash(path.name), group=group)
        coverage = case.get("coverage")
        if coverage not in VALID_COVERAGE:
            _diagnostic(errors, "bad_coverage", f"case {case_id} has invalid coverage: {coverage}", path=_slash(path.name), group=group)
        owners = case.get("assertion_owners", [])
        if not isinstance(owners, list):
            _diagnostic(errors, "bad_assertion_owners", f"case {case_id} assertion_owners must be an array", path=_slash(path.name), group=group)
            owners = []
        for owner in owners:
            ok, message = _owner_exists(path.parent.parent, owner)
            if not ok:
                _diagnostic(errors, "bad_assertion_owner", message, path=_slash(path.name), group=group)
        if coverage == "asserted" and not owners:
            _diagnostic(errors, "asserted_without_owner", f"case {case_id} is asserted without assertion_owners", path=_slash(path.name), group=group)
        if coverage in {"deferred", "unsupported"} and not str(case.get("gap", "")).strip():
            _diagnostic(errors, "missing_gap", f"case {case_id} requires gap for {coverage}", path=_slash(path.name), group=group)
        if checklist_enabled:
            expected = case.get("expected")
            if not isinstance(expected, str) or not expected.strip():
                _diagnostic(errors, "bad_case_expected", f"case {case_id} expected must be a non-empty string", path=_slash(path.name), group=group)
            preconditions = case.get("preconditions")
            if (
                not isinstance(preconditions, list)
                or not preconditions
                or any(not isinstance(item, str) or not item.strip() for item in preconditions)
            ):
                _diagnostic(errors, "bad_case_preconditions", f"case {case_id} preconditions must be a non-empty string array", path=_slash(path.name), group=group)
            next_action = case.get("next_action")
            if next_action not in VALID_NEXT_ACTIONS:
                _diagnostic(errors, "bad_next_action", f"case {case_id} has invalid next_action: {next_action}", path=_slash(path.name), group=group)
            if coverage != "asserted":
                if not isinstance(case.get("gap"), str) or not case.get("gap", "").strip():
                    _diagnostic(errors, "missing_case_gap", f"case {case_id} requires gap when coverage is {coverage}", path=_slash(path.name), group=group)
                if next_action == "none":
                    _diagnostic(errors, "missing_case_action", f"case {case_id} requires an actionable next_action", path=_slash(path.name), group=group)
        for flow in case.get("flows", []) if isinstance(case.get("flows"), list) else []:
            normal = _normal_flow(flow)
            if normal is None:
                _diagnostic(errors, "bad_flow_ref", f"case {case_id} has an invalid flow reference", path=_slash(path.name), group=group)
            elif normal[2] not in VALID_FLOW_ROLES:
                _diagnostic(errors, "bad_flow_role", f"case {case_id} has invalid flow role: {normal[2]}", path=_slash(path.name), group=group)
        for profile in case.get("required_profiles", []) if isinstance(case.get("required_profiles"), list) else []:
            if _normal_profile(profile) is None:
                _diagnostic(errors, "bad_profile", f"case {case_id} has invalid required profile", path=_slash(path.name), group=group)


from cases.receipts import (  # noqa: E402
    classify_receipt as classify_receipt,
    execution_statuses as execution_statuses,
    runtime_identity as runtime_identity,
    worktree_fingerprint as worktree_fingerprint,
)


def _validate_catalog_references(
    sim_root: Path,
    catalogs: Mapping[str, Mapping[str, Any]],
    scenarios: Mapping[str, Mapping[str, Any]],
    inventory: Mapping[str, Any],
    errors: list[Diagnostic],
    *,
    group: str | None,
) -> None:
    if group is not None and group not in catalogs:
        _diagnostic(errors, "unknown_group", f"unknown group: {group}", group=group)
        return

    all_flow_refs: list[tuple[str, str, str, str]] = []
    scenario_refs: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for group_name, catalog in catalogs.items():
        for case in catalog.get("cases", []) if isinstance(catalog.get("cases"), list) else []:
            case_id = str(case.get("id", "<missing>")) if isinstance(case, dict) else "<invalid>"
            if not isinstance(case, dict):
                continue
            # Scenario ownership is global: a group-filtered audit must not
            # report every scenario owned by another group as orphaned.
            names = _normal_scenario_names(case.get("scenarios"))
            dedicated = _normal_scenario_names(case.get("dedicated_scenarios"))
            for name in names:
                scenario_refs[name].append((group_name, case_id))
                if name not in scenarios:
                    _diagnostic(errors, "orphan_scenario_ref", f"case {case_id} references unknown scenario: {name}", group=group_name)
            for name in dedicated:
                if name not in names:
                    _diagnostic(errors, "dedicated_not_declared", f"dedicated scenario is absent from scenarios: {name}", group=group_name)
            for flow in case.get("flows", []) if isinstance(case.get("flows"), list) else []:
                normal = _normal_flow(flow)
                if normal is None:
                    continue
                all_flow_refs.append((normal[0], normal[1], normal[2], case_id))
                fixture_path = _catalog_path(sim_root, normal[0])
                if not fixture_path.is_file():
                    _diagnostic(errors, "orphan_flow_ref", f"flow fixture does not exist: {normal[0]}", group=group_name)
                parts = _slash(normal[0]).removeprefix("usecase-sim/").split("/")
                if normal[2] == "primary" and parts[:2] == ["metadata", "file"] and len(parts) > 3 and parts[2] != group_name:
                    _diagnostic(errors, "fixture_group_mismatch", f"primary flow {normal[1]} belongs to {group_name}, but fixture folder is {parts[2]}", path=normal[0], group=group_name)
                if normal[2] == "primary" and len(parts) > 5 and parts[0] == "platforms" and parts[2:4] == ["metadata", "file"] and parts[4] != group_name:
                    _diagnostic(errors, "fixture_group_mismatch", f"cloud primary flow {normal[1]} belongs to {group_name}, but fixture folder is {parts[4]}", path=normal[0], group=group_name)
            # Required profiles are reported separately from configured
            # recipes below. A missing recipe is a blocked qualification cell,
            # not a malformed catalog, so it must not fail the structural
            # audit.
            dependencies = case.get("dependencies", [])
            if not isinstance(dependencies, list):
                _diagnostic(errors, "bad_dependency", f"case {case_id} dependencies must be an array", group=group_name)
            else:
                for dependency in dependencies:
                    if not isinstance(dependency, str) or not dependency:
                        _diagnostic(errors, "bad_dependency", f"case {case_id} has an invalid dependency", group=group_name)

    # A primary flow owner is unique; support refs may be repeated by design.
    primary: dict[tuple[str, str], list[str]] = defaultdict(list)
    for fixture, flow, role, case_id in all_flow_refs:
        if role == "primary":
            primary[(_slash(fixture).removeprefix("usecase-sim/"), flow)].append(case_id)
    for key, owners in primary.items():
        if len(owners) > 1:
            _diagnostic(errors, "duplicate_flow_owner", f"flow has multiple primary owners: {key[0]}::{key[1]} ({', '.join(owners)})")

    excluded = _inventory_exclusions(inventory)
    discovered: dict[tuple[str, str], dict[str, str]] = {}
    for relative in discover_authored_json(sim_root):
        path = sim_root / relative
        try:
            value = _read_json(path)
        except CatalogError as exc:
            if relative not in excluded:
                _diagnostic(errors, "malformed_authored_json", str(exc), path=relative)
            continue
        if not isinstance(value, dict) or not isinstance(value.get("dataflows"), list):
            if isinstance(value, dict) and "metadata_documents" in value:
                try:
                    from runner.metadata_composition import load_metadata
                    load_metadata(path, root=sim_root)
                except (ValueError, OSError) as exc:
                    _diagnostic(errors, "invalid_metadata_composition", str(exc), path=relative)
            if relative not in excluded:
                _diagnostic(errors, "unclassified_authored_json", f"authored JSON has no dataflows and no exclusion reason: {relative}", path=relative)
            continue
        if "shared_metadata" in value:
            try:
                from runner.metadata_composition import load_metadata
                load_metadata(path, root=sim_root)
            except (ValueError, OSError) as exc:
                _diagnostic(errors, "invalid_metadata_composition", str(exc), path=relative)
        seen_flow_names: set[str] = set()
        for flow in value["dataflows"]:
            if isinstance(flow, dict) and isinstance(flow.get("name"), str) and flow["name"]:
                flow_name = flow["name"]
                if flow_name in seen_flow_names:
                    _diagnostic(
                        errors,
                        "duplicate_dataflow_name",
                        f"authored JSON repeats dataflow name: {relative}::{flow_name}",
                        path=relative,
                    )
                    continue
                seen_flow_names.add(flow_name)
                discovered[(_slash(relative), flow_name)] = {"fixture": relative, "flow": flow_name}
    discovered_keys = {
        (fixture.removeprefix("usecase-sim/"), flow)
        for fixture, flow in discovered
    }
    for fixture, flow, role, case_id in all_flow_refs:
        key = (_slash(fixture).removeprefix("usecase-sim/"), flow)
        if key not in discovered_keys:
            _diagnostic(
                errors,
                "orphan_flow_ref",
                f"{role} flow reference does not exist in fixture: {fixture}::{flow}",
            )
    for key, item in discovered.items():
        normalized = (key[0].removeprefix("usecase-sim/"), key[1])
        if normalized not in primary:
            _diagnostic(errors, "unowned_flow", f"authored flow has no primary catalog owner: {item['fixture']}::{item['flow']}", path=item["fixture"])

    # Generated routes are allowed to materialize metadata below .runtime, so
    # the generated flow need not be an authored JSON flow.  Still prove the
    # route from its declared setup script: authored names must resolve to a
    # discovered flow, while generated names must be represented by the setup
    # source (including a deterministic f-string family such as
    # ``surface_{case}_{variant}``).
    generated_entries = inventory.get("generated", [])
    authored_names = {flow for _, flow in discovered}
    for item in generated_entries if isinstance(generated_entries, list) else []:
        if not isinstance(item, dict):
            continue
        scenario_name = item.get("scenario")
        setup_script = item.get("setup_script")
        expected_flows = item.get("expected_flows")
        if not isinstance(scenario_name, str) or not isinstance(setup_script, str) or not isinstance(expected_flows, list):
            continue
        if scenario_name not in scenario_refs:
            _diagnostic(errors, "generated_scenario_unowned", f"generated route scenario has no catalog owner: {scenario_name}", path="cases/inventory.json")
        script_path = _script_path(sim_root, setup_script)
        try:
            source = script_path.read_text(encoding="utf-8") if script_path and script_path.is_file() else ""
        except (OSError, UnicodeError):
            source = ""
        for flow in expected_flows:
            if not isinstance(flow, str) or not flow:
                continue
            if flow in authored_names:
                continue
            if not _generated_flow_provable(source, flow):
                _diagnostic(errors, "generated_flow_unprovable", f"generated route cannot prove expected flow {flow!r} from {setup_script}", path="cases/inventory.json")

    for name, refs in scenario_refs.items():
        if len(refs) > 1:
            _diagnostic(errors, "duplicate_scenario_owner", f"scenario has multiple catalog owners: {name}")
    for name in scenarios:
        if name not in scenario_refs:
            _diagnostic(errors, "unowned_scenario", f"scenario has no catalog owner: {name}", path="scenarios/scenarios.json")

    # Integrity is global; group filtering applies only to report presentation.
    all_cases: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for group_name, catalog in catalogs.items():
        for case in catalog.get("cases", []) if isinstance(catalog.get("cases"), list) else []:
            if isinstance(case, dict) and isinstance(case.get("id"), str):
                all_cases.setdefault(case["id"], (group_name, case))
    for case_id, (case_group, case) in all_cases.items():
        dependencies = case.get("dependencies", []) if isinstance(case.get("dependencies"), list) else []
        for dependency in dependencies:
            if isinstance(dependency, str) and dependency not in all_cases:
                _diagnostic(errors, "orphan_dependency", f"case {case_id} depends on unknown case: {dependency}", group=case_group)
    graph = {case_id: [dep for dep in (case.get("dependencies", []) if isinstance(case.get("dependencies"), list) else []) if isinstance(dep, str) and dep in all_cases] for case_id, (_, case) in all_cases.items()}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(case_id: str, trail: list[str]) -> None:
        if case_id in visiting:
            cycle = " -> ".join(trail + [case_id])
            _diagnostic(errors, "dependency_cycle", f"case dependency cycle: {cycle}")
            return
        if case_id in visited:
            return
        visiting.add(case_id)
        for dependency in graph.get(case_id, []):
            visit(dependency, trail + [case_id])
        visiting.remove(case_id)
        visited.add(case_id)

    for case_id in graph:
        visit(case_id, [])


def audit_catalog(root: str | Path | None = None, group: str | None = None) -> dict[str, Any]:
    """Audit catalog references and return a serializable report."""

    sim_root = simulator_root(root)
    errors: list[Diagnostic] = []
    catalogs: dict[str, dict[str, Any]] = {}
    inventory: dict[str, Any]
    scenarios: dict[str, dict[str, Any]]
    try:
        catalogs = load_catalogs(sim_root)
    except CatalogError as exc:
        _diagnostic(errors, "catalog_load", str(exc), path="cases")
    try:
        inventory = load_inventory(sim_root)
    except CatalogError as exc:
        inventory = {"schema_version": 1, "exclusions": [], "scripts": [], "generated": []}
        _diagnostic(errors, "inventory_load", str(exc), path="cases/inventory.json")
    try:
        scenarios = load_scenario_registry(sim_root)
    except CatalogError as exc:
        scenarios = {}
        _diagnostic(errors, "scenario_load", str(exc), path="scenarios/scenarios.json")

    all_cases: dict[str, tuple[str, Mapping[str, Any]]] = {}
    all_behaviors: dict[str, tuple[str, Mapping[str, Any]]] = {}
    all_checks: dict[str, tuple[str, Mapping[str, Any]]] = {}
    checklist_enabled = inventory.get("coverage_schema_version") == 2
    for path in catalog_paths(sim_root):
        try:
            catalog = _read_json(path)
        except CatalogError as exc:
            _diagnostic(errors, "catalog_load", str(exc), path=_relative(sim_root, path))
            continue
        if isinstance(catalog, dict) and isinstance(catalog.get("group"), str):
            _validate_group_shape(
                path,
                catalog,
                catalog["group"],
                all_cases,
                all_behaviors,
                all_checks,
                errors,
                checklist_enabled=checklist_enabled,
            )
    _validate_inventory(sim_root, inventory, scenarios, errors)
    if catalogs:
        _validate_catalog_references(sim_root, catalogs, scenarios, inventory, errors, group=group)
    elif group:
        _diagnostic(errors, "unknown_group", f"unknown group: {group}", group=group)

    receipts = execution_statuses(sim_root, scenarios) if scenarios else {}
    check_rows_by_group: dict[str, list[dict[str, Any]]] = {
        name: checklist_rows(catalog, name)
        for name, catalog in catalogs.items()
    }
    case_ledger_by_group: dict[str, list[dict[str, Any]]] = {
        name: case_ledger_rows(catalog, name)
        for name, catalog in catalogs.items()
    }
    group_rows: list[dict[str, Any]] = []
    for name in sorted(catalogs):
        if group is not None and name != group:
            continue
        catalog = catalogs[name]
        cases = catalog.get("cases", []) if isinstance(catalog.get("cases"), list) else []
        case_coverage = Counter(
            case.get("coverage")
            for case in cases
            if isinstance(case, dict) and isinstance(case.get("coverage"), str)
        )
        behavior_rows = catalog.get("behaviors", []) if isinstance(catalog.get("behaviors"), list) else []
        rows = check_rows_by_group[name]
        rows_by_behavior: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            if isinstance(row.get("behavior"), str):
                rows_by_behavior[row["behavior"]].append(row)
        behavior_coverage = Counter()
        for behavior in behavior_rows:
            if not isinstance(behavior, dict):
                continue
            behavior_id = behavior.get("id")
            if not isinstance(behavior_id, str):
                continue
            coverage = aggregate_behavior_coverage(
                behavior.get("coverage"), rows_by_behavior.get(behavior_id, [])
            )
            behavior_coverage[coverage] += 1
        dedicated = [
            scenario
            for case in cases
            if isinstance(case, dict)
            for scenario in _normal_scenario_names(case.get("dedicated_scenarios"))
        ]
        execution_counts = Counter(
            receipts.get(scenario, {}).get("status", "not_run")
            for scenario in dedicated
        )
        group_rows.append(
            {
                "group": name,
                "path": _relative(sim_root, sim_root / "cases" / f"{name}.json"),
                "behaviors": len(catalog.get("behaviors", [])) if isinstance(catalog.get("behaviors"), list) else 0,
                "cases": len(cases),
                "dedicated_scenarios": dedicated,
                "coverage": dict(sorted(case_coverage.items())),
                "behavior_coverage": dict(sorted(behavior_coverage.items())),
                "execution": dict(sorted(execution_counts.items())),
                "checks": rows,
                "check_coverage": coverage_summary(rows),
                "functional_check_coverage": functional_check_coverage(rows),
                "measurement_check_coverage": measurement_check_coverage(rows),
                "gap_ledger": gap_ledger(rows),
                "case_ledger": case_ledger_by_group[name],
                "case_actions": case_action_summary(case_ledger_by_group[name]),
            }
        )
    profile_cells: list[dict[str, Any]] = []
    for group_name, catalog in catalogs.items():
        if group is not None and group_name != group:
            continue
        for case in catalog.get("cases", []) if isinstance(catalog.get("cases"), list) else []:
            if not isinstance(case, dict) or not isinstance(case.get("id"), str):
                continue
            scenario_names = _normal_scenario_names(case.get("scenarios"))
            configured = sorted(_scenario_profiles(scenarios, scenario_names))
            required = sorted(
                profile
                for profile in (
                    _normal_profile(item)
                    for item in case.get("required_profiles", [])
                    if isinstance(case.get("required_profiles"), list)
                )
                if profile is not None
            )
            configured_set = set(configured)
            for engine, platform_name in required:
                matching = [
                    name
                    for name in scenario_names
                    if _scenario_profiles(scenarios, [name]) == {(engine, platform_name)}
                ]
                statuses = [receipts.get(name, {}).get("status", "not_run") for name in matching]
                if (engine, platform_name) not in configured_set:
                    status = "blocked"
                    reason = "required profile has no configured scenario recipe"
                elif any(value == "failed" for value in statuses):
                    status = "failed"
                    reason = "configured recipe has a failed current receipt"
                elif any(value == "blocked" for value in statuses):
                    status = "blocked"
                    reason = "configured recipe is blocked (runtime or setup qualification unavailable)"
                elif matching and all(value == "passed" for value in statuses):
                    status = "passed"
                    reason = "configured recipe has a current passing receipt"
                elif any(value == "historical" for value in statuses):
                    status = "historical"
                    reason = "only legacy or different-revision receipt is available"
                else:
                    status = "not_run"
                    reason = "configured recipe has no current passing receipt"
                profile_cells.append(
                    {
                        "group": group_name,
                        "case": case["id"],
                        "required": {"engine": engine, "platform": platform_name},
                        "configured_scenarios": matching,
                        "status": status,
                        "reason": reason,
                    }
                )
    scoped_check_rows = [
        row
        for name, rows in check_rows_by_group.items()
        if group is None or name == group
        for row in rows
    ]
    scoped_case_rows = [
        row
        for name, rows in case_ledger_by_group.items()
        if group is None or name == group
        for row in rows
    ]
    scoped_execution_names = {
        scenario
        for row in group_rows
        for scenario in row.get("dedicated_scenarios", [])
    }
    scoped_execution = (
        receipts
        if group is None
        else {
            name: value
            for name, value in receipts.items()
            if name in scoped_execution_names
        }
    )
    return {
        "ok": not errors,
        "root": str(sim_root),
        "group": group,
        "validation_scope": "all_catalogs",
        "groups": group_rows,
        "catalogs": catalogs if group is None else {group: catalogs[group]} if group in catalogs else {},
        "discovered": {
            "authored_json": discover_authored_json(sim_root),
            "flows": discover_flows(sim_root),
            "scenarios": sorted(scenarios),
            "scripts": discover_scripts(sim_root),
        },
        "execution": scoped_execution,
        "profiles": profile_cells,
        "checks": scoped_check_rows,
        "check_coverage": coverage_summary(scoped_check_rows),
        "functional_check_coverage": functional_check_coverage(scoped_check_rows),
        "measurement_check_coverage": measurement_check_coverage(scoped_check_rows),
        "gap_ledger": gap_ledger(scoped_check_rows),
        "case_ledger": scoped_case_rows,
        "case_actions": case_action_summary(scoped_case_rows),
        "errors": [item.as_dict() for item in errors],
    }


from cases.report import (  # noqa: E402
    compact_audit_report,
    list_groups,
)


# Friendly aliases used by the audit script and external unit tests.
from cases.selection import select_group_scenarios as _select_group_scenarios  # noqa: E402


def select_group_scenarios(
    root: str | Path | None,
    group: str,
    scenarios: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[str]:
    """Select a group's dedicated scenarios with this module's error type."""

    try:
        return _select_group_scenarios(root, group, scenarios)
    except ValueError as exc:
        # Importlib-based unit loaders may give this facade an alias module
        # name, while the helper imports ``cases.catalog`` by its executable
        # name.  Re-wrap the stable public exception across that boundary.
        raise CatalogError(str(exc)) from exc


select_scenarios_for_group = select_group_scenarios
audit = audit_catalog
discover_authored_flows = discover_flows


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Audit usecase-sim case catalogs")
    parser.add_argument("--root", default=None, help="repository or usecase-sim root")
    parser.add_argument("--group", help="audit one group")
    parser.add_argument("--list", action="store_true", help="list groups and cases")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument("--details", action="store_true", help="include full catalog payloads")
    args = parser.parse_args()
    report = audit_catalog(args.root, args.group)
    if args.list:
        payload = list_groups(args.root)
        if args.group:
            payload = [row for row in payload if row["group"] == args.group]
    else:
        payload = report if args.details else compact_audit_report(report)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    elif args.list:
        for row in payload:
            marker = "runnable" if row["runnable"] else "empty"
            print(f"{row['group']} ({marker})")
            for case_id in row["case_ids"]:
                print(f"  - {case_id}")
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if report["ok"] or (args.list and not args.group) else 1


if __name__ == "__main__":
    raise SystemExit(_cli())
