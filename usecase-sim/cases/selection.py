"""Pure group-to-scenario selection for the usecase-sim dispatcher."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def select_group_scenarios(
    root: str | Path | None,
    group: str,
    scenarios: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[str]:
    """Select dedicated scenarios in stable dependency/case order.

    Imports are lazy to keep the facade's JSON-only module load side-effect
    free and to avoid a circular import when the facade re-exports this
    function.
    """

    from cases.catalog import (
        CatalogError,
        _normal_scenario_names,
        load_catalogs,
        load_scenario_registry,
        simulator_root,
    )

    sim_root = simulator_root(root)
    catalogs = load_catalogs(sim_root)
    if group not in catalogs:
        raise CatalogError(f"unknown group: {group}")
    catalog = catalogs[group]
    cases = catalog.get("cases")
    if not isinstance(cases, list):
        raise CatalogError(f"group {group} cases must be an array")
    by_id: dict[str, Mapping[str, Any]] = {}
    order: list[str] = []
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            raise CatalogError(f"group {group} has an invalid case")
        case_id = case["id"]
        if case_id in by_id:
            raise CatalogError(f"duplicate case ID in group {group}: {case_id}")
        by_id[case_id] = case
        order.append(case_id)
    if not any(_normal_scenario_names(case.get("dedicated_scenarios")) for case in by_id.values()):
        raise CatalogError(f"group {group} has no runnable dedicated scenarios")

    # Resolve dependencies from all catalogs so a group cannot omit a declared
    # prerequisite.  Case order remains authored order where dependencies do
    # not constrain it.  Duplicate IDs across groups are ambiguous and must
    # fail before the runner has a chance to set up a service.
    all_cases: dict[str, tuple[str, Mapping[str, Any]]] = {}
    for catalog_group, other in catalogs.items():
        other_cases = other.get("cases")
        if not isinstance(other_cases, list):
            continue
        for case in other_cases:
            if not isinstance(case, dict) or not isinstance(case.get("id"), str):
                continue
            case_id = case["id"]
            previous = all_cases.get(case_id)
            if previous is not None and previous[0] != catalog_group:
                raise CatalogError(
                    f"duplicate case ID across groups: {case_id} "
                    f"({previous[0]}, {catalog_group})"
                )
            all_cases[case_id] = (catalog_group, case)

    selected: list[str] = []
    state: dict[str, int] = {}

    def visit(case_id: str, trail: list[str]) -> None:
        status = state.get(case_id, 0)
        if status == 1:
            raise CatalogError(f"case dependency cycle: {' -> '.join(trail + [case_id])}")
        if status == 2:
            return
        entry = all_cases.get(case_id)
        if entry is None:
            raise CatalogError(f"case {trail[-1] if trail else group} depends on unknown case: {case_id}")
        case_group, case = entry
        state[case_id] = 1
        dependencies = case.get("dependencies", [])
        if not isinstance(dependencies, list):
            raise CatalogError(f"case {case_id} dependencies must be an array")
        for dependency in dependencies:
            if not isinstance(dependency, str) or not dependency:
                raise CatalogError(f"case {case_id} has an invalid dependency")
            visit(dependency, trail + [case_id])
        state[case_id] = 2
        # A dependency is part of the requested execution sequence even when
        # it belongs to another group.  A declared dependency without a
        # runnable recipe is an actionable catalog error; silently dropping it
        # would make the selected group appear healthy while skipping its
        # prerequisite.
        dedicated = _normal_scenario_names(case.get("dedicated_scenarios"))
        if not dedicated:
            raise CatalogError(
                f"case {case_id} (group {case_group}) is a dependency without "
                "a dedicated scenario"
            )
        if case_id not in selected:
            selected.append(case_id)

    for case_id in order:
        case = by_id[case_id]
        if _normal_scenario_names(case.get("dedicated_scenarios")):
            visit(case_id, [])

    registry = scenarios if scenarios is not None else load_scenario_registry(sim_root)
    names: list[str] = []
    for case_id in selected:
        _case_group, case = all_cases[case_id]
        for name in _normal_scenario_names(case.get("dedicated_scenarios")):
            if name not in registry:
                raise CatalogError(f"case {case_id} references unknown scenario: {name}")
            if name in names:
                raise CatalogError(f"scenario has multiple dedicated owners: {name}")
            names.append(name)
    if not names:
        raise CatalogError(f"group {group} has no runnable dedicated scenarios")
    return names


__all__ = ["select_group_scenarios"]
