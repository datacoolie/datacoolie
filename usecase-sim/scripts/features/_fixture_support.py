"""Compile one existing authored primary flow into a case-owned runtime document."""

from __future__ import annotations
import copy
from _stateful_support import USECASE_SIM_DIR


def compile_existing(case, group: str, flow_name: str):
    """Project an authored flow while keeping its canonical identity.

    Existing-table qualification needs only one authored flow, but it still
    reads the flat group file through the normal shared-section contract.  The
    source path remains attached to the case so ``BoundFileProvider`` performs
    full model validation against the canonical source; case-specific paths
    are explicit immutable bindings supplied by ``case.document``.
    """

    from runner.metadata_sources import group_document

    source = group_document(group)
    flow = copy.deepcopy(next(row for row in source["dataflows"] if row["name"] == flow_name))
    connection_names = {
        flow[role]["connection_name"] for role in ("source", "destination")
    }
    connections = [
        copy.deepcopy(row)
        for row in source["connections"]
        if row["name"] in connection_names
    ]
    for role, root in [("source", case.input), ("destination", case.output)]:
        connection = next(
            row for row in connections if row["name"] == flow[role]["connection_name"]
        )
        connection.setdefault("configure", {})["base_path"] = root.as_posix()
    selected_hints = [
        copy.deepcopy(row)
        for row in source.get("schema_hints", [])
        if row.get("connection_name") in connection_names
        or row.get("connection_id") in {
            connection.get("connection_id")
            for connection in connections
            if connection.get("connection_id")
        }
    ]
    compiled = {
        "connections": connections,
        "dataflows": [flow],
        "schema_hints": selected_hints,
    }
    case.document = copy.deepcopy(compiled)
    case.sources = (
        str((USECASE_SIM_DIR / "metadata" / "file" / f"{group}.json").resolve()),
    )
    identity = {
        "fixture": f"usecase-sim/metadata/file/{group}.json",
        "authored_flow": flow_name,
        "runtime_flow": flow_name,
        "runtime_stage": flow.get("stage"),
    }
    return compiled, identity
