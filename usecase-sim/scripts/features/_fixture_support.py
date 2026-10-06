"""Compile one existing authored primary flow into a case-owned runtime document."""

from __future__ import annotations
import copy
import json
import uuid
from _stateful_support import USECASE_SIM_DIR


def compile_existing(case, group: str, flow_name: str):
    fixture = USECASE_SIM_DIR / "metadata/file" / group / "local.json"
    document = json.loads(fixture.read_text())
    shared = json.loads((USECASE_SIM_DIR / document["shared_metadata"]).read_text())
    flow = copy.deepcopy(
        next(row for row in document["dataflows"] if row["name"] == flow_name)
    )
    connection_names = {
        flow[role]["connection_name"] for role in ("source", "destination")
    }
    connections = [
        copy.deepcopy(row)
        for row in shared["connections"]
        if row["name"] in connection_names
    ]
    runtime_name = flow_name + "-" + uuid.uuid4().hex[:12]
    flow.update(
        name=runtime_name, stage=runtime_name, group_number=1, execution_order=1
    )
    for role, root in [("source", case.input), ("destination", case.output)]:
        connection = next(
            row for row in connections if row["name"] == flow[role]["connection_name"]
        )
        connection.setdefault("configure", {})["base_path"] = root.as_posix()
    compiled = {"connections": connections, "dataflows": [flow]}
    case.metadata.write_text(json.dumps(compiled, indent=2))
    identity = {
        "fixture": str(fixture.relative_to(USECASE_SIM_DIR.parent)).replace("\\", "/"),
        "authored_flow": flow_name,
        "runtime_flow": runtime_name,
        "runtime_stage": runtime_name,
    }
    return compiled, identity
