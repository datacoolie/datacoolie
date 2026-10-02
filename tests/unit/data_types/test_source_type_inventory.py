"""Keep source datatype coverage explicit without making support claims."""

from __future__ import annotations

import json
from pathlib import Path

from datacoolie.engines.data_types import resolve_schema_hint


FIXTURE = Path(__file__).parents[2] / "fixtures" / "data_types" / "source_type_inventory.json"


def test_inventory_has_non_overlapping_statuses_for_each_source_system() -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert document["contract_version"] == "0.2.0"
    for system, inventory in document["systems"].items():
        statuses = {
            value
            for values in inventory.values()
            for value in values
        }
        assert statuses
        assert set(inventory) == {"mapped", "unsupported", "unverified"}, system
        assert not (
            set(inventory["mapped"]) & set(inventory["unsupported"])
        )
        assert not (
            set(inventory["mapped"]) & set(inventory["unverified"])
        )


def test_every_mapped_source_declaration_resolves() -> None:
    """Keep the coverage inventory executable, not just descriptive."""

    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    for system, inventory in document["systems"].items():
        for authored_type in inventory["mapped"]:
            concrete_type = (
                authored_type.replace("(p,s)", "(18,2)")
                .replace("(p)", "(24)")
            )
            resolved = resolve_schema_hint(concrete_type, type_system=system)
            assert resolved.source_type_system == system
