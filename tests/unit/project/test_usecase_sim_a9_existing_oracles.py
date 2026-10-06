"""Meaningful guards for existing fixture oracles and runtime compilation."""

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SIM = Path(__file__).resolve().parents[3] / "usecase-sim"
sys.path.insert(0, str(SIM / "scripts/features"))
from _fixture_support import compile_existing  # noqa: E402
from _stateful_support import assert_exact_rows  # noqa: E402


def test_existing_compile_preserves_authored_contract_and_isolates_ids(tmp_path):
    original = (SIM / "metadata/file/transform/local.json").read_bytes()
    roots = []
    for name in ["a", "b"]:
        root = tmp_path / name
        root.mkdir()
        case = SimpleNamespace(
            metadata=root / "metadata.json",
            input=root / "input",
            output=root / "output",
        )
        doc, identity = compile_existing(
            case, "transform", "row_filter__on_computed_col"
        )
        authored = next(
            f
            for f in json.loads(original)["dataflows"]
            if f["name"] == "row_filter__on_computed_col"
        )
        assert doc["dataflows"][0]["transform"] == authored["transform"]
        assert doc["dataflows"][0]["source"] == authored["source"]
        assert identity["authored_flow"] == "row_filter__on_computed_col"
        roots.append(identity["runtime_flow"])
    assert roots[0] != roots[1]
    assert (SIM / "metadata/file/transform/local.json").read_bytes() == original


@pytest.mark.parametrize(
    "mutant", ["lost-id", "wrong-value", "duplicate", "extra-field"]
)
def test_exact_existing_fixture_rows_reject_mutants(mutant):
    expected = [{"id": 1, "value": "eligible"}, {"id": 2, "value": "newest"}]
    actual = copy.deepcopy(expected)
    if mutant == "lost-id":
        actual.pop()
    elif mutant == "wrong-value":
        actual[0]["value"] = "excluded"
    elif mutant == "duplicate":
        actual.append(actual[0])
    else:
        actual[0]["unexpected"] = "lost-schema-boundary"
    with pytest.raises(AssertionError):
        assert_exact_rows(actual, expected, key="id")
